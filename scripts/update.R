#!/usr/bin/env Rscript
# CRAN Downloads — fetch daily download counts from the cranlogs API,
# with gradual backfill to October 2012. Writes to SQLite (downloads.db).

options(timeout = 120)

library(RSQLite)
library(jsonlite)

# ---------------------------------------------------------------------------
# CLI argument: path to the SQLite database
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "downloads.db"

cat("Database path:", db_path, "\n")

# ---------------------------------------------------------------------------
# Connect and configure SQLite
# ---------------------------------------------------------------------------
con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# ---------------------------------------------------------------------------
# Create tables
# ---------------------------------------------------------------------------
dbExecute(con, "
CREATE TABLE IF NOT EXISTS downloads_daily (
  package TEXT NOT NULL,
  date    TEXT NOT NULL,
  count   INTEGER NOT NULL,
  PRIMARY KEY (package, date)
)")

dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_dd_date ON downloads_daily(date)")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS downloads_summary (
  package      TEXT PRIMARY KEY,
  total_30d    INTEGER,
  total_90d    INTEGER,
  total_365d   INTEGER,
  rank_30d     INTEGER,
  rank_90d     INTEGER,
  rank_365d    INTEGER,
  avg_daily_30d REAL,
  trend        REAL
)")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS backfill_state (
  key   TEXT PRIMARY KEY,
  value TEXT
)")

# ---------------------------------------------------------------------------
# Fetch CRAN package list once (reused by forward fetch and backfill)
# ---------------------------------------------------------------------------
cran_packages <- tryCatch({
  ap <- available.packages(repos = "https://cloud.r-project.org")
  sort(unique(rownames(ap)))
}, error = function(e) {
  cat("Warning: Could not get available.packages:", e$message, "\n")
  character(0)
})
cat("Found", length(cran_packages), "packages on CRAN\n")

# ---------------------------------------------------------------------------
# Tracking variables for release notes
# ---------------------------------------------------------------------------
rows_added <- 0L
forward_rows <- 0L
backfill_rows <- 0L
repair_rows <- 0L

# ---------------------------------------------------------------------------
# Helper: fetch download data from cranlogs API
# ---------------------------------------------------------------------------
fetch_downloads <- function(packages, start_date, end_date) {
  # Process in batches of 100 packages
  batch_size <- 100
  n_pkgs <- length(packages)
  # Pre-allocate list; grows by doubling if needed (avoids O(n^2) append)
  capacity <- 1024L
  all_results <- vector("list", capacity)
  result_idx <- 0L

  for (batch_start in seq(1, n_pkgs, by = batch_size)) {
    batch_end <- min(batch_start + batch_size - 1, n_pkgs)
    batch_pkgs <- packages[batch_start:batch_end]
    pkg_str <- paste(batch_pkgs, collapse = ",")

    # Process in weekly date chunks to avoid API timeouts
    chunk_start <- as.Date(start_date)
    chunk_end_final <- as.Date(end_date)

    while (chunk_start <= chunk_end_final) {
      chunk_end <- min(chunk_start + 6, chunk_end_final)
      url <- sprintf(
        "https://cranlogs.r-pkg.org/downloads/daily/%s:%s/%s",
        format(chunk_start, "%Y-%m-%d"),
        format(chunk_end, "%Y-%m-%d"),
        pkg_str
      )

      tryCatch({
        raw <- readLines(url, warn = FALSE)
        json_text <- paste(raw, collapse = "\n")
        parsed <- fromJSON(json_text, simplifyVector = FALSE)

        # API returns a list of package objects (or a single object for 1 package)
        if (!is.null(parsed$package)) {
          # Single package response — wrap in list
          parsed <- list(parsed)
        }

        for (pkg_data in parsed) {
          pkg_name <- pkg_data$package
          if (is.null(pkg_name) || is.null(pkg_data$downloads)) next

          downloads <- pkg_data$downloads
          if (length(downloads) == 0) next

          # Extract day and downloads from each entry
          days <- vapply(downloads, function(d) d$day, character(1))
          counts <- vapply(downloads, function(d) as.integer(d$downloads), integer(1))

          # Filter out zero-download days to save space
          nonzero <- counts > 0L
          if (any(nonzero)) {
            result_idx <- result_idx + 1L
            if (result_idx > capacity) {
              capacity <- capacity * 2L
              length(all_results) <- capacity
            }
            all_results[[result_idx]] <- data.frame(
              package = pkg_name,
              date = days[nonzero],
              count = counts[nonzero],
              stringsAsFactors = FALSE
            )
          }
        }
      }, error = function(e) {
        cat("  API error for batch", batch_start, "-", batch_end,
            "dates", format(chunk_start), "-", format(chunk_end),
            ":", e$message, "\n")
      })

      Sys.sleep(0.5)  # Rate limiting
      chunk_start <- chunk_end + 1
    }
  }

  # Combine all results
  if (result_idx > 0L) {
    do.call(rbind, all_results[seq_len(result_idx)])
  } else {
    data.frame(package = character(0), date = character(0),
               count = integer(0), stringsAsFactors = FALSE)
  }
}

# ---------------------------------------------------------------------------
# Helper: insert download data into DB in a transaction
# ---------------------------------------------------------------------------
insert_downloads <- function(con, df) {
  if (is.null(df) || nrow(df) == 0) return(0L)

  dbBegin(con)
  tryCatch({
    # Use parameterized batch insert
    dbExecute(con,
      "INSERT OR REPLACE INTO downloads_daily (package, date, count) VALUES (?, ?, ?)",
      params = list(df$package, df$date, df$count)
    )
    dbCommit(con)
    nrow(df)
  }, error = function(e) {
    cat("  Insert error:", e$message, "\n")
    tryCatch(dbRollback(con), error = function(e2) NULL)
    0L
  })
}

# =========================================================================
# 1. Forward Fetch (new days since last in DB)
# =========================================================================
cat("\n=== 1. Forward Fetch ===\n")
tryCatch({
  today <- Sys.Date()
  yesterday <- today - 1  # cranlogs data delayed ~1 day

  # Find latest date in DB
  latest_row <- dbGetQuery(con, "SELECT MAX(date) AS max_date FROM downloads_daily")
  latest_date <- latest_row$max_date[1]

  if (is.na(latest_date) || is.null(latest_date)) {
    # No data yet: start from 30 days ago
    start_date <- today - 30
    cat("  No existing data. Starting from", format(start_date), "\n")
  } else {
    start_date <- as.Date(latest_date) + 1
    cat("  Latest date in DB:", latest_date, "\n")
    cat("  Fetching from", format(start_date), "to", format(yesterday), "\n")
  }

  if (start_date <= yesterday) {
    pkgs <- cran_packages

    if (length(pkgs) > 0) {
      cat("  Using", length(pkgs), "packages from CRAN\n")
      cat("  Fetching downloads from", format(start_date), "to", format(yesterday), "\n")

      result_df <- fetch_downloads(pkgs, start_date, yesterday)
      if (nrow(result_df) > 0) {
        n <- insert_downloads(con, result_df)
        forward_rows <- n
        rows_added <- rows_added + n
        cat("  Inserted", n, "forward-fetch rows\n")
      } else {
        cat("  No download data returned from API\n")
      }
    } else {
      cat("  No packages found, skipping forward fetch\n")
    }
  } else {
    cat("  Already up to date\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
})

# =========================================================================
# 2. Backfill (extend history backwards by 1 month each run)
# =========================================================================
cat("\n=== 2. Backfill ===\n")
tryCatch({
  today <- Sys.Date()
  backfill_target <- as.Date("2012-10-01")

  # Read current backfill frontier
  frontier_row <- dbGetQuery(con,
    "SELECT value FROM backfill_state WHERE key = 'backfill_frontier'")

  if (nrow(frontier_row) == 0) {
    # First run: set frontier to (today - 30)
    frontier <- today - 30
    dbExecute(con,
      "INSERT OR REPLACE INTO backfill_state (key, value) VALUES ('backfill_frontier', ?)",
      params = list(format(frontier, "%Y-%m-%d")))
    cat("  Initialized backfill frontier to", format(frontier), "\n")
  } else {
    frontier <- as.Date(frontier_row$value[1])
    cat("  Current backfill frontier:", format(frontier), "\n")
  }

  if (frontier > backfill_target) {
    # Fetch one month backwards
    backfill_end <- frontier - 1
    backfill_start <- frontier - 30
    if (backfill_start < backfill_target) backfill_start <- backfill_target

    cat("  Backfilling from", format(backfill_start), "to", format(backfill_end), "\n")

    pkgs <- cran_packages

    if (length(pkgs) > 0) {
      cat("  Fetching backfill downloads for", length(pkgs), "packages\n")
      result_df <- fetch_downloads(pkgs, backfill_start, backfill_end)
      if (nrow(result_df) > 0) {
        n <- insert_downloads(con, result_df)
        backfill_rows <- n
        rows_added <- rows_added + n
        cat("  Inserted", n, "backfill rows\n")
      } else {
        cat("  No backfill data returned from API\n")
      }

      # Update frontier
      dbExecute(con,
        "INSERT OR REPLACE INTO backfill_state (key, value) VALUES ('backfill_frontier', ?)",
        params = list(format(backfill_start, "%Y-%m-%d")))
      cat("  Updated backfill frontier to", format(backfill_start), "\n")
    } else {
      cat("  No packages found, skipping backfill\n")
    }
  } else {
    cat("  Backfill complete (reached", format(backfill_target), ")\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
})

# =========================================================================
# 3. Repair partial-coverage dates
# =========================================================================
# Earlier backfills only fetched 5K packages. Re-fetch dates where coverage
# is below 20K packages using the full CRAN package list. Process up to
# 30 days per run to stay within workflow time limits.
cat("\n=== 3. Repair Partial Coverage ===\n")
tryCatch({
  # Find dates with fewer than 20K packages (partial backfill)
  partial <- dbGetQuery(con, "
    SELECT date, COUNT(DISTINCT package) AS pkg_count
    FROM downloads_daily
    GROUP BY date
    HAVING pkg_count < 20000
    ORDER BY date DESC
    LIMIT 30
  ")

  if (nrow(partial) == 0) {
    cat("  No partial-coverage dates found — all dates have full coverage\n")
  } else {
    cat("  Found", nrow(partial), "dates with partial coverage (of",
        dbGetQuery(con, "SELECT COUNT(DISTINCT date) AS n FROM downloads_daily HAVING n > 0")$n,
        "total dates)\n")
    cat("  Repairing dates:", paste(partial$date, collapse = ", "), "\n")

    pkgs <- cran_packages
    if (length(pkgs) > 0) {
      repair_start <- min(as.Date(partial$date))
      repair_end <- max(as.Date(partial$date))
      cat("  Fetching", length(pkgs), "packages from",
          format(repair_start), "to", format(repair_end), "\n")

      result_df <- fetch_downloads(pkgs, repair_start, repair_end)
      if (nrow(result_df) > 0) {
        n <- insert_downloads(con, result_df)
        repair_rows <- n
        rows_added <- rows_added + n
        cat("  Inserted/updated", n, "repair rows\n")
      } else {
        cat("  No repair data returned from API\n")
      }
    }
  }
}, error = function(e) {
  cat("  Repair ERROR:", e$message, "\n")
})

# =========================================================================
# 4. Rebuild downloads_summary
# =========================================================================
cat("\n=== 4. Rebuild Summary ===\n")
tryCatch({
  today <- Sys.Date()

  # Check if we have any data
  row_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM downloads_daily")$n
  if (row_count == 0) {
    cat("  No download data yet, skipping summary\n")
  } else {
    cat("  Building summary from", row_count, "daily rows\n")

    dbBegin(con)
    dbExecute(con, "DELETE FROM downloads_summary")
    dbExecute(con, sprintf("
      INSERT INTO downloads_summary (package, total_30d, total_90d, total_365d,
                                     avg_daily_30d, trend)
      SELECT
        package,
        SUM(CASE WHEN date >= date('%s', '-30 days') THEN count ELSE 0 END) AS total_30d,
        SUM(CASE WHEN date >= date('%s', '-90 days') THEN count ELSE 0 END) AS total_90d,
        SUM(CASE WHEN date >= date('%s', '-365 days') THEN count ELSE 0 END) AS total_365d,
        ROUND(SUM(CASE WHEN date >= date('%s', '-30 days') THEN count ELSE 0 END) / 30.0, 2) AS avg_daily_30d,
        CASE
          WHEN SUM(CASE WHEN date >= date('%s', '-60 days') AND date < date('%s', '-30 days')
                        THEN count ELSE 0 END) > 0
          THEN ROUND(
            (SUM(CASE WHEN date >= date('%s', '-30 days') THEN count ELSE 0 END) * 1.0 /
             SUM(CASE WHEN date >= date('%s', '-60 days') AND date < date('%s', '-30 days')
                      THEN count ELSE 0 END) - 1.0) * 100.0, 2)
          ELSE NULL
        END AS trend
      FROM downloads_daily
      WHERE date >= date('%s', '-365 days')
      GROUP BY package
    ", format(today), format(today), format(today), format(today),
       format(today), format(today), format(today), format(today),
       format(today), format(today)))

    # Compute ranks using window functions (avoids O(n^2) correlated subqueries)
    dbExecute(con, "
      CREATE TEMP TABLE ranked AS
      SELECT package,
        RANK() OVER (ORDER BY total_30d DESC) as r30,
        RANK() OVER (ORDER BY total_90d DESC) as r90,
        RANK() OVER (ORDER BY total_365d DESC) as r365
      FROM downloads_summary
    ")
    dbExecute(con, "
      UPDATE downloads_summary SET
        rank_30d = (SELECT r30 FROM ranked WHERE ranked.package = downloads_summary.package),
        rank_90d = (SELECT r90 FROM ranked WHERE ranked.package = downloads_summary.package),
        rank_365d = (SELECT r365 FROM ranked WHERE ranked.package = downloads_summary.package)
    ")
    dbExecute(con, "DROP TABLE ranked")
    dbCommit(con)

    summary_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM downloads_summary")$n
    cat("  Summary rebuilt with", summary_count, "packages\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 5. Release Notes
# =========================================================================
cat("\n=== 5. Release Notes ===\n")

total_rows <- tryCatch(
  dbGetQuery(con, "SELECT COUNT(*) AS n FROM downloads_daily")$n,
  error = function(e) 0L
)

date_range <- tryCatch(
  dbGetQuery(con, "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM downloads_daily"),
  error = function(e) data.frame(min_date = NA, max_date = NA)
)

summary_count <- tryCatch(
  dbGetQuery(con, "SELECT COUNT(*) AS n FROM downloads_summary")$n,
  error = function(e) 0L
)

frontier_row <- tryCatch(
  dbGetQuery(con, "SELECT value FROM backfill_state WHERE key = 'backfill_frontier'"),
  error = function(e) data.frame(value = character(0))
)
backfill_progress <- if (nrow(frontier_row) > 0) frontier_row$value[1] else "not started"

db_size_bytes <- file.info(db_path)$size
if (db_size_bytes >= 1024 * 1024) {
  db_size <- sprintf("%.1f MB", db_size_bytes / (1024 * 1024))
} else {
  db_size <- sprintf("%.1f KB", db_size_bytes / 1024)
}

notes <- paste0(
  "## CRAN Downloads Update\n\n",
  "**", format(Sys.time(), "%Y-%m-%d %H:%M UTC", tz = "UTC"), "**\n\n",
  "| Metric | Value |\n",
  "|--------|-------|\n",
  "| Rows added (forward) | ", forward_rows, " |\n",
  "| Rows added (backfill) | ", backfill_rows, " |\n",
  "| Rows added (repair) | ", repair_rows, " |\n",
  "| Total rows added | ", rows_added, " |\n",
  "| Total rows in DB | ", total_rows, " |\n",
  "| Date range | ", date_range$min_date, " to ", date_range$max_date, " |\n",
  "| Summary packages | ", summary_count, " |\n",
  "| Backfill frontier | ", backfill_progress, " |\n",
  "| Database size | ", db_size, " |\n"
)

writeLines(notes, "release_notes.md")
cat("Wrote release_notes.md\n")

cat("\nDone.\n")
