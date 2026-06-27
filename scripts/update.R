#!/usr/bin/env Rscript
# CRAN Downloads — shard-aware producer.
# Pulls only touched-year shards, runs update logic, exports changed shards.

options(timeout = 120)

library(RSQLite)
library(jsonlite)
library(DBI)

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
`%||%` <- function(a, b) if (is.null(a)) b else a

# ---------------------------------------------------------------------------
# Source helpers.R — resolve path whether invoked via Rscript or source()
# ---------------------------------------------------------------------------
helpers_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f)) else "scripts"
  }
)
source(file.path(helpers_dir, "helpers.R"))

# ---------------------------------------------------------------------------
# CLI: output directory for changed shards + manifest
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir <- if (length(args) >= 1) args[1] else "out"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
cat("Output directory:", out_dir, "\n")

WORK_DB                    <- file.path(out_dir, "_working.db")
RECENT_WINDOW              <- 400L
BACKFILL_TARGET            <- as.Date("2012-10-01")
# The 2012-10-01 .. 2020 history was backfilled in one pass (full per-package,
# uncapped) and published directly to the "current" release, so the incremental
# crawl is no longer needed. Disabling it also stops it from re-pulling and
# re-publishing those complete year shards with the capped package set. Set back
# to TRUE to resume the crawl. Forward fetch and repair are unaffected.
BACKFILL_ENABLED           <- FALSE
BACKFILL_CHUNK_DAYS        <- 30L
REPAIR_BATCH               <- 30L
PACKAGE_COVERAGE_THRESHOLD <- 20000L

# ---------------------------------------------------------------------------
# Helper: download one asset from the rolling "current" GH release.
# Returns the exit status (0 = success, non-zero = failure / release absent).
# ---------------------------------------------------------------------------
gh_download <- function(pattern, dir) {
  res <- system2("gh",
    c("release", "download", "current",
      "--repo", "r-observatory/cran-downloads",
      "--pattern", pattern,
      "--dir", dir,
      "--clobber"),
    stdout = TRUE, stderr = TRUE)
  attr(res, "status") %||% 0L
}

# ===========================================================================
# 1. Pull downloads-recent.db from the "current" release (always needed).
#    If the release doesn't exist yet (first run), proceed with empty state.
# ===========================================================================
cat("=== 1. Discover and pull shards ===\n")

recent_status <- gh_download("downloads-recent.db", out_dir)
recent_path   <- file.path(out_dir, "downloads-recent.db")
recent_present <- file.exists(recent_path)

if (!recent_present) {
  cat("  No prior 'current' release found — initializing from scratch\n")
} else {
  cat("  Loaded downloads-recent.db\n")
}

# Open working DB
unlink(WORK_DB)
con <- DBI::dbConnect(RSQLite::SQLite(), WORK_DB)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
on.exit(unlink(WORK_DB), add = TRUE)

invisible(DBI::dbExecute(con, "PRAGMA journal_mode=WAL"))
invisible(DBI::dbExecute(con, "PRAGMA synchronous=NORMAL"))

invisible(DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS downloads_daily (
    package TEXT NOT NULL,
    date    TEXT NOT NULL,
    count   INTEGER NOT NULL,
    PRIMARY KEY (package, date)
  )"))
invisible(DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_dd_date ON downloads_daily(date)"))
invisible(DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS downloads_summary (
    package       TEXT PRIMARY KEY,
    total_30d     INTEGER,
    total_90d     INTEGER,
    total_365d    INTEGER,
    rank_30d      INTEGER,
    rank_90d      INTEGER,
    rank_365d     INTEGER,
    avg_daily_30d REAL,
    trend         REAL
  )"))
invisible(DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS backfill_state (
    key   TEXT PRIMARY KEY,
    value TEXT
  )"))

if (recent_present) {
  invisible(DBI::dbExecute(con, sprintf("ATTACH DATABASE '%s' AS recent",
                              normalizePath(recent_path, mustWork = TRUE))))
  invisible(DBI::dbExecute(con,
    "INSERT OR REPLACE INTO downloads_daily SELECT * FROM recent.downloads_daily"))
  has_bf <- nrow(DBI::dbGetQuery(con,
    "SELECT name FROM recent.sqlite_master WHERE name = 'backfill_state'")) > 0
  if (has_bf) {
    invisible(DBI::dbExecute(con,
      "INSERT OR REPLACE INTO backfill_state SELECT * FROM recent.backfill_state"))
  }
  invisible(DBI::dbExecute(con, "DETACH DATABASE recent"))
  cat("  Seeded working DB from downloads-recent.db\n")
}

# ===========================================================================
# 2. Determine which years this run will touch.
#    We derive forward_dates, backfill_range, and repair_dates from the
#    current working DB state — same logic as in the old update.R — and feed
#    them into compute_touched_years() to decide which year shards to pull.
# ===========================================================================
cat("=== 2. Compute touched years ===\n")

today     <- Sys.Date()
yesterday <- today - 1L

latest_date <- {
  r <- DBI::dbGetQuery(con, "SELECT MAX(date) AS d FROM downloads_daily")
  if (is.na(r$d[1])) NULL else as.Date(r$d[1])
}

forward_start <- if (is.null(latest_date)) today - 30L else latest_date + 1L
forward_dates <- if (forward_start <= yesterday) {
  seq(forward_start, yesterday, by = 1)
} else {
  as.Date(character(0))
}

frontier_row <- DBI::dbGetQuery(con,
  "SELECT value FROM backfill_state WHERE key = 'backfill_frontier'")
frontier <- if (nrow(frontier_row) > 0) as.Date(frontier_row$value[1]) else (today - 30L)

backfill_range <- if (BACKFILL_ENABLED && frontier > BACKFILL_TARGET) {
  chunk_end   <- frontier - 1L
  chunk_start <- max(frontier - BACKFILL_CHUNK_DAYS, BACKFILL_TARGET)
  list(start = chunk_start, end = chunk_end)
} else {
  NULL
}

partial <- DBI::dbGetQuery(con, sprintf("
  SELECT date FROM (
    SELECT date, COUNT(DISTINCT package) AS pkg_count
      FROM downloads_daily
     GROUP BY date
    HAVING pkg_count < %d
  )
  ORDER BY date DESC
  LIMIT %d", PACKAGE_COVERAGE_THRESHOLD, REPAIR_BATCH))
repair_dates <- partial$date

touched_years <- compute_touched_years(forward_dates, backfill_range, repair_dates)
cat("  Forward dates:   ", length(forward_dates), "days\n")
cat("  Backfill range:  ",
    if (is.null(backfill_range)) "none" else paste(backfill_range$start, "to", backfill_range$end),
    "\n")
cat("  Repair dates:    ", length(repair_dates), "dates\n")
cat("  Touched years:   ", paste(touched_years, collapse = ", "), "\n")

# ===========================================================================
# 3. Pull each touched-year shard into the working DB.
# ===========================================================================
cat("=== 3. Pull year shards ===\n")

for (yr in touched_years) {
  shard      <- sprintf("downloads-%04d.db", yr)
  shard_path <- file.path(out_dir, shard)
  if (!file.exists(shard_path)) {
    gh_download(shard, out_dir)
  }
  if (file.exists(shard_path)) {
    invisible(DBI::dbExecute(con, sprintf("ATTACH DATABASE '%s' AS yr",
                                normalizePath(shard_path, mustWork = TRUE))))
    invisible(DBI::dbExecute(con,
      "INSERT OR REPLACE INTO downloads_daily SELECT * FROM yr.downloads_daily"))
    invisible(DBI::dbExecute(con, "DETACH DATABASE yr"))
    cat("  Loaded shard:", shard, "\n")
  } else {
    cat("  Shard not in release (new year):", shard, "\n")
  }
}

# ===========================================================================
# 4. Forward / Backfill / Repair  (verbatim from prior update.R)
# ===========================================================================

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
rows_added    <- 0L
forward_rows  <- 0L
backfill_rows <- 0L
repair_rows   <- 0L

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
# Forward Fetch (new days since last in DB)
# =========================================================================
cat("\n=== 4a. Forward Fetch ===\n")
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
# Backfill (extend history backwards by 1 month each run)
# =========================================================================
cat("\n=== 4b. Backfill ===\n")
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
# Repair partial-coverage dates
# =========================================================================
# Earlier backfills only fetched 5K packages. Re-fetch dates where coverage
# is below 20K packages using the full CRAN package list. Process up to
# 30 days per run to stay within workflow time limits.
cat("\n=== 4c. Repair Partial Coverage ===\n")
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
    cat("  Found", nrow(partial), "dates with partial coverage\n")

    pkgs <- cran_packages
    if (length(pkgs) > 0) {
      # Group consecutive dates into contiguous chunks to minimize API calls
      dates <- sort(as.Date(partial$date))
      chunks <- list()
      chunk_start <- dates[1]
      chunk_end <- dates[1]
      for (i in seq_along(dates)) {
        if (i == 1) next
        if (as.integer(dates[i] - chunk_end) <= 1) {
          chunk_end <- dates[i]
        } else {
          chunks[[length(chunks) + 1]] <- list(start = chunk_start, end = chunk_end)
          chunk_start <- dates[i]
          chunk_end <- dates[i]
        }
      }
      chunks[[length(chunks) + 1]] <- list(start = chunk_start, end = chunk_end)

      cat("  Grouped into", length(chunks), "contiguous chunk(s)\n")
      for (ch in chunks) {
        span <- as.integer(ch$end - ch$start) + 1L
        cat("  Fetching", length(pkgs), "packages for",
            format(ch$start), "to", format(ch$end), "(", span, "days)\n")

        result_df <- fetch_downloads(pkgs, ch$start, ch$end)
        if (nrow(result_df) > 0) {
          n <- insert_downloads(con, result_df)
          repair_rows <- repair_rows + n
          rows_added <- rows_added + n
          cat("    Inserted/updated", n, "rows\n")
        }
      }
    }
  }
}, error = function(e) {
  cat("  Repair ERROR:", e$message, "\n")
})

# =========================================================================
# 5. Rebuild downloads_summary
# =========================================================================
cat("\n=== 5. Rebuild Summary ===\n")
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

# ===========================================================================
# 6. Determine changed years (for now: identical to touched_years).
# ===========================================================================
changed_years <- touched_years

# ===========================================================================
# 7. Export shards: recent, summary, and each changed-year shard.
# ===========================================================================
cat("\n=== 7. Export shards ===\n")

# Export recent shard
recent_rows <- extract_recent_rows(con, today = today, window_days = RECENT_WINDOW)
recent_out  <- file.path(out_dir, "downloads-recent.db")
export_shard(recent_out, recent_rows)
cat("  Exported downloads-recent.db (", nrow(recent_rows), "rows )\n")

# Persist backfill_state into downloads-recent.db so the next run can read it
{
  rc <- DBI::dbConnect(RSQLite::SQLite(), recent_out)
  DBI::dbExecute(rc,
    "CREATE TABLE IF NOT EXISTS backfill_state (key TEXT PRIMARY KEY, value TEXT)")
  # Copy from working DB
  work_bf <- DBI::dbGetQuery(con,
    "SELECT key, value FROM backfill_state")
  if (nrow(work_bf) > 0) {
    DBI::dbExecute(rc,
      "INSERT OR REPLACE INTO backfill_state (key, value) VALUES (?, ?)",
      params = list(work_bf$key, work_bf$value))
  }
  DBI::dbDisconnect(rc)
}

# Export summary shard
summary_df <- DBI::dbGetQuery(con, "SELECT * FROM downloads_summary")
export_summary_shard(file.path(out_dir, "downloads-summary.db"), summary_df)
cat("  Exported downloads-summary.db (", nrow(summary_df), "rows )\n")

# Export each changed year shard
for (yr in changed_years) {
  rows       <- extract_year_rows(con, yr)
  shard_name <- sprintf("downloads-%04d.db", yr)
  export_shard(file.path(out_dir, shard_name), rows)
  cat("  Exported", shard_name, "(", nrow(rows), "rows )\n")
}

# ===========================================================================
# 8. Write manifest.json
# ===========================================================================
cat("\n=== 8. Write manifest ===\n")

changed_shards <- c(
  "downloads-recent.db",
  "downloads-summary.db",
  sprintf("downloads-%04d.db", changed_years)
)

tag <- sprintf("v%s", format(Sys.time(), "%Y%m%d-%H%M%S", tz = "UTC"))

# The working DB only contains rows from touched years + the rolling recent
# window — not every shard on the release. So "working_db_rows" is what we
# can honestly report from this process. Computing the union across every
# year shard would require downloading shards we deliberately skipped.
working_db_rows <- DBI::dbGetQuery(con,
  "SELECT COUNT(*) AS n FROM downloads_daily")$n

write_manifest(
  path           = file.path(out_dir, "manifest.json"),
  changed_shards = changed_shards,
  tag            = tag,
  summary        = list(
    forward_rows     = forward_rows  %||% 0L,
    backfill_rows    = backfill_rows %||% 0L,
    repair_rows      = repair_rows   %||% 0L,
    working_db_rows  = working_db_rows,
    note             = paste("working_db_rows counts rows from touched years +",
                             "recent window only; sum each year shard for the",
                             "true total across all release assets"),
    date_range_in_working_db = list(
      min = DBI::dbGetQuery(con, "SELECT MIN(date) AS d FROM downloads_daily")$d,
      max = DBI::dbGetQuery(con, "SELECT MAX(date) AS d FROM downloads_daily")$d
    )
  )
)
cat("  Wrote manifest.json\n")

# ===========================================================================
# 9. Write release_notes.md (used by the workflow's release publishing step)
# ===========================================================================
writeLines(
  sprintf("## CRAN Downloads %s\n\nSee manifest.json for changed shards.\n", tag),
  file.path(out_dir, "release_notes.md")
)
cat("  Wrote release_notes.md\n")

# Disconnect working DB (on.exit also covers this, but be explicit)
try(DBI::dbDisconnect(con), silent = TRUE)

cat("\nDone. Changed shards:\n  - ", paste(changed_shards, collapse = "\n  - "), "\n")
