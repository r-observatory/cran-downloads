# scripts/helpers.R — pure functions used by update.R, unit-tested in tests/testthat/

#' Compute the set of years touched by this run.
#'
#' @param forward_dates  Date vector — dates being forward-fetched (may be empty)
#' @param backfill_range NULL or list(start=Date, end=Date) — single backfill chunk
#' @param repair_dates   Character vector — YYYY-MM-DD strings of dates needing repair
#' @param request        NULL or parse_backfill_request()'s list(packages, start, end)
#' @return integer vector of years, sorted ascending, no duplicates
compute_touched_years <- function(forward_dates, backfill_range, repair_dates,
                                  request = NULL) {
  years <- integer(0)

  if (!is.null(request)) {
    years <- c(years, seq(as.integer(format(request$start, "%Y")),
                          as.integer(format(request$end, "%Y"))))
  }

  if (length(forward_dates) > 0) {
    years <- c(years, as.integer(format(forward_dates, "%Y")))
  }

  if (!is.null(backfill_range)) {
    span <- seq(backfill_range$start, backfill_range$end, by = "year")
    # Include both endpoint years explicitly in case the range is short
    span <- c(span, backfill_range$start, backfill_range$end)
    years <- c(years, as.integer(format(span, "%Y")))
  }

  if (length(repair_dates) > 0) {
    years <- c(years, as.integer(substr(repair_dates, 1, 4)))
  }

  sort(unique(years))
}

#' Stop when a year shard the run must load did not download. Every past year is
#' on the release, so a missing one would be republished holding only this
#' run's rows; only the current year may be missing, on the first run that
#' reaches it. A file left by a failed download is never loaded.
check_shard_pulled <- function(year, present, current_year, status = 0L) {
  shard <- sprintf("downloads-%04d.db", as.integer(year))
  if (present && status != 0L) {
    stop(shard, ": the download failed part way (gh exit ", status,
         "); stopping before anything is published", call. = FALSE)
  }
  if (!present && year < current_year) {
    stop(shard, " could not be downloaded (gh exit ", status,
         "); stopping before it is republished without its rows", call. = FALSE)
  }
  invisible(TRUE)
}

#' The distinct days and the rows in a shard file's downloads_daily.
shard_stats <- function(path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  r <- DBI::dbGetQuery(con,
    "SELECT COUNT(DISTINCT date) AS days, COUNT(*) AS n FROM downloads_daily")
  list(days = as.integer(r$days[1]), rows = as.numeric(r$n[1]))
}

#' Stop when a year shard holds fewer days or fewer rows than the published one
#' it would replace. `published` is NULL for a shard the release does not have.
check_shard_not_shrunk <- function(shard, new, published) {
  if (is.null(published)) return(invisible(TRUE))
  if (new$days < published$days || new$rows < published$rows) {
    stop(sprintf(paste("%s holds %d days and %.0f rows but the published shard holds",
                       "%d days and %.0f rows; stopping before it is replaced"),
                 shard, new$days, new$rows, published$days, published$rows),
         call. = FALSE)
  }
  invisible(TRUE)
}

#' Asset names from `gh release view --json assets --jq '.assets[].name'`. A
#' release that does not exist yet has none; any other failure stops the run.
release_asset_names <- function(output, status) {
  if (status == 0L) return(output[nzchar(output)])
  if (any(grepl("release not found", output, fixed = TRUE))) return(character(0))
  stop("could not list the assets on the release (gh exit ", status,
       "); stopping before a year shard is replaced unchecked", call. = FALSE)
}

#' shard_stats() of a year shard as the release holds it now, or NULL when the
#' release does not list it. `list_assets()` returns the release's asset names;
#' `download(shard, dir)` fetches the shard into `dir` and returns gh's exit
#' status. The downloaded copy is removed afterwards.
published_shard_stats <- function(shard, list_assets, download, dir) {
  if (!(shard %in% list_assets())) return(NULL)
  path <- file.path(dir, shard)
  on.exit(unlink(path), add = TRUE)
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  status <- download(shard, dir)
  if (status != 0L || !file.exists(path)) {
    stop(shard, " is on the release but could not be downloaded (gh exit ", status,
         "); stopping before it is replaced unchecked", call. = FALSE)
  }
  shard_stats(path)
}

#' A dispatch's request to fetch named packages again, from the
#' backfill_packages and backfill_from inputs, as list(packages, start, end), or
#' NULL when neither is set. Names split on commas or whitespace; the start is a
#' YYYY-MM-DD day from the cranlogs start (2012-10-01) to `yesterday`, and the
#' end is `yesterday`. Anything else stops the run before a download.
parse_backfill_request <- function(packages, from, yesterday,
                                   earliest = as.Date("2012-10-01")) {
  packages <- trimws(if (is.null(packages)) "" else packages)
  from     <- trimws(if (is.null(from)) "" else from)
  if (!nzchar(packages) && !nzchar(from)) return(NULL)
  if (!nzchar(packages) || !nzchar(from)) {
    stop("backfill_packages and backfill_from must be given together")
  }
  pkgs <- unique(strsplit(packages, "[,[:space:]]+")[[1]])
  pkgs <- pkgs[nzchar(pkgs)]
  if (length(pkgs) == 0L) stop("backfill_packages names no package")
  bad  <- pkgs[!grepl("^[A-Za-z][A-Za-z0-9.]*[A-Za-z0-9]$", pkgs)]
  if (length(bad) > 0L) stop("not a CRAN package name: ", paste(bad, collapse = ", "))
  start <- as.Date(from, format = "%Y-%m-%d")
  if (is.na(start) || format(start, "%Y-%m-%d") != from) {
    stop("backfill_from is not a YYYY-MM-DD date: ", from)
  }
  yesterday <- as.Date(yesterday)
  if (start < earliest || start > yesterday) {
    stop(sprintf("backfill_from must fall between %s and %s", earliest, yesterday))
  }
  list(packages = pkgs, start = start, end = yesterday)
}

#' The partial-coverage dates the repair pass may take: those in a year this run
#' exports for its own reasons. A year loaded only for a requested backfill is
#' left alone, since most of its days sit below the coverage threshold for good.
repair_candidates <- function(partial, repair_years) {
  partial[as.integer(substr(partial$date, 1, 4)) %in% repair_years, , drop = FALSE]
}

#' Extract all downloads_daily rows for a single year.
#'
#' @param con  SQLite connection (working DB with downloads_daily table)
#' @param year integer
#' @return data.frame(package, date, count)
extract_year_rows <- function(con, year) {
  year_prefix <- sprintf("%04d", as.integer(year))
  DBI::dbGetQuery(
    con,
    "SELECT package, date, count
       FROM downloads_daily
      WHERE substr(date, 1, 4) = ?
      ORDER BY package, date",
    params = list(year_prefix)
  )
}

#' Extract the rolling N-day window of downloads_daily rows.
#'
#' @param con         SQLite connection
#' @param today       Date — reference "now"
#' @param window_days integer — how many days back, inclusive of cutoff
#' @return data.frame(package, date, count)
extract_recent_rows <- function(con, today, window_days) {
  cutoff <- format(today - as.integer(window_days), "%Y-%m-%d")
  DBI::dbGetQuery(
    con,
    "SELECT package, date, count
       FROM downloads_daily
      WHERE date >= ?
      ORDER BY package, date",
    params = list(cutoff)
  )
}

#' Every package name CRAN's PACKAGES index lists, sorted. Only the duplicates
#' filter is applied: the default filters would drop OS_type: windows packages
#' and any package needing a newer R than this runner.
cran_package_names <- function(repos) {
  ap <- utils::available.packages(repos = repos, type = "source", filters = "duplicates")
  sort(unique(rownames(ap)))
}

#' The newest day in downloads_daily as "YYYY-MM-DD", or NA when it is empty.
#' Published as the manifest's summary$data_through.
latest_daily_date <- function(con) {
  d <- DBI::dbGetQuery(con, "SELECT MAX(date) AS d FROM downloads_daily")$d[1]
  if (is.null(d) || is.na(d) || !nzchar(d)) NA_character_ else as.character(d)
}

#' Compute the lowercase hex SHA-256 of a file's exact on-disk bytes.
#'
#' Uses whatever the runner already provides, in preference order:
#'   1. digest  package        (if installed)
#'   2. openssl package        (if installed)
#'   3. sha256sum (coreutils)  - present on the ubuntu-latest CI runner
#'   4. shasum -a 256 (BSD)    - macOS/local fallback
#' No heavy dependency is declared: on CI (which installs only RSQLite,
#' jsonlite, testthat, DBI) the coreutils `sha256sum` path is used. If a
#' sibling pipeline already declares `digest`, that path wins automatically.
file_sha256 <- function(path) {
  if (requireNamespace("digest", quietly = TRUE)) {
    return(tolower(digest::digest(file = path, algo = "sha256")))
  }
  if (requireNamespace("openssl", quietly = TRUE)) {
    con <- file(path, open = "rb")
    on.exit(close(con), add = TRUE)
    return(tolower(as.character(openssl::sha256(con))))
  }
  sha_tool <- Sys.which("sha256sum")
  if (nzchar(sha_tool)) {
    out <- system2(sha_tool, shQuote(path), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  shasum_tool <- Sys.which("shasum")
  if (nzchar(shasum_tool)) {
    out <- system2(shasum_tool, c("-a", "256", shQuote(path)), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  stop("No SHA-256 backend found (need one of: digest, openssl, sha256sum, shasum)")
}

#' Build the integrity / completeness core describing a finalized SQLite file.
#'
#' Returns a named list of TOP-LEVEL manifest fields computed from the exact
#' on-disk bytes of `db_path` (call this only after the file is finalized):
#'   * db_filename - basename of the file
#'   * db_bytes    - byte size of the file as a double. Deliberately NOT cast
#'                   to integer: R's integer range is 32-bit and overflows to
#'                   NA (serialized as the string "NA") for files >= ~2 GiB.
#'   * db_sha256   - lowercase hex sha256 of the file's exact bytes
#'   * tables      - named list mapping each user table to its row count
#'   * complete    - passed through by the caller. complete = the DB holds the
#'                   full, non-partial dataset (a full rebuild each run);
#'                   freshness is tracked separately via generated_at and the
#'                   fingerprint. A pipeline with a genuine partial/bootstrap
#'                   state would derive this instead of hardcoding it.
#' Lets a downstream merge content-verify the asset it pulls and confirm the
#' expected tables/rows are present.
summary_integrity_core <- function(db_path, complete = TRUE) {
  stopifnot(file.exists(db_path))

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  tables <- tryCatch({
    tbl_names <- DBI::dbGetQuery(con, "
      SELECT name FROM sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
       ORDER BY name")$name

    stats::setNames(
      lapply(tbl_names, function(t) {
        DBI::dbGetQuery(con, sprintf('SELECT count(*) AS n FROM "%s"', t))$n
      }),
      tbl_names
    )
  }, finally = DBI::dbDisconnect(con))

  # db_bytes/db_sha256 read the raw on-disk file only after the connection
  # above is closed, so no open handle or journal file skews the hash/size.
  list(
    db_filename = basename(db_path),
    db_bytes    = file.size(db_path),
    db_sha256   = file_sha256(db_path),
    tables      = tables,
    complete    = complete
  )
}

#' Write the manifest.json describing which shards changed this run.
#'
#' Empty arrays are preserved (jsonlite default is to drop them — we force them).
#' `core` (optional) is a named list of TOP-LEVEL fields to merge into the
#' manifest - used to attach the integrity/completeness core built by
#' summary_integrity_core() (db_filename, db_bytes, db_sha256, tables, complete).
write_manifest <- function(path, changed_shards, tag, summary, core = NULL) {
  obj <- list(
    tag            = tag,
    generated_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    changed_shards = as.list(changed_shards),
    summary        = summary
  )
  if (!is.null(core)) {
    obj <- c(obj, core)  # merge as top-level fields, not nested
  }
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE, null = "null")
  writeLines(json, path)
}

#' Write the given rows into a fresh SQLite file at `path`.
#'
#' Overwrites any existing file. Always creates the downloads_daily table
#' with the canonical schema and idx_dd_date index. Runs VACUUM at end so
#' the file is minimal.
export_shard <- function(path, rows) {
  if (file.exists(path)) unlink(path)

  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "PRAGMA journal_mode=DELETE")  # no WAL in published shards

  DBI::dbExecute(con, "
    CREATE TABLE downloads_daily (
      package TEXT NOT NULL,
      date    TEXT NOT NULL,
      count   INTEGER NOT NULL,
      PRIMARY KEY (package, date)
    )")
  DBI::dbExecute(con, "CREATE INDEX idx_dd_date ON downloads_daily(date)")

  if (nrow(rows) > 0) {
    DBI::dbBegin(con)
    DBI::dbExecute(
      con,
      "INSERT INTO downloads_daily (package, date, count) VALUES (?, ?, ?)",
      params = list(rows$package, rows$date, rows$count)
    )
    DBI::dbCommit(con)
  }

  DBI::dbExecute(con, "VACUUM")
  invisible(NULL)
}

#' Write a minimal SQLite file containing ONLY the downloads_summary table.
export_summary_shard <- function(path, summary) {
  if (file.exists(path)) unlink(path)

  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "PRAGMA journal_mode=DELETE")
  DBI::dbExecute(con, "
    CREATE TABLE downloads_summary (
      package       TEXT PRIMARY KEY,
      total_30d     INTEGER,
      total_90d     INTEGER,
      total_365d    INTEGER,
      rank_30d      INTEGER,
      rank_90d      INTEGER,
      rank_365d     INTEGER,
      avg_daily_30d REAL,
      trend         REAL
    )")

  if (nrow(summary) > 0) {
    DBI::dbWriteTable(con, "downloads_summary", summary, append = TRUE)
  }

  DBI::dbExecute(con, "VACUUM")
  invisible(NULL)
}

# ---------------------------------------------------------------------------
# cranlogs fetch + repair scheduling
# ---------------------------------------------------------------------------

#' Fetch daily downloads from cranlogs for `packages` over [start_date, end_date].
#'
#' One request per 100-package batch per 7-day window. When `deadline` passes,
#' no further requests are made and the result carries attr "truncated" = TRUE.
#' `reader`, `now` and `pause` are injectable for tests.
fetch_downloads <- function(packages, start_date, end_date,
                            deadline = Inf,
                            reader   = function(url) readLines(url, warn = FALSE),
                            now      = Sys.time,
                            pause    = function() Sys.sleep(0.5),
                            batch_size = 100L) {
  n_pkgs <- length(packages)
  capacity <- 1024L
  all_results <- vector("list", capacity)
  result_idx <- 0L
  truncated <- FALSE

  for (batch_start in seq(1, max(n_pkgs, 1L), by = batch_size)) {
    if (n_pkgs == 0L) break
    batch_end  <- min(batch_start + batch_size - 1L, n_pkgs)
    pkg_str    <- paste(packages[batch_start:batch_end], collapse = ",")
    chunk_start     <- as.Date(start_date)
    chunk_end_final <- as.Date(end_date)

    while (chunk_start <= chunk_end_final) {
      if (as.numeric(now()) >= as.numeric(deadline)) {
        truncated <- TRUE
        break
      }
      chunk_end <- min(chunk_start + 6, chunk_end_final)
      url <- sprintf("%s/downloads/daily/%s:%s/%s",
                     Sys.getenv("CRANLOGS_URL", "https://cranlogs.r-pkg.org"),
                     format(chunk_start, "%Y-%m-%d"),
                     format(chunk_end, "%Y-%m-%d"), pkg_str)

      tryCatch({
        parsed <- jsonlite::fromJSON(paste(reader(url), collapse = "\n"),
                                     simplifyVector = FALSE)
        if (!is.null(parsed$package)) parsed <- list(parsed)

        for (pkg_data in parsed) {
          pkg_name <- pkg_data$package
          if (is.null(pkg_name) || is.null(pkg_data$downloads)) next
          downloads <- pkg_data$downloads
          if (length(downloads) == 0) next
          days   <- vapply(downloads, function(d) d$day, character(1))
          counts <- vapply(downloads, function(d) as.integer(d$downloads), integer(1))
          nonzero <- counts > 0L  # zero days are not stored
          if (any(nonzero)) {
            result_idx <- result_idx + 1L
            if (result_idx > capacity) {
              capacity <- capacity * 2L
              length(all_results) <- capacity
            }
            all_results[[result_idx]] <- data.frame(
              package = pkg_name, date = days[nonzero], count = counts[nonzero],
              stringsAsFactors = FALSE)
          }
        }
      }, error = function(e) {
        cat("  API error for batch", batch_start, "-", batch_end,
            "dates", format(chunk_start), "-", format(chunk_end),
            ":", conditionMessage(e), "\n")
      })

      pause()
      chunk_start <- chunk_end + 1
    }
    if (truncated) break
  }

  out <- if (result_idx > 0L) {
    do.call(rbind, all_results[seq_len(result_idx)])
  } else {
    data.frame(package = character(0), date = character(0),
               count = integer(0), stringsAsFactors = FALSE)
  }
  attr(out, "truncated") <- truncated
  out
}

#' Group dates into runs of consecutive days.
#' @return list of list(start = Date, end = Date), ascending.
group_contiguous_dates <- function(dates) {
  dates <- sort(unique(as.Date(dates)))
  if (length(dates) == 0L) return(list())
  breaks <- c(TRUE, diff(as.integer(dates)) > 1L)
  run_id <- cumsum(breaks)
  lapply(split(dates, run_id), function(d) list(start = min(d), end = max(d))) |>
    unname()
}

REPAIR_LEDGER_SQL <- "
  CREATE TABLE IF NOT EXISTS repair_ledger (
    date         TEXT PRIMARY KEY,
    attempted_on TEXT NOT NULL,
    pkg_count    INTEGER NOT NULL
  )"

#' Read the repair ledger (one row per date last attempted by the repair pass).
read_repair_ledger <- function(con, schema = "main") {
  has <- nrow(DBI::dbGetQuery(con, sprintf(
    "SELECT name FROM %s.sqlite_master WHERE type = 'table' AND name = 'repair_ledger'",
    schema))) > 0
  if (!has) {
    return(data.frame(date = character(0), attempted_on = character(0),
                      pkg_count = integer(0), stringsAsFactors = FALSE))
  }
  DBI::dbGetQuery(con, sprintf(
    "SELECT date, attempted_on, pkg_count FROM %s.repair_ledger", schema))
}

#' Record that `dates` were repaired on `attempted_on`, storing each date's
#' package count as it stands in downloads_daily after the repair.
record_repair_attempts <- function(con, dates, attempted_on) {
  if (length(dates) == 0L) return(invisible(0L))
  DBI::dbExecute(con, REPAIR_LEDGER_SQL)
  dates  <- format(as.Date(dates), "%Y-%m-%d")
  counts <- vapply(dates, function(d) {
    DBI::dbGetQuery(con,
      "SELECT COUNT(DISTINCT package) AS n FROM downloads_daily WHERE date = ?",
      params = list(d))$n
  }, numeric(1))
  DBI::dbExecute(con,
    "INSERT OR REPLACE INTO repair_ledger (date, attempted_on, pkg_count) VALUES (?, ?, ?)",
    params = list(dates, rep(format(as.Date(attempted_on), "%Y-%m-%d"), length(dates)),
                  as.integer(counts)))
  invisible(length(dates))
}

#' Pick which partial-coverage dates the repair pass should refetch.
#'
#' A date is due when it has never been attempted, when its package count
#' differs from the count recorded after the last attempt, or when the last
#' attempt is at least `retry_days` old. Never-attempted and changed dates go
#' first, then the stalest attempts; ties break newest date first.
#'
#' @param partial data.frame(date, pkg_count) of dates below the threshold
#' @param ledger  data.frame(date, attempted_on, pkg_count)
#' @return character vector of YYYY-MM-DD dates, at most `limit` long
select_repair_dates <- function(partial, ledger, today, retry_days = 7L,
                                limit = 30L) {
  if (nrow(partial) == 0L) return(character(0))
  m <- merge(partial, ledger, by = "date", all.x = TRUE,
             suffixes = c("", "_ledger"))
  never   <- is.na(m$attempted_on)
  changed <- !never & m$pkg_count != m$pkg_count_ledger
  stale   <- !never & as.Date(m$attempted_on) <= as.Date(today) - retry_days
  due <- never | changed | stale
  m <- m[due, , drop = FALSE]
  if (nrow(m) == 0L) return(character(0))
  priority <- ifelse(is.na(m$attempted_on) | m$pkg_count != m$pkg_count_ledger, 0L, 1L)
  last_try <- ifelse(is.na(m$attempted_on), "", m$attempted_on)
  ord <- order(priority, last_try, -as.integer(as.Date(m$date)))
  head(as.character(m$date[ord]), limit)
}

#' Seconds left before `deadline` (POSIXct), never negative.
seconds_left <- function(deadline, now = Sys.time()) {
  max(0, as.numeric(deadline) - as.numeric(now))
}
