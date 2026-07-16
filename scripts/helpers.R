# scripts/helpers.R — pure functions used by update.R, unit-tested in tests/testthat/

#' Compute the set of years touched by this run.
#'
#' @param forward_dates  Date vector — dates being forward-fetched (may be empty)
#' @param backfill_range NULL or list(start=Date, end=Date) — single backfill chunk
#' @param repair_dates   Character vector — YYYY-MM-DD strings of dates needing repair
#' @return integer vector of years, sorted ascending, no duplicates
compute_touched_years <- function(forward_dates, backfill_range, repair_dates) {
  years <- integer(0)

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
