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

#' Write the manifest.json describing which shards changed this run.
#'
#' Empty arrays are preserved (jsonlite default is to drop them — we force them).
write_manifest <- function(path, changed_shards, tag, summary) {
  obj <- list(
    tag            = tag,
    generated_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    changed_shards = as.list(changed_shards),
    summary        = summary
  )
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
