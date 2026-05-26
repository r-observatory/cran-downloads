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
