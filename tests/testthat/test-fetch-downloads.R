fake_payload <- function(pkgs, day, n = 5L) {
  jsonlite::toJSON(lapply(pkgs, function(p) list(
    package = p,
    downloads = list(list(day = day, downloads = n)))), auto_unbox = TRUE)
}

pkgs_from_url <- function(url) strsplit(sub(".*/", "", url), ",")[[1]]

test_that("fetch_downloads collects every batch when there is no deadline", {
  calls <- 0L
  reader <- function(url) {
    calls <<- calls + 1L
    fake_payload(pkgs_from_url(url), "2025-01-11")
  }
  out <- fetch_downloads(sprintf("p%03d", 1:250), "2025-01-11", "2025-01-11",
                         reader = reader, pause = function() NULL)
  expect_equal(calls, 3L)
  expect_equal(nrow(out), 250L)
  expect_false(attr(out, "truncated"))
})

test_that("fetch_downloads stops making requests once the deadline passes", {
  clock <- as.POSIXct("2026-09-28 12:00:00", tz = "UTC")
  calls <- 0L
  reader <- function(url) {
    calls <<- calls + 1L
    clock <<- clock + 60  # each request takes a minute
    fake_payload(pkgs_from_url(url), "2025-01-11")
  }
  deadline <- clock + 150
  out <- fetch_downloads(sprintf("p%03d", 1:1000), "2025-01-11", "2025-01-11",
                         deadline = deadline, reader = reader,
                         now = function() clock, pause = function() NULL)
  expect_equal(calls, 3L)
  expect_equal(nrow(out), 300L)
  expect_true(attr(out, "truncated"))
})

test_that("fetch_downloads drops zero-download days and survives API errors", {
  reader <- function(url) {
    if (grepl("bad", url)) stop("HTTP 502")
    jsonlite::toJSON(list(list(package = "a", downloads = list(
      list(day = "2025-01-11", downloads = 0L),
      list(day = "2025-01-12", downloads = 7L)))), auto_unbox = TRUE)
  }
  out <- fetch_downloads("a", "2025-01-11", "2025-01-12",
                         reader = reader, pause = function() NULL)
  expect_equal(out$date, "2025-01-12")
  expect_equal(out$count, 7L)

  out2 <- suppressMessages(capture.output(
    res <- fetch_downloads("bad", "2025-01-11", "2025-01-11",
                           reader = reader, pause = function() NULL)))
  expect_equal(nrow(res), 0L)
  expect_false(attr(res, "truncated"))
})
