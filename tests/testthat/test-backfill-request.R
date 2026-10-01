# A workflow_dispatch can ask for named packages to be fetched again from a
# start date (the backfill_packages and backfill_from inputs). A bad request
# stops the run before anything is downloaded.

test_that("no inputs means no request", {
  expect_null(parse_backfill_request("", "", as.Date("2026-10-01")))
  expect_null(parse_backfill_request(NULL, NULL, as.Date("2026-10-01")))
})

test_that("names split on commas and spaces, keeping the first of each", {
  req <- parse_backfill_request("RDesk, hespdiv  rFUSION,hespdiv", "2021-01-01",
                                as.Date("2026-10-01"))
  expect_equal(req$packages, c("RDesk", "hespdiv", "rFUSION"))
  expect_equal(req$start, as.Date("2021-01-01"))
  expect_equal(req$end, as.Date("2026-10-01"))
})

test_that("the fourteen OS_type windows packages parse as one request", {
  req <- parse_backfill_request(
    "BiplotGUI,blatr,excel.link,hespdiv,KeyboardSimulator,MDSGUI,MediaNews,R2PPT,R2wd,RDesk,rFUSION,RWinEdt,spectrino,taskscheduleR",
    "2021-01-01", as.Date("2026-10-01"))
  expect_length(req$packages, 14L)
})

test_that("one input without the other is refused", {
  expect_error(parse_backfill_request("hespdiv", "", as.Date("2026-10-01")), "together")
  expect_error(parse_backfill_request("", "2021-01-01", as.Date("2026-10-01")), "together")
})

test_that("a name that is not a CRAN package name is refused", {
  expect_error(parse_backfill_request("hespdiv,rm -rf", "2021-01-01", as.Date("2026-10-01")),
               "not a CRAN package name")
  expect_error(parse_backfill_request("a", "2021-01-01", as.Date("2026-10-01")),
               "not a CRAN package name")
})

test_that("a start date that is malformed or out of range is refused", {
  y <- as.Date("2026-10-01")
  expect_error(parse_backfill_request("hespdiv", "2021-1-1", y), "YYYY-MM-DD")
  expect_error(parse_backfill_request("hespdiv", "yesterday", y), "YYYY-MM-DD")
  expect_error(parse_backfill_request("hespdiv", "2012-09-30", y), "between")
  expect_error(parse_backfill_request("hespdiv", "2026-10-02", y), "between")
})

test_that("a year loaded only for a request is never offered to the repair pass", {
  # Every day of 2021 holds 12,534 to 14,519 packages, under the 20,000-package
  # coverage threshold, so repairing it would refetch every package every day.
  partial <- data.frame(date = c("2021-03-01", "2026-09-20"), pkg_count = c(14000L, 19000L),
                        stringsAsFactors = FALSE)
  expect_equal(repair_candidates(partial, repair_years = 2026L)$date, "2026-09-20")
  expect_equal(nrow(repair_candidates(partial[0, ], repair_years = 2026L)), 0L)
})

test_that("the dispatch inputs reach update.R as environment variables", {
  wf <- paste(readLines(testthat::test_path("..", "..", ".github", "workflows", "update.yml")),
              collapse = "\n")
  expect_match(wf, "      backfill_packages:", fixed = TRUE)
  expect_match(wf, "      backfill_from:", fixed = TRUE)
  expect_match(wf, "BACKFILL_PACKAGES: ${{ inputs.backfill_packages }}", fixed = TRUE)
  expect_match(wf, "BACKFILL_FROM: ${{ inputs.backfill_from }}", fixed = TRUE)
})
