ledger_df <- function(date = character(0), attempted_on = character(0),
                      pkg_count = integer(0)) {
  data.frame(date = date, attempted_on = attempted_on,
             pkg_count = as.integer(pkg_count), stringsAsFactors = FALSE)
}

partial_df <- function(date, pkg_count) {
  data.frame(date = date, pkg_count = as.integer(pkg_count),
             stringsAsFactors = FALSE)
}

test_that("never-attempted partial dates are due", {
  p <- partial_df(c("2025-01-11", "2025-01-14"), c(16191, 18948))
  got <- select_repair_dates(p, ledger_df(), today = as.Date("2026-09-28"))
  expect_setequal(got, c("2025-01-11", "2025-01-14"))
})

test_that("a date attempted recently with an unchanged count is skipped", {
  p <- partial_df("2025-01-11", 16191)
  l <- ledger_df("2025-01-11", "2026-09-27", 16191)
  expect_equal(select_repair_dates(p, l, today = as.Date("2026-09-28")),
               character(0))
})

test_that("a date is retried once the last attempt is a week old", {
  p <- partial_df("2025-01-11", 16191)
  l <- ledger_df("2025-01-11", "2026-09-21", 16191)
  expect_equal(select_repair_dates(p, l, today = as.Date("2026-09-28")),
               "2025-01-11")
  l6 <- ledger_df("2025-01-11", "2026-09-22", 16191)
  expect_equal(select_repair_dates(p, l6, today = as.Date("2026-09-28")),
               character(0))
})

test_that("a date whose package count changed is retried immediately", {
  p <- partial_df("2026-09-26", 12000)
  l <- ledger_df("2026-09-26", "2026-09-28", 5000)
  expect_equal(select_repair_dates(p, l, today = as.Date("2026-09-28")),
               "2026-09-26")
})

test_that("new and changed dates come before stale retries, newest first", {
  p <- partial_df(c("2025-01-11", "2025-03-09", "2026-06-21", "2026-05-24"),
                  c(16191, 18217, 17725, 18113))
  l <- ledger_df(c("2025-01-11", "2025-03-09"),
                 c("2026-09-01", "2026-09-10"), c(16191, 18217))
  got <- select_repair_dates(p, l, today = as.Date("2026-09-28"))
  expect_equal(got, c("2026-06-21", "2026-05-24", "2025-01-11", "2025-03-09"))
})

test_that("the limit caps the selection", {
  p <- partial_df(sprintf("2025-01-%02d", 1:20), rep(15000, 20))
  got <- select_repair_dates(p, ledger_df(), today = as.Date("2026-09-28"),
                             limit = 5L)
  expect_length(got, 5L)
  expect_equal(got[1], "2025-01-20")
})

test_that("no partial dates means nothing to repair", {
  expect_equal(select_repair_dates(partial_df(character(0), integer(0)),
                                   ledger_df(), today = Sys.Date()),
               character(0))
})

test_that("record_repair_attempts stores the post-repair package count", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  insert_rows(con, data.frame(
    package = c("a", "b", "c", "a"),
    date    = c("2025-01-11", "2025-01-11", "2025-01-11", "2025-01-14"),
    count   = c(1L, 2L, 3L, 4L)))

  record_repair_attempts(con, c("2025-01-11", "2025-01-14"),
                         attempted_on = as.Date("2026-09-28"))
  l <- read_repair_ledger(con)
  l <- l[order(l$date), ]
  expect_equal(l$date, c("2025-01-11", "2025-01-14"))
  expect_equal(l$pkg_count, c(3L, 1L))
  expect_equal(unique(l$attempted_on), "2026-09-28")

  # Re-recording replaces the row
  record_repair_attempts(con, "2025-01-14", attempted_on = as.Date("2026-10-05"))
  l <- read_repair_ledger(con)
  expect_equal(nrow(l), 2L)
  expect_equal(l$attempted_on[l$date == "2025-01-14"], "2026-10-05")
})

test_that("read_repair_ledger returns an empty frame before any attempt", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  l <- read_repair_ledger(con)
  expect_equal(nrow(l), 0L)
  expect_named(l, c("date", "attempted_on", "pkg_count"))
})

test_that("group_contiguous_dates merges consecutive days", {
  ch <- group_contiguous_dates(c("2025-02-03", "2025-01-11", "2025-02-02",
                                 "2025-01-14"))
  expect_length(ch, 3L)
  expect_equal(ch[[1]]$start, as.Date("2025-01-11"))
  expect_equal(ch[[3]]$start, as.Date("2025-02-02"))
  expect_equal(ch[[3]]$end,   as.Date("2025-02-03"))
  expect_equal(group_contiguous_dates(character(0)), list())
})

test_that("seconds_left never goes negative", {
  t0 <- as.POSIXct("2026-09-28 12:00:00", tz = "UTC")
  expect_equal(seconds_left(t0 + 90, now = t0), 90)
  expect_equal(seconds_left(t0, now = t0 + 5), 0)
})
