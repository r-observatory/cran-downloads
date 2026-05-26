test_that("forward-fetch on a single date returns just that year", {
  result <- compute_touched_years(
    forward_dates  = as.Date("2026-05-25"),
    backfill_range = NULL,
    repair_dates   = character(0)
  )
  expect_equal(result, 2026L)
})

test_that("forward-fetch spanning year boundary returns both years", {
  result <- compute_touched_years(
    forward_dates  = seq(as.Date("2025-12-30"), as.Date("2026-01-02"), by = 1),
    backfill_range = NULL,
    repair_dates   = character(0)
  )
  expect_equal(sort(result), c(2025L, 2026L))
})

test_that("backfill range within one year returns that year", {
  result <- compute_touched_years(
    forward_dates  = as.Date(character(0)),
    backfill_range = list(start = as.Date("2024-03-01"), end = as.Date("2024-03-30")),
    repair_dates   = character(0)
  )
  expect_equal(result, 2024L)
})

test_that("backfill range crossing year boundary returns both years", {
  result <- compute_touched_years(
    forward_dates  = as.Date(character(0)),
    backfill_range = list(start = as.Date("2024-12-15"), end = as.Date("2025-01-14")),
    repair_dates   = character(0)
  )
  expect_equal(sort(result), c(2024L, 2025L))
})

test_that("repair dates contribute their distinct years", {
  result <- compute_touched_years(
    forward_dates  = as.Date(character(0)),
    backfill_range = NULL,
    repair_dates   = c("2018-04-15", "2019-08-22", "2018-12-31")
  )
  expect_equal(sort(result), c(2018L, 2019L))
})

test_that("union of all three sources, deduplicated and sorted", {
  result <- compute_touched_years(
    forward_dates  = as.Date("2026-05-25"),
    backfill_range = list(start = as.Date("2023-06-01"), end = as.Date("2023-06-30")),
    repair_dates   = c("2026-01-10", "2020-05-05")
  )
  expect_equal(result, c(2020L, 2023L, 2026L))
})

test_that("all empty sources returns empty integer vector", {
  result <- compute_touched_years(
    forward_dates  = as.Date(character(0)),
    backfill_range = NULL,
    repair_dates   = character(0)
  )
  expect_equal(result, integer(0))
})
