test_that("returns only rows within the window", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  today <- as.Date("2026-05-25")
  insert_rows(con, data.frame(
    package = rep("a", 4),
    date    = c("2024-01-01", "2025-04-21", "2026-04-21", "2026-05-25"),
    count   = 1:4,
    stringsAsFactors = FALSE
  ))

  # 400-day window from 2026-05-25 starts on 2025-04-21
  result <- extract_recent_rows(con, today = today, window_days = 400L)
  expect_equal(nrow(result), 3)
  expect_false("2024-01-01" %in% result$date)
})

test_that("inclusive on the lower boundary", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  today <- as.Date("2026-05-25")
  cutoff <- format(today - 400L, "%Y-%m-%d")
  insert_rows(con, data.frame(
    package = "a", date = cutoff, count = 1L, stringsAsFactors = FALSE
  ))

  result <- extract_recent_rows(con, today = today, window_days = 400L)
  expect_equal(nrow(result), 1)
})

test_that("empty DB returns 0-row data frame with correct columns", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  result <- extract_recent_rows(con, today = as.Date("2026-05-25"), window_days = 400L)
  expect_equal(nrow(result), 0)
  expect_equal(colnames(result), c("package", "date", "count"))
})
