test_that("extracts only rows from the requested year", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  insert_rows(con, data.frame(
    package = c("a", "a", "b", "b"),
    date    = c("2024-12-31", "2025-01-01", "2024-06-15", "2025-12-31"),
    count   = c(10L, 20L, 30L, 40L),
    stringsAsFactors = FALSE
  ))

  result <- extract_year_rows(con, 2024L)
  expect_equal(nrow(result), 2)
  expect_setequal(result$date, c("2024-12-31", "2024-06-15"))
})

test_that("returns empty data frame when year is absent", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  insert_rows(con, data.frame(
    package = "a", date = "2025-01-01", count = 1L, stringsAsFactors = FALSE
  ))

  result <- extract_year_rows(con, 2020L)
  expect_equal(nrow(result), 0)
  expect_equal(colnames(result), c("package", "date", "count"))
})

test_that("returned columns match downloads_daily schema", {
  con <- new_test_db()
  on.exit(DBI::dbDisconnect(con))

  insert_rows(con, data.frame(
    package = "a", date = "2024-05-05", count = 7L, stringsAsFactors = FALSE
  ))

  result <- extract_year_rows(con, 2024L)
  expect_equal(colnames(result), c("package", "date", "count"))
  expect_type(result$package, "character")
  expect_type(result$date, "character")
  expect_type(result$count, "integer")
})
