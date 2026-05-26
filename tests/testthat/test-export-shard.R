test_that("exports rows into a fresh SQLite file with expected schema", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  rows <- data.frame(
    package = c("a", "b"),
    date    = c("2024-01-01", "2024-02-02"),
    count   = c(10L, 20L),
    stringsAsFactors = FALSE
  )

  export_shard(path = tmp, rows = rows)

  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  back <- DBI::dbGetQuery(con, "SELECT * FROM downloads_daily ORDER BY package")
  expect_equal(nrow(back), 2)
  expect_equal(back$count, c(10L, 20L))

  idx <- DBI::dbGetQuery(con, "
    SELECT name FROM sqlite_master
     WHERE type = 'index' AND tbl_name = 'downloads_daily'")
  expect_true("idx_dd_date" %in% idx$name)
})

test_that("overwrites any existing file at path", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  con1 <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  DBI::dbExecute(con1, "CREATE TABLE downloads_daily(package TEXT, date TEXT, count INTEGER)")
  DBI::dbExecute(con1, "INSERT INTO downloads_daily VALUES ('old','2020-01-01',999)")
  DBI::dbDisconnect(con1)

  rows <- data.frame(
    package = "new", date = "2024-01-01", count = 1L, stringsAsFactors = FALSE
  )
  export_shard(path = tmp, rows = rows)

  con2 <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con2), add = TRUE)

  back <- DBI::dbGetQuery(con2, "SELECT * FROM downloads_daily")
  expect_equal(nrow(back), 1)
  expect_equal(back$package, "new")
})

test_that("works with empty input (zero rows)", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  rows <- data.frame(
    package = character(0), date = character(0), count = integer(0),
    stringsAsFactors = FALSE
  )
  export_shard(path = tmp, rows = rows)

  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  back <- DBI::dbGetQuery(con, "SELECT * FROM downloads_daily")
  expect_equal(nrow(back), 0)
})
