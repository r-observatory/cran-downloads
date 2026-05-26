test_that("summary shard contains the downloads_summary table with given rows", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  summary <- data.frame(
    package      = c("ggplot2", "dplyr"),
    total_30d    = c(1000000L, 800000L),
    total_90d    = c(3000000L, 2400000L),
    total_365d   = c(12000000L, 9600000L),
    rank_30d     = c(1L, 2L),
    rank_90d     = c(1L, 2L),
    rank_365d    = c(1L, 2L),
    avg_daily_30d = c(33333.33, 26666.67),
    trend        = c(5.2, -1.1),
    stringsAsFactors = FALSE
  )

  export_summary_shard(path = tmp, summary = summary)

  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  back <- DBI::dbGetQuery(con, "SELECT * FROM downloads_summary ORDER BY rank_30d")
  expect_equal(nrow(back), 2)
  expect_equal(back$package, c("ggplot2", "dplyr"))
  expect_equal(back$total_30d, c(1000000L, 800000L))
})

test_that("summary shard does NOT contain downloads_daily", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  export_summary_shard(path = tmp, summary = data.frame(
    package = "a", total_30d = 1L, total_90d = 1L, total_365d = 1L,
    rank_30d = 1L, rank_90d = 1L, rank_365d = 1L,
    avg_daily_30d = 0.0, trend = NA_real_,
    stringsAsFactors = FALSE
  ))

  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  tables <- DBI::dbGetQuery(con,
    "SELECT name FROM sqlite_master WHERE type = 'table'")
  expect_false("downloads_daily" %in% tables$name)
  expect_true("downloads_summary" %in% tables$name)
})
