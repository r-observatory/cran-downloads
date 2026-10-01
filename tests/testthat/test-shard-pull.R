# Every year shard before the current one is on the release, so a past year
# missing after its download stops the run: exporting that year from the
# working database would republish it holding only the rows this run fetched.
# The current year may be missing on the first run that reaches it.

test_that("a past year that could not be downloaded stops the run", {
  expect_error(check_shard_pulled(2022L, present = FALSE, current_year = 2026L, status = 1L),
               "downloads-2022.db could not be downloaded", fixed = TRUE)
  expect_error(check_shard_pulled(2025L, present = FALSE, current_year = 2026L),
               "downloads-2025.db could not be downloaded", fixed = TRUE)
})

test_that("the current year may be missing, as on the first run that reaches it", {
  expect_true(check_shard_pulled(2027L, present = FALSE, current_year = 2027L, status = 1L))
})

test_that("a shard that downloaded passes", {
  expect_true(check_shard_pulled(2021L, present = TRUE, current_year = 2026L))
  expect_true(check_shard_pulled(2026L, present = TRUE, current_year = 2026L, status = 0L))
})

test_that("a file left by a failed download stops the run, in any year", {
  expect_error(check_shard_pulled(2026L, present = TRUE, current_year = 2026L, status = 1L),
               "downloads-2026.db: the download failed part way", fixed = TRUE)
})

test_that("update.R checks each year shard it pulls before loading it", {
  src  <- readLines(testthat::test_path("..", "..", "scripts", "update.R"))
  loop <- grep("^for \\(yr in touched_years\\)", src)
  dl   <- grep("status <- gh_download(shard, out_dir)", src, fixed = TRUE)
  chk  <- grep("check_shard_pulled(yr, file.exists(shard_path)", src, fixed = TRUE)
  att  <- grep("ATTACH DATABASE '%s' AS yr", src, fixed = TRUE)
  expect_length(loop, 1L)
  expect_length(dl, 1L)
  expect_length(chk, 1L)
  expect_true(loop < dl && dl < chk && chk < att)
})
