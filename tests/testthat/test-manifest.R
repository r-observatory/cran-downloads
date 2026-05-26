test_that("manifest serializes changed shards and tag", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  write_manifest(
    path           = tmp,
    changed_shards = c("downloads-recent.db", "downloads-2024.db"),
    tag            = "v20260525-120000",
    summary        = list(forward_rows = 23000L, backfill_rows = 500000L)
  )

  parsed <- jsonlite::fromJSON(tmp)
  expect_equal(parsed$tag, "v20260525-120000")
  expect_equal(parsed$changed_shards,
               c("downloads-recent.db", "downloads-2024.db"))
  expect_equal(parsed$summary$forward_rows, 23000L)
  expect_equal(parsed$summary$backfill_rows, 500000L)
  expect_true(nzchar(parsed$generated_at))
})

test_that("empty changed_shards is still valid JSON array, not null", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))

  write_manifest(
    path           = tmp,
    changed_shards = character(0),
    tag            = "v...",
    summary        = list()
  )

  raw <- readLines(tmp, warn = FALSE)
  expect_true(any(grepl('"changed_shards"\\s*:\\s*\\[\\s*\\]', raw)))
})
