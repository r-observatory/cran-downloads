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

# --- integrity / completeness core -----------------------------------------

# Build a tiny, real summary DB on disk (canonical schema via export_summary_shard).
build_summary_db <- function(n = 3L) {
  tmp <- tempfile(fileext = ".db")
  export_summary_shard(path = tmp, summary = data.frame(
    package       = paste0("pkg", seq_len(n)),
    total_30d     = seq_len(n) * 10L,
    total_90d     = seq_len(n) * 30L,
    total_365d    = seq_len(n) * 100L,
    rank_30d      = seq_len(n),
    rank_90d      = seq_len(n),
    rank_365d     = seq_len(n),
    avg_daily_30d = seq_len(n) * 1.5,
    trend         = rep(NA_real_, n),
    stringsAsFactors = FALSE
  ))
  tmp
}

test_that("summary_integrity_core reports filename, bytes, sha256, tables, complete", {
  db <- build_summary_db(3L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db, complete = TRUE)

  expect_equal(core$db_filename, basename(db))
  # db_bytes is a double (not cast to integer) so files >= ~2 GiB do not
  # overflow to NA; compare against the uncast file.size() directly.
  expect_type(core$db_bytes, "double")
  expect_equal(core$db_bytes, file.size(db))
  # sha256 is lowercase 64-char hex of the exact file bytes
  expect_match(core$db_sha256, "^[0-9a-f]{64}$")
  # tables maps every user table to its row count
  expect_equal(core$tables, list(downloads_summary = 3L))
  expect_true(core$complete)
})

test_that("summary_integrity_core sha256 matches an independent digest of the bytes", {
  # Compute the expected hash via an external CLI tool, independent of
  # file_sha256()'s own preferred backend (digest/openssl), so this test
  # genuinely cross-checks the code path instead of re-running the same
  # library. Skip only if neither tool is on PATH (both are expected on CI).
  sha256sum_bin <- Sys.which("sha256sum")
  shasum_bin    <- Sys.which("shasum")
  if (!nzchar(sha256sum_bin) && !nzchar(shasum_bin)) {
    skip("neither sha256sum nor shasum is on PATH")
  }

  db <- build_summary_db(2L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db)

  if (nzchar(sha256sum_bin)) {
    out <- system2(sha256sum_bin, shQuote(db), stdout = TRUE)
  } else {
    out <- system2(shasum_bin, c("-a", "256", shQuote(db)), stdout = TRUE)
  }
  independent <- tolower(sub("\\s.*$", "", out[1]))

  expect_equal(core$db_sha256, independent)
})

test_that("write_manifest merges the integrity core as top-level fields", {
  db <- build_summary_db(4L)
  on.exit(unlink(db), add = TRUE)
  core <- summary_integrity_core(db, complete = TRUE)

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)

  write_manifest(
    path           = tmp,
    changed_shards = c("downloads-summary.db"),
    tag            = "v20260714-000000",
    core           = core,
    summary        = list(forward_rows = 1L)
  )

  parsed <- jsonlite::fromJSON(tmp)
  # existing fields preserved
  expect_equal(parsed$tag, "v20260714-000000")
  expect_equal(parsed$summary$forward_rows, 1L)
  # new top-level integrity/completeness core
  expect_equal(parsed$db_filename, basename(db))
  expect_equal(parsed$db_bytes, file.size(db))
  expect_match(parsed$db_sha256, "^[0-9a-f]{64}$")
  expect_equal(parsed$tables$downloads_summary, 4L)
  expect_true(parsed$complete)
})
