# The current year's shard may be missing when a run pulls it, and the run then
# exports that year from whatever it holds. If the release was only unreachable,
# the export would replace the published shard with a much shorter one, so each
# exported year shard is compared with the published one before the run ends.

# A shard file holding `pkgs` packages on each of the first `days` days of `year`.
make_shard <- function(year, days, pkgs = c("a", "b")) {
  path  <- tempfile(fileext = ".db")
  dates <- format(as.Date(sprintf("%04d-01-01", year)) + seq_len(days) - 1L, "%Y-%m-%d")
  rows  <- expand.grid(package = pkgs, date = dates, stringsAsFactors = FALSE)
  rows$count <- rep(1L, nrow(rows))
  export_shard(path, rows)
  path
}

test_that("shard_stats counts the days and rows a shard holds", {
  path <- make_shard(2026L, days = 5L, pkgs = c("a", "b", "c"))
  on.exit(unlink(path))
  expect_equal(shard_stats(path), list(days = 5L, rows = 15))
})

test_that("shard_stats reads an empty shard as zero days and zero rows", {
  path <- make_shard(2026L, days = 0L)
  on.exit(unlink(path))
  expect_equal(shard_stats(path), list(days = 0L, rows = 0))
})

test_that("a shard holding fewer days than the published one is refused", {
  published <- make_shard(2026L, days = 273L)
  short     <- make_shard(2026L, days = 30L)
  on.exit(unlink(c(published, short)))
  expect_error(
    check_shard_not_shrunk("downloads-2026.db", shard_stats(short), shard_stats(published)),
    "downloads-2026.db holds 30 days and 60 rows but the published shard holds 273 days and 546 rows",
    fixed = TRUE)
})

test_that("a shard holding the same days but fewer rows is refused", {
  expect_error(
    check_shard_not_shrunk("downloads-2026.db",
                           new       = list(days = 273L, rows = 500),
                           published = list(days = 273L, rows = 546)),
    "stopping before it is replaced", fixed = TRUE)
})

test_that("a shard that grew, or holds the same days and rows, is allowed", {
  published <- make_shard(2026L, days = 273L)
  grown     <- make_shard(2026L, days = 274L)
  on.exit(unlink(c(published, grown)))
  expect_true(check_shard_not_shrunk("downloads-2026.db", shard_stats(grown),
                                     shard_stats(published)))
  expect_true(check_shard_not_shrunk("downloads-2026.db", shard_stats(published),
                                     shard_stats(published)))
  expect_true(check_shard_not_shrunk("downloads-2026.db",
                                     new       = list(days = 273L, rows = 600),
                                     published = list(days = 273L, rows = 546)))
})

test_that("the first publish of a new year is allowed", {
  first <- make_shard(2027L, days = 1L)
  on.exit(unlink(first))
  never_download <- function(shard, dir) stop("nothing to download for a new year")
  published <- published_shard_stats(
    "downloads-2027.db",
    list_assets = function() c("downloads-2026.db", "downloads-recent.db", "manifest.json"),
    download    = never_download,
    dir         = tempfile())
  expect_null(published)
  expect_true(check_shard_not_shrunk("downloads-2027.db", shard_stats(first), published))
})

test_that("a shard the release lists is downloaded apart from the export and counted", {
  src <- make_shard(2026L, days = 273L)
  dir <- tempfile()
  on.exit(unlink(c(src, dir), recursive = TRUE))
  asked <- NULL
  download <- function(shard, dir) {
    asked <<- c(shard, dir)
    dir.create(dir, showWarnings = FALSE, recursive = TRUE)
    file.copy(src, file.path(dir, shard))
    0L
  }
  published <- published_shard_stats(
    "downloads-2026.db",
    list_assets = function() c("downloads-2025.db", "downloads-2026.db"),
    download    = download,
    dir         = dir)
  expect_equal(published, list(days = 273L, rows = 546))
  expect_equal(asked, c("downloads-2026.db", dir))
  expect_false(file.exists(file.path(dir, "downloads-2026.db")))
})

test_that("a listed shard that cannot be downloaded stops the run", {
  listed <- function() "downloads-2026.db"
  expect_error(
    published_shard_stats("downloads-2026.db", listed,
                          download = function(shard, dir) 1L, dir = tempfile()),
    "downloads-2026.db is on the release but could not be downloaded (gh exit 1)",
    fixed = TRUE)

  src <- make_shard(2026L, days = 3L)
  dir <- tempfile()
  on.exit(unlink(c(src, dir), recursive = TRUE))
  part_way <- function(shard, dir) {
    dir.create(dir, showWarnings = FALSE, recursive = TRUE)
    file.copy(src, file.path(dir, shard))
    1L
  }
  expect_error(
    published_shard_stats("downloads-2026.db", listed, download = part_way, dir = dir),
    "could not be downloaded (gh exit 1)", fixed = TRUE)
})

test_that("a release whose assets cannot be listed stops the run", {
  expect_error(
    published_shard_stats(
      "downloads-2026.db",
      list_assets = function() release_asset_names("HTTP 503: Service Unavailable", 1L),
      download    = function(shard, dir) 0L,
      dir         = tempfile()),
    "could not list the assets on the release (gh exit 1)", fixed = TRUE)
})

test_that("release_asset_names reads one asset name per line", {
  out <- c("downloads-2025.db", "downloads-2026.db", "", "manifest.json")
  expect_equal(release_asset_names(out, 0L),
               c("downloads-2025.db", "downloads-2026.db", "manifest.json"))
  expect_equal(release_asset_names(character(0), 0L), character(0))
})

test_that("release_asset_names reads a release that does not exist yet as no assets", {
  expect_equal(release_asset_names("release not found", 1L), character(0))
})

test_that("update.R compares each exported year shard with the published one", {
  src    <- readLines(testthat::test_path("..", "..", "scripts", "update.R"))
  attach <- grep("ATTACH DATABASE '%s' AS yr", src, fixed = TRUE)
  pulled <- grep("published_stats[[shard]] <- shard_stats(shard_path)", src, fixed = TRUE)
  loop   <- grep("^for \\(yr in changed_years\\)", src)
  export <- grep("export_shard(shard_out, rows)", src, fixed = TRUE)
  check  <- grep("check_shard_not_shrunk(shard_name, shard_stats(shard_out), published)",
                 src, fixed = TRUE)
  manifest <- grep("^write_manifest\\(", src)
  expect_length(pulled, 1L)
  expect_length(loop, 1L)
  expect_length(export, 1L)
  expect_length(check, 1L)
  expect_length(manifest, 1L)
  # The pulled copy is counted before the load and before the export replaces it.
  expect_true(pulled < attach && attach < loop)
  expect_true(loop < export && export < check && check < manifest)
})
