# The forward fetch asks cranlogs for every package CRAN's PACKAGES index
# lists, read with only the duplicates filter: a package declaring
# OS_type: windows, or needing a newer R than this runner, is downloaded all
# the same, and a Recommended package listed twice is asked for once.

.fixture_repo <- function() {
  paste0("file://", normalizePath(testthat::test_path("fixtures", "cran-repo")))
}

test_that("Windows-only packages and packages needing a newer R are fetched", {
  expect_equal(cran_package_names(.fixture_repo()), c("boot", "cli", "hespdiv", "laterR"))
})

test_that("a Recommended package listed twice is fetched once", {
  expect_equal(sum(cran_package_names(.fixture_repo()) == "boot"), 1L)
})

test_that("the default filters drop both packages on this runner", {
  skip_on_os("windows")
  expect_equal(sort(rownames(utils::available.packages(repos = .fixture_repo()))),
               c("boot", "cli"))
})
