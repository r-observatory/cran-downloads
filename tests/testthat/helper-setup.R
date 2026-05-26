# Path to the small fixture DB (created during pre-work).
# Resolve the helper's own directory. testthat's source_file() uses eval()
# rather than source(), so sys.frame(1)$ofile is NULL — dirname(NULL) errors
# out and the tryCatch falls through to the testthat_path option (which
# testthat itself sets to the helper file's path).
helper_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) dirname(getOption("testthat_path", "./helper-setup.R"))
)
if (!nzchar(helper_dir)) {
  helper_dir <- dirname(getOption("testthat_path", "./helper-setup.R"))
}

FIXTURE_DB <- normalizePath(
  file.path(helper_dir, "fixtures", "sample-downloads.db"),
  mustWork = FALSE
)

skip_if_no_fixture <- function() {
  if (!file.exists(FIXTURE_DB)) {
    testthat::skip("Fixture DB not present; see pre-work in plan")
  }
}

# Helper to build a tiny in-memory test DB
new_test_db <- function() {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbExecute(con, "
    CREATE TABLE downloads_daily (
      package TEXT NOT NULL,
      date    TEXT NOT NULL,
      count   INTEGER NOT NULL,
      PRIMARY KEY (package, date)
    )")
  DBI::dbExecute(con, "CREATE INDEX idx_dd_date ON downloads_daily(date)")
  con
}

# Insert a small set of rows for a given list of (package, date, count) triples
insert_rows <- function(con, df) {
  DBI::dbExecute(
    con,
    "INSERT INTO downloads_daily (package, date, count) VALUES (?, ?, ?)",
    params = list(df$package, df$date, df$count)
  )
}
