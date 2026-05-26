library(testthat)

# Source helpers from the repo root scripts/ directory.
# Resolve the directory of this script. Works whether invoked via
# Rscript (--file=...) or source()'d interactively from an R session.
script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(f) else "."
  }
)

helpers_path <- normalizePath(
  file.path(script_dir, "..", "scripts", "helpers.R"),
  mustWork = FALSE
)

if (file.exists(helpers_path)) {
  source(helpers_path, chdir = TRUE)
}

test_dir(file.path(script_dir, "testthat"), reporter = "summary", stop_on_failure = TRUE)
