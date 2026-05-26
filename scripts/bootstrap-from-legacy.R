#!/usr/bin/env Rscript
# One-shot: convert the legacy single-file downloads.db into the sharded
# layout. Reads `tmp/downloads.db` (or the path given as arg 1), writes the
# new shards under arg 2 (default "out/").
#
# Usage:
#   Rscript scripts/bootstrap-from-legacy.R tmp/downloads.db out/

options(timeout = 120)
suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(jsonlite)
})

# Locate and source helpers using the same robust pattern as update.R
script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f, mustWork = FALSE)) else "scripts"
  }
)
source(file.path(script_dir, "helpers.R"), chdir = TRUE)

args <- commandArgs(trailingOnly = TRUE)
src_db <- if (length(args) >= 1) args[1] else "tmp/downloads.db"
out_dir <- if (length(args) >= 2) args[2] else "out"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

stopifnot(file.exists(src_db))
cat("Source DB:  ", src_db, "(", format(file.info(src_db)$size, big.mark = ","), "bytes)\n")
cat("Output dir: ", out_dir, "\n\n")

con <- DBI::dbConnect(RSQLite::SQLite(), src_db)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
DBI::dbExecute(con, "PRAGMA query_only = ON")

# --- 1. Discover years present in downloads_daily ----------------------------
years <- DBI::dbGetQuery(con, "
  SELECT DISTINCT CAST(substr(date,1,4) AS INTEGER) AS yr
    FROM downloads_daily
   ORDER BY yr
")$yr
cat("Years present:", paste(years, collapse = ", "), "\n\n")

# --- 2. Export each year shard -----------------------------------------------
for (yr in years) {
  rows <- extract_year_rows(con, yr)
  shard_name <- sprintf("downloads-%04d.db", yr)
  shard_path <- file.path(out_dir, shard_name)
  cat(sprintf("  %s: %s rows\n", shard_name, format(nrow(rows), big.mark = ",")))
  export_shard(shard_path, rows)
}

# --- 3. Export downloads-recent.db (last 400 days + summary + backfill_state) -
RECENT_WINDOW <- 400L
today <- Sys.Date()
recent_rows <- extract_recent_rows(con, today = today, window_days = RECENT_WINDOW)
recent_path <- file.path(out_dir, "downloads-recent.db")
cat(sprintf("\ndownloads-recent.db: %s rows (last %d days)\n",
            format(nrow(recent_rows), big.mark = ","), RECENT_WINDOW))
export_shard(recent_path, recent_rows)

# Also embed downloads_summary AND backfill_state inside downloads-recent.db
# so the next update.R run can read them.
summary_rows <- DBI::dbGetQuery(con, "SELECT * FROM downloads_summary")
bf_rows <- DBI::dbGetQuery(con, "SELECT * FROM backfill_state")

rc <- DBI::dbConnect(RSQLite::SQLite(), recent_path)
DBI::dbExecute(rc, "
  CREATE TABLE downloads_summary (
    package TEXT PRIMARY KEY,
    total_30d INTEGER, total_90d INTEGER, total_365d INTEGER,
    rank_30d INTEGER, rank_90d INTEGER, rank_365d INTEGER,
    avg_daily_30d REAL, trend REAL
  )")
DBI::dbExecute(rc, "
  CREATE TABLE backfill_state (key TEXT PRIMARY KEY, value TEXT)")
if (nrow(summary_rows) > 0) DBI::dbWriteTable(rc, "downloads_summary", summary_rows, append = TRUE)
if (nrow(bf_rows) > 0) DBI::dbWriteTable(rc, "backfill_state", bf_rows, append = TRUE)
DBI::dbExecute(rc, "VACUUM")
DBI::dbDisconnect(rc)

# --- 4. Export downloads-summary.db ------------------------------------------
summary_path <- file.path(out_dir, "downloads-summary.db")
cat(sprintf("downloads-summary.db: %s rows\n",
            format(nrow(summary_rows), big.mark = ",")))
export_summary_shard(summary_path, summary_rows)

# --- 5. Write manifest.json listing ALL shards as "changed" (this is bootstrap)
tag <- sprintf("v%s-bootstrap", format(Sys.time(), "%Y%m%d-%H%M%S", tz = "UTC"))
changed_shards <- c(
  "downloads-recent.db",
  "downloads-summary.db",
  sprintf("downloads-%04d.db", years)
)
total_rows <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM downloads_daily")$n
date_range <- DBI::dbGetQuery(con,
  "SELECT MIN(date) AS mn, MAX(date) AS mx FROM downloads_daily")

write_manifest(
  path           = file.path(out_dir, "manifest.json"),
  changed_shards = changed_shards,
  tag            = tag,
  summary        = list(
    bootstrap   = TRUE,
    years       = as.list(as.integer(years)),
    total_rows  = total_rows,
    date_range  = list(min = date_range$mn, max = date_range$mx)
  )
)

# --- 6. Summary --------------------------------------------------------------
cat("\n--- Outputs ---\n")
for (f in list.files(out_dir, full.names = TRUE)) {
  cat(sprintf("  %s  %s\n",
              format(file.info(f)$size, big.mark = ",", width = 14),
              basename(f)))
}
cat("\nDone. Total assets to upload:", length(changed_shards) + 1, "\n")
