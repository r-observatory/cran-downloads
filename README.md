# CRAN Downloads

Daily download counts for every CRAN package, sourced from the [cranlogs API](https://cranlogs.r-pkg.org/) (RStudio CRAN mirror logs). The pipeline runs daily, fetching new download data and gradually backfilling history to October 2012. Data is published as a set of SQLite shard files attached to a single rolling GitHub release tag (`current`).

## Data Access

All shards live as assets on the [`current` release](https://github.com/r-observatory/cran-downloads/releases/tag/current). Each daily run uploads only the shards that changed; the rest remain unchanged.

### Recent data (last 400 days)

For most use cases, this is the only file you need. It contains the rolling 400-day window of `downloads_daily` plus the full `downloads_summary` table.

```bash
gh release download current \
  --repo r-observatory/cran-downloads \
  --pattern "downloads-recent.db"
```

```r
url <- "https://github.com/r-observatory/cran-downloads/releases/download/current/downloads-recent.db"
download.file(url, "downloads-recent.db", mode = "wb")

library(RSQLite)
con <- dbConnect(SQLite(), "downloads-recent.db")

# Last 30 days of ggplot2 downloads
dbGetQuery(con, "
  SELECT date, count FROM downloads_daily
  WHERE package = 'ggplot2'
  ORDER BY date DESC LIMIT 30
")

# Top packages by 30-day downloads
dbGetQuery(con, "
  SELECT package, total_30d, avg_daily_30d, rank_30d
  FROM downloads_summary
  ORDER BY rank_30d LIMIT 20
")

dbDisconnect(con)
```

### Per-year archives

Each calendar year has its own shard:

```bash
gh release download current \
  --repo r-observatory/cran-downloads \
  --pattern "downloads-2024.db"
```

### Full history (all years)

```bash
gh release download current \
  --repo r-observatory/cran-downloads \
  --pattern "downloads-*.db"
```

To query across years, ATTACH the shards or UNION them:

```r
library(RSQLite)
con <- dbConnect(SQLite(), ":memory:")
for (yr in 2012:2026) {
  shard <- sprintf("downloads-%04d.db", yr)
  if (file.exists(shard)) {
    dbExecute(con, sprintf("ATTACH '%s' AS y%d", shard, yr))
  }
}

# Union ggplot2 history across all attached years
attached_years <- as.integer(sub("^y", "",
  dbGetQuery(con, "PRAGMA database_list")$name |> setdiff("main")))
union_sql <- paste(
  sprintf("SELECT date, count FROM y%d.downloads_daily WHERE package='ggplot2'", attached_years),
  collapse = " UNION ALL "
)
result <- dbGetQuery(con, paste(union_sql, "ORDER BY date"))
```

### Summary only

For top-package lists, ranks, and trends with the smallest download:

```bash
gh release download current \
  --repo r-observatory/cran-downloads \
  --pattern "downloads-summary.db"
```

```sql
SELECT package, total_30d, rank_30d, trend
  FROM downloads_summary
 ORDER BY rank_30d LIMIT 50;
```

### Manifest

`manifest.json` lists which shards changed in the most recent run — useful for downstream consumers doing incremental updates.

```bash
gh release download current --pattern manifest.json --repo r-observatory/cran-downloads
cat manifest.json
```

## Example Queries

### Daily downloads for a package

```sql
SELECT date, count
  FROM downloads_daily
 WHERE package = 'dplyr'
 ORDER BY date DESC LIMIT 30;
```

### Top packages by monthly downloads

```sql
SELECT package, total_30d, avg_daily_30d, rank_30d, trend
  FROM downloads_summary
 ORDER BY rank_30d LIMIT 50;
```

### Fastest growing packages

```sql
SELECT package, total_30d, trend
  FROM downloads_summary
 WHERE total_30d > 1000
 ORDER BY trend DESC LIMIT 20;
```

## Schema

### `downloads_daily`

Daily download counts per package. Present in `downloads-recent.db` (last 400 days only) and in each `downloads-YYYY.db` archive (one year per file).

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK part 1) |
| `date` | TEXT | Date in YYYY-MM-DD format (PK part 2) |
| `count` | INTEGER | Number of downloads that day |

### `downloads_summary`

Aggregated download statistics, rebuilt each run. Present in `downloads-recent.db` and `downloads-summary.db`.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK) |
| `total_30d` | INTEGER | Total downloads in last 30 days |
| `total_90d` | INTEGER | Total downloads in last 90 days |
| `total_365d` | INTEGER | Total downloads in last 365 days |
| `rank_30d` | INTEGER | Rank by 30-day downloads |
| `rank_90d` | INTEGER | Rank by 90-day downloads |
| `rank_365d` | INTEGER | Rank by 365-day downloads |
| `avg_daily_30d` | REAL | Average daily downloads over 30 days |
| `trend` | REAL | Percentage change: last 30 days vs prior 30 days |

### `backfill_state`

Tracks how far back the producer has fetched. Present in `downloads-recent.db`.

| Column | Type | Description |
|---|---|---|
| `key` | TEXT | State key (PK) |
| `value` | TEXT | State value |

## License

Download data is sourced from the [cranlogs API](https://cranlogs.r-pkg.org/), which provides logs from the RStudio CRAN mirror. This repository provides the pipeline infrastructure and daily snapshots. Please respect the cranlogs API terms of use and rate limits.

## Feedback

Found a bug, a wrong number, or a missing package? Report it at [r-observatory/feedback](https://github.com/r-observatory/feedback/issues/new/choose). All feedback about R Observatory, the site, the data, and the pipelines, is tracked in one place.
