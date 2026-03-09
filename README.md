# CRAN Downloads

Daily download counts for every CRAN package, sourced from the [cranlogs API](https://cranlogs.r-pkg.org/) (RStudio CRAN mirror logs). The pipeline runs daily, fetching new download data and gradually backfilling history to October 2012. All data is stored in a single SQLite database (`downloads.db`) and published as a GitHub release.

## Data Access

### CLI

```bash
gh release download latest --repo r-observatory/cran-downloads --pattern "downloads.db"
```

### R

```r
url <- "https://github.com/r-observatory/cran-downloads/releases/latest/download/downloads.db"
download.file(url, "downloads.db", mode = "wb")

library(RSQLite)
con <- dbConnect(SQLite(), "downloads.db")

# Daily downloads for a package
dbGetQuery(con, "
  SELECT date, count
  FROM downloads_daily
  WHERE package = 'ggplot2'
  ORDER BY date DESC
  LIMIT 30
")

# Top packages by monthly downloads
dbGetQuery(con, "
  SELECT package, total_30d, avg_daily_30d, rank_30d
  FROM downloads_summary
  ORDER BY rank_30d
  LIMIT 20
")

dbDisconnect(con)
```

### Python

```python
import urllib.request
import sqlite3

url = "https://github.com/r-observatory/cran-downloads/releases/latest/download/downloads.db"
urllib.request.urlretrieve(url, "downloads.db")

con = sqlite3.connect("downloads.db")
cur = con.cursor()

# Daily downloads for a package
cur.execute("""
    SELECT date, count
    FROM downloads_daily
    WHERE package = 'ggplot2'
    ORDER BY date DESC
    LIMIT 30
""")
print(cur.fetchall())

# Top packages by monthly downloads
cur.execute("""
    SELECT package, total_30d, avg_daily_30d, rank_30d
    FROM downloads_summary
    ORDER BY rank_30d
    LIMIT 20
""")
print(cur.fetchall())

con.close()
```

## Example Queries

### Daily downloads for a package

```sql
SELECT date, count
FROM downloads_daily
WHERE package = 'dplyr'
ORDER BY date DESC
LIMIT 30;
```

### Top packages by monthly downloads

```sql
SELECT package, total_30d, avg_daily_30d, rank_30d, trend
FROM downloads_summary
ORDER BY rank_30d
LIMIT 50;
```

### Download trend for a package

```sql
SELECT package, total_30d, trend, avg_daily_30d
FROM downloads_summary
WHERE package = 'data.table';
```

### Fastest growing packages

```sql
SELECT package, total_30d, trend
FROM downloads_summary
WHERE total_30d > 1000
ORDER BY trend DESC
LIMIT 20;
```

## Schema

### `downloads_daily`

Daily download counts per package. Accumulated over time with gradual backfill.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK part 1) |
| `date` | TEXT | Date in YYYY-MM-DD format (PK part 2) |
| `count` | INTEGER | Number of downloads that day |

### `downloads_summary`

Aggregated download statistics, rebuilt each run.

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

Tracks backfill progress.

| Column | Type | Description |
|---|---|---|
| `key` | TEXT | State key (PK) |
| `value` | TEXT | State value |

The `backfill_frontier` key records how far back data has been fetched.

## Backfill Schedule

The pipeline gradually extends history backwards by one month per daily run. Starting from the current date, it takes approximately 5 months of daily runs to reach full coverage back to October 2012. The `backfill_state` table tracks progress. Forward fetches (new days) always run first, followed by one month of backfill.

## Update Schedule

The database is updated daily at 07:00 UTC via GitHub Actions. Each run fetches new daily download counts, performs one month of backfill, and rebuilds the summary table. The latest database is always available from the most recent GitHub release.

## License

Download data is sourced from the [cranlogs API](https://cranlogs.r-pkg.org/), which provides logs from the RStudio CRAN mirror. This repository provides the pipeline infrastructure and daily snapshots. Please respect the cranlogs API terms of use and rate limits.
