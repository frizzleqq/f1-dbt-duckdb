# F1 dbt warehouse with DuckDB

Some experimenting with dbt/DuckDB using [Ergast API](http://ergast.com/mrd/) as main source.

## Getting Started

Use https://pypi.org/project/poetry/ to setup project
(alternatively a requirements.txt file also exists).
```
poetry install
```

## File Locations

Ideally the environment variable _DUCKDB_DIR_ is set to a location where both the
DuckDB database and the F1 data will be located (the fallback method uses a relative
path).

### Staging location

When running _staging.py_ the files will be in project-root under '_data/raw_'
(unless environment variable _DUCKDB_DIR_ is defined).

### DuckDB location

DuckDB file will be in project-root under '_data/f1.duckdb_'
(unless environment variable _DUCKDB_DIR_ is defined).

## Staging:

Staging is done by
1. Read Ergast API via requests (see [staging/ergast.py](staging/ergast.py))
2. Load response into Pandas DataFrame
3. Write to CSV files
4. (dbt will create external tables using the CSV files)

Stage races since 2000 (may take a while):
```
python ./staging/staging.py --read-full
```

Stage last race only:
```
python ./staging/staging.py
```

### staging.py

By default, only the last race is loaded.
* Option _--season_ allows reading an entire season.
* Option _--read-full_ reads all seasons/races since 2000 from Ergast API. (may take a while)

dbt models use merge, so reloading data will not result in duplicate data. 

Usage:
```
usage: staging.py [-h] [-s SEASON | -r] [table_names ...]

Load data from Ergast WebService into DuckDB, by default only the last race is loaded.

positional arguments:
  table_names           List of table names to load, omit to load all tables

options:
  -h, --help            show this help message and exit
  -s SEASON, --season SEASON
                        Read provided season (=year) fully for fact tables.
  -r, --read-full       Read past seasons (beginning with 2000) for fact tables.
```

## dbt

Install dbt_utils:
```
dbt deps --project-dir="./dbt" --profiles-dir="./dbt"
```

Run models:
```
dbt run --project-dir="./dbt" --profiles-dir="./dbt"
```

Run tests:
```
dbt test --project-dir="./dbt" --profiles-dir="./dbt"
```

### sqlfluff

Run SQL linter on dbt models:
```
sqlfluff lint ./dbt/models/
```

> **_NOTE:_** Due to using external tables, the stage models cannot be properly linted.
