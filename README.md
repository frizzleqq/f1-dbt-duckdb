# F1 dbt warehouse with DuckDB

Some experimenting with dbt/DuckDB using [Ergast API](http://ergast.com/mrd/) as main source.

Used https://pypi.org/project/poetry/ to setup project.
```
poetry install
```

## Staging:

Staging is done by
1. Read Ergast API via requests (see [staging/ergast.py](staging/ergast.py))
2. Load response into Pandas DataFrame
3. Create table in DuckDB via CTAS

> **_NOTE:_** When running _staging.py_ locally the database will be in project-root under
> '_data/f1.duckdb_' (unless environment variable _DUCKDB_DATABASE_ is defined).

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

Run snapshots:
```
dbt snapshot --project-dir="./dbt" --profiles-dir="./dbt"
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


## Airflow

> **_NOTE:_**  We use Airflow version 2.3.2 since as of 2.3.3 'jinja2>=3.0.0' is required, which
> conflicts with dbt-core.
> Also: poetry does not play nice with apache-airflow, so right now we work with all extras
> (unnecessarily) installed.

Initial setup to run it in airflow

```
docker build -f Dockerfile --progress=plain .
docker-compose up airflow-init
docker-compose up
```

Clean up docker containers

```
docker-compose down --volumes --remove-orphans
```