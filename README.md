# F1 dbt warehouse with DuckDB

Some experimenting with dbt/DuckDB and maybe later some reporting using
[Ergast API](http://ergast.com/mrd/) as main source.

Used https://pypi.org/project/poetry/ to setup project.
```
poetry install
```

## Staging:
```
python ./dags/staging/staging.py
```

Staging is done by
1. Read Ergast API via requests (see [staging.ergast.py](dags/staging/ergast.py))
2. Load response into Pandas DataFrame
3. Create table in DuckDB via CTAS

## dbt

Install dbt_utils:
```
dbt deps --project-dir="./dbt" --profiles-dir="./dbt"
```

Run models:
```
dbt run --project-dir="./dbt" --profiles-dir="./dbt"
```

Note that currently the database is assumed to be in the project-directory under '_data/f1.duckdb_'.

## Airflow

Initial setup to run it in airflow

```
poetry export --without-hashes -f requirements.txt --output requirements.txt
docker build -t airflow-extended:latest -f Dockerfile .
docker-compose up airflow-init
docker-compose up
```