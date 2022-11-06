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
1. Read Ergast API via requests (see [staging/ergast.py](staging/ergast.py))
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