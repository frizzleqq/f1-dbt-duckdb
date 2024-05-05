# F1 warehouse with DuckDB

Project with
* Orchestration: [dagster](https://docs.dagster.io/)
* Transformation & Testing: [dbt](https://docs.getdbt.com/)
* Processing Engine & Database: [DuckDB](https://duckdb.org/)
* Data Source: [Ergast API](http://ergast.com/mrd/)

Cutout of the dagster lineage graph:
![alt text](docs/dagster_lineage.png "Title")

## Development

* Python >= 3.10 https://www.python.org/downloads/
* pip >= 21.3 (for editable installs, see [PEP 660](https://peps.python.org/pep-0660/))
  * `python -m pip install --upgrade pip`

This project uses `pyproject.toml` to describe package metadata
(see [PEP 621](https://peps.python.org/pep-0621/)).

### Setup Virtual environment

Following commands create and activate a virtual environment.
The `[dev]` also installs development tools.
The `--editable` makes the CLI script available.

* Bash:
    ```bash
    make requirements
    source .venv/bin/activate
    ```
* PowerShell:
    ```powershell
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install --editable .[dev]
    ```
* Windows CMD:
    ```
    python -m venv .venv
    .venv\Scripts\activate.bat
    python -m pip install --upgrade pip
    pip install --editable .[dev]
    ```

### Dagster

Dagster uses environment variables located in [.env](.env).

Start local dagster server
```bash
dagster dev
```

#### Dagster Launchpad

The _launchpad_ can be used to materialize assets with specified configurations.
Such as staging a specific season or all seasons since a specified season.

Using the dropdown menue next to the Materialize button it is possible to start overrule
the configuration. See also here: https://docs.dagster.io/concepts/webserver/ui#job-details

![alt text](docs/dagster_launchpad.png "Title")

#### Dagster CLI

Launch dagster job without
```bash
dagster job execute -m foneplatform -j ergast_job
```

### Development Tools

* Code linting/formatting: `ruff`
* type checking: `mypy`
* SQL linting/formatting: `sqlfluff`


## File Locations

Ideally the environment variable `DATA_DIR` is set to a location where both the
DuckDB database and the F1 data will be located. Dagster uses `.env` to set the path.

The data directory will look like this:
```
data
├── f1.duckdb
└── ergast
    ├── circuits.parquet
    ├── constructors.parquet
    ├── drivers.parquet
    ├── laps.parquet
    ├── pitstops.parquet
    ├── qualifying.parquet
    ├── races.parquet
    ├── results.parquet
    └── seasons.parquet
```

### Staging:

Staging is done by
1. Read Ergast API via requests (by default only last race)
1. Load response into Pandas DataFrame
1. Write to Parquet files
1. (dbt will create external tables using the Parquet files)

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
> **_NOTE:_** This may require setting the `DATA_DIR` environment variable to be set to the `data` directory containing the duckdb database.
```
sqlfluff lint ./dbt/models/core
```
