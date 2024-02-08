# F1 warehouse with DuckDB

Some experimenting with [dagster](https://docs.dagster.io/),
[dbt](https://docs.getdbt.com/) and [DuckDB](https://duckdb.org/) using
[Ergast API](http://ergast.com/mrd/) as main source.

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
    ├── circuits.csv
    ├── constructors.csv
    ├── drivers.csv
    ├── laps.csv
    ├── pitstops.csv
    ├── qualifying.csv
    ├── races.csv
    ├── results.csv
    └── seasons.csv
```

### Staging:

Staging is done by
1. Read Ergast API via requests
1. Load response into Pandas DataFrame
1. Write to CSV files
1. (dbt will create external tables using the CSV files)

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
# requires env variable `DATA_DIR` to be set
sqlfluff lint ./dbt/models/core
```

> **_NOTE:_** Due to using external tables, the stage models cannot be properly linted.
