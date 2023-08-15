# F1 dbt warehouse with DuckDB

Some experimenting with dbt/DuckDB using [Ergast API](http://ergast.com/mrd/) as
main source.

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
    python -m venv .venv
    source .venv/bin/activate
    make install
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

### Development Tools

* Code linting: `ruff`
* Code formatting: `black`
* Import sorting: `isort`
* type checking: `mypy`
* SQL linting/formatting: `sqlfluff`


## File Locations

Ideally the environment variable `DUCKDB_DIR` is set to a location where both the
DuckDB database and the F1 data will be located (the fallback method uses a relative
path).

The data directory will look like this:
```
data
├── f1.duckdb
└── raw
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

### Staging location

When running `foneload` the files will be in project-root under `data/raw`
(unless environment variable `DUCKDB_DIR` is defined).

### DuckDB location

DuckDB file will be in project-root under `data/f1.duckdb`
(unless environment variable `DUCKDB_DIR` is defined).


## Staging:

Staging is done by
1. Read Ergast API via requests (see [foneload/ergast.py](foneload/ergast.py))
1. Load response into Pandas DataFrame
1. Write to CSV files
1. (dbt will create external tables using the CSV files)

Stage races since 2000 (may take a while):
```
foneload --read-full
```

Stage last race only:
```
foneload
```

If not in editable mode it can also be run from root:
```
python ./foneload
```

### CLI

By default, only the last race is loaded.
* Option _--season_ allows reading an entire season.
* Option _--read-full_ reads all seasons/races since 2000 from Ergast API. (may take a while)

dbt models use merge, so reloading data will not result in duplicate rows. 

Usage:
```
usage: foneload [-h] [-s SEASON | -r] [table_names ...]

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
