PACKAGE := foneplatform

SHELL=/bin/bash
VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.venv:  ## Set up Python virtual environment and install requirements
	python -m venv $(VENV)
	$(MAKE) requirements

.PHONY: requirements
requirements: .venv  ## Install/refresh Python project requirements
	$(VENV_BIN)/python -m pip install --upgrade pip
	$(VENV_BIN)/python -m pip install --editable .[dev]

.PHONY: build
build:
	$(VENV_BIN)/python -m pip install build
	$(VENV_BIN)/python -m build

.PHONY: dagster
dagster:
	"$(VENV_BIN)/dbt" deps --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dbt" parse --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dagster" dev

.PHONY: dbt
dbt:
	"$(VENV_BIN)/dbt" deps --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dbt" build --project-dir="./dbt" --profiles-dir="./dbt"

.PHONY: dbt-test
dbt-test:
	"$(VENV_BIN)/dbt" test --project-dir="./dbt" --profiles-dir="./dbt"

.PHONY: doc
doc:
	"$(VENV_BIN)/dbt" docs generate --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dbt" docs serve --project-dir="./dbt" --profiles-dir="./dbt"

.PHONY: format
format:
	$(VENV_BIN)/ruff $(PACKAGE) --fix
	$(VENV_BIN)/isort $(PACKAGE)
	$(VENV_BIN)/black $(PACKAGE)

.PHONY: lint
lint:
	$(VENV_BIN)/ruff $(PACKAGE)
	$(VENV_BIN)/isort $(PACKAGE) --check
	$(VENV_BIN)/black $(PACKAGE) --check
	$(VENV_BIN)/mypy $(PACKAGE)

.PHONY: load
load:
	"$(VENV_BIN)/dbt" deps --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dbt" parse --project-dir="./dbt" --profiles-dir="./dbt"
	"$(VENV_BIN)/dagster" job execute -m "foneplatform" -j "ergast_job"

.PHONY: test
test:
	$(MAKE) lint
