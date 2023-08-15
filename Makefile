.PHONY: build dbt-test format install lint load test


PACKAGE := foneload

build:
	pip install build
	python -m build

dbt:
	dbt deps --project-dir="./dbt" --profiles-dir="./dbt"
	dbt build --project-dir="./dbt" --profiles-dir="./dbt"

dbt-test:
	dbt test --project-dir="./dbt" --profiles-dir="./dbt"

format:
	ruff $(PACKAGE) --fix
	isort $(PACKAGE)
	black $(PACKAGE)

install:
	python -m pip install --upgrade pip
	pip install --editable .[dev]

lint:
	ruff $(PACKAGE)
	isort $(PACKAGE) --check
	black $(PACKAGE) --check
	mypy $(PACKAGE)

load:
	$(PACKAGE)
	make dbt

test:
	make lint
