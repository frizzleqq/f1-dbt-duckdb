.PHONY: build dbt dbt-test format install lint load load-full test


PACKAGE := foneload

build:
	pip install build
	python -m build

dbt:
	dbt deps --project-dir="./dbt" --profiles-dir="./dbt"
	dbt build --project-dir="./dbt" --profiles-dir="./dbt"

dbt-test:
	dbt test --project-dir="./dbt" --profiles-dir="./dbt"

doc:
	dbt docs generate --project-dir="./dbt" --profiles-dir="./dbt"
	dbt docs serve --project-dir="./dbt" --profiles-dir="./dbt"

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

load-full:
	$(PACKAGE) --read-full
	make dbt

test:
	make lint
