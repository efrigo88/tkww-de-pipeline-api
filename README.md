# TKWW Take Home Test

This is a take home test project focused on creating an ETL process and a Flask REST API to handle a Movies catalog.

## Index
- [Prerequisites](#prerequisites)
- [Prepare Environment](#prepare-environment)
- [Repository Structure](#repository-structure)

## Prerequisites

- A working Python install. I recommend using [pyenv](https://formulae.brew.sh/formula/pyenv) to administrate your Python environments.
- [poetry](https://formulae.brew.sh/formula/poetry) to handle the project dependencies properly.

## Prepare Environment
The following section will give you the steps to configure an environment you need to create new data pipelines or run the tests locally.

1. Create and configure the project virtual environment:
```shell
cd tkww-de-take-home-test
poetry env use 3.10
poetry install
```

2. Pre commit hooks installation
```shell
poetry run pre-commit install
```

3. Test project configuration executing the tests
```shell
poetry run pytest
```

## Repository Structure
```
/tkww-de-take-home-test
│
├── /data               # Folder where source data is stored.
├── /scripts            # PySpark and API scripts.
│   ├── etl.py          # Pyspark e2e pipeline.
│   ├── api.py          # Flask REST API script.
├── database.db         # SQLite database.
├── pyproject.toml      # Poetry dependencies.
└── README.md           # Intructions on how to run the project.
```
