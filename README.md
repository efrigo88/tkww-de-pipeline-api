# TKWW Take Home Test

This is a take home test project focused on creating an ETL process and a Flask REST API to handle a Movies catalog.

## Index
- [Prerequisites](#prerequisites)
- [Prepare Environment](#prepare-environment)
- [Running the Pipeline](#running-the-pipeline)
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

## Running the Pipeline
To execute the ETL pipeline, run the following command in your terminal:

1. Make sure you are inside the project:
```shell
cd tkww-de-take-home-test
```
2. Run the following command:
```shell
python scripts/etl.py
```
The whole pipeline will start executing, It will read the source file, proccess it accordingly and persist it in the database.


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

### Summary of Changes:
- **Added "Running the Pipeline" Section**: Clear instructions on how to run the ETL pipeline using the command `python scripts/etl.py`.

This updated `README.md` should provide all the necessary information for users to set up and run the project effectively.
