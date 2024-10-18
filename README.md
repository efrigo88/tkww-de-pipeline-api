# TKWW Take Home Test

This is a take home test project focused on creating an ETL process and a Flask REST API to handle a Movies catalog.

## Index
- [Prerequisites](#prerequisites)
- [Prepare Environment](#prepare-environment)
- [Running the Pipeline](#running-the-pipeline)
- [Running the API](#running-the-api)
- [Repository Structure](#repository-structure)


## Prerequisites

- A working Python install. I recommend using [pyenv](https://formulae.brew.sh/formula/pyenv) to administrate your Python environments.
- [poetry](https://formulae.brew.sh/formula/poetry) to handle the project dependencies properly.

## Prepare Environment
The following section will give you the steps to configure an environment you need to create new data pipelines or run the tests locally.

1. Create and configure the project virtual environment:
```shell
cd tkww-de-take-home-test
poetry env use 3.10.3 # or the python 3.10 version you've got installed.
poetry install
```

2. Pre commit hooks installation (make sure the python venv is activated)
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
The pipeline will start executing, It will read the source file, proccess it accordingly and store it in the database.


## Running the API
To execute the API sucessfully, run the following commands in your terminal.
Remember to keep the terminal open while making requests, otherwise you won't be able to do so.

1. Make sure you are inside the project:
```shell
cd tkww-de-take-home-test
```
2. Run the following command:
```shell
python scripts/api.py
```
3. Make API calls:
Open the web browser of your preference and make requests as shown below:

Movies between two years:
```shell
http://localhost:4000/movies_between_years?start_year=2000&end_year=2010
```
Movies from a specific genre:
```shell
http://localhost:4000/movies_by_genre?genre=Action
```
Best rated director (in average):
```shell
http://localhost:4000/best_director
```
Movies from a specific director:
```shell
http://localhost:4000/movies_by_director?director=Peter Jackson
```

## Repository Structure
```
/tkww-de-take-home-test
│
├── /data                   # Folder where source data is stored.
├── /scripts                # PySpark and API scripts.
│   ├── /helpers            # Folder containing helper functions.
│   │   └── helpers.py      # Helper functions for the ETL process.
│   ├── etl.py              # Pyspark e2e pipeline main logic.
│   ├── api.py              # Flask REST API script.
├── /tests                  # Tests folder.
│   ├── /unit               # Folder containing unit tests.
│   │   └── /helpers        # Helpers unit tests.
├── tkww_movies_catalog.db  # SQLite database.
├── pyproject.toml          # Poetry dependencies.
└── README.md               # Instructions on how to run the project.
```
