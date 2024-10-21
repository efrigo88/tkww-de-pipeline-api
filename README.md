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

1. Create, activate and configure a simple project virtual environment. (make sure python 3.10 is already installed in your system):

```shell
cd tkww-de-take-home-test
python -m venv .venv
source .venv/bin/activate
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
Remember that will run every 10 seconds, so do not close the terminal, you can press (Ctrl+C) to exit it at any time.


## Running the API
To execute the API sucessfully, run the following commands in your terminal (you must run the commands in another terminal if you want to keep the pipeline going).
Remember to keep the terminal open while making requests, otherwise you won't be able to do so. Press (Ctrl+C) to exit it at any time.

1. Make sure you are inside the project:
```shell
cd tkww-de-take-home-test
```
2. Run the following command:
```shell
python scripts/api.py
```
3. Make API calls:
You can view the full API documentation and try out the endpoints interactively via Swagger. 
To access Swagger, open the following URL in your web browser:
```shell
http://localhost:4000/apidocs/
```

#### Example API requests:

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
│   ├── /helpers            # Helpers unit tests folder.
├── tkww_movies_catalog.db  # SQLite database.
├── pyproject.toml          # Poetry dependencies.
└── README.md               # Instructions on how to run the project.
```
