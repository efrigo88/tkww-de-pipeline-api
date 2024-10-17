import time
import logging
import sqlite3
from pathlib import Path

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from helpers.helpers import (
    FINAL_SCHEMA,
    INITIAL_SCHEMA,
    deduplicate,
    cast_col_types,
    normalize_year,
    columns_to_lower,
    get_spark_session,
    normalize_gross_value,
    parse_directors_and_stars,
    apply_column_transformations,
)

# Get the current working directory
abs_path = Path(__file__).absolute()
base_path = str(abs_path.parent.parent)

db_name = f"{base_path}/tkww_movies_catalog.db"

# Logging configurations.
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("pyspark_logger")
logger.setLevel(logging.INFO)


def read_file(path: str) -> DataFrame:
    df = (
        spark.read.option("multiLine", True)
        .option("header", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", True)
        .schema(INITIAL_SCHEMA)
        .csv(path)
    )
    return df


def transform(df: DataFrame):
    # Rename the first column as the id of the dataset and cast it.
    df = (
        df.withColumnRenamed("_c0", "id")
        .withColumn("id", F.col("id").cast(T.IntegerType()))
        .filter(F.col("id").isNotNull())
    )

    df = df.withColumnRenamed("ONE-LINE", "plot")

    # Remove any leading or trailing whitespaces.
    transformations = {
        "genre": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
        "plot": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
        "stars": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
    }

    # Main pipeline transformations.
    df = columns_to_lower(df)
    df = apply_column_transformations(df, transformations)
    df = deduplicate(df, partition_by=["movies"], order_by={"id": "desc"})
    df = normalize_year(df, "year")
    df = cast_col_types(df, FINAL_SCHEMA)
    df = parse_directors_and_stars(df, "stars")
    df = normalize_gross_value(df, "gross")

    return (
        df
        .select(
            "id",
            "movies",
            "year_from",
            "year_to",
            "genre",
            "rating",
            "plot",
            "stars",
            "directors",
            "votes",
            "runtime",
            "gross",
        )
    )


def write_df(df: DataFrame):
    # Conexión a SQLite
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER,
            movies TEXT,
            year_from INTEGER,
            year_to INTEGER,
            genre TEXT,
            rating REAL,
            plot TEXT,
            stars TEXT,
            directors TEXT,
            votes INTEGER,
            runtime TEXT,
            gross REAL
        )
    """
    )

    # Empty the table to avoid duplications
    cursor.execute("DELETE FROM movies")

    for row in df.collect():
        cursor.execute(
            """
            INSERT INTO movies (id,
                                movies,
                                year_from,
                                year_to,
                                genre,
                                rating,
                                plot,
                                stars,
                                directors,
                                votes,
                                runtime,
                                gross)
            VALUES (?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?)
        """,
            (
                row["id"],
                row["movies"],
                row["year_from"],
                row["year_to"],
                row["genre"],
                row["rating"],
                row["plot"],
                row["stars"],
                row["directors"],
                row["votes"],
                row["runtime"],
                row["gross"],
            ),
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f"Get SparkSession")
    spark = get_spark_session()

    # while True:
    #     # Procesa un archivo CSV
    #     process_file("/path/to/your/csv")

    #     # Espera 10 segundos
    #     time.sleep(10)

    # File to process
    file_path = f"{base_path}/data/1.csv"

    logger.info(f"Read file in path: {file_path}")
    df = read_file(file_path)

    logger.info(f"Transform DataFrame")
    df = transform(df)
    df.cache()  # Caching only for visualization purposes as it's not a big dataset.
    logger.info(f"{df.count()} rows processed")
    logger.info("DataFrame successfully processed")

    logger.info("DataFrame preview:")
    df.show()

    logger.info("Write DataFrame to db")
    write_df(df)
    logger.info("Pipeline finished successfully.")
