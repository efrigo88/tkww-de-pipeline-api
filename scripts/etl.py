import time
import logging
import sqlite3

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from scripts.helpers.helpers import (
    deduplicate,
    cast_col_types,
    normalize_year,
    columns_to_lower,
    get_spark_session,
    normalize_gross_value,
    parse_directors_and_stars,
    apply_column_transformations,
)

# Logging configurations.
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("pyspark_logger")
logger.setLevel(logging.INFO)

FILE_PATH = "/Users/emif/Documents/tkww-de-take-home-test/data/1.csv"

# Schema for the selected columns.
SCHEMA = {
    "movies": T.StringType(),
    "year_from": T.IntegerType(),
    "year_to": T.IntegerType(),
    "genre": T.StringType(),
    "rating": T.FloatType(),
    "one-line": T.StringType(),
    "stars": T.StringType(),
    "votes": T.IntegerType(),
    "runtime": T.IntegerType(),
    "gross": T.StringType(),
}


def read_file(path: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", True)
        .csv(path)
    )
    return df


def transform(df: DataFrame):
    # Rename the first column as the id of the dataset and cast it.
    df = df.withColumnRenamed("_c0", "id").withColumn(
        "id", F.col("id").cast(T.IntegerType())
    )

    # Transformations to remove any leading or trailing whitespaces.
    transformations = {
        "genre": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
        "one-line": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
        "stars": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
    }

    # Main pipeline transformations.
    df = columns_to_lower(df)
    df = apply_column_transformations(df, transformations)
    df = deduplicate(df, partition_by=["movies"], order_by={"id": "desc"})
    df = normalize_year(df, "year")
    df = cast_col_types(df, SCHEMA)
    df = parse_directors_and_stars(df, "stars")
    df = normalize_gross_value(df, "gross")

    df = df.select(
        "id",
        "movies",
        "year_from",
        "year_to",
        "genre",
        "rating",
        "one-line",
        "stars",
        "directors",
        "votes",
        "runtime",
        "gross",
    )
    return df


def write_df(df: DataFrame):
    # Conexión a SQLite
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # Inserta los datos en la base de datos
    for row in df.collect():
        cursor.execute(
            """
            INSERT INTO movies (movie, year, genre, rating, description, stars, votes, runtime, gross)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                row["movie"],
                row["year"],
                row["genre"],
                row["rating"],
                row["description"],
                row["stars"],
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
    logger.info(f"Read file in path: {FILE_PATH}")
    df = read_file(FILE_PATH)

    logger.info(f"Transform DataFrame")
    df = transform(df)
    logger.info("DataFrame successfully processed")

    logger.info("DataFrame first rows:")
    df.show(100, truncate=False)
    # write_df(df)
