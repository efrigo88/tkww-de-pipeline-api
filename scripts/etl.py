import sys
import logging
import sqlite3

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from helpers.helpers import (
    CREATE_TBL,
    FINAL_SCHEMA,
    INITIAL_SCHEMA,
    INSERT_STATEMENT,
    db_name,
    data_path,
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

    return df.select(
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


def write_df(df: DataFrame, batch_size: int = 100):
    # Conexión a SQLite
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute(CREATE_TBL)

    # Collect rows in batches
    batch = []
    try:
        for row in df.toLocalIterator():
            batch.append(
                (
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
                )
            )

            # When the batch is full, insert the rows
            if len(batch) >= batch_size:
                cursor.executemany(INSERT_STATEMENT, batch)
                conn.commit()
                batch.clear()

        # Insert any remaining rows in the batch
        if batch:
            cursor.executemany(INSERT_STATEMENT, batch)
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.info(f"Error while inserting/updating data: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    logger.info(f"Get SparkSession")
    spark = get_spark_session()

    logger.info(f"Data path: {data_path}.")
    try:
        df = read_file(data_path)
    except Exception as e:
        logger.info(f"There is no data to process or is incorrect: {e}.")
        sys.exit(1)

    logger.info(f"Transform DataFrame.")
    try:
        df = transform(df)
    except Exception as e:
        logger.info(f"Problem found during pipeline execution. Details: {e}.")
        sys.exit(1)

    df.cache()  # Caching only for visualization purposes as it's not a big dataset.
    logger.info(f"{df.count()} rows processed.")
    logger.info("DataFrame successfully processed.")

    # logger.info("DataFrame preview:")
    # df.show()

    logger.info("Write DataFrame to db.")
    try:
        write_df(df)
    except Exception as e:
        logger.info(f"Problem found when writing results to DB. Details: {e}.")
        sys.exit(1)

    logger.info("Pipeline finished successfully.")
