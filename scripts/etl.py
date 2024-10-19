import sys
import sqlite3

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from helpers.helpers import (
    CREATE_TBL,
    FINAL_SCHEMA,
    INSERT_STATEMENT,
    db_name,
    read_csv,
    data_path,
    deduplicate,
    setup_logger,
    normalize_year,
    columns_to_lower,
    get_spark_session,
    normalize_gross_value,
    parse_directors_and_stars,
    apply_column_transformations,
)

# Logging configurations.
logger = setup_logger("pyspark_logger")


class Pipeline:
    def __init__(self, data_path: str, db_path: str, read_data_config: str):
        self.data_path = data_path
        self.db_name = db_path
        self.read_config = read_data_config
        self.spark = get_spark_session()

    def read_source(self) -> DataFrame:
        df = read_csv(self.spark, self.data_path, self.read_config)
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        # Define the transformations, renames, and casts
        rename_cols = {"_c0": "id", "ONE-LINE": "plot"}
        transformations = {
            "genre": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
            "plot": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
            "stars": lambda col: F.trim(F.regexp_replace(col, "\n", "")),
        }
        cast_id = {"id": T.IntegerType()}

        # Main pipeline transformations.
        df = apply_column_transformations(
            df,
            transformations=transformations,
            renames=rename_cols,
            casts=cast_id,
            filter_nulls="id",
        )
        df = columns_to_lower(df)
        df = deduplicate(df, partition_by=["movies"], order_by={"id": "desc"})
        df = normalize_year(df, "year")
        df = apply_column_transformations(df, casts=FINAL_SCHEMA)
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

    def write_df(self, df: DataFrame, batch_size: int = 100):
        # Conexión a SQLite
        conn = sqlite3.connect(self.db_name)
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

    def execute(self):
        logger.info(f"Data path: {data_path}.")
        try:
            logger.info("Read CSV data.")
            df = self.read_source()
        except Exception as e:
            logger.info(f"There is no data to process or It's incorrect: {e}.")
            sys.exit(1)

        logger.info(f"Transform DataFrame.")
        try:
            df = self.transform(df)
        except Exception as e:
            logger.info(
                f"Problem found during pipeline transformation process. Details: {e}."
            )
            sys.exit(1)

        df.cache()  # Caching as it's not a big dataset.
        logger.info(f"{df.count()} rows processed.")

        # logger.info("DataFrame preview:")
        # df.show()

        logger.info("Write DataFrame to db.")
        try:
            self.write_df(df)
        except Exception as e:
            logger.info(f"Problem found when writing results to DB. Details: {e}.")
            sys.exit(1)


if __name__ == "__main__":
    logger.info(f"Pipeline started executing.")

    read_options = {
        "multiLine": "true",
        "header": "false",
        "quote": '"',
        "escape": '"',
        "ignoreLeadingWhiteSpace": "true",
    }

    pipeline = Pipeline(
        data_path=data_path, db_path=db_name, read_data_config=read_options
    )

    pipeline.execute()

    logger.info("Pipeline finished successfully.")
