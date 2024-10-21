import sys
import time
import sqlite3

import schedule
import helpers.helpers as H
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# Logging configurations.
logger = H.setup_logger("pyspark_logger")


class Pipeline:
    def __init__(
        self,
        data_path: str,
        db_path: str,
        read_data_config: str,
        write_batch_size: int = 100,
    ):
        self.data_path = data_path
        self.db_name = db_path
        self.read_config = read_data_config
        self.batch_size = write_batch_size
        self.spark = H.get_spark_session()

    def read_source(self) -> DataFrame:
        df = H.read_csv(self.spark, self.data_path, self.read_config)
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
        df = H.apply_column_transformations(
            df,
            transformations=transformations,
            renames=rename_cols,
            casts=cast_id,
            filter_nulls="id",
        )
        df = H.columns_to_lower(df)
        df = H.deduplicate(df, partition_by=["movies"], order_by={"id": "desc"})
        df = H.normalize_year(df, "year")
        df = H.apply_column_transformations(df, casts=H.FINAL_SCHEMA)
        df = H.parse_directors_and_stars(df, "stars")
        df = H.normalize_gross_value(df, "gross")

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

    def write_df(self, df: DataFrame):
        # Connection to SQLite
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute(H.CREATE_TBL)

        try:
            H.write_to_db(df, conn, cursor, self.batch_size)
        except Exception as e:
            conn.rollback()
            logger.info(f"Error while inserting/updating data: {e}")
            sys.exit(1)
        finally:
            conn.close()

    def execute(self):
        try:
            logger.info(f"Data path: {H.data_path}.")
            logger.info("Read CSV data.")
            df = self.read_source()

            logger.info(f"Transform DataFrame.")
            df = self.transform(df)

            df.cache()  # Caching as it's not a big dataset.
            # logger.info(f"{df.count()} rows processed.")

            # logger.info("DataFrame preview:")
            # df.show()

            logger.info("Write DataFrame to db.")
            self.write_df(df)
        except Exception as e:
            logger.info(f"Poblem found when executing the pipeline. Details: {e}.")
            sys.exit(1)


# Function to run the pipeline every x seconds
def run_pipeline():
    logger.info(f"Pipeline started executing.")
    read_options = {
        "multiLine": "true",
        "header": "false",
        "quote": '"',
        "escape": '"',
        "ignoreLeadingWhiteSpace": "true",
    }
    pipeline = Pipeline(
        data_path=H.data_path, db_path=H.db_name, read_data_config=read_options
    )
    pipeline.execute()
    logger.info("Pipeline finished successfully.")
    logger.info(
        f"Waiting for {H.WAIT_TIME} seconds. Press (Ctrl+C) to exit the program."
    )


if __name__ == "__main__":
    # Schedule the pipeline to run every x seconds
    schedule.every(H.WAIT_TIME).seconds.do(run_pipeline)

    try:
        # Keep the script running and checking for scheduled tasks
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        # Handle keyboard interruption (Ctrl+C) gracefully.
        logger.info("Pipeline execution interrupted. Exiting gracefully.")
        sys.exit(0)
