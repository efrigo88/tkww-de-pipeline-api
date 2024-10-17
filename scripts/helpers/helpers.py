from typing import Dict, List

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame, SparkSession
from pyspark.sql.functions import col, when, row_number, regexp_extract


def get_spark_session() -> SparkSession:
    # master configuration to use only 4 CPU cores
    spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
    # basic configuration
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def apply_column_transformations(df: DataFrame, transformations: Dict) -> DataFrame:
    """
    Apply transformations to a DataFrame based on the provided dictionary of transformations.

    Parameters:
    df: The PySpark DataFrame to transform.
    transformations: A dictionary where keys are column names, and values are the Spark functions to apply.

    Returns:
    Transformed DataFrame.
    """
    for col_name, func in transformations.items():
        df = df.withColumn(col_name, func(df[col_name]))
    return df


def columns_to_lower(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        col_to_lower = col_name.lower()
        df = df.withColumnRenamed(col_name, col_to_lower)
    return df


def cast_col_types(df: DataFrame, col_datatypes: Dict) -> DataFrame:
    for col_name, dtype in col_datatypes.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df


def deduplicate(df: DataFrame, partition_by: List, order_by: Dict) -> DataFrame:
    window_func = Window.partitionBy(partition_by).orderBy(
        *[
            F.col(col).desc() if order == "desc" else F.col(col).asc()
            for col, order in order_by.items()
        ]
    )
    df = df.withColumn("wf_col", row_number().over(window_func))
    return df.filter(col("wf_col") == 1).drop("wf_col")


def normalize_year(df: DataFrame, year_col: str) -> DataFrame:
    # Regular expressions for different patterns
    single_year_pattern = r"\((\d{4})\)"  # Matches (YYYY)
    year_start_pattern = r"\((\d{4})–\s*\)"  # Matches open-ended ranges (YYYY– )
    year_range_pattern = r"\((\d{4})–(\d{4})\)"  # Matches (YYYY–YYYY)
    year_with_text_pattern = r"\((\d{4})\s+[^)]*\)"  # Matches cases (2020 TV Special)

    # Create aux columns based on regex patterns
    df = (
        df.withColumn(
            "year_single", regexp_extract(col(year_col), single_year_pattern, 1)
        )
        .withColumn(
            "year_from_start", regexp_extract(col(year_col), year_start_pattern, 1)
        )
        .withColumn(
            "year_with_text_pattern",
            regexp_extract(col(year_col), year_with_text_pattern, 1),
        )
        # Extract year_from and year_to from year ranges
        .withColumn("year_from", regexp_extract(col(year_col), year_range_pattern, 1))
        .withColumn("year_to", regexp_extract(col(year_col), year_range_pattern, 2))
    )

    # Condense all auxiliary columns into a single 'normalized_year' column
    df = df.withColumn(
        "normalized_year",
        when(col("year_single") != "", col("year_single"))
        .when(col("year_from_start") != "", col("year_from_start"))
        .when(col("year_with_text_pattern") != "", col("year_with_text_pattern"))
        .when(col("year_from") != "", col("year_from"))
        .when(col("year_to") != "", col("year_to"))
        .otherwise(None),
    )

    # Fill year_from and year_to with normalized_year
    df = df.withColumn(
        "year_from",
        when((col("year_from") == ""), col("normalized_year")).otherwise(
            col("year_from")
        ),
    ).withColumn(
        "year_to",
        when((col("year_to") == ""), col("normalized_year")).otherwise(col("year_to")),
    )

    # Drop aux columns and return the final df
    return df.drop(
        "year_single", "year_from_start", "year_with_text_pattern", "normalized_year"
    )


def parse_directors_and_stars(df: DataFrame, stars_col: str) -> DataFrame:
    """
    Extracts directors and stars from the provided column and creates two new columns in JSON format.

    Args:
        df (DataFrame): The input DataFrame containing the stars column.
        stars_col (str): The name of the column containing stars and directors data.

    Returns:
        DataFrame: The DataFrame with two new columns for directors and stars in JSON format.
    """
    # Regular expressions to extract directors and stars
    director_pattern = r"(?:Directors?|Director):([^|]+)"
    stars_pattern = r"Stars?:([^|]+)"

    # Extract directors and stars
    df = df.withColumn(
        "directors", F.regexp_extract(F.col(stars_col), director_pattern, 1)
    ).withColumn("stars", F.regexp_extract(F.col(stars_col), stars_pattern, 1))

    # Convert the comma-separated strings to lists and then to JSON format
    df = df.withColumn(
        "directors",
        F.when(
            F.col("directors") != "",
            F.to_json(F.array_distinct(F.split(F.col("directors"), ","))),
        ).otherwise(F.lit(None)),
    ).withColumn(
        "stars",
        F.when(
            F.col("stars") != "",
            F.to_json(F.array_distinct(F.split(F.col("stars"), ","))),
        ).otherwise(F.lit(None)),
    )
    return df


def normalize_gross_value(df: DataFrame, gross_col: str):
    """
    Standardizes the gross column values in a PySpark DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        gross_col (str): The name of the gross column to be formatted.

    Returns:
        DataFrame: The DataFrame with the formatted gross column.
    """
    # Remove '$' and 'M', and convert to float
    df = df.withColumn(
        gross_col, F.regexp_replace(col(gross_col), r"\$", "")  # Remove dollar sign
    )
    df = df.withColumn(
        gross_col, F.regexp_replace(col(gross_col), r"M", "")  # Remove 'M'
    )
    df = df.withColumn(
        gross_col, F.regexp_replace(col(gross_col), r"\s+", "")  # Remove any whitespace
    )

    # Convert to float and multiply by 1,000,000
    df = df.withColumn(gross_col, col(gross_col).cast("float") * 1_000_000)

    return df
