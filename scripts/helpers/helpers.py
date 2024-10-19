import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union, Callable, Generator

import pyspark.sql.types as T
import pyspark.sql.functions as F
from flask import Response, jsonify
from pyspark.sql import Column, Window, DataFrame, SparkSession
from pyspark.sql.functions import col, when, row_number, regexp_extract

# Main working paths
abs_path = Path(__file__).absolute()
working_dir_path = str(abs_path.parent.parent.parent)
data_path = f"{working_dir_path}/data/1.csv"
db_name = f"{working_dir_path}/tkww_movies_catalog.db"

INITIAL_SCHEMA = T.StructType(
    [
        T.StructField("_c0", T.StringType(), True),
        T.StructField("MOVIES", T.StringType(), True),
        T.StructField("YEAR", T.StringType(), True),
        T.StructField("GENRE", T.StringType(), True),
        T.StructField("RATING", T.StringType(), True),
        T.StructField("ONE-LINE", T.StringType(), True),
        T.StructField("STARS", T.StringType(), True),
        T.StructField("VOTES", T.StringType(), True),
        T.StructField("RunTime", T.StringType(), True),
        T.StructField("Gross", T.StringType(), True),
    ]
)

FINAL_SCHEMA = {
    "movies": T.StringType(),
    "year_from": T.IntegerType(),
    "year_to": T.IntegerType(),
    "genre": T.StringType(),
    "rating": T.FloatType(),
    "plot": T.StringType(),
    "stars": T.StringType(),
    "votes": T.IntegerType(),
    "runtime": T.IntegerType(),
    "gross": T.StringType(),
}

CREATE_TBL = """
CREATE TABLE IF NOT EXISTS movies (
    movies TEXT UNIQUE,
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
)"""

INSERT_STATEMENT = """
INSERT INTO movies (movies,
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
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(movies)
DO UPDATE SET 
    year_from = excluded.year_from,
    year_to = excluded.year_to,
    genre = excluded.genre,
    rating = excluded.rating,
    plot = excluded.plot,
    stars = excluded.stars,
    directors = excluded.directors,
    votes = excluded.votes,
    runtime = excluded.runtime,
    gross = excluded.gross;
"""


def setup_logger(name: str):
    msg_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    datetime_format = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=msg_format, datefmt=datetime_format)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def get_spark_session() -> SparkSession:
    """
    Creates and returns a Spark session configured for ETL operations.

    This function initializes a Spark session with the following configurations:
    - Uses only 4 CPU cores (master set to "local[4]")
    - Sets the default number of shuffle partitions to 4
    - Configures the session time zone to UTC

    Returns:
        SparkSession: A Spark session configured for data processing.
    """
    # master configuration to use only 4 CPU cores
    spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

    # basic configuration
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    return spark


def read_csv(
    spark_session: SparkSession,
    path: str,
    read_options: Dict[str, str],
    schema: T.StructType = INITIAL_SCHEMA,
) -> DataFrame:
    """
    Reads a CSV file/folder from the given path using the provided Spark session, schema, and read options.

    Args:
        spark_session (SparkSession): The Spark session used to read the file.
        path (str): The file path or directory containing the data to be read.
        schema (StructType): The schema to be applied to the DataFrame.
        read_options (Dict[str, str]): A dictionary of read options to configure how the file is read.

    Returns:
        DataFrame: A Spark DataFrame containing the loaded data.
    """
    df_reader = spark_session.read.schema(schema)

    # Add each read option to the DataFrame reader
    for option, value in read_options.items():
        df_reader = df_reader.option(option, value)

    # Read the CSV file into a DataFrame
    df = df_reader.csv(path)

    return df


def apply_column_transformations(
    df: DataFrame, 
    transformations: Dict[str, Callable[[Column], Column]], 
    renames: Dict[str, str] = None,
    casts: Dict[str, str] = None,
    filter_nulls: str = None
) -> DataFrame:
    """
    Apply transformations, renaming, casting, and filtering to a DataFrame.

    Parameters:
    df (DataFrame): The PySpark DataFrame to transform.
    transformations (Dict[str, Callable[[Column], Column]]): A dictionary where keys are column names, and values are the Spark functions to apply.
    renames (Dict[str, str], optional): A dictionary mapping old column names to new column names. Defaults to None.
    casts (Dict[str, str], optional): A dictionary mapping column names to target data types for casting. Defaults to None.
    filter_nulls (str, optional): A column name to filter out rows with null values. Defaults to None.

    Returns:
    DataFrame: The transformed DataFrame.
    """
    # Apply column renaming if renames are provided
    if renames:
        for old_col, new_col in renames.items():
            df = df.withColumnRenamed(old_col, new_col)

    # Apply column transformations
    for col_name, func in transformations.items():
        df = df.withColumn(col_name, func(df[col_name]))

    # Apply column casting if casts are provided
    if casts:
        for col_name, data_type in casts.items():
            df = df.withColumn(col_name, F.col(col_name).cast(data_type))

    # Filter out rows with null values in the specified column, if provided
    if filter_nulls:
        df = df.filter(F.col(filter_nulls).isNotNull())

    return df


def columns_to_lower(df: DataFrame) -> DataFrame:
    """
    Renames all columns in the given DataFrame to lowercase.

    This function iterates through each column in the DataFrame and
    renames it to its lowercase equivalent.

    Args:
        df (DataFrame): The input DataFrame whose columns are to be renamed.

    Returns:
        DataFrame: A new DataFrame with all column names in lowercase.
    """
    for col_name in df.columns:
        col_to_lower = col_name.lower()
        df = df.withColumnRenamed(col_name, col_to_lower)
    return df


def cast_col_types(df: DataFrame, col_datatypes: Dict[str, str]) -> DataFrame:
    """
    Casts the specified columns in the DataFrame to the given data types.

    This function iterates through a dictionary of column names and their
    desired data types, casting each column in the DataFrame to its
    corresponding data type.

    Args:
        df (DataFrame): The input DataFrame to modify.
        col_datatypes (Dict[str, str]): A dictionary where the keys are
            column names and the values are the desired data types as strings
            (e.g., 'integer', 'string', 'float').

    Returns:
        DataFrame: A new DataFrame with the specified columns cast to the
            desired data types.
    """
    for col_name, dtype in col_datatypes.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df


def deduplicate(
    df: DataFrame, partition_by: List[str], order_by: Dict[str, str]
) -> DataFrame:
    """
    Deduplicates a DataFrame based on specified partitioning and ordering.

    This function creates a window specification based on the provided
    columns to partition and order the DataFrame. It assigns a row number
    to each row within its partition and filters the DataFrame to retain
    only the first occurrence of each partition.

    Args:
        df (DataFrame): The input DataFrame to deduplicate.
        partition_by (List[str]): A list of column names to partition the DataFrame by.
        order_by (Dict[str, str]): A dictionary where the keys are column names
            to order by, and the values specify the order ('asc' or 'desc').

    Returns:
        DataFrame: A new DataFrame with duplicates removed based on the
            specified partitioning and ordering criteria.
    """
    window_func = Window.partitionBy(partition_by).orderBy(
        *[
            F.col(col).desc() if order == "desc" else F.col(col).asc()
            for col, order in order_by.items()
        ]
    )

    df = df.withColumn("wf_col", row_number().over(window_func))
    return df.filter(col("wf_col") == 1).drop("wf_col")


def normalize_year(df: DataFrame, year_col: str) -> DataFrame:
    """
    Normalizes the year information in the specified column of a DataFrame.

    This function extracts year information from a specified column using
    various regular expression patterns. It handles cases such as single
    years, open-ended ranges, and year ranges, and condenses the extracted
    information into 'year_from' and 'year_to' columns.

    Args:
        df (DataFrame): The input DataFrame containing the year information.
        year_col (str): The name of the column containing year data.

    Returns:
        DataFrame: A new DataFrame with 'year_from' and 'year_to' normalized
                    based on the patterns extracted from the specified column.
    """
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


def bad_request(message: str) -> Response:
    """
    Returns a JSON response with a 400 Bad Request status code.

    Args:
        message (str): The error message to return in the JSON response.

    Returns:
        Response: A Flask Response object containing the error message and a 400 status code.
    """
    response = jsonify({"error": message})
    response.status_code = 400
    return response


def query_db(
    query: str, args: Tuple = (), one: bool = False
) -> Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
    """
    Executes a query on the SQLite database and returns the results.

    Args:
        query (str): The SQL query to execute.
        args (Tuple, optional): The parameters to pass to the SQL query. Defaults to an empty tuple.
        one (bool, optional): If True, return only one result as a dictionary. Defaults to False.

    Returns:
        Union[Generator[Dict[str, Any], None, None], Dict[str, Any]]:
            If `one` is False, returns a generator of dictionaries (each representing a row in the result set).
            If `one` is True, returns a single dictionary representing one row.
    """
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()

    # If `one` is True, return a single row as a dictionary, else return a generator of dictionaries
    return (dict(row) for row in rv) if not one else dict(rv[0])
