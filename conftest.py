import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:

    # master configuration to use only 4 CPU cores
    spark = SparkSession.builder.master("local[4]").getOrCreate()

    # basic configuration to use only a reasonable number of partitions
    spark.conf.set("spark.sql.shuffle.partition", 4)

    # configuration to work in UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Yield the SparkSession object to provide it to test functions
    yield spark

    # After all tests, stop the SparkSession
    spark.stop()
