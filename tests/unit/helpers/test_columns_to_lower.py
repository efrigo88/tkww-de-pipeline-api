from pyspark.sql import functions as F

from scripts.helpers.helpers import columns_to_lower


def test_columns_to_lower(spark_session):
    input_df = spark_session.createDataFrame(
        [
            {"TEST1": 1},
            {"TEST2": 2},
        ]
    )
    expected_df = spark_session.createDataFrame(
        [
            {"test1": 1},
            {"test2": 2},
        ]
    )

    df_transformed = columns_to_lower(input_df)

    assert expected_df.collect() == df_transformed.collect()
