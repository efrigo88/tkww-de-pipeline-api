from pyspark.sql import types as T

from scripts.helpers.helpers import cast_col_types


def test_cast_col_types(spark_session):
    initial_df = spark_session.createDataFrame(
        [
            {"id": "1", "name": "John", "age": "30.5"},
            {"id": "2", "name": "Mary", "age": "25.0"},
        ]
    )

    col_datatypes = {
        "id": T.IntegerType(),
        "name": T.StringType(),
        "age": T.FloatType(),
    }

    df_cast = cast_col_types(initial_df, col_datatypes)

    expected_schema = T.StructType(
        [
            T.StructField("age", T.FloatType(), True),
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
        ]
    )

    # Assert that the schema matches the expected types
    assert df_cast.schema == expected_schema

    # Collect results to verify the values
    results = df_cast.collect()
    assert results[0]["id"] == 1
    assert results[0]["age"] == 30.5
    assert results[1]["id"] == 2
    assert results[1]["age"] == 25.0
