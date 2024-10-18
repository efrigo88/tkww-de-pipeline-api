from pyspark.sql import functions as F

from scripts.helpers.helpers import apply_column_transformations


def test_apply_column_transformations(spark_session):
    # Create the input DataFrame
    input_df = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Mary"},
        ]
    )
    # Create the expected DataFrame
    expected_df = spark_session.createDataFrame(
        [
            {"id": 11, "name": "JOHN"},
            {"id": 12, "name": "MARY"},
        ]
    )

    transformations = {"name": F.upper, "id": lambda col: col + 10}

    # Apply the transformations
    df_transformed = apply_column_transformations(input_df, transformations)

    # Assert expected transformation results
    assert expected_df.collect() == df_transformed.collect()
