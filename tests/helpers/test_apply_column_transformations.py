from pyspark.sql import functions as F

from scripts.helpers.helpers import apply_column_transformations


def test_transformation(spark_session):
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


def test_rename_columns(spark_session):
    # Create the input DataFrame for renaming
    input_df_rename = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Mary"},
            {"id": None, "name": "Alice"},  # Adding a row with a null value
        ]
    )
    
    # Test renaming columns
    renames = {"name": "full_name"}
    df_renamed = apply_column_transformations(input_df_rename, renames=renames)
    
    # Reorder expected DataFrame columns to match the transformed DataFrame
    expected_renamed_df = spark_session.createDataFrame(
        [
            {"id": 1, "full_name": "John"},
            {"id": 2, "full_name": "Mary"},
            {"id": None, "full_name": "Alice"},
        ]
    ).select("id", "full_name")  # Explicitly select the columns in the order you expect
    
    # Assert equality
    assert expected_renamed_df.collect() == df_renamed.collect()

def test_cast_columns(spark_session):
    # Create the input DataFrame for casting
    input_df_cast = spark_session.createDataFrame(
        [
            {"id": 1, "age_str": "25"},
            {"id": 2, "age_str": "30"},
        ]
    )
    
    casts = {"age_str": "integer"}
    df_casted = apply_column_transformations(input_df_cast, casts=casts)
    expected_casted_df = spark_session.createDataFrame(
        [
            {"id": 1, "age_str": 25},
            {"id": 2, "age_str": 30},
        ]
    )
    
    # Assert equality
    assert expected_casted_df.collect() == df_casted.collect()

def test_filter_nulls(spark_session):
    # Create the input DataFrame for filtering
    input_df_filter = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John"},
            {"id": 2, "name": None},  # Adding a null value
            {"id": 3, "name": "Mary"},
        ]
    )
    
    filter_nulls = "name"
    df_filtered = apply_column_transformations(input_df_filter, filter_nulls=filter_nulls)
    expected_filtered_df = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John"},
            {"id": 3, "name": "Mary"},
        ]
    )
    
    # Assert equality
    assert expected_filtered_df.collect() == df_filtered.collect()
