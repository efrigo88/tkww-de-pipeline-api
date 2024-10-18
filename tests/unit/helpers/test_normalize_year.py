from scripts.helpers.helpers import normalize_year


def test_normalize_year(spark_session):
    # Create the input DataFrame with different year formats
    input_df = spark_session.createDataFrame(
        [
            {"year_data": "(2020)"},  # Single year
            {"year_data": "(2020– )"},  # Open-ended range
            {"year_data": "(2015–2020)"},  # Year range
            {"year_data": "(2020 TV Special)"},  # Year with additional text
        ]
    )

    # Expected DataFrame
    expected_df = spark_session.createDataFrame(
        [
            {"year_data": "(2020)", "year_from": "2020", "year_to": "2020"},
            {"year_data": "(2020– )", "year_from": "2020", "year_to": "2020"},
            {"year_data": "(2015–2020)", "year_from": "2015", "year_to": "2020"},
            {"year_data": "(2020 TV Special)", "year_from": "2020", "year_to": "2020"},
        ]
    )

    # Apply the normalization function
    df_normalized = normalize_year(input_df, "year_data")

    # Collect results and assert they match the expected DataFrame
    assert df_normalized.collect() == expected_df.collect()
