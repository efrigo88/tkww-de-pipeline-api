from scripts.helpers.helpers import normalize_gross_value


def test_normalize_gross_value(spark_session):
    input_df = spark_session.createDataFrame(
        [
            {"gross": "$1.5M"},
            {"gross": "$2.3M"},
            {"gross": "$5M"},
            {"gross": "1.75M"},
            {"gross": "$ 3.0 M"},  # Space included
            {"gross": None},  # None case
        ]
    )

    expected_df = spark_session.createDataFrame(
        [
            {"gross": 1_500_000.0},
            {"gross": 2_300_000.0},
            {"gross": 5_000_000.0},
            {"gross": 1_750_000.0},
            {"gross": 3_000_000.0},
            {"gross": None},  # Ensure None remains None
        ]
    )

    df_normalized = normalize_gross_value(input_df, "gross")

    assert df_normalized.collect() == expected_df.collect()
