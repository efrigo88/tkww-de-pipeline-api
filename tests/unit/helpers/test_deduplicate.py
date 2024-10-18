from scripts.helpers.helpers import deduplicate


def test_deduplicate(spark_session):
    # Create the input DataFrame
    input_df = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John", "age": 30, "timestamp": "2019-01-01 00:00:00"},
            {"id": 1, "name": "John", "age": 31, "timestamp": "2019-01-01 00:00:01"},
            {"id": 2, "name": "Mary", "age": 28, "timestamp": "2019-01-01 00:00:00"},
            {"id": 2, "name": "Mary", "age": 29, "timestamp": "2019-01-01 00:00:01"},
            {"id": 2, "name": "Mary", "age": 29, "timestamp": "2019-01-01 00:00:02"},
            {"id": 3, "name": "Peter", "age": 32, "timestamp": "2019-01-01 00:00:00"},
            {"id": 3, "name": "Peter", "age": 33, "timestamp": "2019-01-01 00:00:01"},
            {"id": 3, "name": "Peter", "age": 34, "timestamp": "2019-01-01 00:00:02"},
            {"id": 3, "name": "Peter", "age": 35, "timestamp": "2019-01-01 00:00:03"},
            {"id": 4, "name": "Peter", "age": 1, "timestamp": "2019-01-01 00:00:01"},
            {"id": 4, "name": "Peter", "age": 2, "timestamp": "2019-01-01 00:00:00"},
            {"id": 5, "name": "Peter", "age": 1, "timestamp": "2019-01-01 00:00:00"},
            {"id": 6, "name": "John", "age": 31, "timestamp": "2019-01-01 00:00:01"},
        ]
    )

    # Expected DataFrame
    expected_df = spark_session.createDataFrame(
        [
            {"id": 1, "name": "John", "age": 31, "timestamp": "2019-01-01 00:00:01"},
            {"id": 2, "name": "Mary", "age": 29, "timestamp": "2019-01-01 00:00:02"},
            {"id": 3, "name": "Peter", "age": 35, "timestamp": "2019-01-01 00:00:03"},
            {"id": 4, "name": "Peter", "age": 1, "timestamp": "2019-01-01 00:00:01"},
            {"id": 5, "name": "Peter", "age": 1, "timestamp": "2019-01-01 00:00:00"},
            {"id": 6, "name": "John", "age": 31, "timestamp": "2019-01-01 00:00:01"},
        ]
    )

    actual_df = deduplicate(
        input_df, partition_by=["id"], order_by={"timestamp": "desc"}
    )

    assert actual_df.collect() == expected_df.collect()
