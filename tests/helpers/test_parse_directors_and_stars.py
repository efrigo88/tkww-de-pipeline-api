from pyspark.sql import types as T

from scripts.helpers.helpers import parse_directors_and_stars


def test_parse_directors_and_stars(spark_session):
    input_df = spark_session.createDataFrame(
        [
            {"stars": "Director: John Doe | Stars: Jane Doe, Bob Smith"},
            {"stars": "Directors: Alice Cooper, Eve Adams | Stars: Tom Hanks"},
            {
                "stars": "Director: Sarah Johnson | Stars: Chris Evans, Robert Downey Jr."
            },
            {"stars": "Stars: Emma Watson, Daniel Radcliffe"},  # No director
            {"stars": "Director: Steven Spielberg"},  # No stars
        ]
    )

    # Expected DataFrame
    expected_df = spark_session.createDataFrame(
        [
            {"directors": '[" John Doe "]', "stars": '[" Jane Doe"," Bob Smith"]'},
            {"directors": '[" Alice Cooper"," Eve Adams "]', "stars": '[" Tom Hanks"]'},
            {
                "directors": '[" Sarah Johnson "]',
                "stars": '[" Chris Evans"," Robert Downey Jr."]',
            },
            {"stars": '[" Emma Watson"," Daniel Radcliffe"]'},
            {"directors": '[" Steven Spielberg"]'},
        ]
    )

    # Apply the parsing function
    df_parsed = parse_directors_and_stars(input_df, "stars")

    # Collect results and assert equality
    assert (
        df_parsed.select("stars", "directors").collect()
        == expected_df.select("stars", "directors").collect()
    )
