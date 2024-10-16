import json
import logging
from typing import List

from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType, StructField


def schemas_are_equal(
    schema_left: StructType, schema_right: StructType, ignore_nullable: bool = False
) -> bool:
    if len(schema_left) != len(schema_right):
        return False

    def is_field_equal(left: StructField, right: StructField) -> bool:
        if ignore_nullable:
            return (left.name, left.dataType) == (right.name, right.dataType)
        else:
            return (left.name, left.dataType, left.nullable) == (
                right.name,
                right.dataType,
                right.nullable,
            )

    return all(is_field_equal(a, b) for a, b in zip(schema_left, schema_right))


def data_frames_are_equal(
    df_left: DataFrame,
    df_right: DataFrame,
    ignore_nullable: bool = False,
    ignore_fields: List[str] = None,
) -> bool:
    if ignore_fields:
        df_left_to_compare = df_left.drop(*ignore_fields)
        df_right_to_compare = df_right.drop(*ignore_fields)

        if len(df_left_to_compare.columns) == 0:
            raise ValueError("All fields in Left dataset are excluded for assertion.")
        if len(df_right_to_compare.columns) == 0:
            raise ValueError("All fields in Right dataset are excluded for assertion.")
    else:
        df_left_to_compare = df_left
        df_right_to_compare = df_right

    if not schemas_are_equal(
        df_left_to_compare.schema, df_right_to_compare.schema, ignore_nullable
    ):
        logging.error(
            _schema_diff_error_message(
                df_left_to_compare.schema, df_right_to_compare.schema
            )
        )
        return False

    schema = df_left_to_compare.schema
    content_left = (
        df_left_to_compare.repartition(1).orderBy(schema.fieldNames()).collect()
    )
    content_right = (
        df_right_to_compare.repartition(1).orderBy(schema.fieldNames()).collect()
    )
    if content_left != content_right:
        logging.error(_data_diff_error_message(content_left, content_right))
        return False

    return True


def assert_columns_are_equal(
    df1: DataFrame, df2: DataFrame, columns: List[str]
) -> None:
    """
    Asserts that the selected columns in two DataFrames are equal.

    Args:
        df1 (DataFrame): First DataFrame.
        df2 (DataFrame): Second DataFrame.
        columns (List[str]): List of column names to compare.

    Raises:
        AssertionError: If the selected columns are not equal.

    Returns:
        None
    """
    for column in columns:
        assert (
            df1.select(column).collect() == df2.select(column).collect()
        ), f"Assertion error in column '{column}'"


def trim_string(s: str) -> str:
    return json.dumps(json.loads(s), separators=(",", ":"))


def _schema_diff_error_message(
    schema_left: StructType, schema_right: StructType
) -> str:
    left_schema_length = len(schema_left)
    right_schema_length = len(schema_right)

    smaller_schema = None
    bigger_schema = None
    if left_schema_length > right_schema_length:
        smaller_schema = schema_right
        bigger_schema = schema_left
        return "Right dataset has less fields: " + _schema_get_first_diff_field(
            smaller_schema, bigger_schema
        )

    elif left_schema_length < right_schema_length:
        smaller_schema = schema_left
        bigger_schema = schema_right
        return "Left dataset has less fields: " + _schema_get_first_diff_field(
            smaller_schema, bigger_schema
        )

    else:
        return "Schema mismatch: " + _schema_get_first_diff_field(
            schema_left, schema_right
        )


def _schema_get_first_diff_field(smaller_schema: StructType, bigger_schema: StructType):
    for idx, item in enumerate(bigger_schema):
        element_to_check = None
        try:
            element_to_check = smaller_schema[idx]
        except IndexError:
            return f"Field {item.name} is missing."

        # at this point we know the lengths are the same, and the convention is
        # that bigger_schema corresponds to right dataset.
        if item.name != element_to_check.name:
            return (
                f"Left field name '{element_to_check.name}'. "
                f"Right name '{item.name}'."
            )
        if item.dataType != element_to_check.dataType:
            return (
                f"Left type for field '{element_to_check.name}' is {element_to_check.dataType}. "
                f"Right type is {item.dataType}."
            )

        if item.nullable != element_to_check.nullable:
            return (
                f"Left type for field '{element_to_check.name}' nullability is {element_to_check.nullable}. "
                f"Right type nullability is {item.nullable}."
            )


def _data_diff_error_message(d_left: List[Row], d_right: List[Row]) -> str:
    l_left = len(d_left)
    l_right = len(d_right)

    if l_left > l_right:
        return f"Left data: {l_left} records, right data: {l_right} records."
    elif l_left < l_right:
        return f"Left data: {l_left} records, right data: {l_right} records."
    else:
        pass

    for idx in range(min(l_left, l_right)):
        left_row = d_left[idx]
        right_row = d_right[idx]
        left_row = _freeze_elements(left_row.asDict(recursive=True).items())
        right_row = _freeze_elements(right_row.asDict(recursive=True).items())
        diff = left_row ^ right_row
        if len(diff) > 0:
            left_non_matching_values = [d for d in diff if d in left_row]
            right_non_matching_values = [d for d in diff if d in right_row]
            details = (
                f"left values: {left_non_matching_values}; "
                f"right values: {right_non_matching_values}"
            )
            return f"Line {idx} has non-matching elements. {details}."

    return "Unknown diff case :( This must be a bug in utils"


def _freeze_elements(items):
    frozen = set()
    for k, v in items:
        if isinstance(v, list) or isinstance(v, dict):
            frozen.add((k, json.dumps(v)))
        else:
            frozen.add((k, v))
    return frozen
