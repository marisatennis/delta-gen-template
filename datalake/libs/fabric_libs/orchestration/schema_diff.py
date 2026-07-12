"""Schema diff helpers for Delta-Gen table configs."""
from __future__ import annotations

from typing import Any

from deltagen.model import TableConfig


def _normalize_type(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace(" ", "").lower()


def _expected_schema(table: TableConfig) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for col in table.get_persistent_columns():
        typespec = col.get_typespec()
        expected[col.name] = {
            "data_type": typespec.to_spark_sql() if typespec else col.data_type,
            "nullable": col.nullable,
        }
    return expected


def diff_table_schema(spark, table: TableConfig) -> dict[str, Any]:
    """Compare expected Delta-Gen schema with the current Spark table schema."""
    target_table = table.get_target_table_name()
    exists = spark.catalog.tableExists(target_table)

    if not exists:
        return {
            "target_table": target_table,
            "exists": False,
            "added": list(_expected_schema(table).keys()),
            "removed": [],
            "type_changes": {},
            "nullability_changes": {},
        }

    actual_schema = {
        field.name: {
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in spark.table(target_table).schema.fields
    }
    expected_schema = _expected_schema(table)

    expected_cols = set(expected_schema.keys())
    actual_cols = set(actual_schema.keys())

    added = sorted(expected_cols - actual_cols)
    removed = sorted(actual_cols - expected_cols)

    type_changes: dict[str, dict[str, Any]] = {}
    nullability_changes: dict[str, dict[str, Any]] = {}

    for col in sorted(expected_cols & actual_cols):
        exp_type = _normalize_type(expected_schema[col]["data_type"])
        act_type = _normalize_type(actual_schema[col]["data_type"])
        if exp_type and act_type and exp_type != act_type:
            type_changes[col] = {
                "expected": expected_schema[col]["data_type"],
                "actual": actual_schema[col]["data_type"],
            }
        if expected_schema[col]["nullable"] != actual_schema[col]["nullable"]:
            nullability_changes[col] = {
                "expected": expected_schema[col]["nullable"],
                "actual": actual_schema[col]["nullable"],
            }

    return {
        "target_table": target_table,
        "exists": True,
        "added": added,
        "removed": removed,
        "type_changes": type_changes,
        "nullability_changes": nullability_changes,
    }
