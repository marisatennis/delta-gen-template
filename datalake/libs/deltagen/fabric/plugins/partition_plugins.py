"""Composite partition replacement plugin for Delta-Gen.

Stage plugin for replacing data by multiple partition columns (e.g. source_system
+ period). Extends the built-in period_replace to support composite keys.

Usage:
    stages:
      - name: partition_load
        extensions:
          stage_plugin: composite_period_replace
          period_column: DateNaturalID
          partition_columns: [source_system]
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from deltagen.model import StageConfig
    from deltagen.plugins.context import PluginContext


@register_stage(
    "composite_period_replace",
    description=(
        "Period replacement with additional partition columns. "
        "Deletes rows per partition value + its periods, then inserts."
    ),
)
def composite_period_replace(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Identify per-partition period sets for targeted replacement."""
    extensions = stage.extensions or {}
    period_column = extensions.get("period_column")
    target_period_column = extensions.get("target_period_column", period_column)
    partition_columns = extensions.get("partition_columns", [])
    target_table = extensions.get("target_table", "target")

    if not period_column:
        ctx.log_error("composite_period_replace requires 'period_column' in stage extensions")
        return df

    if not partition_columns:
        ctx.log_error("composite_period_replace requires 'partition_columns' in stage extensions")
        return df

    from pyspark.sql import functions as F

    group_cols = partition_columns
    grouped = (
        df.select([period_column] + group_cols)
        .distinct()
        .groupBy(group_cols)
        .agg(F.collect_set(period_column).alias("_periods"))
    )

    partition_period_sets = []
    for row in grouped.collect():
        entry = {col: row[col] for col in group_cols}
        entry["periods"] = sorted(row["_periods"])
        partition_period_sets.append(entry)

    ctx.log_info(f"Composite period replacement: {len(partition_period_sets)} partition group(s)")
    for entry in partition_period_sets:
        partition_desc = ", ".join(f"{col}={entry[col]}" for col in group_cols)
        ctx.log_info(f"  {partition_desc}: {len(entry['periods'])} period(s)")

    ctx.set_state("period_column", period_column)
    ctx.set_state("target_period_column", target_period_column)
    ctx.set_state("partition_columns", partition_columns)
    ctx.set_state("partition_period_sets", partition_period_sets)
    all_periods = []
    for entry in partition_period_sets:
        all_periods.extend(entry["periods"])
    ctx.set_state("periods_to_replace", list(set(all_periods)))

    row_count = df.count()
    ctx.log_info(f"Total records for composite replacement: {row_count}")

    ctx.metrics.record_source_read(
        source_name=f"composite_period_replace_{target_table}",
        row_count=row_count,
    )

    return df
