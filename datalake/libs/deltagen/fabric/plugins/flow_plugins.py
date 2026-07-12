"""Flow calculation plugins for Delta-Gen.

Stage plugins for computing period-on-period flow using a self-join approach
instead of LAG window functions. This captures both "new" and "lost" rows,
where transfers between entities net to zero.

Usage:
    stages:
      - name: self_join_previous
        extensions:
          stage_plugin: self_join_previous_period
          period_column: source_period
          join_keys: [product, customer, region]
          fum_column: amount
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@register_stage(
    "self_join_previous_period",
    description="FULL OUTER JOIN each period to its previous period for flow calculation with lost-key detection",
    version="2.0.0",
    author="deltagen_helpers",
    tags={"flow", "netflow"},
)
def self_join_previous_period(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Self-join each period to its previous period using FULL OUTER JOIN.

    Replaces LAG-based flow calculation. The LAG approach misses "lost" rows
    (keys that existed in the previous period but not the current one).

    Approach:
    1. Collect sorted distinct periods from the DataFrame
    2. Build a period->prev_period mapping
    3. Create a "previous" copy, shifting each row's period forward by one step
    4. FULL OUTER JOIN the original to the shifted copy on (join_keys + period)
    5. Lost rows get amount=0, is_lost_key=True

    Extensions:
        period_column: Column identifying the period (required)
        join_keys: List of columns to join on (required)
        fum_column: Column containing amount values (optional, defaults to 'fum')
    """
    from pyspark.sql import functions as F, Row

    extensions = stage.extensions or {}
    period_col = extensions.get("period_column")
    join_keys = extensions.get("join_keys")
    fum_col = extensions.get("fum_column", "fum")

    if not period_col:
        ctx.log_error("self_join_previous_period requires 'period_column' in stage extensions")
        return df
    if not join_keys:
        ctx.log_error("self_join_previous_period requires 'join_keys' in stage extensions")
        return df

    periods = sorted([row[0] for row in df.select(period_col).distinct().collect()])

    if len(periods) < 2:
        ctx.log_info(f"self_join_previous_period: only {len(periods)} period(s), nothing to join")
        return (
            df.withColumn("last_month_fum", F.lit(None).cast("decimal"))
            .withColumn("is_lost_key", F.lit(False))
        )

    ctx.log_info(
        f"self_join_previous_period: {len(periods)} periods "
        f"({periods[0]} to {periods[-1]}), keys={join_keys}"
    )

    spark = df.sparkSession
    map_rows = [Row(prev_period=periods[i], next_period=periods[i + 1]) for i in range(len(periods) - 1)]
    period_map_df = spark.createDataFrame(map_rows)

    _SENTINEL = "__NULL__"
    for k in join_keys:
        df = df.withColumn(k, F.coalesce(F.col(k), F.lit(_SENTINEL)))

    prev_side = (
        df.select(*join_keys, period_col, F.col(fum_col).alias("last_month_fum"))
        .join(period_map_df, df[period_col] == period_map_df["prev_period"], how="inner")
        .drop("prev_period", period_col)
        .withColumnRenamed("next_period", period_col)
    )

    join_on = join_keys + [period_col]
    joined = df.join(prev_side, on=join_on, how="full_outer")

    for k in join_keys:
        joined = joined.withColumn(
            k, F.when(F.col(k) == _SENTINEL, F.lit(None)).otherwise(F.col(k))
        )

    joined = joined.withColumn(
        fum_col,
        F.coalesce(F.col(fum_col), F.lit(0).cast("decimal")),
    )
    joined = joined.withColumn(
        "is_lost_key",
        F.when(
            (F.col(fum_col) == 0) & F.col("last_month_fum").isNotNull(),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    ctx.log_info(f"self_join_previous_period: complete")

    return joined
