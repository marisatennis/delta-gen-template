"""FK re-resolution scheduling plugin for Fabric.

Runs as a final stage in dimension pipelines and schedules re-processing
of fact tables that still carry NO_* FK sentinels for this dimension.

How it works:
1. Scans all fact YAML files in inputs_path to discover active fact tables.
2. For each fact table, finds any FK_* column containing the sentinel value
   within a rolling lookback window (default: 90 days).
3. If unresolved rows are found, writes a watermark override to
   log.watermark_overrides so the fact pipeline re-processes those records.

Table: log.watermark_overrides
  pipeline_name     STRING
  watermark_override DATE
  unresolved_count  LONG
  dim_table         STRING
  fk_column         STRING
  created_at        TIMESTAMP
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from deltagen.model.stage import StageConfig
    from deltagen.plugins.context import PluginContext

logger = logging.getLogger(__name__)

OVERRIDES_TABLE_DEFAULT = "log.watermark_overrides"
DEFAULT_LOOKBACK_DAYS = 90


@register_stage(
    "schedule_fk_reresolution",
    description="Schedule fact re-processing when dimension refreshes may resolve previously unresolved FKs",
    version="2.0.0",
    author="deltagen_helpers",
    tags={"dq", "dimension", "foreign-key", "reresolution"},
)
def schedule_fk_reresolution(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Schedule fact FK re-resolution after a dimension refresh.

    Extensions:
        sentinel:        Sentinel value to search for (e.g. "NO_PRODUCT")
        inputs_path:     Path to scan for fact YAML files
        overrides_table: Delta table for watermark overrides (default: "log.watermark_overrides")
        lookback_days:   Only check rows within this many days (default: 90)
    """
    extensions = stage.extensions or {}
    sentinel = extensions.get("sentinel")
    inputs_path = extensions.get("inputs_path")
    overrides_table = extensions.get("overrides_table", OVERRIDES_TABLE_DEFAULT)
    lookback_days = int(extensions.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    if not sentinel:
        ctx.log_warning("schedule_fk_reresolution: 'sentinel' not configured -- skipping")
        return df
    if not inputs_path:
        ctx.log_warning("schedule_fk_reresolution: 'inputs_path' not configured -- skipping")
        return df

    spark = df.sparkSession
    dim_table_name = ctx.table_name

    fact_yamls = _discover_fact_yamls(inputs_path, ctx)
    if not fact_yamls:
        ctx.log_info(f"schedule_fk_reresolution: no active fact YAMLs found in {inputs_path}")
        return df

    ctx.log_info(
        f"schedule_fk_reresolution: checking {len(fact_yamls)} fact tables "
        f"for sentinel '{sentinel}' (lookback: {lookback_days} days)"
    )

    for fact_info in fact_yamls:
        try:
            _process_fact_table(
                spark=spark,
                fact_info=fact_info,
                sentinel=sentinel,
                lookback_days=lookback_days,
                overrides_table=overrides_table,
                dim_table_name=dim_table_name,
                ctx=ctx,
            )
        except Exception as e:
            ctx.log_warning(
                f"schedule_fk_reresolution: error processing {fact_info.get('name', '?')}: {e}"
            )

    return df


def _discover_fact_yamls(inputs_path: str, ctx: "PluginContext") -> list[dict]:
    import yaml

    fact_yamls = []
    try:
        files = os.listdir(inputs_path)
    except Exception as e:
        ctx.log_warning(f"schedule_fk_reresolution: cannot list {inputs_path}: {e}")
        return []

    for filename in sorted(files):
        if not filename.endswith(".yaml"):
            continue
        filepath = os.path.join(inputs_path, filename)
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                continue
            name = data.get("name")
            if not name:
                continue
            policies = data.get("policies") or {}
            orchestration = policies.get("orchestration") or {}
            if orchestration.get("active") is False:
                continue
            target_schema = data.get("target_schema", "fact")
            incremental = data.get("incremental") or {}
            watermark_column = incremental.get("watermark_column", "DateNaturalID")
            fact_yamls.append({
                "name": name,
                "fact_table": f"{target_schema}.{name}",
                "watermark_column": watermark_column,
            })
        except Exception as e:
            ctx.log_warning(f"schedule_fk_reresolution: could not parse {filename}: {e}")

    return fact_yamls


def _process_fact_table(spark, fact_info, sentinel, lookback_days, overrides_table, dim_table_name, ctx):
    from datetime import date
    from pyspark.sql import functions as F

    pipeline_name = fact_info["name"]
    fact_table = fact_info["fact_table"]
    date_col = fact_info["watermark_column"]

    try:
        if not spark.catalog.tableExists(fact_table):
            return
    except Exception:
        return

    try:
        schema = spark.table(fact_table).schema
    except Exception:
        return

    fk_columns = [f.name for f in schema.fields if f.name.startswith("FK_")]
    if not fk_columns:
        return

    cutoff = date.today() - timedelta(days=lookback_days)
    fact_df = spark.table(fact_table).filter(F.col(date_col) >= F.lit(cutoff))

    min_date = None
    matched_fk = None

    for fk_col in fk_columns:
        row = (
            fact_df.filter(F.col(fk_col) == sentinel)
            .agg(F.min(date_col).alias("min_date"))
            .collect()[0]
        )
        if row["min_date"] is not None:
            if min_date is None or row["min_date"] < min_date:
                min_date = row["min_date"]
                matched_fk = fk_col

    if min_date is None:
        return

    if isinstance(min_date, date):
        override_watermark = min_date - timedelta(days=1)
    else:
        try:
            parsed = datetime.strptime(str(min_date)[:10], "%Y-%m-%d").date()
            override_watermark = parsed - timedelta(days=1)
        except Exception:
            return

    unresolved_count = int(fact_df.filter(F.col(matched_fk) == sentinel).count())

    ctx.log_info(
        f"schedule_fk_reresolution: scheduling re-resolution for {pipeline_name} "
        f"from {override_watermark} ({unresolved_count} '{sentinel}' rows)"
    )

    _write_watermark_override(
        spark=spark,
        overrides_table=overrides_table,
        target_pipeline=pipeline_name,
        override_watermark=override_watermark,
        unresolved_count=unresolved_count,
        dim_table=dim_table_name,
        fk_column=matched_fk,
    )


def _write_watermark_override(spark, overrides_table, target_pipeline, override_watermark, unresolved_count, dim_table, fk_column):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType

    override_schema = StructType([
        StructField("pipeline_name", StringType(), False),
        StructField("watermark_override", DateType(), False),
        StructField("unresolved_count", LongType(), False),
        StructField("dim_table", StringType(), False),
        StructField("fk_column", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ])

    now = datetime.now(timezone.utc)

    try:
        if not spark.catalog.tableExists(overrides_table):
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {overrides_table} (
                    pipeline_name STRING NOT NULL,
                    watermark_override DATE NOT NULL,
                    unresolved_count BIGINT NOT NULL,
                    dim_table STRING NOT NULL,
                    fk_column STRING NOT NULL,
                    created_at TIMESTAMP NOT NULL
                ) USING DELTA
            """)

        existing = (
            spark.table(overrides_table)
            .filter(F.col("pipeline_name") == target_pipeline)
            .select(F.min("watermark_override").alias("existing_min"))
            .collect()[0]
        )
        existing_min = existing["existing_min"]

        if existing_min is not None and existing_min <= override_watermark:
            return

        spark.sql(f"DELETE FROM {overrides_table} WHERE pipeline_name = '{target_pipeline}'")

        override_df = spark.createDataFrame(
            [(target_pipeline, override_watermark, unresolved_count, dim_table, fk_column, now)],
            schema=override_schema,
        )
        override_df.write.format("delta").mode("append").saveAsTable(overrides_table)

    except Exception as e:
        logger.error(f"schedule_fk_reresolution: failed to write watermark override for {target_pipeline}: {e}")
