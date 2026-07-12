"""Data Quality plugins for Fabric environments.

Column Plugins:
    log_nulls_to_table    -- validate not-null and log violations to a Delta table
    log_invalid_to_table  -- validate in-set and log violations to a Delta table

Stage Plugins:
    check_unresolved_fks  -- detect fact rows where a FK resolved to a NO_* sentinel
                            and write both a summary and the individual records to DQ log tables.

Usage in YAML:
    columns:
      - name: customer_id
        type: string
        extensions:
          transform: log_nulls_to_table
          log_table: logging_dq_nulls
          on_null: reject

    stages:
      - name: check_unresolved_fks
        extensions:
          stage_plugin: check_unresolved_fks
          summary_table: ${defaults.dq.unresolved_fks_table}
          records_table: ${defaults.dq.unresolved_records_table}
          checks:
            - column: FK_ProductID
              sentinel: NO_PRODUCT
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_column, register_stage
from deltagen.plugins.metrics import MetricAction

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from deltagen.model.column import ColumnConfig
    from deltagen.plugins.context import PluginContext

logger = logging.getLogger(__name__)


@register_column(
    "log_nulls_to_table",
    description="Validate not null and log violations to a Delta table",
    version="1.0.0",
    author="deltagen_helpers",
    tags={"dq", "validation", "fabric", "logging"},
)
def log_nulls_to_table(
    df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Validate column for nulls and log violations to a Delta table."""
    from pyspark.sql import functions as F

    col_name = column.name
    extensions = column.extensions or {}
    log_table = extensions.get("log_table")
    action = extensions.get("on_null", "reject")
    fill_value = extensions.get("fill_value")
    sample_limit = extensions.get("sample_limit", 100)

    if not log_table:
        ctx.log_error(f"log_nulls_to_table requires 'log_table' for column {col_name}")
        return df

    null_rows = df.filter(F.col(col_name).isNull())
    null_count = null_rows.count()

    if null_count == 0:
        ctx.log_debug(f"Column {col_name}: no nulls found")
        return df

    ctx.log_info(f"Column {col_name}: {null_count} nulls, logging to {log_table}")

    _log_violations_to_table(
        spark=df.sparkSession,
        violations_df=null_rows,
        log_table=log_table,
        issue_type="null",
        column_name=col_name,
        rule_name="not_null",
        table_name=ctx.table_name,
        load_id=ctx.load_id,
        sample_limit=sample_limit,
    )

    ctx.metrics.record_nulls(
        column_name=col_name,
        count=null_count,
        action=MetricAction.REJECTED if action == "reject" else MetricAction.FLAGGED,
    )

    if action == "reject":
        return df.filter(F.col(col_name).isNotNull())
    elif action == "fill" and fill_value is not None:
        return df.fillna({col_name: fill_value})
    else:
        return df


@register_column(
    "log_invalid_to_table",
    description="Validate values in set and log violations to a Delta table",
    version="1.0.0",
    author="deltagen_helpers",
    tags={"dq", "validation", "fabric", "logging"},
)
def log_invalid_to_table(
    df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Validate column values against allowed set and log violations."""
    from pyspark.sql import functions as F

    col_name = column.name
    extensions = column.extensions or {}
    log_table = extensions.get("log_table")
    allowed_values = extensions.get("allowed_values", [])
    action = extensions.get("on_violation", "reject")
    fill_value = extensions.get("fill_value")
    sample_limit = extensions.get("sample_limit", 100)

    if not log_table:
        ctx.log_error(f"log_invalid_to_table requires 'log_table' for column {col_name}")
        return df

    if not allowed_values:
        ctx.log_error(f"log_invalid_to_table requires 'allowed_values' for column {col_name}")
        return df

    invalid_filter = ~F.col(col_name).isin(allowed_values) & F.col(col_name).isNotNull()
    invalid_rows = df.filter(invalid_filter)
    invalid_count = invalid_rows.count()

    if invalid_count == 0:
        ctx.log_debug(f"Column {col_name}: all values valid")
        return df

    ctx.log_info(f"Column {col_name}: {invalid_count} invalid values, logging to {log_table}")

    _log_violations_to_table(
        spark=df.sparkSession,
        violations_df=invalid_rows,
        log_table=log_table,
        issue_type="invalid_value",
        column_name=col_name,
        rule_name="in_set",
        table_name=ctx.table_name,
        load_id=ctx.load_id,
        sample_limit=sample_limit,
        extra_cols=[col_name],
    )

    ctx.metrics.record_validation_failure(
        rule_name="in_set",
        column_name=col_name,
        count=invalid_count,
        action=MetricAction.REJECTED if action == "reject" else MetricAction.FLAGGED,
    )

    if action == "reject":
        return df.filter(F.col(col_name).isin(allowed_values) | F.col(col_name).isNull())
    elif action == "fill" and fill_value is not None:
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isin(allowed_values) | F.col(col_name).isNull(),
                F.col(col_name),
            ).otherwise(F.lit(fill_value)),
        )
    else:
        return df


def _log_violations_to_table(
    spark,
    violations_df: "DataFrame",
    log_table: str,
    issue_type: str,
    column_name: str,
    rule_name: str,
    table_name: str,
    load_id: str | None,
    sample_limit: int = 100,
    extra_cols: list[str] | None = None,
) -> None:
    """Log DQ violations to a Delta table."""
    from pyspark.sql import functions as F

    now = datetime.now(timezone.utc)

    sample_df = violations_df.limit(sample_limit)

    log_df = sample_df.select(
        F.lit(table_name).alias("source_table"),
        F.lit(column_name).alias("column_name"),
        F.lit(issue_type).alias("issue_type"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(load_id).alias("load_id"),
        F.lit(now.isoformat()).alias("detected_at"),
        F.lit(now.year).alias("partition_year"),
        F.lit(now.month).alias("partition_month"),
        *([F.col(c).cast("string").alias(f"value_{c}") for c in (extra_cols or [])]),
    )

    try:
        log_df.write.format("delta").mode("append").saveAsTable(log_table)
        logger.debug(f"Logged {sample_df.count()} violations to {log_table}")
    except Exception as e:
        logger.error(f"Failed to log violations to {log_table}: {e}")


@register_stage(
    "check_unresolved_fks",
    description="Detect fact rows where a FK resolved to a NO_* sentinel and log to DQ tables",
    version="1.0.0",
    author="deltagen_helpers",
    tags={"dq", "dimension", "foreign-key", "sentinel"},
)
def check_unresolved_fks(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Check for fact rows where a foreign key resolved to a NO_* sentinel.

    This plugin is read-only -- it returns df unchanged so it can be
    placed anywhere in the stage pipeline.

    Extensions:
        summary_table: Target table for per-run summary rows (required)
        records_table: Target table for individual unresolved records (required)
        checks: List of {column, sentinel} dicts (required)
        sample_size: Max records to write per FK column (default: 100)
    """
    from pyspark.sql import functions as F

    extensions = stage.extensions or {}
    summary_table = extensions.get("summary_table")
    records_table = extensions.get("records_table")
    checks: list[dict] = extensions.get("checks", [])
    sample_size: int = extensions.get("sample_size", 100)

    if not summary_table or not records_table:
        ctx.log_error(
            "check_unresolved_fks requires 'summary_table' and 'records_table' "
            "in stage extensions"
        )
        return df

    if not checks:
        ctx.log_warning("check_unresolved_fks: no checks configured -- skipping")
        return df

    total_rows = df.count()
    if total_rows == 0:
        ctx.log_info("check_unresolved_fks: DataFrame is empty -- skipping")
        return df

    natural_keys = []
    if ctx.config and hasattr(ctx.config, "get_natural_key_columns"):
        natural_keys = [c.name for c in ctx.config.get_natural_key_columns()]
    data_cols = df.columns
    key_cols_present = [c for c in natural_keys if c in data_cols]

    natural_id_expr = (
        F.to_json(F.struct(*[F.col(c) for c in key_cols_present]))
        if key_cols_present
        else F.lit(None).cast("string")
    )

    now = datetime.now(timezone.utc)
    table_name = ctx.table_name
    run_id = ctx.run_id
    load_id = ctx.load_id

    for check in checks:
        fk_col = check.get("column")
        sentinel = check.get("sentinel")

        if not fk_col or not sentinel:
            continue
        if fk_col not in data_cols:
            continue

        unresolved = df.filter(F.col(fk_col) == sentinel)
        unresolved_count = unresolved.count()

        if unresolved_count == 0:
            continue

        pct = round((unresolved_count / total_rows) * 100, 2)
        ctx.log_warning(
            f"{fk_col}: {unresolved_count:,} unresolved rows "
            f"({pct}% of {total_rows:,}) -> '{sentinel}'"
        )

        ctx.metrics.record_validation_failure(
            rule_name="unresolved_fk",
            column_name=fk_col,
            count=unresolved_count,
            action=MetricAction.FLAGGED,
        )

        _write_unresolved_summary(
            spark=df.sparkSession,
            summary_table=summary_table,
            table_name=table_name,
            fk_column=fk_col,
            sentinel_value=sentinel,
            unresolved_count=unresolved_count,
            total_rows=total_rows,
            pct_unresolved=pct,
            run_id=run_id,
            load_id=load_id,
            now=now,
        )

        _write_unresolved_records(
            unresolved_df=unresolved,
            records_table=records_table,
            natural_id_expr=natural_id_expr,
            data_cols=data_cols,
            table_name=table_name,
            fk_column=fk_col,
            sentinel_value=sentinel,
            run_id=run_id,
            load_id=load_id,
            sample_size=sample_size,
        )

    return df


def _write_unresolved_summary(
    spark, summary_table, table_name, fk_column, sentinel_value,
    unresolved_count, total_rows, pct_unresolved, run_id, load_id, now,
) -> None:
    from pyspark.sql import functions as F

    summary_df = spark.range(1).select(
        F.lit(table_name).alias("_dq_table_name"),
        F.lit(fk_column).alias("_dq_fk_column"),
        F.lit(sentinel_value).alias("_dq_sentinel_value"),
        F.lit(unresolved_count).alias("_dq_unresolved_count"),
        F.lit(total_rows).alias("_dq_total_rows"),
        F.lit(pct_unresolved).alias("_dq_pct_unresolved"),
        F.lit(run_id).alias("_dq_run_id"),
        F.lit(load_id).alias("_dq_load_id"),
        F.lit(now.isoformat()).cast("timestamp").alias("_dq_checked_at"),
    )

    try:
        summary_df.write.format("delta").mode("append").saveAsTable(summary_table)
    except Exception as e:
        logger.error(f"Failed to write unresolved FK summary to {summary_table}: {e}")


def _write_unresolved_records(
    unresolved_df, records_table, natural_id_expr, data_cols,
    table_name, fk_column, sentinel_value, run_id, load_id, sample_size,
) -> None:
    from pyspark.sql import functions as F

    records_df = (
        unresolved_df.limit(sample_size)
        .withColumn("_dq_natural_id", natural_id_expr)
        .withColumn("_dq_record", F.to_json(F.struct(*[F.col(c) for c in data_cols])))
        .select("_dq_natural_id", "_dq_record")
        .withColumn("_dq_fk_column", F.lit(fk_column))
        .withColumn("_dq_sentinel_value", F.lit(sentinel_value))
        .withColumn("_dq_table_name", F.lit(table_name))
        .withColumn("_dq_run_id", F.lit(run_id))
        .withColumn("_dq_load_id", F.lit(load_id))
        .withColumn("_dq_detected_at", F.current_timestamp())
    )

    try:
        records_df.write.format("delta").mode("append").saveAsTable(records_table)
    except Exception as e:
        logger.error(f"Failed to write unresolved FK records to {records_table}: {e}")
