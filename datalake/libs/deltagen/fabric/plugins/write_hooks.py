"""Post-write hooks for Fabric Delta tables.

These hooks integrate with Delta-Gen's DeltaWriter to provide
automatic logging of write operations to tracking tables.

Usage:
    from deltagen.runner import DeltaWriter
    from deltagen.fabric.plugins.write_hooks import create_write_logging_hook

    post_write_hook = create_write_logging_hook(spark, schema="logging")
    writer = DeltaWriter(post_write_hook=post_write_hook)
    writer.write(df, target_table, mode="append")
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from deltagen.runner.writer import WriteResult

logger = logging.getLogger(__name__)


def create_write_logging_hook(
    spark: "SparkSession",
    schema: str = "logging",
    table_name: str = "lakehouse_tableloadlog",
) -> Callable[["WriteResult"], None]:
    """Create a post-write hook that logs to a tracking table."""
    full_table_name = f"{schema}_{table_name}"

    def hook(result: "WriteResult") -> None:
        log_write_to_table(spark, result, full_table_name)

    return hook


def log_write_to_table(
    spark: "SparkSession",
    result: "WriteResult",
    log_table: str,
) -> None:
    """Log a write operation result to a tracking table."""
    now = datetime.now(timezone.utc)

    log_record = {
        "table_name": result.target_table,
        "write_mode": result.mode,
        "rows_affected": result.rows_affected or 0,
        "rows_inserted": getattr(result, "rows_inserted", None),
        "rows_updated": getattr(result, "rows_updated", None),
        "rows_deleted": getattr(result, "rows_deleted", None),
        "merge_strategy": getattr(result, "strategy", None),
        "success": result.success,
        "error_message": result.error_message,
        "load_timestamp": now.isoformat(),
        "load_id": getattr(result, "load_id", None),
        "partition_year": now.year,
        "partition_month": now.month,
    }

    try:
        df = spark.createDataFrame([log_record])
        df.write.format("delta").mode("append").saveAsTable(log_table)
        logger.info(f"Logged write to {log_table}: {result.target_table}")
    except Exception as e:
        logger.error(f"Failed to log write to {log_table}: {e}")


def create_table_load_log_hook(
    spark: "SparkSession",
    schema: str = "logging",
    run_id: str | None = None,
) -> Callable[["WriteResult"], None]:
    """Create a hook that logs to lakehouse_tableloadlog format."""
    table_name = f"{schema}_lakehouse_tableloadlog"

    def hook(result: "WriteResult") -> None:
        now = datetime.now(timezone.utc)
        record = {
            "TableName": result.target_table,
            "RecordCount": result.rows_affected or 0,
            "LastLoadTimestamp": now.isoformat(),
            "LoadID": run_id or "unknown",
            "PartitionYear": now.year,
            "PartitionMonth": now.month,
        }
        try:
            df = spark.createDataFrame([record])
            df.write.format("delta").mode("append").saveAsTable(table_name)
        except Exception as e:
            logger.error(f"Failed to log to {table_name}: {e}")

    return hook
