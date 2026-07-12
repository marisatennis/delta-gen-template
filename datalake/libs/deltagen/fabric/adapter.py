"""Fabric Metrics Adapter - Bridge between Delta-Gen metrics and Fabric storage.

This module provides the FabricMetricsAdapter class that handles:
1. Automatic persistence of metrics to Delta tables
2. Structured logging for notebook observability
3. Integration with Fabric monitoring patterns

Usage:
    from deltagen.fabric.adapter import FabricMetricsAdapter

    adapter = FabricMetricsAdapter(spark, schema="logging")
    collector = MetricsCollector(
        table_name="customer_dim",
        load_id="batch_001",
        on_complete=adapter.persist_metrics,
    )
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from deltagen.plugins.metrics import RunMetrics

logger = logging.getLogger(__name__)


@dataclass
class MetricsTableConfig:
    """Configuration for metrics table names and partitioning."""
    schema: str = "logging"
    prefix: str = "deltagen"
    partition_by_date: bool = True

    @property
    def run_table(self) -> str:
        return f"{self.schema}.{self.prefix}_run_metrics"

    @property
    def source_table(self) -> str:
        return f"{self.schema}.{self.prefix}_source_metrics"

    @property
    def stage_table(self) -> str:
        return f"{self.schema}.{self.prefix}_stage_metrics"

    @property
    def quality_table(self) -> str:
        return f"{self.schema}.{self.prefix}_quality_metrics"

    @property
    def write_table(self) -> str:
        return f"{self.schema}.{self.prefix}_write_metrics"

    @property
    def plugin_table(self) -> str:
        return f"{self.schema}.{self.prefix}_plugin_metrics"


class FabricMetricsAdapter:
    """Bridges Delta-Gen metrics to Fabric Delta tables."""

    def __init__(
        self,
        spark: "SparkSession",
        schema: str = "logging",
        prefix: str = "deltagen",
        auto_create_tables: bool = True,
        log_summary: bool = True,
    ):
        self._spark = spark
        self._config = MetricsTableConfig(schema=schema, prefix=prefix)
        self._auto_create = auto_create_tables
        self._log_summary = log_summary
        self._tables_verified: set[str] = set()

    def persist_metrics(self, metrics: "RunMetrics") -> None:
        """Persist RunMetrics to Delta tables."""
        try:
            print(f"[FabricMetricsAdapter] Persisting metrics for run_id={metrics.run_id}")
            self._write_run_metrics(metrics)
            self._write_source_metrics(metrics)
            self._write_stage_metrics(metrics)
            self._write_quality_metrics(metrics)
            self._write_write_metrics(metrics)
            self._write_plugin_metrics(metrics)

            if self._log_summary:
                self._log_metrics_summary(metrics)

            print(f"[FabricMetricsAdapter] All metrics persisted successfully")
        except Exception as e:
            logger.error(f"[{metrics.run_id}] Failed to persist metrics: {e}")
            raise

    def _get_partition_cols(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        return {"partition_year": now.year, "partition_month": now.month, "partition_day": now.day}

    def _get_table_schema(self, table_name: str, partition: bool) -> StructType:
        def fields(base_fields: list[StructField]) -> StructType:
            if partition and self._config.partition_by_date:
                base_fields.extend([
                    StructField("partition_year", IntegerType(), True),
                    StructField("partition_month", IntegerType(), True),
                    StructField("partition_day", IntegerType(), True),
                ])
            return StructType(base_fields)

        if table_name == self._config.run_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("environment", StringType(), True),
                StructField("full_load", BooleanType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("total_rows_read", LongType(), True),
                StructField("total_rows_written", LongType(), True),
                StructField("total_rows_rejected", LongType(), True),
                StructField("source_count", LongType(), True),
                StructField("stage_count", LongType(), True),
                StructField("quality_issue_count", LongType(), True),
                StructField("schema_change_count", LongType(), True),
            ])
        if table_name == self._config.source_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("source_name", StringType(), True),
                StructField("row_count", LongType(), True),
                StructField("columns_read", LongType(), True),
                StructField("bytes_read", LongType(), True),
                StructField("read_duration_ms", LongType(), True),
                StructField("timestamp", StringType(), True),
            ])
        if table_name == self._config.stage_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("stage_name", StringType(), True),
                StructField("input_row_count", LongType(), True),
                StructField("output_row_count", LongType(), True),
                StructField("rows_added", LongType(), True),
                StructField("rows_removed", LongType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("columns_added", StringType(), True),
                StructField("columns_removed", StringType(), True),
            ])
        if table_name == self._config.quality_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("environment", StringType(), True),
                StructField("issue_type", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("columns", StringType(), True),
                StructField("row_count", LongType(), True),
                StructField("action", StringType(), True),
                StructField("rule_name", StringType(), True),
                StructField("timestamp", StringType(), True),
            ])
        if table_name == self._config.write_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("target_table", StringType(), True),
                StructField("write_mode", StringType(), True),
                StructField("merge_strategy", StringType(), True),
                StructField("rows_inserted", LongType(), True),
                StructField("rows_updated", LongType(), True),
                StructField("rows_deleted", LongType(), True),
                StructField("rows_unchanged", LongType(), True),
                StructField("rows_expired", LongType(), True),
                StructField("total_rows_written", LongType(), True),
                StructField("total_rows_in_target_before", LongType(), True),
                StructField("total_rows_in_target_after", LongType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("bytes_written", LongType(), True),
                StructField("timestamp", StringType(), True),
            ])
        if table_name == self._config.plugin_table:
            return fields([
                StructField("run_id", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_id", StringType(), True),
                StructField("plugin_name", StringType(), True),
                StructField("plugin_type", StringType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("input_rows", LongType(), True),
                StructField("output_rows", LongType(), True),
                StructField("timestamp", StringType(), True),
            ])
        return fields([])

    def _append_rows(self, rows: list[dict[str, Any]], table_name: str, partition: bool = True) -> None:
        if not rows:
            return
        if partition and self._config.partition_by_date:
            partition_cols = self._get_partition_cols()
            rows = [{**row, **partition_cols} for row in rows]

        schema = self._get_table_schema(table_name, partition)
        _max_retries = 3
        for _attempt in range(_max_retries):
            df = self._spark.createDataFrame(rows, schema=schema)
            writer = df.write.format("delta").mode("append")
            if partition and self._config.partition_by_date:
                writer = writer.partitionBy("partition_year", "partition_month")
            try:
                writer.saveAsTable(table_name)
                return
            except Exception as e:
                if "DELTA_PROTOCOL_CHANGED" in str(e) and _attempt < _max_retries - 1:
                    time.sleep(2 ** _attempt)
                else:
                    raise

    def _write_run_metrics(self, metrics: "RunMetrics") -> None:
        self._append_rows([{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "environment": metrics.environment,
            "full_load": metrics.full_load, "start_time": metrics.start_time,
            "end_time": metrics.end_time, "duration_ms": metrics.duration_ms,
            "status": metrics.status, "error_message": metrics.error_message,
            "total_rows_read": metrics.total_rows_read,
            "total_rows_written": metrics.total_rows_written,
            "total_rows_rejected": metrics.total_rows_rejected,
            "source_count": len(metrics.source_reads),
            "stage_count": len(metrics.stages),
            "quality_issue_count": len(metrics.data_quality),
            "schema_change_count": len(metrics.schema_changes),
        }], self._config.run_table)

    def _write_source_metrics(self, metrics: "RunMetrics") -> None:
        rows = [{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "source_name": m.source_name,
            "row_count": m.row_count, "columns_read": m.columns_read,
            "bytes_read": m.bytes_read, "read_duration_ms": m.read_duration_ms,
            "timestamp": m.timestamp,
        } for m in metrics.source_reads]
        self._append_rows(rows, self._config.source_table)

    def _write_stage_metrics(self, metrics: "RunMetrics") -> None:
        rows = [{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "stage_name": m.stage_name,
            "input_row_count": m.input_row_count, "output_row_count": m.output_row_count,
            "rows_added": m.rows_added, "rows_removed": m.rows_removed,
            "duration_ms": m.duration_ms, "start_time": m.start_time, "end_time": m.end_time,
            "columns_added": ",".join(m.columns_added) if m.columns_added else None,
            "columns_removed": ",".join(m.columns_removed) if m.columns_removed else None,
        } for m in metrics.stages]
        self._append_rows(rows, self._config.stage_table)

    def _write_quality_metrics(self, metrics: "RunMetrics") -> None:
        rows = [{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "environment": metrics.environment,
            "issue_type": m.issue_type, "column_name": m.column_name,
            "columns": ",".join(m.columns) if m.columns else None,
            "row_count": m.row_count, "action": m.action,
            "rule_name": m.rule_name, "timestamp": m.timestamp,
        } for m in metrics.data_quality]
        self._append_rows(rows, self._config.quality_table)

    def _write_write_metrics(self, metrics: "RunMetrics") -> None:
        if not metrics.write:
            return
        w = metrics.write
        self._append_rows([{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "target_table": w.target_table,
            "write_mode": w.write_mode, "merge_strategy": w.merge_strategy,
            "rows_inserted": w.rows_inserted, "rows_updated": w.rows_updated,
            "rows_deleted": w.rows_deleted, "rows_unchanged": w.rows_unchanged,
            "rows_expired": w.rows_expired, "total_rows_written": w.total_rows_written,
            "total_rows_in_target_before": w.total_rows_in_target_before,
            "total_rows_in_target_after": w.total_rows_in_target_after,
            "duration_ms": w.duration_ms, "bytes_written": w.bytes_written,
            "timestamp": w.timestamp,
        }], self._config.write_table)

    def _write_plugin_metrics(self, metrics: "RunMetrics") -> None:
        if not metrics.plugins_executed:
            return
        rows = [{
            "run_id": metrics.run_id, "table_name": metrics.table_name,
            "load_id": metrics.load_id, "plugin_name": p.get("plugin_name"),
            "plugin_type": p.get("plugin_type"), "duration_ms": p.get("duration_ms"),
            "input_rows": p.get("input_rows"), "output_rows": p.get("output_rows"),
            "timestamp": p.get("timestamp"),
        } for p in metrics.plugins_executed]
        self._append_rows(rows, self._config.plugin_table)

    def _log_metrics_summary(self, metrics: "RunMetrics") -> None:
        if hasattr(metrics, "get_summary_table"):
            print(metrics.get_summary_table())
        else:
            print(f"\n{'=' * 60}")
            print(f"Run Complete: {metrics.table_name}")
            print(f"  Status: {metrics.status} | Duration: {metrics.duration_ms}ms")
            print(f"  Rows: read={metrics.total_rows_read:,}, written={metrics.total_rows_written:,}")
            print(f"{'=' * 60}\n")


def create_fabric_adapter(spark: "SparkSession", schema: str = "logging", prefix: str = "deltagen") -> FabricMetricsAdapter:
    """Create a FabricMetricsAdapter instance."""
    return FabricMetricsAdapter(spark, schema=schema, prefix=prefix)
