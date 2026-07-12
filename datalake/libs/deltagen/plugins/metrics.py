"""Metrics and observability for Delta-Gen pipelines.

This module provides structured metrics collection for data pipeline observability.
It tracks row counts, timing, data quality issues, and schema changes at each
stage of the transformation pipeline.

Best Practices Implemented:
- Structured metrics (JSON-serializable for log aggregation)
- Row-level accounting (reads, writes, updates, deletes, rejects)
- Stage-level timing and row counts
- Data quality metrics (nulls, duplicates, validation failures)
- Schema drift tracking
- Correlation IDs for distributed tracing
- Both streaming updates and final summaries

Example usage:
    from deltagen.plugins.metrics import MetricsCollector, RunMetrics

    # Create a metrics collector for this run
    metrics = MetricsCollector(
        run_id="run_20240115_001",
        table_name="customer_dim",
        load_id="daily_batch_42"
    )

    # Record source reads
    metrics.record_source_read("raw_customers", row_count=10000)

    # Record stage processing
    with metrics.stage_context("transform"):
        df = transform(df)
        metrics.record_stage_output("transform", row_count=df.count())

    # Record data quality issues
    metrics.record_nulls("customer_id", count=5, action="rejected")
    metrics.record_duplicates(["customer_id"], count=12, action="kept_latest")

    # Record write results
    metrics.record_write(
        inserted=8500,
        updated=1200,
        deleted=50,
        unchanged=200
    )

    # Get summary
    summary = metrics.get_summary()
    print(summary.to_json())
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Iterator

logger = logging.getLogger(__name__)


class MetricAction(str, Enum):
    """Actions taken on data quality issues."""

    REJECTED = "rejected"  # Rows removed from pipeline
    KEPT_FIRST = "kept_first"  # Kept first occurrence (dedupe)
    KEPT_LATEST = "kept_latest"  # Kept latest occurrence (dedupe)
    FILLED_DEFAULT = "filled_default"  # Null replaced with default
    FLAGGED = "flagged"  # Marked but kept in pipeline
    LOGGED = "logged"  # Only logged, no action


class SchemaChangeType(str, Enum):
    """Types of schema changes detected."""

    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    COLUMN_NULLABLE_CHANGED = "column_nullable_changed"


# -----------------------------------------------------------------------------
# Metric Data Classes
# -----------------------------------------------------------------------------


@dataclass
class SourceReadMetric:
    """Metrics for reading from a source."""

    source_name: str
    row_count: int
    columns_read: int | None = None
    bytes_read: int | None = None
    read_duration_ms: int | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class StageMetric:
    """Metrics for a transformation stage."""

    stage_name: str
    input_row_count: int | None = None
    output_row_count: int | None = None
    rows_added: int = 0
    rows_removed: int = 0
    duration_ms: int | None = None
    start_time: str | None = None
    end_time: str | None = None
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)


@dataclass
class DataQualityMetric:
    """Metrics for data quality issues."""

    issue_type: str  # "null", "duplicate", "validation_failed", "type_mismatch"
    column_name: str | None  # None for row-level issues like duplicates
    columns: list[str] | None = None  # For multi-column issues (composite key dupes)
    row_count: int = 0
    action: str = MetricAction.LOGGED.value
    rule_name: str | None = None  # e.g., "not_null", "in_set", "regex"
    sample_values: list[Any] | None = None  # Optional sample of bad values
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class SchemaChangeMetric:
    """Metrics for schema drift detection."""

    change_type: str
    column_name: str
    old_value: str | None = None  # Old type/nullable status
    new_value: str | None = None  # New type/nullable status
    action: str = "applied"  # "applied", "ignored", "failed"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class WriteMetric:
    """Metrics for write operations."""

    target_table: str
    write_mode: str  # "append", "merge", "overwrite"
    merge_strategy: str | None = None  # "update_all", "scd_type2", etc.

    # Row counts
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_unchanged: int = 0  # For merge: matched but no changes
    rows_expired: int = 0  # For SCD2: expired records

    # Totals
    total_rows_written: int = 0
    total_rows_in_target_before: int | None = None
    total_rows_in_target_after: int | None = None

    # Performance
    duration_ms: int | None = None
    bytes_written: int | None = None

    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class RunMetrics:
    """Complete metrics for a pipeline run."""

    # Identification
    run_id: str
    table_name: str
    load_id: str | None = None
    environment: str | None = None

    # Timing
    start_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    end_time: str | None = None
    duration_ms: int | None = None

    # Status
    status: str = "running"  # "running", "completed", "failed"
    error_message: str | None = None
    full_load: bool = False  # True = full load, False = incremental

    # Aggregated counts
    total_rows_read: int = 0
    total_rows_written: int = 0
    total_rows_rejected: int = 0

    # Detailed metrics
    source_reads: list[SourceReadMetric] = field(default_factory=list)
    stages: list[StageMetric] = field(default_factory=list)
    data_quality: list[DataQualityMetric] = field(default_factory=list)
    schema_changes: list[SchemaChangeMetric] = field(default_factory=list)
    write: WriteMetric | None = None

    # Plugin executions
    plugins_executed: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def get_summary_table(self) -> str:
        """Generate a human-readable summary table."""
        lines = [
            "=" * 60,
            f"Run Summary: {self.table_name}",
            "=" * 60,
            f"Run ID:      {self.run_id}",
            f"Load ID:     {self.load_id or 'N/A'}",
            f"Status:      {self.status}",
            f"Duration:    {self.duration_ms or 0}ms",
            "",
            "Row Counts:",
            f"  Read:      {self.total_rows_read:,}",
            f"  Written:   {self.total_rows_written:,}",
            f"  Rejected:  {self.total_rows_rejected:,}",
        ]

        if self.write:
            lines.extend([
                "",
                "Write Details:",
                f"  Mode:      {self.write.write_mode}",
                f"  Inserted:  {self.write.rows_inserted:,}",
                f"  Updated:   {self.write.rows_updated:,}",
                f"  Deleted:   {self.write.rows_deleted:,}",
                f"  Unchanged: {self.write.rows_unchanged:,}",
            ])
            if self.write.rows_expired > 0:
                lines.append(f"  Expired:   {self.write.rows_expired:,}")

        if self.data_quality:
            lines.extend(["", "Data Quality Issues:"])
            # Group by issue type
            by_type: dict[str, int] = {}
            for dq in self.data_quality:
                key = f"{dq.issue_type} ({dq.action})"
                by_type[key] = by_type.get(key, 0) + dq.row_count
            for issue, count in by_type.items():
                lines.append(f"  {issue}: {count:,} rows")

        if self.schema_changes:
            lines.extend(["", "Schema Changes:"])
            for sc in self.schema_changes:
                lines.append(f"  {sc.change_type}: {sc.column_name}")

        if self.stages:
            lines.extend(["", "Stage Timing:"])
            for stage in self.stages:
                duration = f"{stage.duration_ms}ms" if stage.duration_ms else "N/A"
                in_count = stage.input_row_count or "?"
                out_count = stage.output_row_count or "?"
                lines.append(f"  {stage.stage_name}: {in_count} -> {out_count} ({duration})")

        lines.append("=" * 60)
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Metrics Collector
# -----------------------------------------------------------------------------


class MetricsCollector:
    """Collects and aggregates metrics during pipeline execution.

    This is the primary interface for recording metrics. It provides
    convenient methods for common operations and manages the lifecycle
    of a run's metrics.

    Thread Safety:
        This class is NOT thread-safe. Use one collector per pipeline run.
        If you accidentally share a collector across multiple threads, metrics
        data may become corrupted. Always create a new collector for each
        pipeline run and do not share instances across concurrent operations.

    Callbacks:
        The on_complete callback enables automatic persistence of metrics
        when a run completes. This is the recommended pattern for integrating
        with platform-specific storage (e.g., writing to Delta tables in Fabric).

        Example:
            def persist_to_fabric(metrics: RunMetrics) -> None:
                adapter.write_metrics(metrics)

            collector = MetricsCollector(
                table_name="customer_dim",
                on_complete=persist_to_fabric,
            )
    """

    def __init__(
        self,
        table_name: str,
        run_id: str | None = None,
        load_id: str | None = None,
        environment: str | None = None,
        auto_log: bool = True,
        on_complete: "Callable[[RunMetrics], None] | None" = None,
        full_load: bool = False,
    ):
        """Initialize a metrics collector.

        Args:
            table_name: Name of the table being processed
            run_id: Unique identifier for this run (auto-generated if not provided)
            load_id: Business identifier for the load batch
            environment: Environment name (dev, test, prod)
            auto_log: If True, automatically log metrics at key points
            on_complete: Optional callback invoked when run completes or fails.
                         Receives the final RunMetrics object. Use this to
                         automatically persist metrics to storage.
            full_load: If True, indicates a full load run (not incremental)
        """
        self._metrics = RunMetrics(
            run_id=run_id or f"run_{uuid.uuid4().hex[:12]}",
            table_name=table_name,
            load_id=load_id,
            environment=environment,
            full_load=full_load,
        )
        self._auto_log = auto_log
        self._on_complete = on_complete
        self._stage_timers: dict[str, float] = {}
        self._start_time = time.perf_counter()
        self._completed = False

    @property
    def run_id(self) -> str:
        """Get the run ID."""
        return self._metrics.run_id

    @property
    def metrics(self) -> RunMetrics:
        """Get the current metrics (read-only snapshot)."""
        return self._metrics

    # -------------------------------------------------------------------------
    # Source Reading
    # -------------------------------------------------------------------------

    def record_source_read(
        self,
        source_name: str,
        row_count: int,
        columns_read: int | None = None,
        bytes_read: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        """Record metrics for reading from a source.

        Args:
            source_name: Name of the source being read
            row_count: Number of rows read
            columns_read: Number of columns read
            bytes_read: Bytes read (if available)
            duration_ms: Time to read in milliseconds
        """
        metric = SourceReadMetric(
            source_name=source_name,
            row_count=row_count,
            columns_read=columns_read,
            bytes_read=bytes_read,
            read_duration_ms=duration_ms,
        )
        self._metrics.source_reads.append(metric)
        self._metrics.total_rows_read += row_count

        if self._auto_log:
            logger.info(
                f"[{self._metrics.run_id}] Read {row_count:,} rows from {source_name}"
            )

    # -------------------------------------------------------------------------
    # Stage Processing
    # -------------------------------------------------------------------------

    def start_stage(self, stage_name: str, input_row_count: int | None = None) -> None:
        """Mark the start of a stage.

        Args:
            stage_name: Name of the stage
            input_row_count: Number of input rows (if known)
        """
        self._stage_timers[stage_name] = time.perf_counter()

        # Find or create stage metric
        stage = self._find_or_create_stage(stage_name)
        stage.start_time = datetime.now(timezone.utc).isoformat()
        stage.input_row_count = input_row_count

        if self._auto_log:
            logger.debug(f"[{self._metrics.run_id}] Starting stage: {stage_name}")

    def end_stage(
        self,
        stage_name: str,
        output_row_count: int | None = None,
        columns_added: list[str] | None = None,
        columns_removed: list[str] | None = None,
    ) -> None:
        """Mark the end of a stage.

        Args:
            stage_name: Name of the stage
            output_row_count: Number of output rows
            columns_added: List of columns added in this stage
            columns_removed: List of columns removed in this stage
        """
        stage = self._find_or_create_stage(stage_name)
        stage.end_time = datetime.now(timezone.utc).isoformat()
        stage.output_row_count = output_row_count

        if columns_added:
            stage.columns_added = columns_added
        if columns_removed:
            stage.columns_removed = columns_removed

        # Calculate duration
        if stage_name in self._stage_timers:
            duration = time.perf_counter() - self._stage_timers[stage_name]
            stage.duration_ms = int(duration * 1000)
            del self._stage_timers[stage_name]

        # Calculate rows added/removed
        if stage.input_row_count is not None and stage.output_row_count is not None:
            diff = stage.output_row_count - stage.input_row_count
            if diff > 0:
                stage.rows_added = diff
            elif diff < 0:
                stage.rows_removed = abs(diff)

        if self._auto_log:
            in_count = stage.input_row_count or "?"
            out_count = stage.output_row_count or "?"
            logger.info(
                f"[{self._metrics.run_id}] Stage '{stage_name}': "
                f"{in_count} -> {out_count} rows ({stage.duration_ms}ms)"
            )

    @contextmanager
    def stage_context(
        self, stage_name: str, input_row_count: int | None = None
    ) -> Iterator[None]:
        """Context manager for stage timing.

        Automatically starts timing when entering and ends timing when exiting.

        Note: The output row count is not automatically recorded. To record it,
        call `record_stage_output(stage_name, count)` inside the context block.

        Args:
            stage_name: Name of the stage
            input_row_count: Number of input rows (if known)

        Yields:
            None

        Example:
            with metrics.stage_context("transform", input_row_count=1000):
                df = transform(df)
                metrics.record_stage_output("transform", df.count())
        """
        self.start_stage(stage_name, input_row_count)
        try:
            yield
        finally:
            self.end_stage(stage_name)

    def record_stage_output(self, stage_name: str, row_count: int) -> None:
        """Record the output row count for a stage (if not using end_stage).

        Args:
            stage_name: Name of the stage
            row_count: Number of output rows
        """
        stage = self._find_or_create_stage(stage_name)
        stage.output_row_count = row_count

    def _find_or_create_stage(self, stage_name: str) -> StageMetric:
        """Find existing stage metric or create new one.

        Note: If called with a stage_name that already exists, returns the
        existing StageMetric. This is intentional to support updating metrics
        for a stage across multiple calls (e.g., start_stage, end_stage).
        """
        for stage in self._metrics.stages:
            if stage.stage_name == stage_name:
                return stage
        stage = StageMetric(stage_name=stage_name)
        self._metrics.stages.append(stage)
        return stage

    # -------------------------------------------------------------------------
    # Data Quality
    # -------------------------------------------------------------------------

    def record_nulls(
        self,
        column_name: str,
        count: int,
        action: MetricAction | str = MetricAction.LOGGED,
        sample_values: list[Any] | None = None,
    ) -> None:
        """Record null values found in a column.

        Args:
            column_name: Column with nulls
            count: Number of null values
            action: Action taken (rejected, filled_default, flagged, logged)
            sample_values: Optional sample of affected row identifiers
        """
        action_str = action.value if isinstance(action, MetricAction) else action
        metric = DataQualityMetric(
            issue_type="null",
            column_name=column_name,
            row_count=count,
            action=action_str,
            rule_name="not_null",
            sample_values=sample_values,
        )
        self._metrics.data_quality.append(metric)

        if action_str == MetricAction.REJECTED.value:
            self._metrics.total_rows_rejected += count

        if self._auto_log and count > 0:
            logger.warning(
                f"[{self._metrics.run_id}] Found {count:,} nulls in '{column_name}' "
                f"(action: {action_str})"
            )

    def record_duplicates(
        self,
        columns: list[str],
        count: int,
        action: MetricAction | str = MetricAction.LOGGED,
        sample_values: list[Any] | None = None,
    ) -> None:
        """Record duplicate rows found.

        Args:
            columns: Columns forming the duplicate key
            count: Number of duplicate rows (not unique keys)
            action: Action taken (rejected, kept_first, kept_latest, etc.)
            sample_values: Optional sample of duplicate key values
        """
        action_str = action.value if isinstance(action, MetricAction) else action
        metric = DataQualityMetric(
            issue_type="duplicate",
            column_name=None,
            columns=columns,
            row_count=count,
            action=action_str,
            rule_name="unique",
            sample_values=sample_values,
        )
        self._metrics.data_quality.append(metric)

        if action_str == MetricAction.REJECTED.value:
            self._metrics.total_rows_rejected += count

        if self._auto_log and count > 0:
            cols_str = ", ".join(columns)
            logger.warning(
                f"[{self._metrics.run_id}] Found {count:,} duplicates on [{cols_str}] "
                f"(action: {action_str})"
            )

    def record_validation_failure(
        self,
        rule_name: str,
        column_name: str | None,
        count: int,
        action: MetricAction | str = MetricAction.LOGGED,
        sample_values: list[Any] | None = None,
    ) -> None:
        """Record validation rule failures.

        Args:
            rule_name: Name of the validation rule (e.g., "in_set", "regex")
            column_name: Column that failed validation
            count: Number of failing rows
            action: Action taken
            sample_values: Optional sample of failing values
        """
        action_str = action.value if isinstance(action, MetricAction) else action
        metric = DataQualityMetric(
            issue_type="validation_failed",
            column_name=column_name,
            row_count=count,
            action=action_str,
            rule_name=rule_name,
            sample_values=sample_values,
        )
        self._metrics.data_quality.append(metric)

        if action_str == MetricAction.REJECTED.value:
            self._metrics.total_rows_rejected += count

        if self._auto_log and count > 0:
            logger.warning(
                f"[{self._metrics.run_id}] Validation '{rule_name}' failed for "
                f"{count:,} rows on '{column_name}' (action: {action_str})"
            )

    # -------------------------------------------------------------------------
    # Schema Drift
    # -------------------------------------------------------------------------

    def record_schema_change(
        self,
        change_type: SchemaChangeType | str,
        column_name: str,
        old_value: str | None = None,
        new_value: str | None = None,
        action: str = "applied",
    ) -> None:
        """Record a schema change.

        Args:
            change_type: Type of change (column_added, column_removed, etc.)
            column_name: Name of the affected column
            old_value: Previous value (type, nullable status)
            new_value: New value
            action: Action taken (applied, ignored, failed)
        """
        type_str = (
            change_type.value if isinstance(change_type, SchemaChangeType) else change_type
        )
        metric = SchemaChangeMetric(
            change_type=type_str,
            column_name=column_name,
            old_value=old_value,
            new_value=new_value,
            action=action,
        )
        self._metrics.schema_changes.append(metric)

        if self._auto_log:
            logger.info(
                f"[{self._metrics.run_id}] Schema change: {type_str} '{column_name}' "
                f"({old_value} -> {new_value}) [{action}]"
            )

    # -------------------------------------------------------------------------
    # Write Operations
    # -------------------------------------------------------------------------

    def record_write(
        self,
        target_table: str,
        write_mode: str,
        inserted: int = 0,
        updated: int = 0,
        deleted: int = 0,
        unchanged: int = 0,
        expired: int = 0,
        merge_strategy: str | None = None,
        duration_ms: int | None = None,
        bytes_written: int | None = None,
        target_rows_before: int | None = None,
        target_rows_after: int | None = None,
    ) -> None:
        """Record write operation metrics.

        Args:
            target_table: Name of the target table
            write_mode: Write mode (append, merge, overwrite)
            inserted: Rows inserted
            updated: Rows updated
            deleted: Rows deleted (hard delete)
            unchanged: Rows matched but not changed (merge)
            expired: Rows expired (SCD Type 2)
            merge_strategy: Merge strategy used (if applicable)
            duration_ms: Write duration in milliseconds
            bytes_written: Bytes written (if available)
            target_rows_before: Row count in target before write
            target_rows_after: Row count in target after write
        """
        total_written = inserted + updated
        if write_mode == "overwrite":
            total_written = inserted  # Overwrite replaces all

        self._metrics.write = WriteMetric(
            target_table=target_table,
            write_mode=write_mode,
            merge_strategy=merge_strategy,
            rows_inserted=inserted,
            rows_updated=updated,
            rows_deleted=deleted,
            rows_unchanged=unchanged,
            rows_expired=expired,
            total_rows_written=total_written,
            total_rows_in_target_before=target_rows_before,
            total_rows_in_target_after=target_rows_after,
            duration_ms=duration_ms,
            bytes_written=bytes_written,
        )
        self._metrics.total_rows_written = total_written

        if self._auto_log:
            logger.info(
                f"[{self._metrics.run_id}] Write to '{target_table}' ({write_mode}): "
                f"inserted={inserted:,}, updated={updated:,}, deleted={deleted:,}"
            )

    # -------------------------------------------------------------------------
    # Plugin Tracking
    # -------------------------------------------------------------------------

    def record_plugin_execution(
        self,
        plugin_name: str,
        plugin_type: str,
        duration_ms: int | None = None,
        input_rows: int | None = None,
        output_rows: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a plugin execution.

        Args:
            plugin_name: Name of the plugin
            plugin_type: Type of plugin (column, stage, writer)
            duration_ms: Execution duration
            input_rows: Input row count
            output_rows: Output row count
            metadata: Additional plugin-specific metadata
        """
        record = {
            "plugin_name": plugin_name,
            "plugin_type": plugin_type,
            "duration_ms": duration_ms,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if metadata:
            record["metadata"] = metadata

        self._metrics.plugins_executed.append(record)

        if self._auto_log:
            logger.debug(
                f"[{self._metrics.run_id}] Plugin '{plugin_name}' ({plugin_type}) "
                f"executed in {duration_ms}ms"
            )

    # -------------------------------------------------------------------------
    # Finalization
    # -------------------------------------------------------------------------

    def complete(self, status: str = "completed") -> RunMetrics:
        """Mark the run as complete and finalize metrics.

        Args:
            status: Final status ("completed" or "failed")

        Returns:
            Final RunMetrics object

        Note:
            Can only be called once per collector instance. Subsequent calls
            will log a warning and return the existing metrics without updating.

            If an on_complete callback was provided, it will be invoked after
            metrics are finalized. Callback errors are logged but don't prevent
            the metrics from being returned.
        """
        if self._completed:
            logger.warning(
                f"[{self._metrics.run_id}] complete() called on already-completed run. "
                f"Returning existing metrics."
            )
            return self._metrics

        self._completed = True
        self._metrics.status = status
        self._metrics.end_time = datetime.now(timezone.utc).isoformat()
        self._metrics.duration_ms = int((time.perf_counter() - self._start_time) * 1000)

        if self._auto_log:
            logger.info(
                f"[{self._metrics.run_id}] Run {status}: "
                f"read={self._metrics.total_rows_read:,}, "
                f"written={self._metrics.total_rows_written:,}, "
                f"rejected={self._metrics.total_rows_rejected:,} "
                f"({self._metrics.duration_ms}ms)"
            )

        # Invoke callback for automatic persistence
        if self._on_complete:
            try:
                self._on_complete(self._metrics)
            except Exception as e:
                logger.error(
                    f"[{self._metrics.run_id}] on_complete callback failed: {e}"
                )

        return self._metrics

    def fail(self, error_message: str) -> RunMetrics:
        """Mark the run as failed.

        Args:
            error_message: Error description

        Returns:
            Final RunMetrics object

        Note:
            Can only be called once per collector instance. If complete() or fail()
            was already called, this logs a warning and returns existing metrics
            without updating the error_message.
        """
        if self._completed:
            logger.warning(
                f"[{self._metrics.run_id}] fail() called on already-completed run. "
                f"Returning existing metrics without updating error_message."
            )
            return self._metrics
        self._metrics.error_message = error_message
        return self.complete(status="failed")

    def get_summary(self) -> RunMetrics:
        """Get current metrics (without finalizing).

        Returns:
            Current RunMetrics snapshot
        """
        return self._metrics


# -----------------------------------------------------------------------------
# Convenience Functions
# -----------------------------------------------------------------------------


def create_run_metrics(
    table_name: str,
    load_id: str | None = None,
    environment: str | None = None,
    full_load: bool = False,
) -> MetricsCollector:
    """Create a new metrics collector for a pipeline run.

    This is the recommended entry point for creating metrics.

    Args:
        table_name: Name of the table being processed
        load_id: Business identifier for the load batch
        environment: Environment name (dev, test, prod)
        full_load: If True, indicates a full load run (not incremental)

    Returns:
        Configured MetricsCollector
    """
    return MetricsCollector(
        table_name=table_name,
        load_id=load_id,
        environment=environment,
        full_load=full_load,
    )
