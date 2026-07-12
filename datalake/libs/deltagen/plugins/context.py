"""Plugin execution context for Delta-Gen.

The PluginContext provides plugins with access to:
- Metrics collection for observability
- Configuration settings
- Shared state between plugins in a run
- Helper methods for common operations

This context is passed to every plugin invocation, providing a consistent
interface without tight coupling to Delta-Gen internals.

Example plugin using context:
    @register_stage("dedupe_latest")
    def dedupe_latest(df: DataFrame, stage: StageConfig, ctx: PluginContext) -> DataFrame:
        # Get configuration from extensions
        keys = stage.extensions.get("partition_by", [])
        order_col = stage.extensions.get("order_by")

        # Record input metrics
        input_count = df.count()
        ctx.metrics.start_stage("dedupe_latest", input_count)

        # Perform deduplication
        window = Window.partitionBy(keys).orderBy(F.desc(order_col))
        result = df.withColumn("_rn", F.row_number().over(window)) \
                   .filter("_rn = 1").drop("_rn")

        # Record output metrics
        output_count = result.count()
        removed = input_count - output_count

        ctx.metrics.record_duplicates(
            columns=keys,
            count=removed,
            action=MetricAction.KEPT_LATEST
        )
        ctx.metrics.end_stage("dedupe_latest", output_count)

        # Log custom info
        ctx.log_info(f"Removed {removed} duplicate rows")

        return result
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterator

from deltagen.plugins.metrics import MetricsCollector, RunMetrics, create_run_metrics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from deltagen.model.incremental import DQConfig, IncrementalConfig
    from deltagen.model.table import TableConfig

logger = logging.getLogger(__name__)


@dataclass
class PluginContext:
    """Execution context passed to plugins.

    Provides plugins with access to metrics collection, configuration,
    and shared state without tight coupling to Delta-Gen internals.

    Attributes:
        metrics: MetricsCollector or NullMetricsCollector for recording observability data
        config: The table configuration being processed
        run_id: Unique identifier for this pipeline run
        load_id: Business identifier for the batch/load
        environment: Environment name (dev, test, prod)
        debug: Whether debug mode is enabled
        state: Shared state dict for plugin communication
        options: Additional options passed at runtime
    """

    metrics: "MetricsCollector | NullMetricsCollector"
    config: "TableConfig | None" = None
    run_id: str = ""
    load_id: str | None = None
    environment: str | None = None
    debug: bool = False
    state: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize run_id from metrics if not set."""
        if not self.run_id and self.metrics:
            self.run_id = self.metrics.run_id

    @property
    def table_name(self) -> str:
        """Get the table name being processed.

        Returns the table name from the metrics collector, which is
        always set. Falls back to config.name if metrics doesn't have it.
        """
        if hasattr(self.metrics, "_metrics") and hasattr(self.metrics._metrics, "table_name"):
            return self.metrics._metrics.table_name
        if self.config and hasattr(self.config, "name"):
            return self.config.name
        return "unknown"

    # -------------------------------------------------------------------------
    # Logging Helpers
    # -------------------------------------------------------------------------

    def log_debug(self, message: str) -> None:
        """Log a debug message with run context.

        Args:
            message: Message to log
        """
        if self.debug:
            logger.debug(f"[{self.run_id}] {message}")

    def log_info(self, message: str) -> None:
        """Log an info message with run context.

        Args:
            message: Message to log
        """
        logger.info(f"[{self.run_id}] {message}")

    def log_warning(self, message: str) -> None:
        """Log a warning message with run context.

        Args:
            message: Message to log
        """
        logger.warning(f"[{self.run_id}] {message}")

    def log_error(self, message: str) -> None:
        """Log an error message with run context.

        Args:
            message: Message to log
        """
        logger.error(f"[{self.run_id}] {message}")

    # -------------------------------------------------------------------------
    # State Management
    # -------------------------------------------------------------------------

    def get_state(self, key: str, default: Any = None) -> Any:
        """Get a value from shared state.

        Args:
            key: State key
            default: Default value if key not found

        Returns:
            Value or default
        """
        return self.state.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Set a value in shared state.

        Args:
            key: State key
            value: Value to store
        """
        self.state[key] = value

    def update_state(self, updates: dict[str, Any]) -> None:
        """Update multiple state values.

        Args:
            updates: Dictionary of key-value pairs to set
        """
        self.state.update(updates)

    # -------------------------------------------------------------------------
    # Options/Configuration Access
    # -------------------------------------------------------------------------

    def get_option(self, key: str, default: Any = None) -> Any:
        """Get a runtime option.

        Args:
            key: Option key
            default: Default value if not found

        Returns:
            Option value or default
        """
        return self.options.get(key, default)

    def get_extension(self, key: str, default: Any = None) -> Any:
        """Get a value from the table config's extensions.

        Args:
            key: Extension key
            default: Default value if not found

        Returns:
            Extension value or default
        """
        if self.config and hasattr(self.config, "extensions"):
            return self.config.extensions.get(key, default)
        return default

    # -------------------------------------------------------------------------
    # Config Access Helpers
    # -------------------------------------------------------------------------

    @property
    def dq_config(self) -> "DQConfig | None":
        """Get the DQ configuration from the table config.

        Returns:
            DQConfig from table config, or None if not available.
        """
        if self.config and hasattr(self.config, "dq"):
            return self.config.dq
        return None

    @property
    def incremental_config(self) -> "IncrementalConfig | None":
        """Get the incremental loading configuration.

        Returns:
            IncrementalConfig from table config, or None if not available.
        """
        if self.config and hasattr(self.config, "incremental"):
            return self.config.incremental
        return None

    # -------------------------------------------------------------------------
    # DQ Table Helpers
    # -------------------------------------------------------------------------

    def write_rejected_records(
        self,
        df: "DataFrame",
        rejected_df: "DataFrame",
        reason: str,
        column_name: str | None = None,
        rule_name: str | None = None,
    ) -> None:
        """Write rejected records to the DQ rejected table.

        Rejected records are stored with a fixed schema regardless of source table
        structure. The full rejected row is serialized as JSON into ``_dq_record``
        and natural key values are serialized into ``_dq_natural_id`` for easy
        identification — no ``mergeSchema`` or wide sparse tables required.

        Args:
            df: The original DataFrame (unused, kept for API compatibility)
            rejected_df: DataFrame containing rejected records
            reason: Reason for rejection (e.g., "null_value", "invalid_value")
            column_name: Column that caused rejection (if applicable)
            rule_name: Validation rule name (if applicable)

        Note:
            Only writes if dq_config.rejected_table is configured.
            Respects dq_config.log_sample_size for limiting records.
        """
        dq = self.dq_config
        if not dq or not dq.rejected_table:
            self.log_debug("No rejected_table configured, skipping rejected record logging")
            return

        if rejected_df.isEmpty():
            return

        from pyspark.sql import functions as F

        sample_size = dq.log_sample_size
        data_cols = rejected_df.columns
        sampled = rejected_df.limit(sample_size)

        # Natural key columns for record identification (columns marked natural=True in YAML)
        natural_keys = []
        if self.config and hasattr(self.config, "get_natural_key_columns"):
            natural_keys = [c.name for c in self.config.get_natural_key_columns()]
        key_cols_present = [c for c in natural_keys if c in data_cols]

        natural_id_expr = (
            F.to_json(F.struct(*[F.col(c) for c in key_cols_present]))
            if key_cols_present
            else F.lit(None).cast("string")
        )

        # Fixed schema: natural key JSON + full row JSON + _dq_* metadata.
        # F.to_json(F.struct(...)) is vectorized — no performance penalty.
        metadata_df = (
            sampled
            .withColumn("_dq_natural_id", natural_id_expr)
            .withColumn("_dq_record", F.to_json(F.struct(*[F.col(c) for c in data_cols])))
            .select("_dq_natural_id", "_dq_record")
            .withColumn("_dq_rejection_reason", F.lit(reason))
            .withColumn("_dq_column_name", F.lit(column_name))
            .withColumn("_dq_rule_name", F.lit(rule_name))
            .withColumn("_dq_table_name", F.lit(self.table_name))
            .withColumn("_dq_run_id", F.lit(self.run_id))
            .withColumn("_dq_load_id", F.lit(self.load_id))
            .withColumn("_dq_rejected_at", F.current_timestamp())
        )

        try:
            # Use spark.sql DDL to create the table if it doesn't exist.
            # Avoids calling saveAsTable on a non-existent table, which triggers
            # Fabric Trident catalog's getActiveSparkSession() and can corrupt
            # the catalog state for all subsequent Spark operations in the pool.
            spark = metadata_df.sparkSession
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {dq.rejected_table} (
                    _dq_natural_id STRING,
                    _dq_record STRING,
                    _dq_rejection_reason STRING,
                    _dq_column_name STRING,
                    _dq_rule_name STRING,
                    _dq_table_name STRING,
                    _dq_run_id STRING,
                    _dq_load_id STRING,
                    _dq_rejected_at TIMESTAMP
                ) USING DELTA
            """)
            metadata_df.write.mode("append").saveAsTable(dq.rejected_table)
            self.log_info(
                f"Logged {min(rejected_df.count(), sample_size)} rejected records "
                f"to {dq.rejected_table} (reason: {reason})"
            )
        except Exception as e:
            self.log_warning(f"Failed to write rejected records to {dq.rejected_table}: {e}")

    def write_duplicate_records(
        self,
        df: "DataFrame",
        duplicate_keys_df: "DataFrame",
        natural_keys: list[str],
    ) -> None:
        """Write unexpected duplicate records to the DQ duplicates table.

        This helper method logs duplicate keys for investigation. Use this
        when duplicates are unexpected (incremental_append strategy) and
        indicate a data quality issue.

        Args:
            df: The source DataFrame containing duplicates
            duplicate_keys_df: DataFrame with duplicate key combinations and counts
            natural_keys: List of columns forming the natural key

        Note:
            Only writes if dq_config.duplicates_table is configured.
            Respects dq_config.log_sample_size for limiting records.
        """
        dq = self.dq_config
        if not dq or not dq.duplicates_table:
            self.log_debug("No duplicates_table configured, skipping duplicate logging")
            return

        if duplicate_keys_df.isEmpty():
            return

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        sample_size = dq.log_sample_size
        data_cols = df.columns

        # Get the full duplicate rows (not just the key+count summary) so the
        # record can be serialised to JSON like the rejected_records table.
        # Join back to the original df on all natural keys then limit sample.
        dup_rows = (
            df.join(
                duplicate_keys_df.select(natural_keys).distinct(),
                on=natural_keys,
                how="inner",
            )
            .limit(sample_size)
        )

        natural_id_expr = F.to_json(F.struct(*[F.col(k) for k in natural_keys]))

        # Fixed schema — no variable key columns, everything serialised to JSON.
        # Mirrors write_rejected_records so both DQ tables have the same shape.
        metadata_df = (
            dup_rows
            .withColumn("_dq_natural_id", natural_id_expr)
            .withColumn("_dq_record", F.to_json(F.struct(*[F.col(c) for c in data_cols])))
            .select("_dq_natural_id", "_dq_record")
            .withColumn("_dq_table_name", F.lit(self.table_name))
            .withColumn("_dq_run_id", F.lit(self.run_id))
            .withColumn("_dq_load_id", F.lit(self.load_id))
            .withColumn("_dq_natural_keys", F.lit(",".join(natural_keys)))
            .withColumn("_dq_detected_at", F.current_timestamp())
        )

        try:
            # Use spark.sql DDL to create the table if it doesn't exist.
            # Avoids calling saveAsTable on a non-existent table, which triggers
            # Fabric Trident catalog's getActiveSparkSession() and can corrupt
            # the catalog state for all subsequent Spark operations in the pool.
            spark = metadata_df.sparkSession
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {dq.duplicates_table} (
                    _dq_natural_id STRING,
                    _dq_record STRING,
                    _dq_table_name STRING,
                    _dq_run_id STRING,
                    _dq_load_id STRING,
                    _dq_natural_keys STRING,
                    _dq_detected_at TIMESTAMP
                ) USING DELTA
            """)
            metadata_df.write.mode("append").saveAsTable(dq.duplicates_table)
            self.log_info(
                f"Logged {min(duplicate_keys_df.count(), sample_size)} duplicate keys "
                f"to {dq.duplicates_table}"
            )
        except Exception as e:
            self.log_warning(f"Failed to write duplicates to {dq.duplicates_table}: {e}")

    # -------------------------------------------------------------------------
    # Timing Helpers
    # -------------------------------------------------------------------------

    @contextmanager
    def timed_operation(self, operation_name: str) -> Iterator[dict[str, Any]]:
        """Context manager for timing operations.

        Args:
            operation_name: Name of the operation

        Yields:
            Dict that will contain 'duration_ms' after the context exits

        Example:
            with ctx.timed_operation("transform") as timing:
                result = do_transform()
            print(f"Transform took {timing['duration_ms']}ms")
        """
        result: dict[str, Any] = {}
        start = time.perf_counter()
        try:
            yield result
        finally:
            result["duration_ms"] = int((time.perf_counter() - start) * 1000)
            self.log_debug(f"{operation_name} completed in {result['duration_ms']}ms")

    # -------------------------------------------------------------------------
    # Plugin Execution Tracking
    # -------------------------------------------------------------------------

    def record_plugin_start(self, plugin_name: str, plugin_type: str) -> float:
        """Record the start of a plugin execution.

        Args:
            plugin_name: Name of the plugin
            plugin_type: Type of plugin (column, stage, writer)

        Returns:
            Start time for duration calculation
        """
        self.log_debug(f"Starting plugin: {plugin_name} ({plugin_type})")
        return time.perf_counter()

    def record_plugin_end(
        self,
        plugin_name: str,
        plugin_type: str,
        start_time: float,
        input_rows: int | None = None,
        output_rows: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record the end of a plugin execution.

        Args:
            plugin_name: Name of the plugin
            plugin_type: Type of plugin
            start_time: Start time from record_plugin_start
            input_rows: Input row count
            output_rows: Output row count
            metadata: Additional plugin-specific metadata
        """
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        self.metrics.record_plugin_execution(
            plugin_name=plugin_name,
            plugin_type=plugin_type,
            duration_ms=duration_ms,
            input_rows=input_rows,
            output_rows=output_rows,
            metadata=metadata,
        )


# -----------------------------------------------------------------------------
# Context Factory
# -----------------------------------------------------------------------------


def create_plugin_context(
    table_name: str,
    config: "TableConfig | None" = None,
    load_id: str | None = None,
    environment: str | None = None,
    debug: bool = False,
    options: dict[str, Any] | None = None,
    on_complete: Callable[[RunMetrics], None] | None = None,
    full_load: bool = False,
) -> PluginContext:
    """Create a new plugin context for a pipeline run.

    This is the recommended factory for creating PluginContext instances.

    Args:
        table_name: Name of the table being processed
        config: TableConfig being processed
        load_id: Business identifier for the batch
        environment: Environment name (dev, test, prod)
        debug: Enable debug mode
        options: Additional runtime options
        on_complete: Optional callback invoked when metrics.complete() is called.
                     Use this for automatic persistence to storage systems.
        full_load: If True, indicates a full load run (not incremental)

    Returns:
        Configured PluginContext

    Example:
        # Basic usage
        ctx = create_plugin_context(
            table_name="customer_dim",
            config=cfg,
            load_id="batch_2024_01_15",
            debug=True
        )

        # With auto-persistence callback
        def persist_metrics(metrics: RunMetrics):
            write_to_delta_table(metrics)

        ctx = create_plugin_context(
            table_name="customer_dim",
            on_complete=persist_metrics,
        )
    """
    metrics = MetricsCollector(
        table_name=table_name,
        load_id=load_id,
        environment=environment,
        on_complete=on_complete,
        full_load=full_load,
    )

    return PluginContext(
        metrics=metrics,
        config=config,
        load_id=load_id,
        environment=environment,
        debug=debug,
        options=options or {},
    )


# -----------------------------------------------------------------------------
# Null Context (for testing/optional metrics)
# -----------------------------------------------------------------------------


class NullMetricsCollector:
    """A no-op metrics collector for when metrics are disabled.

    All methods are no-ops, allowing code to use the metrics interface
    without checking if metrics are enabled.
    """

    run_id = "null"

    def record_source_read(self, *args, **kwargs) -> None:
        pass

    def start_stage(self, *args, **kwargs) -> None:
        pass

    def end_stage(self, *args, **kwargs) -> None:
        pass

    def record_stage_output(self, *args, **kwargs) -> None:
        pass

    def record_nulls(self, *args, **kwargs) -> None:
        pass

    def record_duplicates(self, *args, **kwargs) -> None:
        pass

    def record_validation_failure(self, *args, **kwargs) -> None:
        pass

    def record_schema_change(self, *args, **kwargs) -> None:
        pass

    def record_write(self, *args, **kwargs) -> None:
        pass

    def record_plugin_execution(self, *args, **kwargs) -> None:
        pass

    def complete(self, *args, **kwargs) -> None:
        pass

    def fail(self, *args, **kwargs) -> None:
        pass

    @contextmanager
    def stage_context(self, *args, **kwargs) -> Iterator[None]:
        yield


def create_null_context(debug: bool = False) -> PluginContext:
    """Create a context with null metrics (for testing).

    Args:
        debug: Enable debug logging

    Returns:
        PluginContext with NullMetricsCollector
    """
    return PluginContext(
        metrics=NullMetricsCollector(),
        debug=debug,
    )
