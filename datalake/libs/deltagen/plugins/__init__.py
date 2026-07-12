"""Delta-Gen Plugin System.

This module provides the extensibility layer for Delta-Gen, allowing external
packages (like Lakehouse-Build) to inject custom transforms and stage processors
into the transformation pipeline.

Components:
- Registry: Register and lookup column/stage plugins by name
- Metrics: Observability and metrics collection for pipeline runs
- Context: Execution context passed to plugins
- Defaults: Starter plugins (mask_email, not_null, in_set, dedupe_keep_last)

Note: Writer customization is handled via hooks in DeltaWriter, not plugins.
See deltagen.runner.writer for pre_write_hook and post_write_hook.

Quick Start:
    # Register a plugin (in your plugin package)
    from deltagen.plugins import register_column, register_stage

    @register_column("mask_email")
    def mask_email(df, column, ctx):
        return df.withColumn(column.name, F.lit("***@***"))

    # Use plugins (in Delta-Gen or user code)
    from deltagen.plugins import get_column_plugin, create_plugin_context

    ctx = create_plugin_context("customer_dim", load_id="batch_001")
    plugin = get_column_plugin("mask_email")
    if plugin:
        df = plugin(df, column_config, ctx)

    # Get metrics summary
    summary = ctx.metrics.complete()
    print(summary.get_summary_table())

Default Plugins (available out of the box):
    - mask_email: GDPR-compliant email masking
    - not_null: Validate column has no nulls (reject/warn/fill)
    - in_set: Validate column values in allowed set
    - delta_load: Incremental load with watermark filter and dedupe
    - dedupe_keep_last: Dedupe keeping latest record per key
"""

# Registry - Registration decorators and lookup functions
from deltagen.plugins.registry import (
    # Decorators
    register_column,
    register_stage,
    # Lookup functions
    get_column_plugin,
    get_stage_plugin,
    get_plugin_info,
    # Introspection
    list_plugins,
    clear_registry,
    # Types
    PluginInfo,
    ColumnPlugin,
    StagePlugin,
)

# Metrics - Observability and metrics collection
from deltagen.plugins.metrics import (
    # Main classes
    MetricsCollector,
    RunMetrics,
    # Metric data classes
    SourceReadMetric,
    StageMetric,
    DataQualityMetric,
    SchemaChangeMetric,
    WriteMetric,
    # Enums
    MetricAction,
    SchemaChangeType,
    # Factory function
    create_run_metrics,
)

# Context - Plugin execution context
from deltagen.plugins.context import (
    PluginContext,
    create_plugin_context,
    create_null_context,
)

# Default plugins - importing registers them automatically
from deltagen.plugins.defaults import (
    # Column plugins
    mask_email,
    not_null,
    in_set,
    # Stage plugins
    dedupe_keep_last,
    check_duplicates,
    period_replace,
)

__all__ = [
    # Registry
    "register_column",
    "register_stage",
    "get_column_plugin",
    "get_stage_plugin",
    "get_plugin_info",
    "list_plugins",
    "clear_registry",
    "PluginInfo",
    "ColumnPlugin",
    "StagePlugin",
    # Metrics
    "MetricsCollector",
    "RunMetrics",
    "SourceReadMetric",
    "StageMetric",
    "DataQualityMetric",
    "SchemaChangeMetric",
    "WriteMetric",
    "MetricAction",
    "SchemaChangeType",
    "create_run_metrics",
    # Context
    "PluginContext",
    "create_plugin_context",
    "create_null_context",
    # Default plugins - column
    "mask_email",
    "not_null",
    "in_set",
    # Default plugins - stage
    "dedupe_keep_last",
    "check_duplicates",
    "period_replace",
]
