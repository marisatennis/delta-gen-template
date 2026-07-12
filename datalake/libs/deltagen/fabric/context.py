"""Fabric-specific context creation for Delta-Gen.

Usage:
    from deltagen.fabric import create_fabric_context

    ctx = create_fabric_context(
        spark=spark,
        table_name="customer_dim",
        load_id="batch_001",
    )
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from deltagen.model.table import TableConfig
    from deltagen.plugins.context import PluginContext


def create_fabric_context(
    spark: "SparkSession",
    table_name: str,
    config: "TableConfig | None" = None,
    load_id: str | None = None,
    schema: str = "logging",
    prefix: str = "deltagen",
    environment: str | None = None,
    auto_persist: bool = True,
    log_summary: bool = True,
    full_load: bool = False,
) -> "PluginContext":
    """Create a PluginContext configured for Fabric environments."""
    from deltagen.plugins.context import create_plugin_context
    from deltagen.fabric.adapter import FabricMetricsAdapter
    from deltagen.fabric.plugins import register_fabric_plugins

    register_fabric_plugins()

    adapter = FabricMetricsAdapter(
        spark=spark, schema=schema, prefix=prefix, log_summary=log_summary,
    )

    on_complete = adapter.persist_metrics if auto_persist else None

    ctx = create_plugin_context(
        table_name=table_name, config=config, load_id=load_id,
        environment=environment, on_complete=on_complete, full_load=full_load,
    )

    ctx._fabric_adapter = adapter
    return ctx


def create_fabric_context_with_hooks(
    spark: "SparkSession",
    table_name: str,
    load_id: str | None = None,
    schema: str = "logging",
    prefix: str = "deltagen",
    environment: str | None = None,
) -> tuple["PluginContext", dict]:
    """Create a PluginContext and write hooks for Fabric."""
    from deltagen.fabric.plugins.write_hooks import create_table_load_log_hook

    ctx = create_fabric_context(
        spark=spark, table_name=table_name, load_id=load_id,
        schema=schema, prefix=prefix, environment=environment,
    )

    post_write_hook = create_table_load_log_hook(spark=spark, schema=schema, run_id=load_id)

    return ctx, {"post_write_hook": post_write_hook}
