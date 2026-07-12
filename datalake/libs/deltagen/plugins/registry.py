"""Plugin registry for Delta-Gen extensibility.

This module provides the core registry pattern that allows external packages
(like Lakehouse-Build) to inject custom transforms and stage processors
into Delta-Gen's transformation pipeline.

Note: Writer customization is handled via hooks in DeltaWriter, not plugins.
See deltagen.runner.writer for pre_write_hook and post_write_hook.

Example usage:
    # In Lakehouse-Build or custom plugin package
    from deltagen.plugins.registry import register_column, register_stage

    @register_column("mask_email")
    def mask_email(df, column, context):
        '''GDPR-compliant email masking.'''
        return df.withColumn(
            column.name,
            F.concat(F.substring(F.col(column.name), 1, 2), F.lit("***@***"))
        )

    @register_stage("dedupe_latest")
    def dedupe_latest(df, stage, context):
        '''Keep only the most recent record per key.'''
        keys = stage.extensions.get("partition_by", [])
        order_col = stage.extensions.get("order_by")
        window = Window.partitionBy(keys).orderBy(F.desc(order_col))
        return df.withColumn("_rn", F.row_number().over(window)) \
                 .filter("_rn = 1").drop("_rn")

    # In Delta-Gen PlanBuilder (internal usage)
    from deltagen.plugins.registry import get_column_plugin

    plugin = get_column_plugin("mask_email")
    if plugin:
        df = plugin(df, column_config, context)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from deltagen.model.column import ColumnConfig
    from deltagen.model.stage import StageConfig
    from deltagen.plugins.context import PluginContext

logger = logging.getLogger(__name__)

# Type aliases for plugin signatures
ColumnPluginFn = Callable[["DataFrame", "ColumnConfig", "PluginContext"], "DataFrame"]
StagePluginFn = Callable[["DataFrame", "StageConfig", "PluginContext"], "DataFrame"]

T = TypeVar("T", bound=Callable)


# -----------------------------------------------------------------------------
# Plugin Protocols (for type checking)
# -----------------------------------------------------------------------------


@runtime_checkable
class ColumnPlugin(Protocol):
    """Protocol for column transform plugins.

    Column plugins operate on a single column, transforming its values.
    They receive the full DataFrame but should only modify the target column.
    """

    def __call__(
        self,
        df: "DataFrame",
        column: "ColumnConfig",
        context: "PluginContext",
    ) -> "DataFrame":
        """Apply the column transformation.

        Args:
            df: Input DataFrame
            column: Column configuration with transform details in extensions
            context: Plugin context with metrics collector and config

        Returns:
            DataFrame with the transformed column
        """
        ...


@runtime_checkable
class StagePlugin(Protocol):
    """Protocol for stage transform plugins.

    Stage plugins operate on entire DataFrames between transformation stages.
    Use for operations like deduplication, data quality checks, or aggregations.
    """

    def __call__(
        self,
        df: "DataFrame",
        stage: "StageConfig",
        context: "PluginContext",
    ) -> "DataFrame":
        """Apply the stage transformation.

        Args:
            df: Input DataFrame
            stage: Stage configuration with plugin details in extensions
            context: Plugin context with metrics collector and config

        Returns:
            Transformed DataFrame
        """
        ...


# -----------------------------------------------------------------------------
# Plugin Metadata
# -----------------------------------------------------------------------------


@dataclass
class PluginInfo:
    """Metadata about a registered plugin."""

    name: str
    plugin_type: str  # "column" or "stage"
    fn: Callable
    description: str | None = None
    version: str = "1.0.0"
    author: str | None = None
    tags: set[str] = field(default_factory=set)


# -----------------------------------------------------------------------------
# Global Registries
# -----------------------------------------------------------------------------

# The registries - simple dicts with plugin metadata
#
# Thread Safety Note:
# These registries are module-level globals. Plugin registration should occur
# at module import time (using the decorators) before any concurrent execution
# begins. This follows Python's standard pattern for plugin registration.
# Do NOT register plugins dynamically during parallel Spark execution.
#
# Important: This is a best-practice recommendation only. The registry does not
# enforce or detect late/dynamic registration at runtime, so callers are
# responsible for following this guidance in concurrent or Spark environments.
_column_plugins: dict[str, PluginInfo] = {}
_stage_plugins: dict[str, PluginInfo] = {}


# -----------------------------------------------------------------------------
# Registration Decorators
# -----------------------------------------------------------------------------


def register_column(
    name: str,
    *,
    description: str | None = None,
    version: str = "1.0.0",
    author: str | None = None,
    tags: set[str] | None = None,
) -> Callable[[T], T]:
    """Register a column transform plugin.

    Column plugins transform individual columns in a DataFrame. They're invoked
    when a column's `extensions.transform` matches the registered name.

    Args:
        name: Unique identifier for the plugin (e.g., "mask_email")
        description: Human-readable description of what the plugin does
        version: Plugin version string
        author: Plugin author/maintainer
        tags: Tags for categorization (e.g., {"pii", "gdpr"})

    Returns:
        Decorator function

    Example:
        @register_column("mask_email", description="GDPR email masking", tags={"pii"})
        def mask_email(df: DataFrame, column: ColumnConfig, ctx: PluginContext) -> DataFrame:
            return df.withColumn(column.name, F.lit("***@***"))
    """

    def decorator(fn: T) -> T:
        if name in _column_plugins:
            logger.warning(f"Overwriting existing column plugin: {name}")

        _column_plugins[name] = PluginInfo(
            name=name,
            plugin_type="column",
            fn=fn,
            description=description or fn.__doc__,
            version=version,
            author=author,
            tags=tags or set(),
        )
        logger.debug(f"Registered column plugin: {name}")
        return fn

    return decorator


def register_stage(
    name: str,
    *,
    description: str | None = None,
    version: str = "1.0.0",
    author: str | None = None,
    tags: set[str] | None = None,
) -> Callable[[T], T]:
    """Register a stage transform plugin.

    Stage plugins operate on entire DataFrames between transformation stages.
    They're invoked when a stage's `extensions.stage_plugin` matches the name.

    Args:
        name: Unique identifier for the plugin (e.g., "dedupe_latest")
        description: Human-readable description
        version: Plugin version string
        author: Plugin author/maintainer
        tags: Tags for categorization (e.g., {"dedup", "quality"})

    Returns:
        Decorator function

    Example:
        @register_stage("dedupe_latest", description="Keep latest record per key")
        def dedupe_latest(df: DataFrame, stage: StageConfig, ctx: PluginContext) -> DataFrame:
            # ... deduplication logic
            return df
    """

    def decorator(fn: T) -> T:
        if name in _stage_plugins:
            logger.warning(f"Overwriting existing stage plugin: {name}")

        _stage_plugins[name] = PluginInfo(
            name=name,
            plugin_type="stage",
            fn=fn,
            description=description or fn.__doc__,
            version=version,
            author=author,
            tags=tags or set(),
        )
        logger.debug(f"Registered stage plugin: {name}")
        return fn

    return decorator


# -----------------------------------------------------------------------------
# Plugin Lookup Functions
# -----------------------------------------------------------------------------


def get_column_plugin(name: str) -> ColumnPluginFn | None:
    """Look up a registered column plugin by name.

    Args:
        name: Plugin identifier

    Returns:
        Plugin function if found, None otherwise
    """
    info = _column_plugins.get(name)
    return info.fn if info else None


def get_stage_plugin(name: str) -> StagePluginFn | None:
    """Look up a registered stage plugin by name.

    Args:
        name: Plugin identifier

    Returns:
        Plugin function if found, None otherwise
    """
    info = _stage_plugins.get(name)
    return info.fn if info else None


def get_plugin_info(name: str, plugin_type: str | None = None) -> PluginInfo | None:
    """Get metadata about a registered plugin.

    Args:
        name: Plugin identifier
        plugin_type: Optional filter by type ("column" or "stage")

    Returns:
        PluginInfo if found, None otherwise
    """
    if plugin_type == "column" or plugin_type is None:
        if name in _column_plugins:
            return _column_plugins[name]
    if plugin_type == "stage" or plugin_type is None:
        if name in _stage_plugins:
            return _stage_plugins[name]
    return None


# -----------------------------------------------------------------------------
# Registry Introspection
# -----------------------------------------------------------------------------


def list_plugins(
    plugin_type: str | None = None, tags: set[str] | None = None
) -> list[PluginInfo]:
    """List all registered plugins, optionally filtered.

    Args:
        plugin_type: Filter by type ("column" or "stage")
        tags: Filter by tags (any match)

    Returns:
        List of PluginInfo for matching plugins
    """
    results: list[PluginInfo] = []

    registries = []
    if plugin_type in (None, "column"):
        registries.append(_column_plugins)
    if plugin_type in (None, "stage"):
        registries.append(_stage_plugins)

    for registry in registries:
        for info in registry.values():
            if tags is None or (info.tags & tags):
                results.append(info)

    return results


def clear_registry(plugin_type: str | None = None) -> None:
    """Clear registered plugins. Primarily for testing.

    Args:
        plugin_type: Type to clear, or None for all
    """
    if plugin_type in (None, "column"):
        _column_plugins.clear()
    if plugin_type in (None, "stage"):
        _stage_plugins.clear()
    logger.debug(f"Cleared registry: {plugin_type or 'all'}")
