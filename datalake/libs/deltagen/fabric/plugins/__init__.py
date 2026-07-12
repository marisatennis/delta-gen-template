"""Fabric-specific plugins for Delta-Gen.

Includes DQ plugins, dimension plugins, flow plugins, partition plugins,
write hooks, and FK re-resolution scheduling.

Usage:
    from deltagen.fabric.plugins import register_fabric_plugins
    register_fabric_plugins()
"""
from deltagen.fabric.plugins.write_hooks import create_write_logging_hook, log_write_to_table
from deltagen.fabric.plugins.dq_plugins import log_nulls_to_table, log_invalid_to_table, check_unresolved_fks
from deltagen.fabric.plugins.dimension_plugins import ensure_sentinels
from deltagen.fabric.plugins.reresolution_plugins import schedule_fk_reresolution
from deltagen.fabric.plugins.flow_plugins import self_join_previous_period
from deltagen.fabric.plugins.partition_plugins import composite_period_replace


def register_fabric_plugins() -> None:
    """Register all Fabric-specific plugins with Delta-Gen."""
    pass  # Plugins are auto-registered via decorators when imported


__all__ = [
    "register_fabric_plugins",
    "create_write_logging_hook", "log_write_to_table",
    "log_nulls_to_table", "log_invalid_to_table", "check_unresolved_fks",
    "ensure_sentinels", "schedule_fk_reresolution",
    "self_join_previous_period", "composite_period_replace",
]
