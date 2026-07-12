"""Delta-Gen Fabric integration layer.

Provides integration between Delta-Gen and Microsoft Fabric, including:
- FabricMetricsAdapter for automatic metrics persistence to Delta tables
- Fabric-specific plugins (DQ, write hooks, dimension management)
- Context creation helpers

Quick Start:
    from deltagen.fabric import create_fabric_context

    ctx = create_fabric_context(
        spark=spark,
        table_name="customer_dim",
        load_id="batch_001",
    )

    builder = PlanBuilder(config)
    df = builder.build(spark, context=ctx)
    ctx.metrics.complete()
"""
from deltagen.fabric.adapter import (
    FabricMetricsAdapter,
    MetricsTableConfig,
    create_fabric_adapter,
)
from deltagen.fabric.context import (
    create_fabric_context,
    create_fabric_context_with_hooks,
)

__all__ = [
    "FabricMetricsAdapter",
    "MetricsTableConfig",
    "create_fabric_adapter",
    "create_fabric_context",
    "create_fabric_context_with_hooks",
]
