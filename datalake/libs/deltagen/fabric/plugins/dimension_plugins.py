"""Dimension management plugins for Fabric environments.

These plugins handle concerns specific to gold-layer dimension tables,
such as ensuring sentinel/default records exist for unresolvable foreign
keys in fact tables.

Stage Plugins:
- ensure_sentinels: Union static sentinel rows into the dimension before
  write so that fact-table FKs always resolve to a real dimension record
  instead of NULL.

Usage in YAML (last stage of a dimension table):

    stages:
      - name: add_sentinels
        extensions:
          stage_plugin: ensure_sentinels
          records:
            - ProductID: "NO_PRODUCT"
              ProductNaturalID: "NO_PRODUCT"
              ProductName: "!! Unknown"
            - ProductID: "NA_PRODUCT"
              ProductNaturalID: "NA_PRODUCT"
              ProductName: "N/A"

The stage simply unions the sentinel rows into the DataFrame. Because
dimension tables use ``merge_strategy: insert_only``, the Delta merge will
INSERT sentinel rows on the first run and leave them untouched on every
subsequent run -- giving "insert-if-not-exists" behaviour for free.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from deltagen.model.stage import StageConfig
    from deltagen.plugins.context import PluginContext

logger = logging.getLogger(__name__)


@register_stage(
    "ensure_sentinels",
    description="Union static sentinel rows into the dimension before write",
    version="1.0.0",
    author="deltagen_helpers",
    tags={"dimension", "sentinel", "dq"},
)
def ensure_sentinels(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Add sentinel rows to a dimension DataFrame before the write step."""
    from pyspark.sql import functions as F

    extensions = stage.extensions or {}
    records: list[dict] = extensions.get("records", [])

    if not records:
        ctx.log_warning("ensure_sentinels: no records configured -- skipping")
        return df

    schema = df.schema
    spark = df.sparkSession

    sentinel_dfs: list["DataFrame"] = []
    for i, record in enumerate(records):
        exprs = []
        for field in schema.fields:
            value = record.get(field.name)
            if value is None:
                expr = F.lit(None).cast(field.dataType).alias(field.name)
            else:
                expr = F.lit(value).cast(field.dataType).alias(field.name)
            exprs.append(expr)

        row_df = spark.range(1).select(*exprs)
        sentinel_dfs.append(row_df)

    sentinel_combined = sentinel_dfs[0]
    for sdf in sentinel_dfs[1:]:
        sentinel_combined = sentinel_combined.union(sdf)

    ctx.log_info(
        f"ensure_sentinels: adding {len(records)} sentinel row(s) to dimension"
    )

    return df.union(sentinel_combined)
