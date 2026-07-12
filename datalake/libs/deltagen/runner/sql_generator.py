"""SQL generation and plan explanation for PlanBuilder."""
from __future__ import annotations

from typing import Any

from deltagen.model import TableConfig


def generate_sql(
    config: TableConfig,
    sql_parts: dict[str, dict[str, Any]],
    stage: str | None = None,
) -> str:
    """Generate SQL representation of the transformation.

    Args:
        config: Table configuration
        sql_parts: Dictionary of SQL parts by stage
        stage: Specific stage name, or None for all stages

    Returns:
        SQL query string
    """
    if stage:
        return _stage_to_sql(stage, sql_parts)

    # Generate SQL for all stages
    sql_list = []
    for i, stage_config in enumerate(config.stages):
        stage_sql = _stage_to_sql(stage_config.name, sql_parts)
        if i == 0:
            sql_list.append(stage_sql)
        else:
            sql_list.append(f"\n-- Stage: {stage_config.name}\n{stage_sql}")

    return "\n".join(sql_list)


def _stage_to_sql(stage_name: str, sql_parts: dict[str, dict[str, Any]]) -> str:
    """Generate SQL for a single stage.

    Args:
        stage_name: Name of the stage
        sql_parts: Dictionary of SQL parts by stage

    Returns:
        SQL query string
    """
    parts = sql_parts.get(stage_name, {})

    # Build SELECT clause
    select_cols = parts.get("select", ["*"])
    select_clause = ",\n    ".join(select_cols)

    # Build FROM clause
    from_source = parts.get("from", "source")

    # Build JOIN clause
    joins = parts.get("joins", [])
    join_clause = "\n".join(joins) if joins else ""

    # Build WHERE clause
    where_parts = parts.get("where", [])
    where_clause = ""
    if where_parts:
        where_clause = "\nWHERE " + "\n  AND ".join(where_parts)

    sql = f"""SELECT
    {select_clause}
FROM {from_source} src"""

    if join_clause:
        sql += f"\n{join_clause}"

    if where_clause:
        sql += where_clause

    return sql


def explain_plan(config: TableConfig) -> str:
    """Generate a human-readable explanation of the plan.

    Args:
        config: Table configuration

    Returns:
        Multi-line explanation string
    """
    lines = [
        f"Table: {config.name}",
        f"Layer: {config.layer or 'not specified'}",
        "",
    ]

    # Sources
    lines.append("Sources:")
    for source in config.sources:
        if source.path:
            lines.append(f"  - {source.name}: {source.path} ({source.format})")
        elif source.table:
            table_ref = source.table
            if source.schema:
                table_ref = f"{source.schema}.{table_ref}"
            if source.catalog:
                table_ref = f"{source.catalog}.{table_ref}"
            lines.append(f"  - {source.name}: {table_ref}")

    lines.append("")

    # Stages
    for stage in config.stages:
        lines.append(f"Stage: {stage.name} ({stage.mode})")

        if stage.columns:
            lines.append("  Columns:")
            for col in stage.columns:
                type_str = col.data_type or "untyped"
                flags = []
                if col.natural:
                    flags.append("natural")
                if col.temporary:
                    flags.append("temp")
                if not col.nullable:
                    flags.append("not null")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                lines.append(f"    - {col.name}: {type_str}{flag_str}")

        if stage.joins:
            lines.append("  Joins:")
            for join in stage.joins:
                conds = [
                    f"{c.left} {c.operator} {c.right}" for c in join.conditions
                ]
                lines.append(
                    f"    - {join.type.upper()} {join.source}: {', '.join(conds)}"
                )

        if stage.filters:
            lines.append("  Filters:")
            for f in stage.filters:
                lines.append(f"    - {f}")

        lines.append("")

    # Natural keys
    natural_keys = config.get_natural_key_columns()
    if natural_keys:
        lines.append(f"Natural Keys: {[c.name for c in natural_keys]}")

    return "\n".join(lines)
