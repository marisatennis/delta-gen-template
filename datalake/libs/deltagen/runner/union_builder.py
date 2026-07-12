"""Union handling logic for PlanBuilder."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from deltagen.model.stage import StageConfig, UnionConfig, UnionSource
from deltagen.runner.exceptions import PlanBuilderError


def _get_spark_functions():
    """Lazily import pyspark.sql.functions."""
    from pyspark.sql import functions as F

    return F


def apply_unions(
    stage: StageConfig,
    sources: dict[str, "DataFrame"],
    sql_parts: dict[str, Any],
    debug: bool = False,
) -> "DataFrame":
    """Apply union operation to combine multiple sources.

    Args:
        stage: Stage configuration containing union config
        sources: Available source DataFrames
        sql_parts: Dictionary to store SQL representations
        debug: If True, print union details

    Returns:
        Unified DataFrame combining all specified sources

    Raises:
        PlanBuilderError: If union sources are not found or have incompatible schemas
    """
    union_config = stage.unions
    if union_config is None:
        raise PlanBuilderError(
            "apply_unions called but no union config defined",
            stage=stage.name,
        )

    if debug:
        print("  UNION:")
        print(f"    Sources: {union_config.sources}")
        print(f"    Mode: {union_config.mode}")
        if union_config.distinct:
            print("    Distinct: True")

    # Get all DataFrames to union, applying per-source column_map if present
    dfs_to_union: list["DataFrame"] = []
    for union_source in union_config.sources:
        df = sources.get(union_source.name)
        if df is None:
            raise PlanBuilderError(
                f"Union source '{union_source.name}' not found in sources",
                stage=stage.name,
            )
        df = _apply_column_map(df, union_source, debug)
        dfs_to_union.append(df)

    # Apply union based on mode
    if union_config.mode == "by_name":
        result = _union_by_name(
            dfs_to_union,
            union_config,
            stage.name,
            debug,
        )
    else:  # by_position
        result = _union_by_position(
            dfs_to_union,
            union_config,
            stage.name,
            debug,
        )

    # Apply distinct if requested
    if union_config.distinct:
        result = result.distinct()

    # Store SQL representation
    union_type = "UNION" if union_config.distinct else "UNION ALL"
    source_names = [s.name for s in union_config.sources]
    sql_union = f"{union_type} ({', '.join(source_names)})"
    if stage.name in sql_parts:
        sql_parts[stage.name]["from"] = sql_union

    if debug:
        print(f"    Result columns: {list(result.columns)}")

    return result


def _apply_column_map(
    df: "DataFrame",
    source: UnionSource,
    debug: bool = False,
) -> "DataFrame":
    """Apply column_map to a source DataFrame before unioning.

    For each entry {target: expr} in column_map:
    - Adds a new column ``target`` computed from ``expr`` (evaluated as SQL).
    - If ``expr`` exactly matches an existing column name, that original
      column is dropped (it has been renamed).
    - All other original columns pass through unchanged.

    Args:
        df: Source DataFrame to transform
        source: UnionSource config (may have column_map=None)
        debug: If True, print mapping details

    Returns:
        Transformed DataFrame ready for unioning
    """
    if not source.column_map:
        return df

    F = _get_spark_functions()

    # Columns being renamed away — map values that exactly match an existing column name
    existing_cols = set(df.columns)
    renamed_away = {
        expr.strip()
        for expr in source.column_map.values()
        if expr.strip() in existing_cols
    }

    # Build select: pass-through non-renamed cols, then add all mapped targets
    select_exprs = [F.col(c) for c in df.columns if c not in renamed_away]
    for target, expr in source.column_map.items():
        select_exprs.append(F.expr(expr).alias(target))

    if debug:
        print(f"    [column_map] '{source.name}': {source.column_map}")
        if renamed_away:
            print(f"    [column_map] dropped source cols: {sorted(renamed_away)}")

    return df.select(*select_exprs)


def _union_by_name(
    dfs: list["DataFrame"],
    config: UnionConfig,
    stage_name: str,
    debug: bool = False,
) -> "DataFrame":
    """Union DataFrames by column name.

    Matches columns across DataFrames by their names, not positions.
    Missing columns can optionally be filled with NULL.

    Args:
        dfs: List of DataFrames to union
        config: Union configuration
        stage_name: Name of the current stage
        debug: If True, print details

    Returns:
        Unified DataFrame
    """
    if not dfs:
        raise PlanBuilderError("No DataFrames to union", stage=stage_name)

    if len(dfs) == 1:
        return dfs[0]

    F = _get_spark_functions()

    # Get all unique column names across all DataFrames
    all_columns: set[str] = set()
    df_columns: list[set[str]] = []
    for df in dfs:
        cols = set(df.columns)
        df_columns.append(cols)
        all_columns.update(cols)

    # Check for missing columns
    if not config.allow_missing_columns:
        # Verify all DataFrames have the same columns
        first_cols = df_columns[0]
        for i, cols in enumerate(df_columns[1:], 1):
            if cols != first_cols:
                missing_in_first = cols - first_cols
                missing_in_current = first_cols - cols
                msg_parts = []
                if missing_in_first:
                    msg_parts.append(f"columns {missing_in_first} not in first source")
                if missing_in_current:
                    msg_parts.append(f"columns {missing_in_current} missing in source {config.sources[i]}")
                raise PlanBuilderError(
                    f"Union sources have different columns: {'; '.join(msg_parts)}. "
                    f"Set allow_missing_columns=true to fill missing columns with NULL.",
                    stage=stage_name,
                )

    # Sort columns for consistent ordering
    sorted_columns = sorted(all_columns)

    # Normalize all DataFrames to have the same columns in the same order
    normalized_dfs: list["DataFrame"] = []
    for df, cols in zip(dfs, df_columns):
        select_exprs = []
        for col_name in sorted_columns:
            if col_name in cols:
                select_exprs.append(F.col(col_name))
            else:
                # Fill missing column with NULL
                select_exprs.append(F.lit(None).alias(col_name))
        normalized_dfs.append(df.select(*select_exprs))

    if debug and config.allow_missing_columns:
        print(f"    Normalized to columns: {sorted_columns}")

    # Chain unions
    result = normalized_dfs[0]
    for df in normalized_dfs[1:]:
        result = result.union(df)

    return result


def _union_by_position(
    dfs: list["DataFrame"],
    config: UnionConfig,
    stage_name: str,
    debug: bool = False,
) -> "DataFrame":
    """Union DataFrames by column position.

    Matches columns across DataFrames by their position (index), not names.
    All DataFrames must have the same number of columns.

    Args:
        dfs: List of DataFrames to union
        config: Union configuration
        stage_name: Name of the current stage
        debug: If True, print details

    Returns:
        Unified DataFrame
    """
    if not dfs:
        raise PlanBuilderError("No DataFrames to union", stage=stage_name)

    if len(dfs) == 1:
        return dfs[0]

    # Verify all DataFrames have the same number of columns
    first_col_count = len(dfs[0].columns)
    for i, df in enumerate(dfs[1:], 1):
        if len(df.columns) != first_col_count:
            raise PlanBuilderError(
                f"Union by position requires same column count. "
                f"First source has {first_col_count} columns, "
                f"source {config.sources[i]} has {len(df.columns)} columns.",
                stage=stage_name,
            )

    if debug:
        print(f"    Using first source column names: {list(dfs[0].columns)}")

    # Chain unions (Spark's union is by position)
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)

    return result
