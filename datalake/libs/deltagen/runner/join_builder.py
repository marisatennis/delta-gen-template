"""Join handling logic for PlanBuilder."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from deltagen.model.join import JoinConfig
from deltagen.model.source import SourceConfig
from deltagen.model.stage import StageConfig
from deltagen.runner.exceptions import PlanBuilderError


def _get_spark_functions():
    """Lazily import pyspark.sql.functions."""
    from pyspark.sql import functions as F

    return F


def _get_broadcast():
    """Lazily import broadcast function."""
    from pyspark.sql.functions import broadcast

    return broadcast


def apply_joins(
    df: "DataFrame",
    stage: StageConfig,
    sources: dict[str, "DataFrame"],
    source_configs: list[SourceConfig],
    sql_parts: dict[str, Any],
    debug: bool = False,
) -> "DataFrame":
    """Apply all joins for a stage.

    Args:
        df: Input DataFrame
        stage: Stage configuration
        sources: Available source DataFrames
        source_configs: List of source configurations (for broadcast hints)
        sql_parts: Dictionary to store SQL representations
        debug: If True, print join details

    Returns:
        DataFrame with joins applied
    """
    if debug and stage.joins:
        print("  JOINS:")

    for join_config in stage.joins:
        df = apply_single_join(
            df, join_config, sources, source_configs, sql_parts, stage.name, debug
        )

    return df


def apply_single_join(
    df: "DataFrame",
    join_config: JoinConfig,
    sources: dict[str, "DataFrame"],
    source_configs: list[SourceConfig],
    sql_parts: dict[str, Any],
    stage_name: str,
    debug: bool = False,
) -> "DataFrame":
    """Apply a single join.

    Args:
        df: Input DataFrame
        join_config: Join configuration
        sources: Available source DataFrames
        source_configs: List of source configurations (for broadcast hints)
        sql_parts: Dictionary to store SQL representations
        stage_name: Name of the current stage
        debug: If True, print join details

    Returns:
        DataFrame with join applied
    """
    F = _get_spark_functions()

    # Get the DataFrame to join with
    join_df = sources.get(join_config.source)
    if join_df is None:
        raise PlanBuilderError(
            f"Join source '{join_config.source}' not found in sources"
        )

    # Deduplicate the join source to one row per partition before joining.
    # Prevents row explosion when the join key is not unique in the source
    # (e.g. joining d_ifa on FCANumber where multiple postcodes exist per firm).
    # Uses ROW_NUMBER() OVER (PARTITION BY dedupe_by ORDER BY dedupe_order_by).
    if join_config.dedupe_by:
        if not join_config.dedupe_order_by:
            raise PlanBuilderError(
                f"Join '{join_config.name}': 'dedupe_order_by' is required when 'dedupe_by' is set"
            )
        from pyspark.sql import Window
        from pyspark.sql.functions import asc, desc
        order_col = F.col(join_config.dedupe_order_by)
        order_expr = desc(order_col) if join_config.dedupe_order_desc else asc(order_col)
        window = Window.partitionBy(*join_config.dedupe_by).orderBy(order_expr)
        join_df = (
            join_df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        if debug:
            print(f"    - Deduped '{join_config.source}' by {join_config.dedupe_by} "
                  f"order by {join_config.dedupe_order_by} "
                  f"({'desc' if join_config.dedupe_order_desc else 'asc'})")

    # Determine if we should broadcast this join
    should_broadcast = _should_broadcast(join_config, source_configs)

    # Detect duplicate columns and apply alias renaming BEFORE building conditions.
    # This is critical: if we rename join_df columns after capturing Column references
    # from it, Spark cannot resolve those references against the renamed DataFrame.
    left_cols = set(df.columns)
    right_cols = set(join_df.columns)
    duplicate_cols = left_cols.intersection(right_cols)

    renamed: dict[str, str] = {}
    if join_config.alias:
        alias = join_config.alias

        for col_name in duplicate_cols:
            new_name = f"{alias}__{col_name}"
            join_df = join_df.withColumnRenamed(col_name, new_name)
            renamed[col_name] = new_name

        direct: list[str] = [c for c in join_df.columns if c not in renamed.values()]

        if debug and renamed:
            print(f"      Alias '{alias}': renamed duplicates {sorted(renamed.keys())} → {sorted(renamed.values())}")

        # Store rename info so column_builder can preprocess alias.col expressions
        stage_parts = sql_parts.setdefault(stage_name, {})
        alias_maps = stage_parts.setdefault("join_alias_maps", {})
        alias_maps[alias] = {"renamed": renamed, "direct": direct}

    # Collect alias maps from all previous joins in this stage (already in sql_parts)
    # so we can resolve "alias.column" references on the left side of conditions.
    stage_alias_maps: dict[str, dict] = sql_parts.get(stage_name, {}).get("join_alias_maps", {})

    # Build join conditions against the (potentially renamed) join_df
    conditions = []
    condition_strs = []

    for cond in join_config.conditions:
        # Resolve left side: supports both bare column names and "alias.column" notation.
        # "alias.column" is resolved via the alias maps built by preceding joins so that
        # a column introduced by an earlier join can be referenced explicitly.
        if "." in cond.left:
            from deltagen.runner.column_builder import _preprocess_expression
            resolved_left = _preprocess_expression(cond.left, stage_alias_maps)
            left_col = df[resolved_left]
        else:
            left_col = df[cond.left]
        # If the right column was renamed due to alias dedup, use the new name
        right_col_name = renamed.get(cond.right, cond.right)
        right_col = join_df[right_col_name]

        if cond.operator == "=":
            conditions.append(left_col == right_col)
        elif cond.operator == "!=":
            conditions.append(left_col != right_col)
        elif cond.operator == "<":
            conditions.append(left_col < right_col)
        elif cond.operator == "<=":
            conditions.append(left_col <= right_col)
        elif cond.operator == ">":
            conditions.append(left_col > right_col)
        elif cond.operator == ">=":
            conditions.append(left_col >= right_col)
        else:
            # Default to equals
            conditions.append(left_col == right_col)

        condition_strs.append(f"{cond.left} {cond.operator} {cond.right}")

    # Combine conditions with AND
    if conditions:
        join_condition = conditions[0]
        for c in conditions[1:]:
            join_condition = join_condition & c
    else:
        # Cross join if no conditions
        join_condition = F.lit(True)

    # Map join type
    join_type_map = {
        "inner": "inner",
        "left": "left",
        "right": "right",
        "full": "outer",
        "cross": "cross",
    }
    spark_join_type = join_type_map.get(join_config.type, "inner")

    broadcast_hint = " (BROADCAST)" if should_broadcast else ""
    if debug:
        print(f"    - {join_config.type.upper()} JOIN {join_config.source}{broadcast_hint}")
        print(f"      ON {' AND '.join(condition_strs)}")

    # Store SQL representation
    sql_join = (
        f"{join_config.type.upper()} JOIN {join_config.source}{broadcast_hint}\n"
        f"    ON {' AND '.join(condition_strs)}"
    )
    if stage_name in sql_parts:
        sql_parts[stage_name]["joins"].append(sql_join)

    # Apply broadcast hint if configured
    if should_broadcast:
        broadcast_fn = _get_broadcast()
        join_df = broadcast_fn(join_df)

    result = df.join(join_df, join_condition, spark_join_type)

    # When no alias is set, duplicate columns are not renamed before the join.
    # Resolve ambiguity by explicitly keeping the left side for every duplicate.
    if not join_config.alias and duplicate_cols:
        if debug:
            print(f"      Duplicate columns detected: {sorted(duplicate_cols)}")
            print(f"      Keeping from left, dropping from {join_config.source}")

        cols_to_select = []
        seen = set()

        for col_name in result.columns:
            if col_name in seen:
                continue
            seen.add(col_name)

            if col_name in duplicate_cols:
                cols_to_select.append(df[col_name].alias(col_name))
            else:
                cols_to_select.append(F.col(col_name))

        result = result.select(*cols_to_select)

    return result


def _should_broadcast(
    join_config: JoinConfig,
    source_configs: list[SourceConfig],
) -> bool:
    """Determine if a join should use broadcast.

    Priority:
    1. If join_config.broadcast is explicitly set, use that
    2. Otherwise, check the source's broadcast setting

    Args:
        join_config: Join configuration
        source_configs: List of source configurations

    Returns:
        True if broadcast should be used
    """
    # Explicit join-level setting takes priority
    if join_config.broadcast is not None:
        return join_config.broadcast

    # Check source-level setting
    source_config = next(
        (s for s in source_configs if s.name == join_config.source),
        None
    )
    if source_config and source_config.broadcast:
        return True

    return False
