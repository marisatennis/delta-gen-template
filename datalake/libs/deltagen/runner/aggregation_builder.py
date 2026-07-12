"""Aggregation and GROUP BY handling logic for PlanBuilder."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column

from deltagen.model.stage import StageConfig, GroupByConfig, AggregationConfig
from deltagen.runner.exceptions import PlanBuilderError


def _get_spark_functions():
    """Lazily import pyspark.sql.functions."""
    from pyspark.sql import functions as F

    return F


def apply_group_by(
    df: "DataFrame",
    stage: StageConfig,
    sql_parts: dict[str, Any],
    debug: bool = False,
) -> "DataFrame":
    """Apply GROUP BY and aggregations to a DataFrame.

    Args:
        df: Input DataFrame
        stage: Stage configuration containing group_by config
        sql_parts: Dictionary to store SQL representations
        debug: If True, print aggregation details

    Returns:
        Aggregated DataFrame

    Raises:
        PlanBuilderError: If aggregation configuration is invalid
    """
    group_by_config = stage.group_by
    if group_by_config is None:
        raise PlanBuilderError(
            "apply_group_by called but no group_by config defined",
            stage=stage.name,
        )

    if debug:
        print("  GROUP BY:")
        print(f"    Columns: {group_by_config.columns}")
        if group_by_config.aggregations:
            print(f"    Aggregations: {len(group_by_config.aggregations)}")
        if group_by_config.having:
            print(f"    Having: {group_by_config.having}")

    F = _get_spark_functions()

    # Build group by columns
    group_cols = [F.col(col) for col in group_by_config.columns]

    # Build aggregation expressions
    agg_exprs = _build_aggregation_expressions(
        group_by_config.aggregations,
        stage.name,
        debug,
    )

    # Apply groupBy and aggregations
    if agg_exprs:
        result = df.groupBy(*group_cols).agg(*agg_exprs)
    else:
        # If no aggregations specified, just do a distinct on group columns
        result = df.select(*group_cols).distinct()

    # Apply HAVING filters
    if group_by_config.having:
        for having_expr in group_by_config.having:
            if debug:
                print(f"    Applying HAVING: {having_expr}")
            result = result.filter(having_expr)

    # Store SQL representation
    agg_strs = []
    for agg in group_by_config.aggregations:
        distinct_str = "DISTINCT " if agg.distinct else ""
        agg_strs.append(f"{agg.function.upper()}({distinct_str}{agg.column}) AS {agg.alias}")

    sql_group_by = f"GROUP BY {', '.join(group_by_config.columns)}"
    if agg_strs:
        sql_group_by = f"{', '.join(agg_strs)}\n{sql_group_by}"
    if group_by_config.having:
        sql_group_by += f"\nHAVING {' AND '.join(group_by_config.having)}"

    if stage.name in sql_parts:
        sql_parts[stage.name]["group_by"] = sql_group_by

    if debug:
        print(f"    Result columns: {list(result.columns)}")

    return result


def _build_aggregation_expressions(
    aggregations: list[AggregationConfig],
    stage_name: str,
    debug: bool = False,
) -> list["Column"]:
    """Build Spark aggregation expressions from config.

    Args:
        aggregations: List of aggregation configurations
        stage_name: Name of the current stage
        debug: If True, print details

    Returns:
        List of Spark Column expressions for aggregation
    """
    F = _get_spark_functions()

    agg_exprs: list["Column"] = []

    for agg in aggregations:
        expr = _build_single_aggregation(agg, stage_name, F)
        agg_exprs.append(expr.alias(agg.alias))

        if debug:
            distinct_str = "DISTINCT " if agg.distinct else ""
            print(f"      - {agg.function.upper()}({distinct_str}{agg.column}) AS {agg.alias}")

    return agg_exprs


def _build_single_aggregation(
    agg: AggregationConfig,
    stage_name: str,
    F: Any,
) -> "Column":
    """Build a single Spark aggregation expression.

    Supports common aggregation functions:
    - sum, avg, mean, min, max, count
    - first, last
    - collect_list, collect_set
    - stddev, stddev_pop, stddev_samp
    - variance, var_pop, var_samp
    - approx_count_distinct
    - countDistinct (alternative to count with distinct=True)

    Args:
        agg: Aggregation configuration
        stage_name: Name of the current stage
        F: pyspark.sql.functions module

    Returns:
        Spark Column expression

    Raises:
        PlanBuilderError: If aggregation function is not supported
    """
    func_name = agg.function.lower()
    col_expr = F.col(agg.column)

    # Handle distinct versions
    if agg.distinct:
        if func_name == "count":
            return F.countDistinct(col_expr)
        elif func_name == "sum":
            return F.sumDistinct(col_expr)
        elif func_name in ("avg", "mean"):
            # No built-in avgDistinct, use sum/count
            return F.sumDistinct(col_expr) / F.countDistinct(col_expr)
        # For other functions, apply distinct doesn't make sense or isn't supported
        # Fall through to regular function

    # Standard aggregation functions
    agg_functions = {
        "sum": F.sum,
        "avg": F.avg,
        "mean": F.mean,
        "min": F.min,
        "max": F.max,
        "count": F.count,
        "first": F.first,
        "last": F.last,
        "collect_list": F.collect_list,
        "collect_set": F.collect_set,
        "stddev": F.stddev,
        "stddev_pop": F.stddev_pop,
        "stddev_samp": F.stddev_samp,
        "variance": F.variance,
        "var_pop": F.var_pop,
        "var_samp": F.var_samp,
        "approx_count_distinct": F.approx_count_distinct,
        "count_distinct": F.countDistinct,
        "countdistinct": F.countDistinct,
    }

    if func_name in agg_functions:
        return agg_functions[func_name](col_expr)

    # Check for expression-based aggregation
    # If column looks like an expression (contains operators), use expr
    if any(op in agg.column for op in ["+", "-", "*", "/", "(", ")"]):
        expr_col = F.expr(agg.column)
        if func_name in agg_functions:
            return agg_functions[func_name](expr_col)

    raise PlanBuilderError(
        f"Unsupported aggregation function: {agg.function}. "
        f"Supported functions: {', '.join(sorted(agg_functions.keys()))}",
        stage=stage_name,
    )
