"""Filter handling logic for PlanBuilder.

Filters are specified as SQL expression strings in the YAML configuration.
These expressions are passed directly to Spark's DataFrame.filter() method,
which means you can use any valid Spark SQL expression syntax.

Simple filter examples:
    - "status = 'active'"
    - "amount > 100"
    - "created_date >= '2020-01-01'"

Complex boolean expressions are fully supported:
    - "((x = 1 OR y = 2) AND (b = 1 OR c = 3)) OR (g = 2 AND j = 5)"
    - "category IN ('A', 'B', 'C') AND (price > 50 OR is_premium = true)"
    - "COALESCE(status, 'unknown') != 'deleted' AND created_at IS NOT NULL"

You can also use Spark SQL functions in filter expressions:
    - "YEAR(created_date) = 2024"
    - "LOWER(country_code) = 'us'"
    - "LENGTH(description) > 10"

Filter pushdown (auto-detect):
    Filters with source prefixes can be automatically pushed down to sources
    before joins for better performance:
    - "src.order_date >= '2024-01-01'"  → pushed to 'src' source
    - "dim_customer.is_active = true"   → pushed to 'dim_customer' source
    - "amount > 100"                     → applied after joins (no prefix)

Note: Multiple filters in a stage are combined with AND logic.
For OR logic between top-level conditions, combine them in a single filter string.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from deltagen.model.stage import StageConfig


def apply_filters(
    df: "DataFrame",
    stage: StageConfig,
    sql_parts: dict[str, Any],
    debug: bool = False,
) -> "DataFrame":
    """Apply all filters for a stage.

    Filters are SQL expression strings passed directly to Spark's filter() method.
    This supports full Spark SQL syntax including complex boolean expressions,
    nested conditions, SQL functions, and any valid WHERE clause logic.

    Examples of valid filter expressions:
        - Simple: "status = 'active'"
        - Boolean: "((a = 1 OR b = 2) AND c = 3) OR d = 4"
        - Functions: "YEAR(created_date) = 2024 AND LOWER(status) = 'active'"

    Note: Multiple filters are combined with AND. Use a single filter string
    with OR for top-level OR logic.

    Args:
        df: Input DataFrame
        stage: Stage configuration
        sql_parts: Dictionary to store SQL representations
        debug: If True, print filter details

    Returns:
        DataFrame with filters applied
    """
    if debug and stage.filters:
        print("  FILTERS:")

    for filter_expr in stage.filters:
        df = df.filter(filter_expr)
        sql_parts[stage.name]["where"].append(filter_expr)

        if debug:
            print(f"    - {filter_expr}")

    return df


def extract_source_references(filter_expr: str) -> set[str]:
    """Extract source/alias references from a filter expression.

    Looks for patterns like 'source.column' or 'alias.column' in the filter.
    Returns the set of unique source/alias names found.

    Args:
        filter_expr: SQL filter expression string

    Returns:
        Set of source/alias names referenced in the filter

    Examples:
        >>> extract_source_references("src.order_date >= '2024-01-01'")
        {'src'}
        >>> extract_source_references("src.id = cust.order_id")
        {'src', 'cust'}
        >>> extract_source_references("amount > 100")
        set()
    """
    # Pattern to match identifier.identifier (source.column)
    # Uses lookaround for better SQL identifier boundary detection
    # Matches: src.column, dim_customer.id, t1.field_name
    # Does NOT match: YEAR(date), LOWER(text), function.call()
    pattern = r'(?<![A-Za-z0-9_`"])([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)(?![A-Za-z0-9_`"])'

    # Find all matches
    matches = re.findall(pattern, filter_expr)

    # Extract just the source/alias names (first part of the match)
    sources = set()
    for source_name, column_name in matches:
        # Exclude common SQL functions that might look like source.column
        # These are typically uppercase in SQL
        if source_name.upper() not in {
            'CAST', 'COALESCE', 'NULLIF', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'DATE', 'TIMESTAMP', 'STRING', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE',
            'FLOAT', 'BOOLEAN', 'ARRAY', 'MAP', 'STRUCT'
        }:
            sources.add(source_name)

    return sources


def analyze_filter_pushdown(
    filters: list[str],
    known_sources: set[str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Analyze filters and categorize them for pushdown optimization.

    Determines which filters can be pushed down to individual sources
    (applied before joins) and which must be applied after joins.

    A filter can be pushed down if:
    1. It references exactly one source (via source.column syntax)
    2. That source is in the known_sources set

    Args:
        filters: List of filter expressions
        known_sources: Set of known source names and aliases

    Returns:
        Tuple of:
        - Dictionary mapping source names to list of pushdown filters
        - List of filters that must be applied after joins

    Examples:
        >>> pushdown, post_join = analyze_filter_pushdown(
        ...     ["src.date >= '2024-01-01'", "amount > 100"],
        ...     {"src", "cust"}
        ... )
        >>> pushdown
        {'src': ["src.date >= '2024-01-01'"]}
        >>> post_join
        ['amount > 100']
    """
    pushdown_filters: dict[str, list[str]] = {}
    post_join_filters: list[str] = []

    for filter_expr in filters:
        sources_referenced = extract_source_references(filter_expr)

        # Check if filter references exactly one known source
        known_refs = sources_referenced & known_sources

        if len(known_refs) == 1:
            # Can push down to this single source
            source_name = known_refs.pop()
            if source_name not in pushdown_filters:
                pushdown_filters[source_name] = []
            pushdown_filters[source_name].append(filter_expr)
        else:
            # Multiple sources, no sources, or unknown source - apply after joins
            post_join_filters.append(filter_expr)

    return pushdown_filters, post_join_filters
