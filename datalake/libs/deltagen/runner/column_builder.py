"""Column building logic for PlanBuilder."""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column

    from deltagen.plugins.context import PluginContext

from deltagen.model.column import ColumnConfig, ColumnInput
from deltagen.model.source import SourceConfig
from deltagen.model.stage import StageConfig
from deltagen.runner.exceptions import PlanBuilderError

logger = logging.getLogger(__name__)


def _get_spark_functions():
    """Lazily import pyspark.sql.functions."""
    from pyspark.sql import functions as F

    return F


def _preprocess_expression(expression: str, alias_maps: dict[str, dict]) -> str:
    """Rewrite alias.col references using the join alias rename map.

    For each join alias, two substitutions are applied (in order):
      - ``alias.col`` → ``alias__col``  when *col* is a renamed duplicate
      - ``alias.col`` → ``col``          when *col* is a direct (non-duplicate) right column

    This lets YAML authors write natural ``alias.col`` syntax in expressions
    regardless of whether the column was renamed after the join.
    """
    import re

    for alias, maps in alias_maps.items():
        # Renamed duplicate columns: alias.col → alias__col
        for orig_col, renamed_col in maps.get("renamed", {}).items():
            expression = re.sub(
                rf'\b{re.escape(alias)}\.{re.escape(orig_col)}\b',
                renamed_col,
                expression,
            )
        # Direct (non-duplicate) columns: alias.col → col
        for col in maps.get("direct", []):
            expression = re.sub(
                rf'\b{re.escape(alias)}\.{re.escape(col)}\b',
                col,
                expression,
            )
    return expression


def build_columns(
    df: "DataFrame",
    stage: StageConfig,
    sources: dict[str, "DataFrame"],
    sql_parts: dict[str, Any],
    debug: bool = False,
    context: "PluginContext | None" = None,
) -> "DataFrame":
    """Build all columns for a stage.

    Args:
        df: Input DataFrame
        stage: Stage configuration
        sources: Available source DataFrames
        sql_parts: Dictionary to store SQL representations
        debug: If True, print column details
        context: Optional PluginContext for column plugin invocation

    Returns:
        DataFrame with new columns selected
    """
    columns = []

    if debug:
        print("\n  Building columns:")
        print("  " + "-" * 50)

    # Collect columns that need plugin transforms
    columns_with_transforms: list[ColumnConfig] = []

    # For union stages, infer column inputs from column names when not specified
    infer_from_name = stage.unions is not None

    # Collect any join alias maps stored by join_builder for this stage
    alias_maps: dict[str, dict] = sql_parts.get(stage.name, {}).get("join_alias_maps", {})

    for idx, col_config in enumerate(stage.columns, 1):
        try:
            col_expr = build_single_column(col_config, sources, infer_from_name, alias_maps)
            columns.append(col_expr.alias(col_config.name))

            sql_expr = column_to_sql(col_config)
            sql_parts[stage.name]["select"].append(f"{sql_expr} AS {col_config.name}")

            if debug:
                # Build input description
                if col_config.inputs:
                    inp = col_config.inputs[0]
                    if inp.expression:
                        input_desc = f"expr: {inp.expression[:50]}{'...' if len(inp.expression) > 50 else ''}"
                    elif inp.source and inp.column:
                        input_desc = f"{inp.source}.{inp.column}"
                    else:
                        input_desc = inp.column or "literal"
                elif infer_from_name:
                    input_desc = f"{col_config.name} (inferred from union)"
                else:
                    input_desc = "no input"

                type_info = f" -> {col_config.data_type}" if col_config.data_type else ""
                transform_info = f" [plugin: {col_config.extensions.get('transform')}]" if col_config.extensions.get("transform") else ""
                natural_info = " (natural key)" if col_config.natural else ""

                print(f"    [{idx}/{len(stage.columns)}] {col_config.name}: {input_desc}{type_info}{transform_info}{natural_info}")

            # Track columns with transform plugins
            if col_config.extensions.get("transform"):
                columns_with_transforms.append(col_config)

        except Exception as e:
            if debug:
                print(f"    [{idx}/{len(stage.columns)}] {col_config.name}: FAILED - {str(e)}")
            raise PlanBuilderError(
                "Failed to build column",
                stage=stage.name,
                column=col_config.name,
                detail=str(e),
            )

    if debug:
        print("  " + "-" * 50)
        print(f"  Applying SELECT with {len(columns)} columns...")

    result_df = df.select(*columns)

    # Apply column transform plugins after selecting columns
    if columns_with_transforms and debug:
        print(f"\n  Applying {len(columns_with_transforms)} column plugin(s):")

    for col_config in columns_with_transforms:
        result_df = _invoke_column_plugin(
            result_df, col_config, stage.name, context, debug
        )

    return result_df


def _invoke_column_plugin(
    df: "DataFrame",
    col_config: ColumnConfig,
    stage_name: str,
    context: "PluginContext | None",
    debug: bool = False,
) -> "DataFrame":
    """Invoke a column plugin by name.

    Args:
        df: Input DataFrame
        col_config: Column configuration with transform in extensions
        stage_name: Name of the current stage (for error context)
        context: Plugin context for metrics and state
        debug: If True, print plugin details

    Returns:
        Transformed DataFrame from the plugin
    """
    from deltagen.plugins.registry import get_column_plugin

    plugin_name = col_config.extensions.get("transform")
    if not plugin_name:
        return df

    plugin = get_column_plugin(plugin_name)
    if not plugin:
        logger.warning(
            f"Column plugin '{plugin_name}' not found in registry, skipping"
        )
        return df

    if debug:
        on_null = col_config.extensions.get("on_null", "default")
        print(f"    - {col_config.name}: {plugin_name} (on_null={on_null})")

    # Create a null context if none provided
    if context is None:
        from deltagen.plugins.context import create_null_context

        context = create_null_context()

    try:
        result = plugin(df, col_config, context)
        if debug:
            print(f"      ✓ Applied")
        return result
    except Exception as e:
        raise PlanBuilderError(
            f"Column plugin '{plugin_name}' failed for column '{col_config.name}'",
            stage=stage_name,
            detail=str(e),
        )


def build_single_column(
    col_config: ColumnConfig,
    sources: dict[str, "DataFrame"],
    infer_from_name: bool = False,
    alias_maps: dict[str, dict] | None = None,
) -> "Column":
    """Build a single column expression.

    Args:
        col_config: Column configuration
        sources: Available source DataFrames
        infer_from_name: If True and no inputs specified, infer input from column name.
                         Useful for union stages where columns pass through directly.

    Returns:
        Spark Column expression
    """
    F = _get_spark_functions()

    if not col_config.inputs:
        if infer_from_name:
            # For union stages: infer input from the column's own name
            col_expr = F.col(col_config.name)
            # Apply default and type casting below
        elif col_config.default is not None:
            return F.lit(col_config.default)
        else:
            return F.lit(None)
    elif len(col_config.inputs) == 1:
        # Single input
        col_expr = build_column_input(col_config.inputs[0], alias_maps)
    else:
        # Multiple inputs - concatenate with delimiter
        delimiter = col_config.extensions.get("delimiter", "||")
        input_exprs = [build_column_input(inp, alias_maps) for inp in col_config.inputs]
        col_expr = F.concat_ws(delimiter, *input_exprs)

    # Apply default value
    if col_config.default is not None:
        col_expr = F.coalesce(col_expr, F.lit(col_config.default))

    # Apply type casting
    typespec = col_config.get_typespec()
    if typespec:
        spark_type = typespec.to_spark_sql()
        col_expr = col_expr.cast(spark_type)

    return col_expr


def build_column_input(col_input: ColumnInput, alias_maps: dict[str, dict] | None = None) -> "Column":
    """Build a column expression from a single input.

    Args:
        col_input: Column input configuration
        alias_maps: Optional mapping of join aliases → rename info, used to
                    preprocess ``alias.col`` references in SQL expressions.

    Returns:
        Spark Column expression
    """
    F = _get_spark_functions()

    if col_input.expression:
        expr = col_input.expression
        if alias_maps:
            expr = _preprocess_expression(expr, alias_maps)
        return F.expr(expr)
    elif col_input.source and col_input.column:
        return F.col(f"{col_input.source}.{col_input.column}")
    elif col_input.column:
        return F.col(col_input.column)
    else:
        raise ValueError("ColumnInput has neither expression nor column")


def column_to_sql(col_config: ColumnConfig) -> str:
    """Convert column config to SQL representation.

    Args:
        col_config: Column configuration

    Returns:
        SQL expression string
    """
    if not col_config.inputs:
        if col_config.default is not None:
            return repr(col_config.default)
        return "NULL"

    if len(col_config.inputs) == 1:
        inp = col_config.inputs[0]
        if inp.expression:
            expr = inp.expression
        elif inp.source and inp.column:
            expr = f"{inp.source}.{inp.column}"
        else:
            expr = inp.column or "NULL"
    else:
        delimiter = col_config.extensions.get("delimiter", "||")
        parts = []
        for inp in col_config.inputs:
            if inp.expression:
                parts.append(inp.expression)
            elif inp.source and inp.column:
                parts.append(f"{inp.source}.{inp.column}")
            else:
                parts.append(inp.column or "NULL")
        expr = f"CONCAT_WS('{delimiter}', {', '.join(parts)})"

    # Add COALESCE for default
    if col_config.default is not None:
        expr = f"COALESCE({expr}, {repr(col_config.default)})"

    # Add CAST for type
    typespec = col_config.get_typespec()
    if typespec:
        expr = f"CAST({expr} AS {typespec.to_spark_sql()})"

    return expr


def _extract_column_refs_from_expression(expression: str) -> list[tuple[str, str]]:
    """Extract source.column references from a SQL expression.

    Args:
        expression: SQL expression string

    Returns:
        List of (source, column) tuples found in the expression

    Examples:
        >>> _extract_column_refs_from_expression("src.amount * fx.rate")
        [('src', 'amount'), ('fx', 'rate')]
    """
    # Pattern to match source.column references
    # Uses lookaround for better SQL identifier boundary detection
    pattern = r'(?<![A-Za-z0-9_`"])([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)(?![A-Za-z0-9_`"])'
    matches = re.findall(pattern, expression)

    # Filter out SQL function patterns
    sql_keywords = {
        'CAST', 'COALESCE', 'NULLIF', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'DATE', 'TIMESTAMP', 'STRING', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE',
        'FLOAT', 'BOOLEAN', 'ARRAY', 'MAP', 'STRUCT'
    }

    return [
        (source, column)
        for source, column in matches
        if source.upper() not in sql_keywords
    ]


def analyze_required_columns(
    stages: list[StageConfig],
    sources: list[SourceConfig],
) -> dict[str, set[str]]:
    """Analyze stages to determine which columns are needed from each source.

    Examines all column inputs, join conditions, filters, and expressions
    to build a map of required columns per source.

    Args:
        stages: List of stage configurations to analyze
        sources: List of source configurations (for name/alias mapping)

    Returns:
        Dictionary mapping source names to sets of required column names

    Example:
        >>> required = analyze_required_columns(stages, sources)
        >>> required
        {
            'raw_orders': {'order_id', 'customer_id', 'amount'},
            'dim_customer': {'customer_id', 'customer_name'}
        }
    """
    # Build alias -> source_name mapping.
    # Covers both source-level aliases (SourceConfig.alias) and join-level
    # aliases (JoinConfig.alias), so that "alias.column" references in join
    # conditions are correctly resolved to the underlying source for pruning.
    source_aliases: dict[str, str] = {}  # alias -> source_name
    for source in sources:
        if source.alias:
            source_aliases[source.alias] = source.name
    for stage in stages:
        for join in stage.joins:
            if join.alias:
                source_aliases[join.alias] = join.source

    # Track required columns per source
    required: dict[str, set[str]] = {}

    def add_column(source_or_alias: str, column: str) -> None:
        """Add a column requirement, resolving alias to source name."""
        # Resolve alias to source name if needed
        source_name = source_aliases.get(source_or_alias, source_or_alias)
        if source_name not in required:
            required[source_name] = set()
        required[source_name].add(column)

    def process_expression(expression: str) -> None:
        """Extract column references from a SQL expression."""
        for source, column in _extract_column_refs_from_expression(expression):
            add_column(source, column)

    for stage in stages:
        # 1. Analyze column inputs
        for col_config in stage.columns:
            for col_input in col_config.inputs:
                if col_input.source and col_input.column:
                    # Direct source.column reference
                    add_column(col_input.source, col_input.column)
                elif col_input.expression:
                    # Parse expression for source.column patterns
                    process_expression(col_input.expression)

        # 2. Analyze join conditions
        for join_idx, join in enumerate(stage.joins):
            for condition in join.conditions:
                # Parse left side (e.g., "src.customer_id" or "customer_id")
                if '.' in condition.left:
                    source, column = condition.left.split('.', 1)
                    add_column(source, column)
                else:
                    # Bare left-side column (no dot) comes from the initial stage
                    # DataFrame and is always available — no pruning action needed.
                    # To reference a column introduced by a preceding join, use
                    # "alias.column" notation instead (e.g. "mapping_ifa.ul_model_id").
                    pass

                # Parse right side: prefixed ("cust.id") or bare ("id").
                # Bare right-side columns always belong to the join's source —
                # without this, column pruning would omit them and Spark would
                # fail to resolve the column at join time.
                if '.' in condition.right:
                    source, column = condition.right.split('.', 1)
                    add_column(source, column)
                else:
                    add_column(join.source, condition.right)

        # 3. Analyze filters
        for filter_expr in stage.filters:
            process_expression(filter_expr)

        # 4. Analyze source_filters
        if stage.source_filters:
            for source_name, filters in stage.source_filters.items():
                for filter_expr in filters:
                    # Source filters reference their own source
                    for _, column in _extract_column_refs_from_expression(filter_expr):
                        add_column(source_name, column)
                    # Also check for unprefixed column names in simple expressions
                    # e.g., "is_current = true" → column is "is_current"
                    # Simple pattern: identifier at start of comparison
                    simple_match = re.match(
                        r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]', filter_expr
                    )
                    if simple_match:
                        add_column(source_name, simple_match.group(1))

        # 5. Analyze union column_maps
        # column_map values are arbitrary SQL expressions — reliably extracting every
        # column reference would require a full SQL parser. Instead, any source that
        # has a column_map is excluded from pruning so it arrives with all its columns
        # intact. Cross-source refs (other_source.col) are still collected for those
        # other sources since those expressions are already handled by the extractor.
        if stage.unions:
            no_prune_sources: set[str] = set()
            for union_source in stage.unions.sources:
                if union_source.column_map:
                    no_prune_sources.add(union_source.name)
                    for expr in union_source.column_map.values():
                        for src, col in _extract_column_refs_from_expression(expr):
                            if src != union_source.name:
                                add_column(src, col)
            # Drop any previously accumulated requirements for no-prune sources so
            # the pruner falls through to loading all columns for them.
            for src_name in no_prune_sources:
                required.pop(src_name, None)

    return required
