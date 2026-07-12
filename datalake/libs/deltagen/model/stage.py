"""Stage configuration models."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from .base import StrictBaseModel
from .column import ColumnConfig
from .join import JoinConfig

StageMode = Literal["definition", "transformation", "output"]
UnionMode = Literal["by_name", "by_position"]


class UnionSource(StrictBaseModel):
    """A single source entry in a union configuration.

    Attributes:
        name: Source name or alias to union
        column_map: Optional mapping of target_column -> SQL expression applied
                    to this source before unioning. Supports simple renames
                    (bare column name) or full SQL expressions (casts, literals,
                    CASE WHEN, etc.). Original source columns whose name matches
                    a simple bare-identifier value are dropped automatically.

    Examples:
        Simple rename::

            - name: adl_tranx
              column_map:
                amount: transaction_value
                trade_date: settlement_date

        With expressions::

            - name: ssnc_tranx
              column_map:
                amount: "CAST(net_flow AS DECIMAL(18,4))"
                source_system: "'SSNC'"

        No mapping needed (pass-through)::

            - name: normalised_source
    """

    name: str
    column_map: dict[str, str] | None = None  # target_col: sql_expression


class UnionConfig(StrictBaseModel):
    """Configuration for unioning multiple sources in a stage.

    Allows combining multiple DataFrames using UNION ALL or UNION DISTINCT.

    Attributes:
        sources: List of sources to union. Each entry can be a plain string
                 (source name, no column mapping) or a UnionSource object with
                 an optional column_map for per-source column renaming/transformation.
        mode: How to match columns - 'by_name' (default) or 'by_position'
        distinct: If True, remove duplicates (UNION DISTINCT). Default False (UNION ALL)
        allow_missing_columns: If True, missing columns are filled with NULL. Default False

    Examples:
        Basic union — plain strings still work (backward compatible)::

            unions:
              sources: [raw_sales_2023, raw_sales_2024]

        Union with per-source column mapping::

            unions:
              sources:
                - name: adl_tranx
                  column_map:
                    amount: transaction_value
                    trade_date: "CAST(settlement_date AS DATE)"
                - name: ssnc_tranx          # no column_map needed
              mode: by_name
              allow_missing_columns: true

        Union by position (legacy sources)::

            unions:
              sources: [legacy_data, new_data]
              mode: by_position
    """

    sources: list[UnionSource] = Field(..., min_length=2)
    mode: UnionMode = "by_name"
    distinct: bool = False
    allow_missing_columns: bool = False

    @model_validator(mode="before")
    @classmethod
    def _coerce_sources(cls, data: object) -> object:
        """Allow plain strings in sources list for backward compatibility.

        Accepts both::

            sources: [source_a, source_b]           # list of strings
            sources:
              - name: source_a
                column_map: {amount: net_flow}       # list of objects
        """
        if isinstance(data, dict) and "sources" in data:
            coerced = []
            for s in data["sources"]:
                if isinstance(s, str):
                    coerced.append({"name": s})
                else:
                    coerced.append(s)
            data["sources"] = coerced
        return data


class AggregationConfig(StrictBaseModel):
    """Configuration for an aggregation function on a column.

    Defines how to aggregate a column within a GROUP BY operation.

    Attributes:
        column: Source column name or expression to aggregate
        function: Aggregation function (sum, avg, count, min, max, first, last, collect_list, etc.)
        alias: Output column name for the aggregated result
        distinct: If True, aggregate only distinct values (e.g., COUNT DISTINCT)

    Examples:
        Sum of sales::

            aggregations:
              - column: amount
                function: sum
                alias: total_amount

        Count distinct customers::

            aggregations:
              - column: customer_id
                function: count
                alias: customer_count
                distinct: true

        Expression-based aggregation::

            aggregations:
              - column: "price * quantity"
                function: sum
                alias: total_revenue
    """

    column: str
    function: str
    alias: str | None = None
    distinct: bool = False

    @model_validator(mode="after")
    def _set_default_alias(self) -> "AggregationConfig":
        """Set default alias if not provided."""
        if self.alias is None:
            # Generate alias from function and column
            object.__setattr__(self, "alias", f"{self.function}_{self.column.replace('.', '_')}")
        return self


class GroupByConfig(StrictBaseModel):
    """Configuration for GROUP BY operations in a stage.

    Enables aggregation of data by grouping on specified columns.

    Attributes:
        columns: List of column names or expressions to group by
        aggregations: List of aggregation configurations
        having: Optional list of HAVING clause filters (applied after aggregation)

    Examples:
        Group by region with aggregations::

            group_by:
              columns: [region, product_category]
              aggregations:
                - column: sales_amount
                  function: sum
                  alias: total_sales
                - column: order_id
                  function: count
                  alias: order_count

        With HAVING clause::

            group_by:
              columns: [customer_id]
              aggregations:
                - column: amount
                  function: sum
                  alias: total_spent
              having:
                - "total_spent > 1000"
    """

    columns: list[str] = Field(..., min_length=1)
    aggregations: list[AggregationConfig] = Field(default_factory=list)
    having: list[str] = Field(default_factory=list)


class StageConfig(StrictBaseModel):
    """Represents a logical transformation stage in a table plan.

    A stage is a logical step in a transformation pipeline. Stages are processed
    sequentially, with each stage's output becoming the next stage's input.

    Processing order within a stage:
    1. Unions (if configured) - combine multiple sources
    2. Source filters (pushdown) - filter sources before joins
    3. Joins - join with lookup/dimension tables
    4. Columns - build/transform columns
    5. Group by (if configured) - aggregate data
    6. Filters - apply post-transformation filters
    7. Stage plugin - apply custom transformations

    Attributes:
        name: Unique identifier for this stage
        mode: Stage mode - 'definition', 'transformation', or 'output'
        description: Optional human-readable description
        columns: Column definitions and transformations
        joins: Join configurations for combining with other sources
        filters: Post-transformation filter expressions
        source_filters: Pre-join filters applied to specific sources
        unions: Configuration for unioning multiple sources
        group_by: Configuration for GROUP BY aggregations
        tags: Optional tags for categorization
    """

    name: str
    mode: StageMode = "transformation"
    description: str | None = None
    columns: list[ColumnConfig] = Field(default_factory=list)
    joins: list[JoinConfig] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    source_filters: dict[str, list[str]] | None = None  # Filters applied to sources BEFORE joins
    unions: UnionConfig | None = None
    group_by: GroupByConfig | None = None
    tags: set[str] = Field(default_factory=set)

    def temporary_columns(self) -> tuple[str, ...]:
        """Return the names of temporary columns defined in this stage."""
        return tuple(column.name for column in self.columns if column.temporary)

    def filter_columns(self, **kwargs: Any) -> tuple[ColumnConfig, ...]:
        """Filter columns by any attribute (core or extension).

        Filters columns based on provided keyword arguments. Checks both core
        attributes (name, data_type, temporary, natural, nullable, etc.) and
        extension attributes stored in the ``extensions`` dict.

        Args:
            **kwargs: Attribute name-value pairs to filter by. Can reference
                      core attributes or extension keys.

        Returns:
            Tuple of ColumnConfig objects matching all filter criteria.

        Examples:
            >>> stage.filter_columns(temporary=False)
            # Returns columns where temporary=False

            >>> stage.filter_columns(natural=True, temporary=False)
            # Returns columns where natural=True AND temporary=False

            >>> stage.filter_columns(pii=True)
            # Returns columns where extensions["pii"]=True
        """
        if not kwargs:
            return tuple(self.columns)

        filtered = []
        for column in self.columns:
            # Check if column matches all filter criteria
            matches = True
            for attr_name, expected_value in kwargs.items():
                # First try core attributes
                try:
                    actual_value = getattr(column, attr_name)
                except AttributeError:
                    # If not a core attribute, check extensions
                    actual_value = column.extensions.get(attr_name, None)

                if actual_value != expected_value:
                    matches = False
                    break

            if matches:
                filtered.append(column)

        return tuple(filtered)

    def get_persistent_columns(self) -> tuple[ColumnConfig, ...]:
        """Return columns that should be persisted (excludes temporary columns).

        This is a convenience method equivalent to filter_columns(temporary=False).
        Used by the Writer to determine which columns to write to storage.

        Returns:
            Tuple of non-temporary ColumnConfig objects.

        Examples:
            >>> stage.get_persistent_columns()
            # Returns all columns except those marked temporary=True
        """
        return self.filter_columns(temporary=False)

    def get_natural_key_columns(self) -> tuple[ColumnConfig, ...]:
        """Return columns that are part of the natural key.

        This is a convenience method equivalent to filter_columns(natural=True).
        Used for deduplication, merge operations, and identifying unique records.

        Returns:
            Tuple of ColumnConfig objects marked as natural keys.

        Examples:
            >>> stage.get_natural_key_columns()
            # Returns columns where natural=True
        """
        return self.filter_columns(natural=True)
