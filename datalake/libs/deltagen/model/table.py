"""Top-level table configuration model."""
from __future__ import annotations

from typing import Any, Iterable

from pydantic import Field

from .base import StrictBaseModel
from .column import ColumnConfig
from .incremental import DQConfig, IncrementalConfig
from .policies import PoliciesConfig
from .source import SourceConfig
from .stage import StageConfig


class TableConfig(StrictBaseModel):
    """Represents an entire table plan, decoupled from any execution engine."""

    # Core identifiers from the table metadata
    name: str
    layer: str | None = None  # Semantic layer (bronze/silver/gold) - for documentation/filtering
    target_schema: str | None = None  # Physical schema for target table (e.g., "sharepoint")
    natural_id: str | None = None
    description: str | None = None
    sources: list[SourceConfig] = Field(default_factory=list)
    stages: list[StageConfig] = Field(default_factory=list)
    policies: PoliciesConfig = Field(default_factory=PoliciesConfig)

    # Incremental loading configuration
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)

    # Data quality logging configuration
    dq: DQConfig = Field(default_factory=DQConfig)

    # Lightweight tagging for observability/governance
    tags: set[str] = Field(default_factory=set)

    def iter_columns(self) -> Iterable[ColumnConfig]:
        """Yield every column across all stages."""

        for stage in self.stages:
            for column in stage.columns:
                yield column

    @property
    def temporary_columns(self) -> tuple[str, ...]:
        """Return the names of all temporary columns declared anywhere in the plan."""

        return tuple(column.name for column in self.iter_columns() if column.temporary)

    def filter_columns(self, **kwargs: Any) -> tuple[ColumnConfig, ...]:
        """Filter columns across all stages by any attribute (core or extension).

        Aggregates columns from all stages and filters based on provided keyword
        arguments. Checks both core attributes (name, data_type, temporary, natural,
        nullable, etc.) and extension attributes stored in the ``extensions`` dict.

        Args:
            **kwargs: Attribute name-value pairs to filter by. Can reference
                      core attributes or extension keys.

        Returns:
            Tuple of ColumnConfig objects matching all filter criteria from all stages.

        Examples:
            >>> table.filter_columns(temporary=False)
            # Returns all non-temporary columns across all stages

            >>> table.filter_columns(natural=True, temporary=False)
            # Returns columns where natural=True AND temporary=False

            >>> table.filter_columns(pii=True)
            # Returns columns where extensions["pii"]=True
        """
        if not kwargs:
            return tuple(self.iter_columns())

        filtered = []
        for column in self.iter_columns():
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
        Used by the Writer (DG-7) to determine which columns to write to storage.

        Returns:
            Tuple of non-temporary ColumnConfig objects from all stages.

        Examples:
            >>> table.get_persistent_columns()
            # Returns all columns except those marked temporary=True
        """
        return self.filter_columns(temporary=False)

    def get_natural_key_columns(self) -> tuple[ColumnConfig, ...]:
        """Return columns that are part of the natural key.

        This is a convenience method equivalent to filter_columns(natural=True).
        Used for deduplication, merge operations, and identifying unique records.

        Returns:
            Tuple of ColumnConfig objects marked as natural keys from all stages.

        Examples:
            >>> table.get_natural_key_columns()
            # Returns columns where natural=True
        """
        return self.filter_columns(natural=True)

    def get_target_table_name(self) -> str:
        """Get the fully qualified target table name.

        Combines layer/target_schema with table name to form the target.

        Returns:
            Table name like "silver.customer_dim" or just "customer_dim"
        """
        # Prefer target_schema if set, otherwise use layer
        schema = self.target_schema or self.layer
        if schema:
            return f"{schema}.{self.name}"
        return self.name
