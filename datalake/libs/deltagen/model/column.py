"""Column configuration models."""
from __future__ import annotations

from typing import Any

from pydantic import Field, model_validator

from .base import StrictBaseModel
from .typespec import TypeSpec, parse_type


class ColumnInput(StrictBaseModel):
    """One source input for a target column.

    Either ``source``+``column`` (for physical columns) or ``expression``
    (for computed values) should be provided. Validation of this rule is
    enforced at the model level.
    """

    source: str | None = None
    column: str | None = None
    expression: str | None = None

    @model_validator(mode="after")
    def _validate_source_or_expression(self) -> "ColumnInput":
        has_expr = self.expression is not None
        has_source_col = self.source is not None or self.column is not None

        if has_expr and has_source_col:
            raise ValueError(
                "ColumnInput must use either expression or source+column, not both"
            )

        if not has_expr and not has_source_col:
            raise ValueError(
                "ColumnInput requires either expression or source+column to be set"
            )

        return self


class ColumnConfig(StrictBaseModel):
    """Describes a column generated in a stage."""

    # Core, technology-neutral attributes
    name: str
    data_type: str | None = None
    default: Any = None
    nullable: bool = True
    natural: bool = False
    temporary: bool = False

    # Descriptive metadata stays neutral
    description: str | None = None

    # Mapping to upstream sources or expressions
    inputs: list[ColumnInput] = Field(default_factory=list)

    # Any engine- or platform-specific behaviour (expressions, masking,
    # quality rules, etc.) should be provided under ``extensions`` rather
    # than as first-class schema fields.

    def get_typespec(self) -> TypeSpec | None:
        """Parse the data_type string into a normalized TypeSpec.

        Returns:
            TypeSpec instance if data_type is set and valid, None otherwise

        Raises:
            ValueError: If data_type has invalid syntax

        Examples:
            >>> col = ColumnConfig(name="id", data_type="varchar(255)")
            >>> spec = col.get_typespec()
            >>> spec.to_spark_sql()
            'STRING'
            >>> spec.to_standard_sql()
            'VARCHAR(255)'
        """
        if self.data_type is None:
            return None
        return parse_type(self.data_type)
