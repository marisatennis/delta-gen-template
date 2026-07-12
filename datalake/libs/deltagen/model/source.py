"""Source configuration models."""
from __future__ import annotations

from typing import Any

from pydantic import Field

from .base import StrictBaseModel


class SourceOptions(StrictBaseModel):
    """Additional options to construct a DataFrame from a source."""

    mode: str | None = None
    partitions: int | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class SourceConfig(StrictBaseModel):
    """Represents an input dataset the table depends on."""

    name: str
    catalog: str | None = None
    schema: str | None = None
    table: str | None = None
    format: str | None = None
    path: str | None = None
    alias: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    load_options: SourceOptions | None = None
    broadcast: bool = False  # Hint to broadcast this source in joins (for small tables)
    columns: list[str] | None = None  # Column pruning: only load these columns from source
    load_all_columns: bool = False  # If True, disable auto column pruning and load all columns
    generated: bool = False  # If True, creates a synthetic single-row DataFrame (no actual data load)
    row_count: int = 1  # Number of rows to generate when generated=True
    incremental: bool = True  # If False, skip incremental filtering for this source (load full table)
