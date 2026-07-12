"""Policy configuration models."""
from __future__ import annotations

import warnings
from typing import Literal

from pydantic import Field, model_validator

from .base import StrictBaseModel


LoadMode = Literal["append", "merge", "overwrite", "replace_by_partition"]

MergeStrategy = Literal[
    "update_all",       # Type 1 SCD - overwrite all columns on match
    "update_changed",   # Type 1 SCD - only update if row hash differs
    "insert_only",      # Deduplicate - skip insert if key exists
    "scd_type2",        # Type 2 SCD - track history with effective dates
    "accumulating",     # Accumulating snapshot - update milestone columns only
    "soft_delete",      # Mark records as deleted instead of removing
]


class OptimisationPolicy(StrictBaseModel):
    """Neutral knobs describing how data should be written downstream.

    Attributes:
        load_mode: Write mode - "append" for insert-only, "merge" for upsert,
            "overwrite" for full table replacement (truncate + load).
        partition_scheme: Comma-separated partition columns (e.g., "year,month").
        merge_strategy: Strategy for merge operations (see MergeStrategy).
        hash_columns: Columns to hash for change detection (update_changed strategy).
        effective_date_col: Column for record effective date (scd_type2).
        end_date_col: Column for record end date (scd_type2).
        current_flag_col: Column for current record flag (scd_type2).
        deleted_flag_col: Column for soft delete flag (soft_delete).
        milestone_columns: Columns that get updated incrementally (accumulating).
    """

    load_mode: LoadMode = "append"
    partition_scheme: str | None = None

    # Merge strategy configuration
    merge_strategy: MergeStrategy = "update_all"
    hash_columns: list[str] | None = None
    effective_date_col: str = "effective_date"
    end_date_col: str = "end_date"
    current_flag_col: str = "is_current"
    deleted_flag_col: str = "is_deleted"
    milestone_columns: list[str] | None = None

    @model_validator(mode="after")
    def validate_update_changed_strategy(self) -> "OptimisationPolicy":
        """Validate that update_changed strategy has hash_columns configured."""
        if self.merge_strategy == "update_changed" and not self.hash_columns:
            warnings.warn(
                "merge_strategy 'update_changed' requires 'hash_columns' to be specified. "
                "Without hash_columns, the strategy will fall back to looking for a 'row_hash' "
                "column in your DataFrame, which may not exist and cause runtime errors. "
                "Add 'hash_columns' to your optimisation policy with the list of columns to use "
                "for change detection.",
                UserWarning,
                stacklevel=2,
            )
        return self


class CreationPolicy(StrictBaseModel):
    """Controls table creation behaviour at orchestration time."""

    generic: bool = True
    notebook_name: str | None = None


class OrchestrationPolicy(StrictBaseModel):
    """Describes how and when a table should run."""

    batch: int = 1
    order: int = 1
    active: bool = True


class PoliciesConfig(StrictBaseModel):
    """Top-level policy bundle referenced by tables and stages."""

    optimisation: OptimisationPolicy = Field(default_factory=OptimisationPolicy)
    creation: CreationPolicy = Field(default_factory=CreationPolicy)
    orchestration: OrchestrationPolicy = Field(default_factory=OrchestrationPolicy)
