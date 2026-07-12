"""Incremental loading and data quality configuration models."""
from __future__ import annotations

from enum import Enum

from pydantic import Field, model_validator

from .base import StrictBaseModel


class SourceFilterMode(str, Enum):
    """Mode for filtering source data at load time."""

    WATERMARK = "watermark"
    """Filter source by watermark: load records where source_watermark > max(target.watermark).
    Use for most incremental loads."""

    PERIOD = "period"
    """Filter source by period: identify latest period in target, load that period + newer.
    Use when you need to reload entire periods (e.g., monthly snapshots with corrections)."""

    NONE = "none"
    """No filtering - load all source data every time."""


class IncrementalConfig(StrictBaseModel):
    """Configuration for incremental source filtering at load time.

    This config controls how PlanBuilder filters source data when loading:
    - Watermark mode: Filter to records newer than max watermark in target
    - Period mode: Load records from latest period onwards

    Note: This only controls SOURCE FILTERING. Post-load transformation
    (dedupe, duplicate checking) is handled by stage plugins.

    YAML Example (watermark-based):
        incremental:
          filter_mode: watermark
          source_watermark_column: _source_modified  # Column in source
          watermark_column: modified_on              # Column in target

    YAML Example (period-based):
        incremental:
          filter_mode: period
          period_column: report_month
    """

    filter_mode: SourceFilterMode = Field(
        default=SourceFilterMode.NONE,
        description="How to filter source data at load time",
    )

    source_watermark_column: str | None = Field(
        default=None,
        description="Column in source data for watermark comparison",
    )

    watermark_column: str | None = Field(
        default=None,
        description="Column in target table for max watermark lookup",
    )

    period_column: str | None = Field(
        default=None,
        description="Column identifying the period (for period-based filtering)",
    )

    source_period_column: str | None = Field(
        default=None,
        description=(
            "Source column identifying the period for replace_by_partition expansion. "
            "When set with watermark filter_mode and replace_by_partition load_mode, "
            "the watermark filter identifies which periods have modifications, then "
            "expands to load ALL rows for those periods — not just the modified rows. "
            "This prevents period_replace from deleting a full partition and replacing "
            "it with only the changed subset."
        ),
    )

    @model_validator(mode="after")
    def validate_filter_requirements(self) -> "IncrementalConfig":
        """Validate that required fields are set for each filter mode."""
        if self.filter_mode == SourceFilterMode.WATERMARK:
            if not self.watermark_column:
                raise ValueError("watermark filter mode requires 'watermark_column'")

        elif self.filter_mode == SourceFilterMode.PERIOD:
            if not self.period_column:
                raise ValueError("period filter mode requires 'period_column'")

        return self

    @property
    def effective_source_watermark(self) -> str | None:
        """Get the source watermark column, defaulting to watermark_column if not set."""
        return self.source_watermark_column or self.watermark_column

    @property
    def is_enabled(self) -> bool:
        """Check if incremental filtering is enabled."""
        return self.filter_mode != SourceFilterMode.NONE


class DQConfig(StrictBaseModel):
    """Configuration for data quality logging.

    Controls where rejected records and duplicate issues are logged
    for investigation and monitoring.

    YAML Example:
        dq:
          rejected_table: logging.silver_dq_rejected
          duplicates_table: logging.silver_dq_duplicates
          log_sample_size: 100
    """

    rejected_table: str | None = Field(
        default=None,
        description="Table to log rejected records (nulls, invalid values)",
    )

    duplicates_table: str | None = Field(
        default=None,
        description="Table to log unexpected duplicate records (for 'append' strategy)",
    )

    log_sample_size: int = Field(
        default=100,
        description="Maximum number of rejected/duplicate records to log per issue",
    )

    include_all_columns: bool = Field(
        default=True,
        description="If True, log all columns; if False, log only key columns + violation",
    )
