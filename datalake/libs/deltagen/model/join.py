"""Join configuration models."""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from .base import StrictBaseModel


JoinType = Literal["inner", "left", "right", "full", "cross"]


class JoinCondition(StrictBaseModel):
    """Represents a single, backend-agnostic join predicate.

    ``left`` and ``right`` should be logical column references (e.g. ``"l.id"``),
    and ``operator`` is kept as a simple string so that downstream planners or
    plugins can interpret it for a specific engine.
    """

    left: str
    right: str
    operator: str = "="


class JoinConfig(StrictBaseModel):
    """Captures how an auxiliary source is joined into a stage.

    This model intentionally avoids embedding SQL. Any SQL generation or
    engine-specific behaviour should be handled by PlanBuilder or plugins.
    """

    name: str
    type: JoinType = "inner"
    source: str
    alias: str | None = None  # Optional alias for the join source; duplicate columns are renamed {alias}__{col}
    conditions: list[JoinCondition] = Field(default_factory=list)
    broadcast: bool | None = None  # Override source broadcast hint for this join
    dedupe_by: list[str] | None = None  # Deduplicate the join source to one row per partition before joining
    dedupe_order_by: str | None = None  # Column to order by when deduplicating (keeps first row descending)
    dedupe_order_desc: bool = True  # If True (default), keep the row with the highest dedupe_order_by value
