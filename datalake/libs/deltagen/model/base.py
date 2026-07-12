"""Core Pydantic base classes used across Delta-Gen v2 models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    """Base model that enforces strict core fields but stores unknown keys under ``extensions``.

    The model is frozen to prevent accidental mutation once configs have been validated.
    ``extensions`` behaves like an escape hatch for platform-specific attributes and
    remains available through both dictionary access and dot notation via ``__getattr__``.
    """

    # Use extra="ignore" so our pre-validation hook can capture unknown
    # keys into the ``extensions`` dict instead of raising immediately.
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    extensions: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _relocate_extensions(cls, raw: Any) -> Any:
        """Normalise the explicit ``extensions`` block before validation.

        Only the explicit top-level ``extensions`` mapping is treated as an
        escape hatch. Any other unknown top-level keys are *not* touched here
        so that Pydantic can raise a validation error for true schema breaks.
        """

        if not isinstance(raw, dict):
            return raw

        if "extensions" in raw and isinstance(raw["extensions"], dict):
            # Ensure ``extensions`` is always a plain dict so downstream code
            # and type hints behave consistently.
            raw = {**raw, "extensions": dict(raw["extensions"])}

        return raw

    def __getattr__(self, item: str) -> Any:
        """Provide dot-notation access to extension keys."""

        try:
            extensions = object.__getattribute__(self, "extensions")
            if item in extensions:
                return extensions[item]
        except AttributeError:
            pass
        raise AttributeError(f"{self.__class__.__name__} has no attribute '{item}'")

    def get_extension(self, key: str, default: Any = None) -> Any:
        """Convenience helper for explicit extension access."""

        return self.extensions.get(key, default)
