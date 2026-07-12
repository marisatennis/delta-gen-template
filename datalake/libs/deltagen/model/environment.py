"""Environment configuration models (layers and logical sources)."""
from __future__ import annotations

from pydantic import Field

from .base import StrictBaseModel


class EnvironmentSourceConfig(StrictBaseModel):
    """Describes a single logical source within a layer.

    This stays technology-neutral: ``path`` and ``format`` are generic hints;
    engine-specific handling happens in providers or writers.
    """

    name: str
    path: str
    format: str


class EnvironmentLayerConfig(StrictBaseModel):
    """A logical layer such as raw, cleansed, curated."""

    name: str
    sources: list[EnvironmentSourceConfig] = Field(default_factory=list)


class EnvironmentConfig(StrictBaseModel):
    """Top-level environment configuration, independent of any table config."""

    layers: list[EnvironmentLayerConfig] = Field(default_factory=list)
