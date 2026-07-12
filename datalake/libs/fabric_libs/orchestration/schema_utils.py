"""Schema creation helpers for orchestration notebooks."""
from __future__ import annotations

from typing import Iterable


def ensure_schemas(spark, schemas: Iterable[str]) -> None:
    """Create schemas if they do not exist."""
    unique = {s.strip() for s in schemas if s and str(s).strip()}
    for schema in sorted(unique):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
