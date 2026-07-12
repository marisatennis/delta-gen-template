"""Source loading logic for PlanBuilder."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from deltagen.model.source import SourceConfig
from deltagen.runner.exceptions import PlanBuilderError


def load_sources(
    spark: "SparkSession", sources: list[SourceConfig]
) -> dict[str, "DataFrame"]:
    """Load all source DataFrames defined in the config.

    Args:
        spark: Active SparkSession
        sources: List of source configurations

    Returns:
        Dictionary mapping source names to DataFrames
    """
    loaded: dict[str, DataFrame] = {}

    for source in sources:
        df = load_single_source(spark, source)
        loaded[source.name] = df

        # Also register with alias if provided
        if source.alias:
            loaded[source.alias] = df

    return loaded


def load_single_source(
    spark: "SparkSession", source: SourceConfig
) -> "DataFrame":
    """Load a single source DataFrame.

    Args:
        spark: Active SparkSession
        source: Source configuration

    Returns:
        Loaded DataFrame
    """
    try:
        df: "DataFrame"

        if source.generated:
            # Generated source - create a synthetic DataFrame with specified row count
            # This is useful for tables that are purely expression-based (e.g., date dimensions)
            df = spark.range(source.row_count).toDF("_generated_row_id")

        elif source.path:
            # Path-based source
            reader = spark.read
            if source.format:
                reader = reader.format(source.format)
            if source.options:
                reader = reader.options(**source.options)
            df = reader.load(source.path)

        elif source.table:
            # Catalog-based source
            table_name = source.table
            if source.schema:
                table_name = f"{source.schema}.{table_name}"
            if source.catalog:
                table_name = f"{source.catalog}.{table_name}"
            df = spark.table(table_name)

        else:
            raise PlanBuilderError(
                f"Source '{source.name}' has neither path, table, nor generated=true defined"
            )

        # Apply column pruning if columns are specified
        if source.columns:
            df = df.select(*source.columns)

        return df

    except Exception as e:
        if isinstance(e, PlanBuilderError):
            raise
        raise PlanBuilderError(
            f"Failed to load source '{source.name}'", detail=str(e)
        )
