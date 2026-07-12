"""PlanBuilder: Transforms validated TableConfig into Spark DataFrames.

This module provides the core transformation engine for Delta-Gen v2.
It takes declarative configuration (TableConfig) and produces executable
Spark DataFrame operations.

Usage:
    from deltagen.runner import PlanBuilder
    from deltagen.providers import YamlConfigProvider
    from deltagen.model import TableConfig

    # Load config
    provider = YamlConfigProvider(TableConfig)
    config = provider.load("configs/customer_dim.yaml")

    # Build DataFrame (all stages)
    builder = PlanBuilder(config)
    df = builder.build(spark)

    # Or build stage-by-stage for debugging
    sources = builder.load_sources(spark)
    df = builder.build_stage(spark, config.stages[0], sources, debug=True)
    print(builder.to_sql("stage_1"))  # See generated SQL

    # Build with plugin context for metrics and custom transforms
    from deltagen.plugins import create_plugin_context
    ctx = create_plugin_context("customer_dim", load_id="batch_001")
    df = builder.build(spark, context=ctx)
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from deltagen.plugins.context import PluginContext

from deltagen.model import TableConfig
from deltagen.model.incremental import SourceFilterMode
from deltagen.model.source import SourceConfig
from deltagen.model.stage import StageConfig
from deltagen.runner.aggregation_builder import apply_group_by
from deltagen.runner.column_builder import (
    analyze_required_columns,
    build_columns,
    column_to_sql,
)
from deltagen.runner.exceptions import PlanBuilderError
from deltagen.runner.filter_builder import analyze_filter_pushdown, apply_filters
from deltagen.runner.join_builder import apply_joins
from deltagen.runner.source_loader import load_sources as _load_sources
from deltagen.runner.sql_generator import explain_plan, generate_sql
from deltagen.runner.union_builder import apply_unions

logger = logging.getLogger(__name__)


class PlanBuilder:
    """Builds Spark DataFrames from validated TableConfig.

    Handles all transformation logic including:
    - Loading source DataFrames
    - Building columns (selects, casts, defaults, expressions)
    - Applying joins
    - Applying filters
    - Deduplication

    Does NOT handle write operations - that's the Writer's job.
    """

    def __init__(self, config: TableConfig):
        """Initialize with a validated configuration.

        Args:
            config: Validated TableConfig instance
        """
        self.config = config
        self._sql_parts: dict[str, dict[str, Any]] = {}

    def load_sources(self, spark: "SparkSession") -> dict[str, "DataFrame"]:
        """Load all source DataFrames defined in the config.

        Args:
            spark: Active SparkSession

        Returns:
            Dictionary mapping source names to DataFrames
        """
        return _load_sources(spark, self.config.sources)

    def _validate_duplicates_config(self, debug: bool = False) -> None:
        """Validate that duplicates_table config matches plugin usage.

        Warns if:
        - duplicates_table is configured but check_duplicates plugin is not used
        - check_duplicates plugin is used but duplicates_table is not configured
        """
        has_duplicates_table = (
            self.config.dq is not None and self.config.dq.duplicates_table is not None
        )
        has_check_duplicates = any(
            stage.extensions and stage.extensions.get("stage_plugin") == "check_duplicates"
            for stage in self.config.stages
        )

        if has_duplicates_table and not has_check_duplicates:
            msg = (
                f"WARNING: 'dq.duplicates_table' is configured as "
                f"'{self.config.dq.duplicates_table}' but 'check_duplicates' plugin is not used. "
                "Duplicates will NOT be logged. Use 'check_duplicates' stage plugin to enable logging."
            )
            if debug:
                print(f"\n  ⚠️  {msg}")
            import warnings
            warnings.warn(msg, UserWarning, stacklevel=3)

        if has_check_duplicates and not has_duplicates_table:
            msg = (
                "WARNING: 'check_duplicates' plugin is used but 'dq.duplicates_table' is not configured. "
                "Duplicates will be detected but NOT logged to a table. Configure 'dq.duplicates_table' to enable logging."
            )
            if debug:
                print(f"\n  ⚠️  {msg}")
            import warnings
            warnings.warn(msg, UserWarning, stacklevel=3)

    def build(
        self,
        spark: "SparkSession",
        debug: bool = False,
        context: "PluginContext | None" = None,
        watermark_override: "str | None" = None,
    ) -> "DataFrame":
        """Build and return the transformed DataFrame.

        Processes all stages sequentially and returns the final result.
        Automatically applies column pruning based on stage analysis.

        Args:
            spark: Active SparkSession
            debug: If True, print transformation details
            context: Optional PluginContext for metrics and plugin invocation

        Returns:
            Transformed Spark DataFrame ready for writing
        """
        if debug:
            print("\n" + "=" * 60)
            print(f"[PlanBuilder] Building table: {self.config.name}")
            print("=" * 60)
            print(f"  Layer: {self.config.layer or 'not specified'}")
            print(f"  Sources: {len(self.config.sources)}")
            print(f"  Stages: {len(self.config.stages)}")
            natural_keys = self.config.get_natural_key_columns()
            if natural_keys:
                print(f"  Natural keys: {[c.name for c in natural_keys]}")

        # Analyze required columns from all stages
        required_columns = analyze_required_columns(
            self.config.stages, self.config.sources
        )

        if debug and required_columns:
            print("[PlanBuilder] Auto-detected column requirements:")
            for src, cols in required_columns.items():
                print(f"  - {src}: {sorted(cols)}")

        # Load sources with auto-pruning
        if debug:
            print("\n" + "=" * 60)
            print("[PlanBuilder] STEP 1: Loading sources")
            print("=" * 60)

        sources = self._load_sources_with_pruning(spark, required_columns, debug, context, watermark_override)

        if debug:
            # Iterate over config sources, not the sources dict (which includes aliases)
            for source in self.config.sources:
                name = source.name
                if name not in sources:
                    continue
                if source.path:
                    print(f"  ✓ {name}: {source.path} ({source.format})")
                elif source.table:
                    schema_part = f"{source.schema}." if source.schema else ""
                    print(f"  ✓ {name}: {schema_part}{source.table}")
                    print(f"    Columns: {list(sources[name].columns)}")
                # Show source record count
                try:
                    src_counts = context.get_state("source_row_counts", {}) if context else {}
                    if name in src_counts:
                        src_count = src_counts[name]
                    else:
                        src_count = sources[name].count()
                    print(f"    Source records: {src_count:,}")
                except Exception:
                    print(f"    Source records: (count failed)")

        df: "DataFrame | None" = None

        stage_count = len(self.config.stages)
        for idx, stage in enumerate(self.config.stages, 1):
            if debug:
                print("\n" + "=" * 60)
                print(f"[PlanBuilder] STEP 2.{idx}: Processing stage '{stage.name}' ({idx}/{stage_count})")
                print("=" * 60)
                print(f"  Mode: {stage.mode}")
                print(f"  Columns to build: {len(stage.columns)}")
                if stage.joins:
                    print(f"  Joins: {len(stage.joins)}")
                if stage.filters:
                    print(f"  Filters: {len(stage.filters)}")
                if stage.extensions.get("stage_plugin"):
                    print(f"  Stage plugin: {stage.extensions.get('stage_plugin')}")

            # Track record count before stage (for comparison)
            count_before = None
            if df is not None and debug:
                try:
                    count_before = df.count()
                except Exception:
                    pass

            df = self.build_stage(spark, stage, sources, df, debug=debug, context=context)

            if debug:
                print(f"\n  ✓ Stage '{stage.name}' complete")
                print(f"    Output columns: {list(df.columns)}")
                try:
                    count_after = df.count() if df is not None else 0
                    print(f"    Output records: {count_after:,}")
                    if count_before is not None and isinstance(count_before, int) and count_before != count_after:
                        diff = count_before - count_after
                        pct = abs(diff) / count_before * 100 if count_before > 0 else 0
                        print(f"    Records changed: {'-' if diff > 0 else '+'}{abs(diff):,} ({pct:.1f}% {'removed' if diff > 0 else 'added'})")
                except Exception:
                    print(f"    Output records: (count unavailable)")

        # Validate duplicates_table configuration
        self._validate_duplicates_config(debug=debug)

        if debug:
            print("\n" + "=" * 60)
            print("[PlanBuilder] BUILD COMPLETE")
            print("=" * 60)
            print(f"  Final columns: {list(df.columns) if df else 'None'}")

        return df

    def build_stage(
        self,
        spark: "SparkSession",
        stage: StageConfig,
        sources: dict[str, "DataFrame"],
        input_df: "DataFrame | None" = None,
        debug: bool = False,
        context: "PluginContext | None" = None,
    ) -> "DataFrame":
        """Build a single stage and return the resulting DataFrame.

        Args:
            spark: Active SparkSession
            stage: Stage configuration to process
            sources: Dictionary of source DataFrames
            input_df: Optional input DataFrame from previous stage
            debug: If True, print transformation details
            context: Optional PluginContext for plugin invocation

        Returns:
            Transformed DataFrame for this stage
        """
        self._sql_parts[stage.name] = {
            "select": [],
            "from": None,
            "joins": [],
            "where": [],
        }

        try:
            # Determine filters to apply
            filters_to_apply = stage.filters.copy()  # Copy to avoid mutation
            source_filters_to_apply = (
                stage.source_filters.copy() if stage.source_filters else {}
            )

            # Auto-detect filter pushdown if:
            # 1. There are joins in this stage
            # 2. There are filters to analyze
            # 3. No explicit source_filters were provided
            if stage.joins and stage.filters and not stage.source_filters:
                # Build set of known source names and aliases
                known_sources = set(sources.keys())

                # Analyze filters for pushdown opportunities
                auto_pushdown, post_join = analyze_filter_pushdown(
                    stage.filters, known_sources
                )

                if auto_pushdown:
                    source_filters_to_apply = auto_pushdown
                    filters_to_apply = post_join
                    if debug and auto_pushdown:
                        print("  AUTO-DETECTED FILTER PUSHDOWN:")
                        for src, filts in auto_pushdown.items():
                            for f in filts:
                                print(f"    - {src}: {f}")

            # PERFORMANCE: Apply source filters FIRST (before unions/joins)
            # This reduces data volume as early as possible
            if source_filters_to_apply:
                sources = self._apply_source_filters(
                    sources, source_filters_to_apply, stage.name, debug
                )

            # Step 1: Determine the base DataFrame
            # Priority: input_df > unions > first source
            # NOTE: Unions now use pre-filtered sources for better performance
            if input_df is not None:
                df = input_df
                self._sql_parts[stage.name]["from"] = "previous_stage"
            elif stage.unions is not None:
                # Apply union to combine multiple sources (now pre-filtered)
                df = apply_unions(stage, sources, self._sql_parts, debug)
            elif sources:
                # Use first source as base
                first_source = (
                    self.config.sources[0] if self.config.sources else None
                )
                if first_source:
                    df = sources.get(first_source.name)
                    self._sql_parts[stage.name]["from"] = first_source.name
                else:
                    raise PlanBuilderError(
                        "No sources defined and no input DataFrame provided",
                        stage=stage.name,
                    )
            else:
                raise PlanBuilderError(
                    "No sources available and no input DataFrame provided",
                    stage=stage.name,
                )

            # Apply joins (with broadcast hints)
            if stage.joins:
                df = apply_joins(
                    df, stage, sources, self.config.sources, self._sql_parts, debug
                )

            # Build columns
            if stage.columns:
                df = build_columns(df, stage, sources, self._sql_parts, debug, context)

            # PERFORMANCE: Apply filters BEFORE group_by (WHERE clause semantics)
            # This reduces data volume before expensive aggregation operations
            # Use group_by.having for post-aggregation filters (HAVING clause)
            if filters_to_apply:
                # Create a temporary stage config with only filters
                temp_stage = StageConfig(name=stage.name, filters=filters_to_apply)
                df = apply_filters(df, temp_stage, self._sql_parts, debug)

            # Apply GROUP BY and aggregations
            # Note: HAVING clause filters are handled inside apply_group_by
            if stage.group_by is not None:
                df = apply_group_by(df, stage, self._sql_parts, debug)

            # Invoke stage plugin if configured
            stage_plugin_name = stage.extensions.get("stage_plugin")
            if stage_plugin_name:
                df = self._invoke_stage_plugin(
                    df, stage, stage_plugin_name, context, debug
                )

            return df

        except Exception as e:
            if isinstance(e, PlanBuilderError):
                raise
            raise PlanBuilderError(
                "Failed processing stage",
                stage=stage.name,
                detail=str(e),
            )

    def _apply_source_filters(
        self,
        sources: dict[str, "DataFrame"],
        source_filters: dict[str, list[str]],
        stage_name: str,
        debug: bool = False,
    ) -> dict[str, "DataFrame"]:
        """Apply filters to source DataFrames before joins.

        This implements filter pushdown - applying filters to individual sources
        before joining them, which can significantly improve performance.

        Args:
            sources: Dictionary of source DataFrames
            source_filters: Mapping of source name to list of filter expressions
            stage_name: Name of the current stage (for SQL tracking)
            debug: If True, print filter details

        Returns:
            Updated sources dictionary with filtered DataFrames
        """
        if debug and source_filters:
            print("  SOURCE FILTERS (pushdown):")

        # Create a copy to avoid mutating the original
        filtered_sources = dict(sources)

        for source_name, filters in source_filters.items():
            if source_name not in filtered_sources:
                raise PlanBuilderError(
                    f"Source '{source_name}' in source_filters not found",
                    stage=stage_name,
                )

            df = filtered_sources[source_name]
            for filter_expr in filters:
                df = df.filter(filter_expr)
                if debug:
                    print(f"    - {source_name}: {filter_expr}")

            filtered_sources[source_name] = df

            # Also update alias if present
            source_config = next(
                (s for s in self.config.sources if s.name == source_name), None
            )
            if source_config and source_config.alias:
                filtered_sources[source_config.alias] = df

        return filtered_sources

    def _invoke_stage_plugin(
        self,
        df: "DataFrame",
        stage: StageConfig,
        plugin_name: str,
        context: "PluginContext | None",
        debug: bool = False,
    ) -> "DataFrame":
        """Invoke a stage plugin by name.

        Args:
            df: Input DataFrame
            stage: Stage configuration (passed to the plugin)
            plugin_name: Name of the registered stage plugin
            context: Plugin context for metrics and state
            debug: If True, print plugin details

        Returns:
            Transformed DataFrame from the plugin
        """
        from deltagen.plugins.registry import get_stage_plugin

        plugin = get_stage_plugin(plugin_name)
        if not plugin:
            logger.warning(
                f"Stage plugin '{plugin_name}' not found in registry, skipping"
            )
            return df

        if debug:
            print(f"\n  Invoking stage plugin: {plugin_name}")
            # Show plugin config from extensions
            plugin_config = {k: v for k, v in stage.extensions.items() if k != "stage_plugin"}
            if plugin_config:
                for key, value in plugin_config.items():
                    print(f"    {key}: {value}")

        # Create a null context if none provided
        if context is None:
            from deltagen.plugins.context import create_null_context

            context = create_null_context()

        try:
            result = plugin(df, stage, context)
            if debug:
                print(f"  ✓ Stage plugin '{plugin_name}' complete")
            return result
        except Exception as e:
            raise PlanBuilderError(
                f"Stage plugin '{plugin_name}' failed",
                stage=stage.name,
                detail=str(e),
            )

    def _get_watermark_override(
        self,
        spark: "SparkSession",
        debug: bool = False,
    ) -> any:
        """Read a watermark override from log.watermark_overrides for this pipeline.

        The override table is written by the ``schedule_fk_reresolution`` stage plugin
        after a dimension refresh resolves previously-unresolved FK sentinels.  When an
        override exists, the fact pipeline re-processes records from that earlier date so
        the FK values get updated via the merge.

        Args:
            spark: Active SparkSession
            debug: If True, print details

        Returns:
            Override watermark value (e.g. a date), or None if no override exists.
        """
        from pyspark.sql import functions as F

        pipeline_name = self.config.name
        overrides_table = "log.watermark_overrides"

        try:
            if not spark.catalog.tableExists(overrides_table):
                return None
        except Exception:
            return None

        try:
            row = (
                spark.table(overrides_table)
                .filter(F.col("pipeline_name") == pipeline_name)
                .agg(F.min("watermark_override").alias("override"))
                .collect()[0]
            )
            override_val = row["override"]
            if override_val is not None and debug:
                print(
                    f"  [INCREMENTAL] Watermark override found for '{pipeline_name}': "
                    f"{override_val} (will re-process from this date)"
                )
            return override_val
        except Exception as e:
            logger.warning(f"Could not read watermark override for '{pipeline_name}': {e}")
            return None

    def _get_watermark_filter_value(
        self,
        spark: "SparkSession",
        debug: bool = False,
    ) -> any:
        """Get the max watermark value from the target table.

        Args:
            spark: Active SparkSession
            debug: If True, print details

        Returns:
            Max watermark value, or None if target doesn't exist or has no data.
        """
        inc = self.config.incremental
        if not inc.watermark_column:
            return None

        target_table = self.config.get_target_table_name()
        if not target_table:
            return None

        try:
            if not spark.catalog.tableExists(target_table):
                if debug:
                    print(f"  [INCREMENTAL] Target table {target_table} does not exist yet")
                return None
        except Exception as e:
            logger.warning(f"Could not check if target table exists: {e}")
            return None

        try:
            from pyspark.sql import functions as F

            watermark_df = spark.table(target_table).select(
                F.max(F.col(inc.watermark_column)).alias("max_watermark")
            )
            max_watermark_row = watermark_df.collect()[0]
            max_watermark = max_watermark_row["max_watermark"]

            if debug:
                print(
                    f"  [INCREMENTAL] Max watermark from {target_table}.{inc.watermark_column}: "
                    f"{max_watermark}"
                )

            return max_watermark
        except Exception as e:
            logger.warning(
                f"Could not read watermark from {target_table}.{inc.watermark_column}: {e}"
            )
            return None

    def _get_period_filter_values(
        self,
        spark: "SparkSession",
        debug: bool = False,
    ) -> list[any]:
        """Get the periods to load from target table for period-based filtering.

        Finds the latest period in the target and returns it (so we reload that
        period plus any newer periods from source).

        Args:
            spark: Active SparkSession
            debug: If True, print details

        Returns:
            List containing the latest period value, or empty list if target doesn't exist.
        """
        inc = self.config.incremental
        if not inc.period_column:
            return []

        target_table = self.config.get_target_table_name()
        if not target_table:
            return []

        try:
            if not spark.catalog.tableExists(target_table):
                if debug:
                    print(f"  [INCREMENTAL] Target table {target_table} does not exist yet")
                return []
        except Exception as e:
            logger.warning(f"Could not check if target table exists: {e}")
            return []

        try:
            from pyspark.sql import functions as F

            period_df = spark.table(target_table).select(
                F.max(F.col(inc.period_column)).alias("latest_period")
            )
            latest_period_row = period_df.collect()[0]
            latest_period = latest_period_row["latest_period"]

            if latest_period is None:
                if debug:
                    print(f"  [INCREMENTAL] No periods found in {target_table}")
                return []

            if debug:
                print(
                    f"  [INCREMENTAL] Latest period from {target_table}.{inc.period_column}: "
                    f"{latest_period}"
                )

            return [latest_period]
        except Exception as e:
            logger.warning(
                f"Could not read period from {target_table}.{inc.period_column}: {e}"
            )
            return []

    def _load_sources_with_pruning(
        self,
        spark: "SparkSession",
        required_columns: dict[str, set[str]],
        debug: bool = False,
        context: "PluginContext | None" = None,
        manual_watermark_override: "str | None" = None,
    ) -> dict[str, "DataFrame"]:
        """Load sources with automatic column pruning and incremental filtering.

        For each source:
        1. If explicit `columns` is set in config, use those (user override)
        2. Otherwise, use auto-detected required columns
        3. If no columns detected, load all columns
        4. Apply incremental filter based on filter_mode (watermark or period)

        Args:
            spark: Active SparkSession
            required_columns: Auto-detected required columns per source
            debug: If True, print pruning details
            context: Optional PluginContext for storing filter info

        Returns:
            Dictionary mapping source names to DataFrames
        """
        loaded: dict[str, "DataFrame"] = {}

        # Get incremental config
        inc = self.config.incremental

        # Determine filter mode and get filter values
        max_watermark = None
        periods_to_load = []

        if inc.filter_mode == SourceFilterMode.WATERMARK:
            max_watermark = self._get_watermark_filter_value(spark, debug)
            source_filter_col = inc.effective_source_watermark

            # Manual parameter override takes highest priority (passed from notebook parameter).
            if manual_watermark_override is not None:
                try:
                    from datetime import datetime as _dt
                    parsed = _dt.strptime(str(manual_watermark_override)[:10], "%Y-%m-%d").date()
                    if debug:
                        print(
                            f"  [INCREMENTAL] Manual WATERMARK_OVERRIDE applied: "
                            f"{parsed} (replaces {max_watermark})"
                        )
                    max_watermark = parsed
                    if context:
                        context.set_state("watermark_override_used", True)
                        context.set_state("watermark_override_value", str(parsed))
                        context.set_state("watermark_override_source", "parameter")
                except ValueError:
                    logger.warning(
                        f"Could not parse WATERMARK_OVERRIDE '{manual_watermark_override}' "
                        "— ignoring, using computed watermark"
                    )
            else:
                # Check for a watermark override written by schedule_fk_reresolution plugin.
                # If earlier than the current max watermark, use it so the pipeline
                # re-processes records from that date and picks up corrected FKs.
                override_watermark = self._get_watermark_override(spark, debug)
                if override_watermark is not None and (
                    max_watermark is None or override_watermark < max_watermark
                ):
                    if debug:
                        print(
                            f"  [INCREMENTAL] Applying watermark override: "
                            f"{override_watermark} (replaces {max_watermark})"
                        )
                    max_watermark = override_watermark
                    if context:
                        context.set_state("watermark_override_used", True)
                        context.set_state("watermark_override_value", str(override_watermark))
                        context.set_state("watermark_override_source", "table")

            if context:
                context.set_state("max_watermark", max_watermark)
                context.set_state("filter_mode", "watermark")

            if debug and max_watermark is not None:
                print(
                    f"  [INCREMENTAL] Will filter sources where "
                    f"{source_filter_col} > {max_watermark}"
                )

        elif inc.filter_mode == SourceFilterMode.PERIOD:
            periods_to_load = self._get_period_filter_values(spark, debug)
            source_filter_col = inc.period_column

            if context:
                context.set_state("periods_to_load", periods_to_load)
                context.set_state("filter_mode", "period")

            if debug and periods_to_load:
                print(
                    f"  [INCREMENTAL] Will filter sources where "
                    f"{source_filter_col} >= {periods_to_load[0]}"
                )

        for source in self.config.sources:
            # Determine columns to load
            if source.load_all_columns:
                # User explicitly disabled pruning for this source
                columns_to_load = None
                if debug:
                    print(f"  [PRUNE] {source.name}: load_all_columns=true, skipping pruning")
            elif source.columns:
                # User specified explicit columns - use those
                columns_to_load = source.columns
                if debug:
                    print(f"  [PRUNE] {source.name}: using explicit columns")
            elif source.name in required_columns:
                # Use auto-detected columns
                columns_to_load = list(required_columns[source.name])
                if debug:
                    print(
                        f"  [PRUNE] {source.name}: auto-detected {len(columns_to_load)} columns"
                    )
            else:
                # No columns detected - load all
                columns_to_load = None
                if debug:
                    print(f"  [PRUNE] {source.name}: loading all columns")

            # Create a copy of source config with updated columns
            pruned_source = SourceConfig(
                name=source.name,
                catalog=source.catalog,
                schema=source.schema,
                table=source.table,
                format=source.format,
                path=source.path,
                alias=source.alias,
                options=source.options,
                load_options=source.load_options,
                broadcast=source.broadcast,
                columns=columns_to_load,
                load_all_columns=source.load_all_columns,
                generated=source.generated,
                row_count=source.row_count,
            )

            # Load the source
            from deltagen.runner.source_loader import load_single_source

            df = load_single_source(spark, pruned_source)

            # Apply incremental filter based on mode (skip if source opts out)
            if not source.incremental and debug:
                print(f"  [INCREMENTAL] {source.name}: skipped (incremental: false)")

            if inc.filter_mode == SourceFilterMode.WATERMARK and source.incremental:
                if max_watermark is not None and source_filter_col and source_filter_col in df.columns:
                    from pyspark.sql import functions as F

                    count_before = df.count() if debug else None

                    # When replace_by_partition is used with a source_period_column,
                    # expand the watermark filter to load ALL rows for affected periods.
                    # Without this, period_replace would delete entire partitions and
                    # replace them with only the modified subset.
                    period_col = inc.source_period_column
                    load_mode = self.config.policies.optimisation.load_mode if self.config.policies.optimisation else None

                    if period_col and load_mode == "replace_by_partition" and period_col in df.columns:
                        # Two-pass: find periods with modifications, then load all rows for those periods
                        modified_periods = (
                            df.filter(F.col(source_filter_col) > F.lit(max_watermark))
                            .select(period_col)
                            .distinct()
                            .collect()
                        )
                        period_values = [r[0] for r in modified_periods]

                        if period_values:
                            df = df.filter(F.col(period_col).isin(period_values))
                            if debug:
                                count_after = df.count()
                                print(
                                    f"  [INCREMENTAL] {source.name}: period expansion via {period_col} — "
                                    f"{len(period_values)} period(s) with {source_filter_col} > {max_watermark} "
                                    f"({count_before:,} -> {count_after:,} records)"
                                )
                        else:
                            df = df.filter(F.lit(False))
                            if debug:
                                print(
                                    f"  [INCREMENTAL] {source.name}: no periods with {source_filter_col} > "
                                    f"{max_watermark} ({count_before:,} -> 0 records)"
                                )
                    else:
                        df = df.filter(F.col(source_filter_col) > F.lit(max_watermark))

                        if debug:
                            count_after = df.count()
                            print(
                                f"  [INCREMENTAL] {source.name}: filtered {source_filter_col} > "
                                f"{max_watermark} ({count_before:,} -> {count_after:,} records)"
                            )

            elif inc.filter_mode == SourceFilterMode.PERIOD and source.incremental:
                if periods_to_load and source_filter_col and source_filter_col in df.columns:
                    from pyspark.sql import functions as F

                    count_before = df.count() if debug else None
                    # Load the latest period and any newer periods
                    df = df.filter(F.col(source_filter_col) >= F.lit(periods_to_load[0]))

                    if debug:
                        count_after = df.count()
                        print(
                            f"  [INCREMENTAL] {source.name}: filtered {source_filter_col} >= "
                            f"{periods_to_load[0]} ({count_before:,} -> {count_after:,} records)"
                        )

            # Record source metrics if a context is provided
            if context and hasattr(context, "metrics"):
                columns_read = len(df.columns)
                row_count = None
                duration_ms = None
                try:
                    start = time.perf_counter()
                    row_count = df.count()
                    duration_ms = int((time.perf_counter() - start) * 1000)
                except Exception:
                    row_count = None

                if row_count is not None:
                    context.metrics.record_source_read(
                        source_name=source.name,
                        row_count=row_count,
                        columns_read=columns_read,
                        duration_ms=duration_ms,
                    )
                    source_counts = context.get_state("source_row_counts", {})
                    source_counts[source.name] = row_count
                    context.set_state("source_row_counts", source_counts)

            # Alias the DataFrame with the source name so expressions like "src.column" resolve
            df = df.alias(source.name)
            loaded[source.name] = df

            # Also register with alias if provided
            if source.alias:
                # Create a separate aliased DataFrame for the alias name
                loaded[source.alias] = df.alias(source.alias)

        return loaded

    def to_sql(self, stage: str | None = None) -> str:
        """Generate SQL representation of the transformation.

        Args:
            stage: Specific stage name, or None for all stages

        Returns:
            SQL query string
        """
        return generate_sql(self.config, self._sql_parts, stage)

    def explain(self) -> str:
        """Generate a human-readable explanation of the plan.

        Returns:
            Multi-line explanation string
        """
        return explain_plan(self.config)

    # Keep _column_to_sql accessible for tests
    @staticmethod
    def _column_to_sql(col_config):
        """Convert column config to SQL representation (for testing)."""
        return column_to_sql(col_config)
