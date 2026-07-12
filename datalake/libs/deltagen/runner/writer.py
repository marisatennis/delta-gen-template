"""Writer module for persisting DataFrames to Delta Lake tables.

This module provides the Writer interface and DeltaWriter implementation
for writing transformed DataFrames to target tables. It supports both
append and merge (upsert) operations based on table configuration.

Writer Hooks:
    DeltaWriter supports pre_write_hook and post_write_hook for platform-specific
    customization without replacing core write logic:

    Example:
        def fabric_pre_write(spark, df, config, context):
            '''Setup Fabric shortcuts before write.'''
            create_external_tables_if_not_exists(spark, config)

        def fabric_post_write(spark, df, config, context, write_result):
            '''Log to tracking tables after write.'''
            spark.sql(log_last_table_load_query(config.name, context.load_id))

        writer = DeltaWriter()
        writer.write(
            spark, df, config,
            pre_write_hook=fabric_pre_write,
            post_write_hook=fabric_post_write,
        )
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol, runtime_checkable

from .exceptions import WriterError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from deltagen.model.table import TableConfig
    from deltagen.plugins.context import PluginContext


# Type aliases for hooks
PreWriteHook = Callable[
    ["SparkSession", "DataFrame", "TableConfig", "PluginContext | None"], None
]
PostWriteHook = Callable[
    ["SparkSession", "DataFrame", "TableConfig", "PluginContext | None", "WriteResult"], None
]


@dataclass
class WriteResult:
    """Result of a write operation, passed to post_write_hook.

    Attributes:
        target: Target table name
        mode: Write mode used ("append" or "merge")
        strategy: Merge strategy used (if mode="merge")
        rows_affected: Total rows processed from source DataFrame
        rows_inserted: Number of rows inserted (None if not tracked)
        rows_updated: Number of rows updated (None if not tracked)
        rows_deleted: Number of rows deleted (None if not tracked)
        rows_unchanged: Number of unchanged rows (None if not tracked)
        rows_expired: Number of expired rows (SCD2 only, None if not tracked)
        success: Whether the write succeeded
        error: Error message if failed

    Note:
        For append mode, rows_inserted equals rows_affected.

        For merge mode, Delta Lake provides detailed metrics via the merge
        operation's return value (numTargetRowsInserted, numTargetRowsUpdated,
        etc.). However, the current implementation does not surface these.
        Use rows_affected for the total source rows processed. The individual
        metric fields (rows_inserted, rows_updated, etc.) are None for merges.
    """

    target: str
    mode: str
    strategy: str | None = None
    rows_affected: int = 0
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    rows_unchanged: int | None = None
    rows_expired: int | None = None
    success: bool = True
    error: str | None = None


def drop_temporary_columns(df: "DataFrame", config: "TableConfig") -> "DataFrame":
    """Drop temporary columns from the DataFrame before persistence.

    Temporary columns are used for intermediate calculations during
    transformations but should not be persisted to the target table.

    Args:
        df: The DataFrame to process.
        config: The TableConfig containing column definitions.

    Returns:
        DataFrame with temporary columns removed.

    Examples:
        >>> df = drop_temporary_columns(transformed_df, config)
        >>> # df now excludes any columns marked temporary=True
    """
    temp_cols = config.temporary_columns
    if not temp_cols:
        return df

    # Only drop columns that exist in the DataFrame
    existing_cols = set(df.columns)
    cols_to_drop = [col for col in temp_cols if col in existing_cols]

    if cols_to_drop:
        return df.drop(*cols_to_drop)
    return df


@runtime_checkable
class Writer(Protocol):
    """Protocol defining the interface for writing DataFrames to storage.

    Implementations should handle the specifics of target table resolution,
    temporary column dropping, and the actual persistence mechanism.
    """

    def write(
        self,
        spark: "SparkSession",
        df: "DataFrame",
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Write a DataFrame to the target table.

        Args:
            spark: Active SparkSession for Delta operations.
            df: The DataFrame to persist.
            config: TableConfig with target info and write policies.
            debug: If True, print diagnostic information.
        """
        ...


class DeltaWriter:
    """Writer implementation for Delta Lake tables.

    Supports both append and merge modes based on the table's
    optimisation.load_mode policy. Merge mode requires natural
    key columns to be defined for the merge condition.

    Supports pre_write_hook and post_write_hook for platform-specific
    customization (e.g., Fabric shortcuts, custom logging tables).

    Examples:
        >>> writer = DeltaWriter()
        >>> writer.write(spark, df, config)  # Uses config.policies.optimisation.load_mode

        # With hooks for Fabric
        >>> writer.write(
        ...     spark, df, config,
        ...     pre_write_hook=fabric_setup,
        ...     post_write_hook=fabric_logging,
        ... )
    """

    def write(
        self,
        spark: "SparkSession",
        df: "DataFrame",
        config: "TableConfig",
        debug: bool = False,
        context: "PluginContext | None" = None,
        pre_write_hook: "PreWriteHook | None" = None,
        post_write_hook: "PostWriteHook | None" = None,
    ) -> WriteResult:
        """Write DataFrame to Delta Lake table.

        Automatically drops temporary columns and routes to the appropriate
        write method based on load_mode (append or merge).

        Args:
            spark: Active SparkSession.
            df: DataFrame to persist.
            config: TableConfig with target and policy information.
            debug: If True, print diagnostic information.
            context: Optional PluginContext for metrics and state.
            pre_write_hook: Optional hook called before write (for setup).
            post_write_hook: Optional hook called after write (for logging).

        Returns:
            WriteResult with details about the write operation.

        Raises:
            WriterError: If target resolution fails or merge mode lacks natural keys.
        """
        target = self._resolve_target(config)
        load_mode = config.policies.optimisation.load_mode
        strategy = config.policies.optimisation.merge_strategy if load_mode == "merge" else None

        if debug:
            print(f"DeltaWriter: Writing to '{target}' with mode '{load_mode}'")
            print(f"  Input columns: {df.columns}")
            print(f"  Temporary columns: {config.temporary_columns}")

        # Drop temporary columns before writing
        clean_df = drop_temporary_columns(df, config)

        if debug:
            print(f"  Output columns: {clean_df.columns}")

        # Initialize result
        result = WriteResult(target=target, mode=load_mode, strategy=strategy)

        try:
            # Call pre-write hook if provided
            if pre_write_hook:
                if debug:
                    print("  Calling pre_write_hook...")
                pre_write_hook(spark, clean_df, config, context)

            # Perform the write
            row_count = clean_df.count()
            result.rows_affected = row_count

            if load_mode == "append":
                self._write_append(clean_df, target, config, debug)
                result.rows_inserted = row_count
            elif load_mode == "overwrite":
                self._write_overwrite(clean_df, target, config, debug)
                result.rows_inserted = row_count
            elif load_mode == "merge":
                metrics = self._write_merge(spark, clean_df, target, config, debug)
                # Populate result with Delta Lake merge metrics
                if metrics:
                    result.rows_inserted = int(metrics.get("num_target_rows_inserted", 0))
                    result.rows_updated = int(metrics.get("num_target_rows_updated", 0))
                    result.rows_deleted = int(metrics.get("num_target_rows_deleted", 0))
                    # Calculate unchanged: rows matched but not updated
                    matched = int(metrics.get("num_target_rows_matched_updated", 0)) + int(metrics.get("num_target_rows_matched_deleted", 0))
                    num_source = int(metrics.get("num_source_rows", 0))
                    result.rows_unchanged = max(0, num_source - result.rows_inserted - matched)
                else:
                    # Table didn't exist, was created via append - all rows inserted
                    result.rows_inserted = row_count
                    result.rows_updated = 0
                    result.rows_deleted = 0
                    result.rows_unchanged = 0
            elif load_mode == "replace_by_partition":
                deleted, inserted = self._write_replace_by_partition(spark, clean_df, target, config, context, debug)
                result.rows_deleted = deleted
                result.rows_inserted = inserted
            else:
                raise WriterError(
                    f"Unsupported load mode: {load_mode}",
                    target=target,
                    mode=load_mode,
                    detail="Supported modes are 'append', 'merge', 'overwrite', and 'replace_by_partition'",
                )

            result.success = True

        except Exception as e:
            result.success = False
            result.error = str(e)
            raise

        finally:
            # Call post-write hook if provided.
            # The hook receives the WriteResult which includes success=True/False
            # and error message if failed, allowing hooks to handle both cases.
            if post_write_hook:
                if debug:
                    print("  Calling post_write_hook...")
                post_write_hook(spark, clean_df, config, context, result)

        return result

    def _resolve_target(self, config: "TableConfig") -> str:
        """Resolve the target table path or name from config.

        Target is determined by:
        1. target_schema + name if target_schema is specified (e.g., "sharepoint.customer_dim")
        2. Just name if no target_schema (e.g., "customer_dim")

        Note: 'layer' is semantic (bronze/silver/gold) for documentation.
              'target_schema' is the physical Fabric/database schema for the target.

        Args:
            config: TableConfig containing name and optional target_schema.

        Returns:
            The resolved target table identifier.

        Raises:
            WriterError: If table name is not configured.
        """
        if not config.name:
            raise WriterError(
                "Table name is required",
                detail="Config must have a 'name' attribute for target resolution",
            )

        if config.target_schema:
            return f"{config.target_schema}.{config.name}"
        return config.name

    def _write_append(
        self,
        df: "DataFrame",
        target: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Write DataFrame in append mode.

        Simply appends all rows to the target Delta table.

        Args:
            df: DataFrame to append.
            target: Target table identifier.
            config: TableConfig for partition scheme.
            debug: If True, print diagnostic information.
        """
        writer = df.write.format("delta").mode("append")

        # Apply partition scheme if configured
        partition_scheme = config.policies.optimisation.partition_scheme
        if partition_scheme:
            partition_cols = [col.strip() for col in partition_scheme.split(",")]
            writer = writer.partitionBy(*partition_cols)
            if debug:
                print(f"  Partitioning by: {partition_cols}")

        writer.saveAsTable(target)

        if debug:
            print(f"  Append to {target} completed")

    def _write_overwrite(
        self,
        df: "DataFrame",
        target: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Write DataFrame in overwrite mode.

        Replaces all data in the target Delta table (truncate + load).
        Use for full refresh scenarios where the entire table should be replaced.

        Args:
            df: DataFrame to write.
            target: Target table identifier.
            config: TableConfig for partition scheme.
            debug: If True, print diagnostic information.
        """
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

        # Apply partition scheme if configured
        partition_scheme = config.policies.optimisation.partition_scheme
        if partition_scheme:
            partition_cols = [col.strip() for col in partition_scheme.split(",")]
            writer = writer.partitionBy(*partition_cols)
            if debug:
                print(f"  Partitioning by: {partition_cols}")

        writer.saveAsTable(target)

        if debug:
            print(f"  Overwrite of {target} completed")

    def _write_merge(
        self,
        spark: "SparkSession",
        df: "DataFrame",
        target: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> dict | None:
        """Write DataFrame in merge mode using configured strategy.

        Routes to the appropriate merge strategy based on config.policies.optimisation.merge_strategy.

        Args:
            spark: Active SparkSession for DeltaTable access.
            df: DataFrame to merge.
            target: Target table identifier.
            config: TableConfig containing merge configuration.
            debug: If True, print diagnostic information.

        Returns:
            Dictionary with merge metrics from Delta Lake, or None if table was created via append.

        Raises:
            WriterError: If configuration is invalid for the chosen strategy.
        """
        # Import here to avoid requiring delta-spark when not using merge
        try:
            from delta.tables import DeltaTable
        except ImportError as e:
            raise WriterError(
                "Delta Lake library required for merge operations",
                target=target,
                mode="merge",
                detail="Install delta-spark: pip install delta-spark",
            ) from e

        strategy = config.policies.optimisation.merge_strategy
        natural_keys = config.get_natural_key_columns()

        if not natural_keys:
            raise WriterError(
                "Merge mode requires natural key columns",
                target=target,
                mode="merge",
                detail="Define columns with natural=True in the table config",
            )

        key_names = [col.name for col in natural_keys]
        merge_condition = " AND ".join(
            f"target.{key} = source.{key}" for key in key_names
        )

        if debug:
            print(f"  Merge strategy: {strategy}")
            print(f"  Merge keys: {key_names}")
            print(f"  Merge condition: {merge_condition}")

        # Check if target table exists - create if not
        if not self._table_exists(spark, target):
            if debug:
                print(f"  Target table '{target}' does not exist, creating...")
            self._write_append(df, target, config, debug)
            return None  # No merge metrics for initial append

        delta_table = DeltaTable.forName(spark, target)

        # Route to appropriate strategy and capture metrics
        metrics = None
        if strategy == "update_all":
            metrics = self._merge_update_all(delta_table, df, merge_condition, debug)
        elif strategy == "update_changed":
            metrics = self._merge_update_changed(delta_table, df, merge_condition, config, debug)
        elif strategy == "insert_only":
            metrics = self._merge_insert_only(delta_table, df, merge_condition, debug)
        elif strategy == "scd_type2":
            metrics = self._merge_scd_type2(spark, delta_table, df, target, merge_condition, config, debug)
        elif strategy == "accumulating":
            metrics = self._merge_accumulating(delta_table, df, merge_condition, config, debug)
        elif strategy == "soft_delete":
            metrics = self._merge_soft_delete(delta_table, df, merge_condition, config, debug)
        else:
            raise WriterError(
                f"Unsupported merge strategy: {strategy}",
                target=target,
                mode="merge",
                detail="Supported strategies: update_all, update_changed, insert_only, scd_type2, accumulating, soft_delete",
            )

        if debug:
            print(f"  Merged data into {target} using '{strategy}' strategy")
            if metrics:
                print(f"  Merge metrics: {metrics}")
        
        return metrics

    def _merge_update_all(
        self,
        delta_table: Any,
        df: "DataFrame",
        merge_condition: str,
        debug: bool = False,
    ) -> dict:
        """Type 1 SCD - Update all columns on match, insert on no match.

        This is the simplest merge strategy, equivalent to a standard upsert.
        All columns are overwritten when a matching record is found.

        Use for: Dimensions where you don't need history and always want current values.
        
        Returns:
            Dictionary with merge metrics from Delta Lake.
        """
        metrics = (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return metrics

    def _merge_update_changed(
        self,
        delta_table: Any,
        df: "DataFrame",
        merge_condition: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Type 1 SCD with change detection - Only update if data actually changed.

        Compares a hash of specified columns to detect real changes, avoiding
        unnecessary updates that would change modification timestamps.

        Requires: hash_columns in config or a 'row_hash' column in the DataFrame.

        Use for: Dimensions where you want Type 1 behavior but need to track
                 when records actually changed (not just when they were processed).
        """
        hash_columns = config.policies.optimisation.hash_columns

        if hash_columns:
            # Build hash comparison condition
            hash_expr = f"md5(concat_ws('|', {', '.join(f'source.{c}' for c in hash_columns)}))"
            target_hash_expr = f"md5(concat_ws('|', {', '.join(f'target.{c}' for c in hash_columns)}))"
            change_condition = f"{hash_expr} != {target_hash_expr}"
        else:
            # Assume row_hash column exists in both source and target
            change_condition = "source.row_hash != target.row_hash"

        if debug:
            print(f"  Change detection condition: {change_condition}")

        # Build explicit column mapping for update (Delta doesn't support "*" with aliases)
        update_set = {col: f"source.{col}" for col in df.columns}

        metrics = (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(condition=change_condition, set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )
        return metrics

    def _merge_insert_only(
        self,
        delta_table: Any,
        df: "DataFrame",
        merge_condition: str,
        debug: bool = False,
    ) -> dict:
        """Insert-only deduplication - Skip records that already exist.

        Only inserts new records; existing records are never updated.
        Useful for deduplicating incoming data against existing records.

        Use for: Event/transaction tables where you want to prevent duplicates
                 but never update existing records.
                 
        Returns:
            Dictionary with merge metrics from Delta Lake.
        """
        metrics = (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
        return metrics

    def _merge_scd_type2(
        self,
        spark: "SparkSession",
        delta_table: Any,
        df: "DataFrame",
        target: str,
        merge_condition: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Type 2 SCD - Track full history with effective dates.

        When a record changes:
        1. Expire the current record (set end_date, clear is_current flag)
        2. Insert the new version (set effective_date to now, is_current=True)

        Requires columns: effective_date_col, end_date_col, current_flag_col
        (configurable in optimisation policy, defaults to effective_date, end_date, is_current)

        Change Detection:
            By default, ALL non-key, non-SCD columns are compared to detect changes.
            If hash_columns is specified in the optimisation policy, only those
            columns will trigger new historical versions when changed. This is useful
            when you want to track history for specific business attributes while
            ignoring changes to metadata columns (e.g., last_modified_date).

        The source DataFrame should have is_current=true and appropriate effective_date
        values set. The writer will:
        - Expire existing current records when data has changed
        - Insert new versions for changed records
        - Insert completely new records

        Use for: Dimensions where you need full historical tracking (customer history,
                 product price history, etc.)
        """
        effective_col = config.policies.optimisation.effective_date_col
        end_col = config.policies.optimisation.end_date_col
        current_col = config.policies.optimisation.current_flag_col

        if debug:
            print(f"  SCD Type 2 columns: effective={effective_col}, end={end_col}, current={current_col}")

        # Get natural key column names
        key_names = [col.name for col in config.get_natural_key_columns()]

        # Build condition for changed records
        # Use NULL-safe comparison with <=> operator
        # If hash_columns is specified, only those columns trigger new versions
        # Otherwise, compare all non-key, non-scd columns
        scd_columns = {effective_col, end_col, current_col}
        hash_columns = config.policies.optimisation.hash_columns

        if hash_columns:
            # Only compare specified hash columns for change detection
            compare_columns = hash_columns
            if debug:
                print(f"  Using hash_columns for change detection: {hash_columns}")
        else:
            # Compare all non-key, non-scd columns
            compare_columns = [
                c for c in df.columns
                if c not in key_names and c not in scd_columns
            ]

        if compare_columns:
            # Use NOT (a <=> b) for NULL-safe "not equal" comparison
            change_conditions = [
                f"NOT (source.{c} <=> target.{c})" for c in compare_columns
            ]
            changed_condition = " OR ".join(change_conditions)
        else:
            changed_condition = "1=1"  # Always treat as changed if no compare columns

        # Condition for current records that have changed
        current_and_changed = f"target.{current_col} = true AND ({changed_condition})"

        if debug:
            print(f"  Change detection on columns: {compare_columns}")
            print(f"  Change condition: {current_and_changed}")

        # Build merge condition that only matches current records
        # This ensures we only compare against the current version
        merge_condition_current = f"{merge_condition} AND target.{current_col} = true"

        # SCD Type 2 requires two operations:
        # 1. Expire old versions of changed records
        # 2. Insert new versions (handled by whenNotMatchedInsertAll after expire)

        # The key insight is that after we expire the current record,
        # the source record becomes "not matched" because we only match
        # against current records. So we can use a single merge.
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition_current)
            # Expire current records that have changed
            .whenMatchedUpdate(
                condition=changed_condition,
                set={
                    end_col: "current_timestamp()",
                    current_col: "false",
                }
            )
            # Insert all source records that don't match current target records
            # This includes: new keys AND new versions of changed records
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _merge_accumulating(
        self,
        delta_table: Any,
        df: "DataFrame",
        merge_condition: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Accumulating snapshot - Update only specified milestone columns.

        Updates only the milestone columns that have new non-null values,
        preserving existing values for other milestones. New records are
        inserted with all available data.

        Requires: milestone_columns list in optimisation policy.

        Use for: Order fulfillment tracking, process stage tracking where
                 different columns get populated at different times.
        """
        milestone_cols = config.policies.optimisation.milestone_columns

        if not milestone_cols:
            raise WriterError(
                "Accumulating strategy requires milestone_columns",
                mode="merge",
                detail="Set policies.optimisation.milestone_columns to list of milestone column names",
            )

        if debug:
            print(f"  Milestone columns: {milestone_cols}")

        # Build update set: only update milestone columns when source has non-null value
        update_set = {}
        for col in milestone_cols:
            update_set[col] = f"COALESCE(source.{col}, target.{col})"

        # Also update any non-milestone, non-key columns
        key_names = [c.name for c in config.get_natural_key_columns()]
        for col in df.columns:
            if col not in milestone_cols and col not in key_names and col not in update_set:
                update_set[col] = f"source.{col}"

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _merge_soft_delete(
        self,
        delta_table: Any,
        df: "DataFrame",
        merge_condition: str,
        config: "TableConfig",
        debug: bool = False,
    ) -> None:
        """Soft delete - Mark records as deleted instead of removing.

        Records in target that don't exist in source are marked as deleted
        (is_deleted=True) rather than being physically removed.

        Requires: deleted_flag_col in optimisation policy (defaults to 'is_deleted').
        Source DataFrame should have the deleted flag column.

        Use for: Maintaining referential integrity, audit trails, or when
                 you need to track what was deleted and when.
        """
        deleted_col = config.policies.optimisation.deleted_flag_col

        if debug:
            print(f"  Soft delete column: {deleted_col}")

        # Check if source has the deleted flag column
        if deleted_col not in df.columns:
            raise WriterError(
                f"Soft delete requires '{deleted_col}' column in source DataFrame",
                mode="merge",
                detail=f"Add '{deleted_col}' column to your transformation or change deleted_flag_col in config",
            )

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            # Update existing records (including potentially marking as deleted)
            .whenMatchedUpdateAll()
            # Insert new records
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _write_replace_by_partition(
        self,
        spark: "SparkSession",
        df: "DataFrame",
        target: str,
        config: "TableConfig",
        context: "PluginContext | None",
        debug: bool = False,
    ) -> tuple[int, int]:
        """Delete existing rows for incoming periods then insert fresh rows.

        Relies on the period_replace stage plugin having run first — it stores
        the list of periods and the period column name in context state.

        Args:
            spark: Active SparkSession.
            df: Transformed DataFrame to write.
            target: Target table identifier.
            config: TableConfig.
            context: PluginContext carrying period state from period_replace plugin.
            debug: If True, print diagnostic information.

        Returns:
            Tuple of (rows_deleted, rows_inserted).

        Raises:
            WriterError: If context is missing or period_replace plugin did not run.
        """
        if context is None:
            raise WriterError(
                "replace_by_partition requires a PluginContext",
                target=target,
                mode="replace_by_partition",
                detail="Ensure context is passed to DeltaWriter.write()",
            )

        periods = context.get_state("periods_to_replace")
        period_column = context.get_state("target_period_column") or context.get_state("period_column")

        if not periods or not period_column:
            raise WriterError(
                "replace_by_partition requires the period_replace stage plugin to run first",
                target=target,
                mode="replace_by_partition",
                detail="Add a stage with stage_plugin: period_replace before the write step",
            )

        row_count = df.count()

        # Table doesn't exist yet — just create it via append
        if not self._table_exists(spark, target):
            if debug:
                print(f"  Target '{target}' does not exist, creating via append")
            self._write_append(df, target, config, debug)
            return 0, row_count

        def _sql_in_list(values):
            return ", ".join(
                str(v) if isinstance(v, (int, float)) else f"'{v}'" for v in values
            )

        # Check for per-partition period sets (composite_period_replace plugin)
        partition_period_sets = context.get_state("partition_period_sets") if context else None
        partition_columns = context.get_state("partition_columns") if context else None

        deleted_count = 0
        if partition_period_sets and partition_columns:
            # Issue one DELETE per partition group, each scoped to its own periods
            for entry in partition_period_sets:
                entry_periods = entry.get("periods", [])
                if not entry_periods:
                    continue
                where_clauses = [f"{period_column} IN ({_sql_in_list(entry_periods)})"]
                for col in partition_columns:
                    where_clauses.append(f"{col} = '{entry[col]}'")
                where_sql = " AND ".join(where_clauses)

                if debug:
                    print(f"  Deleting from {target} WHERE {where_sql}")

                result = spark.sql(f"DELETE FROM {target} WHERE {where_sql}")
                try:
                    deleted_count += result.first()["num_deleted_rows"] if result.columns else 0
                except (KeyError, Exception):
                    pass
        else:
            # Standard single-column period replace
            periods_sql = _sql_in_list(periods)

            if debug:
                print(f"  Deleting {len(periods)} period(s) from {target}")

            result = spark.sql(
                f"DELETE FROM {target} WHERE {period_column} IN ({periods_sql})"
            )
            try:
                deleted_count = result.first()["num_deleted_rows"] if result.columns else 0
            except (KeyError, Exception):
                deleted_count = 0

        if debug:
            print(f"  Deleted {deleted_count} rows, inserting {row_count} rows")

        df.write.format("delta").mode("append").saveAsTable(target)

        return deleted_count, row_count

    def _table_exists(self, spark: "SparkSession", target: str) -> bool:
        """Check if a table exists in the catalog.

        Args:
            spark: Active SparkSession.
            target: Table name to check (can include database prefix).

        Returns:
            True if the table exists, False otherwise.
        """
        try:
            return spark.catalog.tableExists(target)
        except Exception:
            return False
