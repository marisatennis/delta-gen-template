"""Default starter plugins for Delta-Gen.

This module provides a small set of commonly-needed plugins that ship with
Delta-Gen out of the box. They serve as:
1. Examples of how to write plugins
2. Commonly-needed functionality available immediately
3. Templates for users creating their own plugins

Column Plugins:
- mask_email: GDPR-compliant email masking
- not_null: Validate column has no null values
- in_set: Validate column values are in an allowed set

Stage Plugins - Incremental Loading:
- incremental_dedupe: Filter by watermark + dedupe (for file sources with corrections)
- incremental_append: Filter by watermark, flag duplicates (for system sources)
- period_replace: Identify periods for full replacement (for snapshot data)
- filter_latest_file_per_period: Keep rows from most recent file per (partition, period)
- dedupe_keep_last: Standalone deduplication keeping latest per key
- distinct: Remove exact duplicate rows (all columns or specific subset)
- delta_load: [DEPRECATED] Alias for incremental_dedupe

For custom/business-specific plugins, create them in your own package
(e.g., lakehouse-utils) and use the same @register_column/@register_stage
decorators.

Example YAML usage:
    columns:
      - name: email
        inputs:
          - source: raw_customers
            column: email_address
        extensions:
          transform: mask_email

      - name: status
        inputs:
          - source: raw_customers
            column: status_code
        extensions:
          transform: in_set
          allowed_values: ["ACTIVE", "INACTIVE", "PENDING"]
          on_violation: reject
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_column, register_stage
from deltagen.plugins.metrics import MetricAction

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from deltagen.model.column import ColumnConfig
    from deltagen.model.stage import StageConfig
    from deltagen.plugins.context import PluginContext

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Column Plugins
# -----------------------------------------------------------------------------


@register_column(
    "mask_email",
    description="GDPR-compliant email masking - preserves first char and domain",
    version="1.0.0",
    author="Delta-Gen",
    tags={"pii", "gdpr", "masking"},
)
def mask_email(
    df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Mask email addresses for GDPR/PII compliance.

    Transforms emails like 'john.doe@example.com' to 'j***@example.com'.
    Preserves the first character and domain for debugging while hiding
    the identifying portion.

    Args:
        df: Input DataFrame
        column: Column configuration (uses column.name as target)
        ctx: Plugin context for logging and metrics

    Returns:
        DataFrame with masked email column

    YAML Config:
        columns:
          - name: email
            inputs:
              - source: customers
                column: email_address
            extensions:
              transform: mask_email
    """
    from pyspark.sql import functions as F

    col_name = column.name

    # Build masked email: first_char + "***@" + domain
    # Handle nulls gracefully - they pass through as null
    masked = F.when(
        F.col(col_name).isNotNull() & F.col(col_name).contains("@"),
        F.concat(
            F.substring(F.col(col_name), 1, 1),
            F.lit("***@"),
            F.element_at(F.split(F.col(col_name), "@"), 2),
        ),
    ).otherwise(F.col(col_name))

    ctx.log_info(f"Masking email column: {col_name}")

    return df.withColumn(col_name, masked)


@register_column(
    "not_null",
    description="Validate column has no null values with configurable action",
    version="1.0.0",
    author="Delta-Gen",
    tags={"dq", "validation", "quality"},
)
def not_null(
    df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Validate that a column contains no null values.

    Configurable actions when nulls are found:
    - reject: Remove rows with nulls (default)
    - warn: Log warning but keep rows
    - fill: Replace nulls with a default value

    Args:
        df: Input DataFrame
        column: Column configuration with optional extensions:
            - on_null: Action to take ("reject", "warn", "fill")
            - fill_value: Value to use when on_null="fill"
        ctx: Plugin context for logging and metrics

    Returns:
        DataFrame with nulls handled per configuration

    YAML Config:
        columns:
          - name: customer_id
            type: string
            extensions:
              transform: not_null
              on_null: reject  # or "warn" or "fill"
              fill_value: "UNKNOWN"  # required if on_null=fill
    """
    from pyspark.sql import functions as F

    col_name = column.name
    extensions = column.extensions or {}
    action = extensions.get("on_null", "reject")
    fill_value = extensions.get("fill_value")

    # Count nulls
    null_rows = df.filter(F.col(col_name).isNull())
    null_count = null_rows.count()

    if null_count == 0:
        ctx.log_debug(f"Column {col_name}: no nulls found")
        return df

    ctx.log_info(f"Column {col_name}: found {null_count} null values, action={action}")

    # Log sample of rejected records for debugging
    show_rejected = extensions.get("show_rejected", True)
    rejected_sample_size = extensions.get("rejected_sample_size", 5)
    if show_rejected and null_count > 0:
        ctx.log_warning(f"Sample of {min(null_count, rejected_sample_size)} rejected records (null {col_name}):")
        sample_rows = null_rows.limit(rejected_sample_size).collect()
        for i, row in enumerate(sample_rows, 1):
            row_dict = row.asDict()
            # Show first few columns to keep output manageable
            preview_cols = list(row_dict.keys())[:6]
            preview = {k: row_dict[k] for k in preview_cols}
            ctx.log_warning(f"  [{i}] {preview}")

    if action == "reject":
        # Record rejected rows in metrics
        ctx.metrics.record_nulls(
            column_name=col_name,
            count=null_count,
            action=MetricAction.REJECTED,
        )
        # Write rejected records to DQ table for investigation
        ctx.write_rejected_records(
            df=df,
            rejected_df=null_rows,
            reason="null_value",
            column_name=col_name,
            rule_name="not_null",
        )
        return df.filter(F.col(col_name).isNotNull())

    elif action == "warn":
        # Just log and record, don't modify data
        ctx.metrics.record_nulls(
            column_name=col_name,
            count=null_count,
            action=MetricAction.FLAGGED,
        )
        ctx.log_warning(f"Column {col_name} has {null_count} null values")
        return df

    elif action == "fill":
        if fill_value is None:
            ctx.log_error(f"on_null=fill requires fill_value for column {col_name}")
            # Fall back to warning
            ctx.metrics.record_nulls(
                column_name=col_name,
                count=null_count,
                action=MetricAction.FLAGGED,
            )
            return df

        ctx.metrics.record_nulls(
            column_name=col_name,
            count=null_count,
            action=MetricAction.FILLED_DEFAULT,
        )
        return df.fillna({col_name: fill_value})

    else:
        ctx.log_warning(f"Unknown on_null action '{action}', defaulting to warn")
        ctx.metrics.record_nulls(
            column_name=col_name,
            count=null_count,
            action=MetricAction.FLAGGED,
        )
        return df


@register_column(
    "in_set",
    description="Validate column values are in an allowed set",
    version="1.0.0",
    author="Delta-Gen",
    tags={"dq", "validation", "quality"},
)
def in_set(
    df: "DataFrame", column: "ColumnConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Validate that column values are within an allowed set.

    Useful for enum-like columns (status codes, categories, etc.)

    Args:
        df: Input DataFrame
        column: Column configuration with extensions:
            - allowed_values: List of valid values (required)
            - on_violation: Action when invalid ("reject", "warn", "fill")
            - fill_value: Replacement value when on_violation="fill"
        ctx: Plugin context for logging and metrics

    Returns:
        DataFrame with invalid values handled per configuration

    YAML Config:
        columns:
          - name: status
            type: string
            extensions:
              transform: in_set
              allowed_values: ["ACTIVE", "INACTIVE", "PENDING"]
              on_violation: reject
    """
    col_name = column.name
    extensions = column.extensions or {}
    allowed_values = extensions.get("allowed_values", [])
    action = extensions.get("on_violation", "reject")
    fill_value = extensions.get("fill_value")

    # Validate config before importing Spark
    if not allowed_values:
        ctx.log_error(f"in_set plugin requires 'allowed_values' for column {col_name}")
        return df

    from pyspark.sql import functions as F

    # Count invalid values (not in set and not null)
    invalid_filter = ~F.col(col_name).isin(allowed_values) & F.col(col_name).isNotNull()
    invalid_rows = df.filter(invalid_filter)
    invalid_count = invalid_rows.count()

    if invalid_count == 0:
        ctx.log_debug(f"Column {col_name}: all values in allowed set")
        return df

    ctx.log_info(
        f"Column {col_name}: {invalid_count} values not in {allowed_values}, "
        f"action={action}"
    )

    # Log sample of rejected records for debugging
    show_rejected = extensions.get("show_rejected", True)
    rejected_sample_size = extensions.get("rejected_sample_size", 5)
    if show_rejected and invalid_count > 0:
        ctx.log_warning(f"Sample of {min(invalid_count, rejected_sample_size)} rejected records (invalid {col_name}):")
        sample_rows = invalid_rows.limit(rejected_sample_size).collect()
        for i, row in enumerate(sample_rows, 1):
            row_dict = row.asDict()
            invalid_value = row_dict.get(col_name)
            preview_cols = list(row_dict.keys())[:6]
            preview = {k: row_dict[k] for k in preview_cols}
            ctx.log_warning(f"  [{i}] {col_name}={invalid_value!r} | {preview}")

    if action == "reject":
        ctx.metrics.record_validation_failure(
            rule_name="in_set",
            column_name=col_name,
            count=invalid_count,
            action=MetricAction.REJECTED,
        )
        # Write rejected records to DQ table for investigation
        ctx.write_rejected_records(
            df=df,
            rejected_df=invalid_rows,
            reason="invalid_value",
            column_name=col_name,
            rule_name="in_set",
        )
        # Keep rows where value is in set OR is null (nulls handled separately)
        return df.filter(F.col(col_name).isin(allowed_values) | F.col(col_name).isNull())

    elif action == "warn":
        ctx.metrics.record_validation_failure(
            rule_name="in_set",
            column_name=col_name,
            count=invalid_count,
            action=MetricAction.FLAGGED,
        )
        ctx.log_warning(
            f"Column {col_name} has {invalid_count} values not in allowed set"
        )
        return df

    elif action == "fill":
        if fill_value is None:
            ctx.log_error(
                f"on_violation=fill requires fill_value for column {col_name}"
            )
            ctx.metrics.record_validation_failure(
                rule_name="in_set",
                column_name=col_name,
                count=invalid_count,
                action=MetricAction.FLAGGED,
            )
            return df

        ctx.metrics.record_validation_failure(
            rule_name="in_set",
            column_name=col_name,
            count=invalid_count,
            action=MetricAction.FILLED_DEFAULT,
        )
        # Replace invalid values with fill_value
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isin(allowed_values) | F.col(col_name).isNull(),
                F.col(col_name),
            ).otherwise(F.lit(fill_value)),
        )

    else:
        ctx.log_warning(f"Unknown on_violation action '{action}', defaulting to warn")
        ctx.metrics.record_validation_failure(
            rule_name="in_set",
            column_name=col_name,
            count=invalid_count,
            action=MetricAction.FLAGGED,
        )
        return df


# -----------------------------------------------------------------------------
# Stage Plugins
# -----------------------------------------------------------------------------


@register_stage(
    "dedupe_keep_last",
    description="Deduplicate rows keeping the latest record per key",
    version="1.0.0",
    author="Delta-Gen",
    tags={"dedupe", "quality"},
)
def dedupe_keep_last(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Deduplicate DataFrame keeping the most recent record per key.

    Use this when source data may contain multiple versions of the same record
    (e.g., file re-uploads with corrections). Keeps only the latest version
    per natural key based on an ordering column.

    Unlike Spark's dropDuplicates() which keeps the first occurrence,
    this plugin uses a window function to keep the latest record based
    on an order column.

    Args:
        df: Input DataFrame
        stage: Stage configuration with extensions:
            - partition_by: List of key columns to dedupe on (optional, defaults to natural keys)
            - order_by: Column to determine "latest" (required)
            - order_desc: If True (default), keep highest value; if False, keep lowest
        ctx: Plugin context for logging and metrics

    Returns:
        Deduplicated DataFrame

    YAML Config:
        # With explicit partition_by
        stages:
          - name: dedupe
            extensions:
              stage_plugin: dedupe_keep_last
              partition_by: [customer_id]
              order_by: updated_at
              order_desc: true  # keep most recent (default)

        # Or default to natural keys (no partition_by needed)
        stages:
          - name: dedupe
            extensions:
              stage_plugin: dedupe_keep_last
              order_by: updated_at
    """
    extensions = stage.extensions or {}
    partition_by = extensions.get("partition_by", [])
    order_by = extensions.get("order_by")
    order_desc = extensions.get("order_desc", True)

    # Default to natural keys if partition_by not specified
    if not partition_by:
        if ctx.config:
            natural_keys = ctx.config.get_natural_key_columns()
            if natural_keys:
                partition_by = [col.name for col in natural_keys]
                ctx.log_debug(f"dedupe_keep_last defaulting to natural keys: {partition_by}")
            else:
                ctx.log_error("dedupe_keep_last requires 'partition_by' in stage extensions or natural keys defined in table config")
                return df
        else:
            ctx.log_error("dedupe_keep_last requires 'partition_by' in stage extensions when no table config available")
            return df

    if not order_by:
        ctx.log_error("dedupe_keep_last requires 'order_by' in stage extensions")
        return df

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Count rows before dedup
    input_count = df.count()

    # Build window spec
    order_col = F.desc(order_by) if order_desc else F.asc(order_by)
    window = Window.partitionBy(partition_by).orderBy(order_col)

    # Add row number and filter to keep first (which is latest due to desc order)
    deduped_df = (
        df.withColumn("_dedupe_rn", F.row_number().over(window))
        .filter(F.col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
    )

    output_count = deduped_df.count()
    dup_count = input_count - output_count

    if dup_count > 0:
        ctx.log_info(
            f"Deduplicated on {partition_by} by {order_by}: "
            f"removed {dup_count} duplicates ({input_count} -> {output_count} rows)"
        )
        ctx.metrics.record_duplicates(
            columns=partition_by,
            count=dup_count,
            action=MetricAction.KEPT_LATEST,
        )
    else:
        ctx.log_debug(f"No duplicates found on {partition_by}")

    return deduped_df


@register_stage(
    "distinct",
    description="Remove exact duplicate rows from the DataFrame",
    version="1.0.0",
    author="Delta-Gen",
    tags={"dedupe", "quality"},
)
def distinct_rows(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Remove exact duplicate rows from the DataFrame.

    Two modes:
    - No columns specified: applies df.distinct() across all columns.
    - Columns specified: applies df.dropDuplicates(columns) keeping the
      first occurrence per key (non-deterministic unless combined with
      an orderBy).

    Args:
        df: Input DataFrame
        stage: Stage configuration with optional extensions:
            - columns: List of column names to distinct on (optional).
                      If omitted, deduplicates on all columns.
        ctx: Plugin context for logging and metrics

    Returns:
        Deduplicated DataFrame

    YAML Config:
        # Distinct on all columns
        stages:
          - name: distinct
            extensions:
              stage_plugin: distinct

        # Distinct on specific columns
        stages:
          - name: distinct
            extensions:
              stage_plugin: distinct
              columns: [customer_id, email]
    """
    extensions = stage.extensions or {}
    columns = extensions.get("columns", [])

    input_count = df.count()

    if columns:
        result = df.dropDuplicates(columns)
        ctx.log_info(f"Distinct on columns {columns}")
    else:
        result = df.distinct()
        ctx.log_info("Distinct on all columns")

    output_count = result.count()
    removed = input_count - output_count

    if removed > 0:
        ctx.log_info(f"Removed {removed} duplicate rows ({input_count} -> {output_count})")
        ctx.metrics.record_duplicates(
            columns=columns or ["*"],
            count=removed,
            action=MetricAction.REJECTED,
        )
    else:
        ctx.log_debug("No duplicate rows found")

    return result


@register_stage(
    "check_duplicates",
    description="Check for unexpected duplicates and log to DQ table",
    version="1.0.0",
    author="Delta-Gen",
    tags={"dq", "validation", "duplicates"},
)
def check_duplicates(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Check for unexpected duplicates - for sources where duplicates indicate issues.

    Use this for data from systems like D365, Salesforce where each natural key
    should have exactly one record. If duplicates exist, they are flagged/rejected
    as data quality issues rather than silently deduped.

    Duplicates are logged to dq.duplicates_table if configured on TableConfig.

    Args:
        df: Input DataFrame
        stage: Stage configuration with extensions:
            - natural_keys: List of columns forming the natural key (required)
            - on_duplicate: Action when duplicates found - "reject" (default) or "flag"
        ctx: Plugin context for logging and metrics

    Returns:
        DataFrame with duplicates handled per config

    YAML Config:
        stages:
          - name: check_dups
            extensions:
              stage_plugin: check_duplicates
              natural_keys: [account_id]
              on_duplicate: reject  # or "flag" to keep but log as DQ issue
    """
    extensions = stage.extensions or {}
    natural_keys = extensions.get("natural_keys", [])
    on_duplicate = extensions.get("on_duplicate", "reject")

    # Validate config
    if not natural_keys:
        ctx.log_error("check_duplicates requires 'natural_keys' in stage extensions")
        return df

    from pyspark.sql import functions as F

    input_count = df.count()

    if input_count == 0:
        ctx.log_info("No records to check for duplicates")
        return df

    # Check for duplicates (don't silently dedupe!)
    dup_check = (
        df.groupBy(natural_keys)
        .count()
        .filter(F.col("count") > 1)
    )
    dup_count = dup_check.count()

    if dup_count > 0:
        # Get total duplicate rows
        dup_key_counts = dup_check.collect()
        total_dup_rows = sum(row["count"] - 1 for row in dup_key_counts)

        ctx.log_warning(
            f"UNEXPECTED DUPLICATE KEYS: {dup_count} keys with {total_dup_rows} extra rows"
        )

        # Log sample of duplicates
        sample_dups = dup_check.limit(5).collect()
        for row in sample_dups:
            key_vals = {k: row[k] for k in natural_keys}
            ctx.log_warning(f"  Duplicate key: {key_vals} (count={row['count']})")

        # Write duplicates to DQ table for investigation
        ctx.write_duplicate_records(df, dup_check, natural_keys)

        if on_duplicate == "reject":
            ctx.log_error(f"Rejecting {total_dup_rows} duplicate rows")
            ctx.metrics.record_duplicates(
                columns=natural_keys,
                count=total_dup_rows,
                action=MetricAction.REJECTED,
            )
            # Keep only first occurrence of each key
            from pyspark.sql.window import Window
            window = Window.partitionBy(natural_keys).orderBy(F.lit(1))
            df = (
                df.withColumn("_dup_rn", F.row_number().over(window))
                .filter(F.col("_dup_rn") == 1)
                .drop("_dup_rn")
            )
        else:  # flag
            ctx.log_warning(f"Flagging {total_dup_rows} duplicate rows as DQ issue (keeping all)")
            ctx.metrics.record_duplicates(
                columns=natural_keys,
                count=total_dup_rows,
                action=MetricAction.FLAGGED,
            )
    else:
        ctx.log_debug(f"No unexpected duplicates found on {natural_keys}")

    return df


@register_stage(
    "period_replace",
    description="Period-based load: identify periods in source for full replacement",
    version="1.0.0",
    author="Delta-Gen",
    tags={"incremental", "period", "snapshot"},
)
def period_replace(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Period-based replacement - for snapshot data without natural keys.

    Use this when you can't identify natural keys and need to replace entire
    periods of data. This plugin identifies the periods in the source data
    and returns the DataFrame. The actual delete + insert happens at write time.

    The plugin:
    1. Identifies distinct periods in the source data
    2. Logs which periods will be replaced
    3. Stores period info in context for the writer to use
    4. Returns the full DataFrame (no filtering/deduping)

    Args:
        df: Input DataFrame (source data)
        stage: Stage configuration with extensions:
            - period_column: Column identifying the period (required)
            - target_period_column: Column name in target (defaults to period_column)
            - target_table: Target table name for logging
        ctx: Plugin context for logging and metrics

    Returns:
        DataFrame as-is (periods recorded in context for writer)

    YAML Config:
        stages:
          - name: load
            extensions:
              stage_plugin: period_replace
              period_column: report_period
              target_period_column: report_period
              target_table: silver.monthly_snapshot

    Note:
        The write step must handle the actual delete + insert logic.
        Use DeltaWriter with mode="replace_by_partition" or handle manually:
          1. DELETE FROM target WHERE period_column IN (periods_to_replace)
          2. INSERT INTO target SELECT * FROM source_df
    """
    extensions = stage.extensions or {}
    period_column = extensions.get("period_column")
    target_period_column = extensions.get("target_period_column", period_column)
    target_table = extensions.get("target_table", "target")

    # Validate config
    if not period_column:
        ctx.log_error("period_replace requires 'period_column' in stage extensions")
        return df

    from pyspark.sql import functions as F

    # Identify distinct periods in source
    periods_df = df.select(period_column).distinct()
    periods = [row[period_column] for row in periods_df.collect()]

    ctx.log_info(f"Period replacement: found {len(periods)} period(s) in source data")
    for period in periods:
        ctx.log_info(f"  - Period: {period}")

    # Store periods in context for writer to use
    ctx.set_state("periods_to_replace", periods)
    ctx.set_state("period_column", period_column)
    ctx.set_state("target_period_column", target_period_column)

    # Record metrics
    row_count = df.count()
    ctx.log_info(f"Total records for period replacement: {row_count}")

    ctx.metrics.record_source_read(
        source_name=f"period_replace_{target_table}",
        row_count=row_count,
    )

    # Return DataFrame as-is - no filtering or deduping
    return df


@register_stage(
    "filter_latest_file_per_period",
    description="Keep only rows from the most recent source file per (partition, period)",
    version="1.0.0",
    author="Delta-Gen",
    tags={"incremental", "dedup", "snapshot"},
)
def filter_latest_file_per_period(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Retain only rows belonging to the latest source file for each period.

    Use this before aggregation when bronze holds multiple file versions for
    the same period (e.g. a file was re-uploaded with corrections). Keeps all
    rows from the most recent file and discards earlier versions.

    It finds max(order_column) per (partition_column, period_column) and keeps
    every row that matches that max — preserving all rows from the latest file,
    not just one row per period.

    Args:
        df: Input DataFrame (columns already normalised by base stage).
        stage: Stage configuration with optional extensions:
            - period_column: Column identifying the period (default: source_period)
            - partition_column: Column identifying the source partition (default: platform)
            - order_column: Column used to pick the latest file (default: source_modified)
        ctx: Plugin context for logging and metrics.

    Returns:
        DataFrame containing only rows from the latest file per (partition, period).

    YAML Config:
        stages:
          - name: latest_file
            extensions:
              stage_plugin: filter_latest_file_per_period
              period_column: source_period
              partition_column: platform
              order_column: source_modified
    """
    from pyspark.sql import functions as F

    extensions = stage.extensions or {}
    period_col = extensions.get("period_column", "source_period")
    partition_col = extensions.get("partition_column", "platform")
    order_col = extensions.get("order_column", "source_modified")

    for col in (period_col, partition_col, order_col):
        if col not in df.columns:
            ctx.log_error(f"filter_latest_file_per_period: column '{col}' not found in DataFrame")
            return df

    before_count = df.count()

    max_modified = (
        df.groupBy(partition_col, period_col)
        .agg(F.max(order_col).alias("_max_modified"))
    )

    result = (
        df.join(max_modified, on=[partition_col, period_col], how="inner")
        .filter(F.col(order_col) == F.col("_max_modified"))
        .drop("_max_modified")
    )

    after_count = result.count()
    ctx.log_info(
        f"filter_latest_file_per_period: kept {after_count} rows "
        f"(dropped {before_count - after_count} rows from older file versions)"
    )

    return result
