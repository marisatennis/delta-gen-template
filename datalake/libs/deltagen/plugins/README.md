# Delta-Gen Plugin System

The plugin system provides an extensibility layer for Delta-Gen, allowing external packages (like Lakehouse-Build) to inject custom transforms and stage processors into the transformation pipeline.

Note: Writer customization is handled via hooks in DeltaWriter, not plugins. See [Writer Hooks](#writer-hooks) for details.

## Table of Contents

- [Overview](#overview)
- [Default Plugins](#default-plugins)
- [Quick Start](#quick-start)
- [Plugin Registry](#plugin-registry)
  - [Column Plugins](#column-plugins)
  - [Stage Plugins](#stage-plugins)
- [Writer Hooks](#writer-hooks)
- [Metrics & Observability](#metrics--observability)
  - [Creating a Metrics Collector](#creating-a-metrics-collector)
  - [Tracking Source Reads](#tracking-source-reads)
  - [Tracking Stages](#tracking-stages)
  - [Data Quality Metrics](#data-quality-metrics)
  - [Write Operation Metrics](#write-operation-metrics)
  - [Schema Drift Tracking](#schema-drift-tracking)
- [Plugin Context](#plugin-context)
- [Lakehouse-Build Integration Examples](#lakehouse-build-integration-examples)
- [API Reference](#api-reference)

---

## Overview

The plugin system consists of three main components:

| Component | Purpose |
|-----------|---------|
| **Registry** | Register and lookup plugins by name |
| **Metrics** | Collect observability data during pipeline execution |
| **Context** | Pass shared state and metrics to plugins |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DELTA-GEN CORE                                    │
│                                                                             │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐    │
│  │ YamlConfigProvider│────>│   PlanBuilder    │────>│   DeltaWriter    │    │
│  └──────────────────┘     └────────┬─────────┘     └────────┬─────────┘    │
│                                    │                        │              │
│  ┌─────────────────────────────────▼────────────────────────┤              │
│  │                      PLUGIN REGISTRY                     │              │
│  │  column_plugins: { "mask_email": fn, "hash_pii": fn }    │ pre_write_   │
│  │  stage_plugins:  { "dedupe_latest": fn, "log_nulls": fn }│ post_write_  │
│  └──────────────────────────────────────────────────────────┘ hooks        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ @register_column, @register_stage
                                    │
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LAKEHOUSE-BUILD                                     │
│  Your custom plugins implementing business logic + writer hooks             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Default Plugins

Delta-Gen ships with 5 starter plugins that are registered automatically when you import `deltagen.plugins`:

| Plugin | Type | Description |
|--------|------|-------------|
| `mask_email` | column | GDPR-compliant email masking (`j.doe@example.com` → `j***@example.com`) |
| `not_null` | column | Validate column has no nulls (reject/warn/fill actions) |
| `in_set` | column | Validate column values are in an allowed set |
| `delta_load` | stage | Incremental load: filter to deltas by watermark, dedupe by natural key |
| `dedupe_keep_last` | stage | Deduplicate keeping the latest record per key |

### Using Default Plugins

```yaml
# YAML Config
columns:
  - name: email
    inputs:
      - source: raw_customers
        column: email_address
    extensions:
      transform: mask_email  # Automatically masks email

  - name: status
    inputs:
      - source: raw_customers
        column: status_code
    extensions:
      transform: in_set
      allowed_values: ["ACTIVE", "INACTIVE", "PENDING"]
      on_violation: reject  # or "warn" or "fill"

  - name: customer_id
    type: string
    extensions:
      transform: not_null
      on_null: reject  # or "warn" or "fill"
      fill_value: "UNKNOWN"  # required if on_null=fill

stages:
  # Incremental load: filter to new records, dedupe by natural key
  - name: incremental
    extensions:
      stage_plugin: delta_load
      natural_keys: [customer_id]
      watermark_column: modified_date
      target_table: silver.customer_dim

  # Simple dedupe without watermark filtering
  - name: dedupe
    extensions:
      stage_plugin: dedupe_keep_last
      partition_by: [customer_id]
      order_by: updated_at
      order_desc: true  # keep most recent (default)
```

### delta_load Flow

```
Source Table (bronze)              Target Table (silver)
┌─────────────┬────────────┐       ┌─────────────┬────────────┐
│ customer_id │ modified   │       │ customer_id │ modified   │
├─────────────┼────────────┤       ├─────────────┼────────────┤
│ A           │ 2024-01-01 │       │ A           │ 2024-01-01 │
│ A           │ 2024-01-05 │       │ B           │ 2024-01-02 │
│ A           │ 2024-01-10 │       └─────────────┴────────────┘
│ B           │ 2024-01-02 │
│ B           │ 2024-01-08 │       1. Get max(modified) from target = 2024-01-02
│ C           │ 2024-01-15 │       2. Filter source where modified > 2024-01-02
└─────────────┴────────────┘       3. Dedupe by customer_id, keep latest

                                   Result:
                                   ┌─────────────┬────────────┐
                                   │ customer_id │ modified   │
                                   ├─────────────┼────────────┤
                                   │ A           │ 2024-01-10 │
                                   │ B           │ 2024-01-08 │
                                   │ C           │ 2024-01-15 │
                                   └─────────────┴────────────┘
```

For custom plugins, create them in your own package (e.g., `lakehouse-utils`) using the same decorators.

---

## Quick Start

### Registering a Plugin

```python
from deltagen.plugins import register_column, register_stage

# Column plugin - transforms a single column
@register_column("mask_email")
def mask_email(df, column, ctx):
    from pyspark.sql import functions as F
    return df.withColumn(
        column.name,
        F.concat(F.substring(F.col(column.name), 1, 2), F.lit("***@***.com"))
    )

# Stage plugin - transforms entire DataFrame
@register_stage("dedupe_latest")
def dedupe_latest(df, stage, ctx):
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    keys = stage.extensions.get("partition_by", [])
    order_col = stage.extensions.get("order_by")

    window = Window.partitionBy(keys).orderBy(F.desc(order_col))
    return df.withColumn("_rn", F.row_number().over(window)) \
             .filter("_rn = 1").drop("_rn")
```

### Debug Output

Enable debug mode to see detailed step-by-step execution:

```python
from deltagen.runner import PlanBuilder
from deltagen.plugins import create_plugin_context

ctx = create_plugin_context(table_name="customer_dim", load_id="batch_001")
df = PlanBuilder(cfg).build(spark, debug=True, context=ctx)
```

**Sample output:**
```
============================================================
[PlanBuilder] Building table: customer_dim
============================================================
  Layer: silver
  Sources: 1
  Stages: 2
  Natural keys: ['customer_id']

============================================================
[PlanBuilder] STEP 1: Loading sources
============================================================
  ✓ src: sh_bronze.raw_customers
    Columns: ['customer_id', 'name', 'email', 'modified_at']
    Source records: 10,000

============================================================
[PlanBuilder] STEP 2.1: Processing stage 'transform' (1/2)
============================================================
  Mode: transformation
  Columns to build: 5
  Stage plugin: delta_load

  Building columns:
  --------------------------------------------------
    [1/5] customer_id: src.customer_id -> string (natural key)
    [2/5] name: src.name -> string
    [3/5] email: src.email -> string [plugin: mask_email]
  --------------------------------------------------
  Applying SELECT with 5 columns...

  ✓ Stage 'transform' complete
    Output columns: ['customer_id', 'name', 'email', ...]
    Output records: 9,500
    Records changed: -500 (5.0% removed)
```

---

### Using a Plugin

```python
from deltagen.plugins import get_column_plugin, get_stage_plugin, create_plugin_context

# Create context for this run
ctx = create_plugin_context(
    table_name="customer_dim",
    load_id="batch_2024_01_15",
    debug=True
)

# Look up and execute plugin
plugin = get_column_plugin("mask_email")
if plugin:
    df = plugin(df, column_config, ctx)

# Get metrics summary
summary = ctx.metrics.complete()
print(summary.get_summary_table())
```

---

## Plugin Registry

### Column Plugins

Column plugins transform individual columns. They're invoked when a column's `extensions.transform` matches the registered name.

**Signature:**
```python
def column_plugin(df: DataFrame, column: ColumnConfig, ctx: PluginContext) -> DataFrame
```

**Registration:**
```python
from deltagen.plugins import register_column

@register_column(
    "mask_email",
    description="GDPR-compliant email masking",
    version="1.0.0",
    author="Data Team",
    tags={"pii", "gdpr"}
)
def mask_email(df, column, ctx):
    """Mask email addresses for GDPR compliance."""
    from pyspark.sql import functions as F

    masked = F.concat(
        F.substring(F.col(column.name), 1, 2),
        F.lit("***@"),
        F.element_at(F.split(F.col(column.name), "@"), 2)
    )
    return df.withColumn(column.name, masked)
```

**YAML Usage:**
```yaml
stages:
  - name: transform
    columns:
      - name: email
        inputs: [{source: raw_customers, column: email_address}]
        extensions:
          transform: mask_email  # Plugin reference
```

### Stage Plugins

Stage plugins operate on entire DataFrames between transformation stages. Use for deduplication, data quality checks, aggregations, etc.

**Signature:**
```python
def stage_plugin(df: DataFrame, stage: StageConfig, ctx: PluginContext) -> DataFrame
```

**Registration:**
```python
from deltagen.plugins import register_stage

@register_stage("dedupe_latest", tags={"dedup"})
def dedupe_latest(df, stage, ctx):
    """Keep only the most recent record per key."""
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    keys = stage.extensions.get("partition_by", [])
    order_col = stage.extensions.get("order_by")

    input_count = df.count()
    ctx.metrics.start_stage("dedupe_latest", input_count)

    window = Window.partitionBy(keys).orderBy(F.desc(order_col))
    result = df.withColumn("_rn", F.row_number().over(window)) \
               .filter("_rn = 1").drop("_rn")

    output_count = result.count()
    removed = input_count - output_count

    ctx.metrics.record_duplicates(keys, removed, action="kept_latest")
    ctx.metrics.end_stage("dedupe_latest", output_count)

    return result
```

**YAML Usage:**
```yaml
stages:
  - name: dedupe
    extensions:
      stage_plugin: dedupe_latest
      partition_by: [customer_id]
      order_by: updated_at
```

---

## Writer Hooks

Writer customization uses hooks in `DeltaWriter` instead of plugins. This avoids duplicating core write logic while allowing platform-specific setup and post-write operations.

### WriteResult

The `WriteResult` dataclass is passed to `post_write_hook` with details about the write operation:

```python
@dataclass
class WriteResult:
    target: str                      # Target table name
    mode: str                        # "append" or "merge"
    strategy: str | None = None      # Merge strategy if mode="merge"
    rows_inserted: int | None = None # None for merge (detailed metrics unavailable)
    rows_updated: int | None = None  # None for merge
    rows_deleted: int | None = None  # None for merge
    rows_unchanged: int | None = None
    rows_expired: int | None = None  # SCD Type 2 only
    rows_affected: int = 0           # Total source rows processed (always set)
    success: bool = True
    error: str | None = None
```

**Note:** For merge operations, Delta Lake's merge metrics are not currently surfaced,
so `rows_inserted` and `rows_updated` are `None`. Use `rows_affected` (total source rows)
for reliable metrics. For append mode, `rows_inserted` equals `rows_affected`.

### Hook Signatures

```python
# Pre-write hook - called before the write operation
def pre_write_hook(
    spark: SparkSession,
    df: DataFrame,
    config: TableConfig,
    context: PluginContext | None
) -> None:
    """Setup before write (e.g., create external tables, shortcuts)."""
    pass

# Post-write hook - called after the write operation
def post_write_hook(
    spark: SparkSession,
    df: DataFrame,
    config: TableConfig,
    context: PluginContext | None,
    write_result: WriteResult
) -> None:
    """Cleanup/logging after write (e.g., log to tracking tables)."""
    pass
```

### Using Hooks

```python
from deltagen.runner.writer import DeltaWriter, WriteResult
from pyspark.sql import functions as F

def fabric_pre_write(spark, df, config, context):
    """Setup Fabric shortcuts before write."""
    create_external_tables_if_not_exists(spark, config)

def fabric_post_write(spark, df, config, context, write_result):
    """Log to tracking tables after write using DataFrame-based append."""
    # Hook is called regardless of success/failure - check result.success
    if not write_result.success:
        if context:
            context.log_error(f"Write failed: {write_result.error}")
        return

    if context and context.load_id:
        # Use DataFrame API to avoid SQL injection
        log_df = spark.createDataFrame(
            [(config.name, context.load_id)],
            ["target_name", "load_id"],
        ).withColumn("logged_at", F.current_timestamp())
        log_df.write.mode("append").saveAsTable("logging_lakehouse_lastload")

    # Record metrics
    if context:
        context.metrics.record_write(
            target_table=write_result.target,
            write_mode=write_result.mode,
            merge_strategy=write_result.strategy,
            inserted=write_result.rows_inserted or 0,
            updated=write_result.rows_updated or 0,
        )

# Use with DeltaWriter
writer = DeltaWriter()
result = writer.write(
    spark, df, config,
    context=ctx,
    pre_write_hook=fabric_pre_write,
    post_write_hook=fabric_post_write,
)

# rows_affected is always set; rows_inserted/rows_updated may be None for merge
print(f"Write completed: {result.rows_affected} rows processed")
```

### Fabric-Specific Example

```python
from deltagen.runner.writer import DeltaWriter
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def fabric_setup_hook(spark, df, config, context):
    """Fabric Lakehouse setup - create shortcuts, external tables."""
    target = f"{config.layer}_{config.name}"

    # Create external table reference if needed
    if config.extensions.get("create_shortcut"):
        # Fabric-specific shortcut creation
        pass

def fabric_logging_hook(spark, df, config, context, result):
    """Log write operation to Fabric tracking tables using DataFrame API."""
    # Log to last load tracking table using DeltaTable API (no SQL injection risk).
    # Note: config.name should be validated upstream by Pydantic models.
    # If accepting user input directly, validate identifiers before use.
    if context:
        load_id = context.load_id or "unknown"

        # Build source DataFrame with values
        source_df = (
            spark.createDataFrame(
                [(config.name, load_id)],
                ["table_name", "load_id"],
            )
            .withColumn("load_time", F.current_timestamp())
        )

        # Perform MERGE using DeltaTable API (preferred over SQL string construction)
        target_table = DeltaTable.forName(spark, "logging_lakehouse_lastload")
        (
            target_table.alias("target")
            .merge(source_df.alias("source"), "target.table_name = source.table_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Log metrics (hook is called regardless of success/failure)
    if context:
        if result.success:
            context.log_info(f"Wrote to {result.target}: {result.rows_affected} rows processed")
        else:
            context.log_error(f"Write to {result.target} failed: {result.error}")

# In your pipeline
writer = DeltaWriter()
result = writer.write(
    spark, df, config,
    context=ctx,
    pre_write_hook=fabric_setup_hook,
    post_write_hook=fabric_logging_hook,
)
```

---

## Metrics & Observability

The metrics system provides comprehensive observability for data pipelines.

### Creating a Metrics Collector

```python
from deltagen.plugins import create_plugin_context, MetricsCollector

# Option 1: Via plugin context (recommended)
ctx = create_plugin_context(
    table_name="customer_dim",
    load_id="batch_2024_01_15",
    environment="prod",
    debug=True
)
metrics = ctx.metrics

# Option 2: Standalone collector
from deltagen.plugins import create_run_metrics
metrics = create_run_metrics(
    table_name="customer_dim",
    load_id="batch_2024_01_15"
)
```

### Tracking Source Reads

```python
# Basic read
metrics.record_source_read("raw_customers", row_count=10000)

# Detailed read
metrics.record_source_read(
    source_name="raw_orders",
    row_count=50000,
    columns_read=25,
    bytes_read=1024000,
    duration_ms=1500
)
```

### Tracking Stages

```python
# Option 1: Manual start/end
metrics.start_stage("transform", input_row_count=10000)
# ... transformation logic ...
metrics.end_stage("transform", output_row_count=9500)

# Option 2: Context manager
with metrics.stage_context("transform", input_row_count=10000):
    df = transform(df)
metrics.end_stage("transform", output_row_count=df.count())

# Track columns added/removed
metrics.end_stage(
    "enrich",
    output_row_count=9500,
    columns_added=["full_name", "age_bucket"],
    columns_removed=["temp_calc"]
)
```

### Data Quality Metrics

```python
from deltagen.plugins import MetricAction

# Record null values
metrics.record_nulls(
    column_name="customer_id",
    count=15,
    action=MetricAction.REJECTED,  # or FILLED_DEFAULT, FLAGGED, LOGGED
    sample_values=["row_1", "row_5"]  # Optional samples
)

# Record duplicates
metrics.record_duplicates(
    columns=["customer_id", "order_date"],
    count=25,
    action=MetricAction.KEPT_LATEST,  # or KEPT_FIRST, REJECTED
)

# Record validation failures
metrics.record_validation_failure(
    rule_name="in_set",
    column_name="status",
    count=5,
    action=MetricAction.FLAGGED,
    sample_values=["UNKNOWN", "INVALID"]
)
```

**Available Actions:**
| Action | Description |
|--------|-------------|
| `REJECTED` | Rows removed from pipeline |
| `KEPT_FIRST` | Kept first occurrence (dedupe) |
| `KEPT_LATEST` | Kept latest occurrence (dedupe) |
| `FILLED_DEFAULT` | Null replaced with default value |
| `FLAGGED` | Marked but kept in pipeline |
| `LOGGED` | Only logged, no action taken |

### Write Operation Metrics

```python
# Append write
metrics.record_write(
    target_table="silver.customer_dim",
    write_mode="append",
    inserted=1000,
    duration_ms=500
)

# Merge write (Type 1 SCD)
metrics.record_write(
    target_table="gold.orders_fact",
    write_mode="merge",
    merge_strategy="update_all",
    inserted=500,
    updated=200,
    deleted=10,
    unchanged=50,
    duration_ms=1500,
    target_rows_before=10000,
    target_rows_after=10490
)

# SCD Type 2 write
metrics.record_write(
    target_table="dim_customer",
    write_mode="merge",
    merge_strategy="scd_type2",
    inserted=100,
    updated=0,
    expired=50,  # Rows that had end_date set
    duration_ms=2000
)
```

### Schema Drift Tracking

```python
from deltagen.plugins import SchemaChangeType

metrics.record_schema_change(
    change_type=SchemaChangeType.COLUMN_ADDED,
    column_name="new_field",
    new_value="STRING",
    action="applied"
)

metrics.record_schema_change(
    change_type=SchemaChangeType.COLUMN_TYPE_CHANGED,
    column_name="amount",
    old_value="INT",
    new_value="DECIMAL(18,2)",
    action="applied"
)

metrics.record_schema_change(
    change_type=SchemaChangeType.COLUMN_REMOVED,
    column_name="deprecated_field",
    action="ignored"  # Policy was set to ignore removals
)
```

### Getting Results

```python
# Complete the run
result = metrics.complete()  # status="completed"
# or
result = metrics.fail("Connection timeout")  # status="failed"

# Get JSON for log aggregation
json_output = result.to_json()

# Get human-readable summary
print(result.get_summary_table())
```

**Example Summary Output:**
```
============================================================
Run Summary: customer_dim
============================================================
Run ID:      run_abc123def456
Load ID:     batch_2024_01_15
Status:      completed
Duration:    2534ms

Row Counts:
  Read:      10,000
  Written:   9,485
  Rejected:  15

Write Details:
  Mode:      merge
  Strategy:  update_all
  Inserted:  8,500
  Updated:   985
  Deleted:   0
  Unchanged: 0

Data Quality Issues:
  null (rejected): 15 rows
  duplicate (kept_latest): 25 rows

Schema Changes:
  column_added: new_field

Stage Timing:
  load_sources: 10000 -> 10000 (523ms)
  transform: 10000 -> 9975 (845ms)
  dedupe: 9975 -> 9500 (312ms)
  quality_check: 9500 -> 9485 (156ms)
============================================================
```

---

## Plugin Context

The `PluginContext` is passed to every plugin, providing access to metrics, shared state, and configuration.

```python
from deltagen.plugins import create_plugin_context

ctx = create_plugin_context(
    table_name="customer_dim",
    config=table_config,  # Optional TableConfig
    load_id="batch_001",
    environment="prod",
    debug=True,
    options={"batch_size": 1000}
)
```

### Context Features

```python
# Access metrics
ctx.metrics.record_source_read("source", 1000)

# Logging (includes run_id automatically)
ctx.log_info("Processing started")
ctx.log_warning("Found null values")
ctx.log_error("Failed to connect")
ctx.log_debug("Detailed debug info")  # Only logs if debug=True

# Shared state between plugins
ctx.set_state("stage1_count", 1000)
count = ctx.get_state("stage1_count")
ctx.update_state({"key1": "val1", "key2": "val2"})

# Runtime options
batch_size = ctx.get_option("batch_size", default=500)

# Table config extensions
writer_type = ctx.get_extension("writer", default="delta")

# Timing utilities
with ctx.timed_operation("transform") as timing:
    result = do_transform()
print(f"Took {timing['duration_ms']}ms")

# Plugin execution tracking
start = ctx.record_plugin_start("my_plugin", "stage")
# ... plugin logic ...
ctx.record_plugin_end("my_plugin", "stage", start, input_rows=1000, output_rows=950)
```

---

## Lakehouse-Build Integration Examples

Here's how to wrap your existing `lakehouseutils` functions as Delta-Gen plugins:

### log_nulls_query → Stage Plugin

**Before (lakehouseutils):**
```python
# In notebook
for field in table.get_fields({'name': 'natural', "value": 'true'}):
    query = log.log_nulls_query(sourceTable, field.name, loadID, target_log_table, 'create')
    spark.sql(query)
```

**After (Delta-Gen Plugin):**
```python
from deltagen.plugins import register_stage, MetricAction

@register_stage("log_nulls", tags={"quality", "logging"})
def log_nulls(df, stage, ctx):
    """Log null values in specified columns to DQ table and metrics."""
    from pyspark.sql import functions as F

    target_fields = stage.extensions.get("target_fields", [])
    log_table = stage.extensions.get("log_table", "logging_lakehouse_nulls")

    for field_name in target_fields:
        null_count = df.filter(F.col(field_name).isNull()).count()

        if null_count > 0:
            # Record in metrics
            ctx.metrics.record_nulls(
                column_name=field_name,
                count=null_count,
                action=MetricAction.LOGGED
            )

            # Optionally still write to your existing log table
            # spark.sql(log_nulls_query(...))

            ctx.log_warning(f"Found {null_count} nulls in {field_name}")

    return df
```

**YAML Usage:**
```yaml
stages:
  - name: quality_checks
    extensions:
      stage_plugin: log_nulls
      target_fields: [customer_id, order_id]
      log_table: logging_lakehouse_nulls
```

### log_duplicates_query → Stage Plugin

```python
@register_stage("log_duplicates", tags={"quality", "logging"})
def log_duplicates(df, stage, ctx):
    """Log duplicate rows based on key columns."""
    from pyspark.sql import functions as F

    key_columns = stage.extensions.get("key_columns", [])
    action = stage.extensions.get("action", "log")  # "log", "reject", "keep_first", "keep_latest"

    # Count duplicate rows: sum of (count - 1) for each key with duplicates
    dup_df = df.groupBy(key_columns).count().filter(F.col("count") > 1)
    dup_count_result = dup_df.select(
        F.sum(F.col("count") - F.lit(1)).alias("dup_count")
    ).collect()[0]
    dup_count = dup_count_result["dup_count"] or 0

    if dup_count > 0:
        if action == "reject":
            # Deduplicate by key, keeping first occurrence per key
            ctx.metrics.record_duplicates(key_columns, dup_count, action="kept_first")
            df = df.dropDuplicates(key_columns)  # Keeps first occurrence
        elif action == "keep_latest":
            ctx.metrics.record_duplicates(key_columns, dup_count, action="kept_latest")
            # Use window function to keep latest
            order_col = stage.extensions.get("order_by", key_columns[0])
            window = Window.partitionBy(key_columns).orderBy(F.desc(order_col))
            df = df.withColumn("_rn", F.row_number().over(window)) \
                   .filter("_rn = 1").drop("_rn")
        else:
            ctx.metrics.record_duplicates(key_columns, dup_count, action="logged")

    return df
```

### find_latest_record_query → Stage Plugin

```python
@register_stage("dedupe_latest", tags={"dedup"})
def dedupe_latest(df, stage, ctx):
    """Keep only the latest record per partition key (replaces find_latest_record_query)."""
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    partition_by = stage.extensions.get("partition_by", [])
    order_by = stage.extensions.get("order_by")

    if not partition_by or not order_by:
        ctx.log_warning("dedupe_latest requires partition_by and order_by")
        return df

    input_count = df.count()
    ctx.metrics.start_stage("dedupe_latest", input_count)

    window = Window.partitionBy(partition_by).orderBy(F.desc(order_by))
    result = df.withColumn("_rn", F.row_number().over(window)) \
               .filter("_rn = 1") \
               .drop("_rn")

    output_count = result.count()
    removed = input_count - output_count

    if removed > 0:
        ctx.metrics.record_duplicates(partition_by, removed, action="kept_latest")
        ctx.log_info(f"Removed {removed} older duplicate records")

    ctx.metrics.end_stage("dedupe_latest", output_count)
    return result
```

### merge_query → DeltaWriter with Hooks

The core `DeltaWriter` already handles merge operations. Use hooks for platform-specific logging:

```python
from deltagen.runner.writer import DeltaWriter
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def fabric_post_write_hook(spark, df, config, ctx, result):
    """Post-write hook that replaces manual merge_query logging."""
    target = result.target

    # Hook is called regardless of success/failure - handle both cases
    if not result.success:
        ctx.log_error(f"Write to {target} failed: {result.error}")
        return

    # Record metrics (DeltaWriter handles the actual merge)
    # Note: rows_inserted/rows_updated may be None for merge operations
    ctx.metrics.record_write(
        target_table=target,
        write_mode=result.mode,
        merge_strategy=result.strategy,
        inserted=result.rows_inserted or 0,
        updated=result.rows_updated or 0,
    )

    # Log to Lakehouse tracking tables using DataFrame API (no SQL injection risk)
    if ctx.load_id:
        source_df = (
            spark.createDataFrame(
                [(config.name, ctx.load_id, result.rows_affected)],
                ["table_name", "load_id", "rows_affected"],
            )
            .withColumn("load_time", F.current_timestamp())
        )

        target_table = DeltaTable.forName(spark, "logging_lakehouse_lastload")
        (
            target_table.alias("target")
            .merge(source_df.alias("source"), "target.table_name = source.table_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    ctx.log_info(f"Merged into {target}: {result.rows_affected} rows processed")

# Usage - DeltaWriter handles merge based on config.policies.optimisation
writer = DeltaWriter()
result = writer.write(
    spark, df, config,
    context=ctx,
    post_write_hook=fabric_post_write_hook,
)
```

**YAML Config for merge:**
```yaml
name: customer_dim
layer: silver                    # Semantic layer (for documentation/filtering)
target_schema: sharepoint        # Physical Fabric/database schema for target table
policies:
  optimisation:
    load_mode: merge
    merge_strategy: update_all  # or: update_changed, insert_only, scd_type2
```

> **Note:** `layer` is semantic (bronze/silver/gold) for documentation purposes.
> `target_schema` is the physical database/Fabric schema where the table is written.
> The target table path becomes `{target_schema}.{name}` (e.g., `sharepoint.customer_dim`).

### add_new_columns_to_existing_table → Pre-Write Hook

Use a `pre_write_hook` for schema drift handling before the write:

> **Note:** This example uses SQL for ALTER TABLE because DeltaTable API doesn't support
> schema modification. For data operations (MERGE, DELETE, UPDATE), prefer DeltaTable API
> as shown in the Fabric logging example above. When SQL is required, always validate
> identifiers to prevent injection vulnerabilities.

```python
from deltagen.runner.writer import DeltaWriter
from deltagen.plugins import SchemaChangeType

def schema_drift_hook(spark, df, config, ctx):
    """Pre-write hook for automatic schema drift handling."""
    import re

    # Validate identifiers to prevent SQL injection (required when using SQL strings)
    def validate_identifier(name: str) -> bool:
        """Check if identifier contains only safe characters."""
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

    if not validate_identifier(config.layer) or not validate_identifier(config.name):
        ctx.log_error("Invalid table/layer name - contains unsafe characters")
        return

    target = f"{config.layer}.{config.name}"

    if spark.catalog.tableExists(target):
        # Get existing columns
        existing_cols = set(spark.table(target).columns)
        source_cols = set(df.columns)

        # Detect additions
        new_cols = source_cols - existing_cols
        for col in new_cols:
            # Validate column name before using in SQL
            if not validate_identifier(col):
                ctx.log_warning(f"Skipping column with unsafe name: {col}")
                continue

            col_type = str(df.schema[col].dataType)

            # Add column to target using backtick quoting for identifiers
            spark.sql(f"ALTER TABLE `{target}` ADD COLUMN `{col}` {col_type}")

            ctx.metrics.record_schema_change(
                change_type=SchemaChangeType.COLUMN_ADDED,
                column_name=col,
                new_value=col_type,
                action="applied"
            )
            ctx.log_info(f"Added column {col} ({col_type}) to {target}")

        # Detect removals (optional - depends on policy)
        removed_cols = existing_cols - source_cols
        for col in removed_cols:
            ctx.metrics.record_schema_change(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                column_name=col,
                action="ignored"  # Or handle based on config
            )
            ctx.log_warning(f"Column {col} exists in target but not in source")

# Use with DeltaWriter
writer = DeltaWriter()
result = writer.write(
    spark, df, config,
    context=ctx,
    pre_write_hook=schema_drift_hook,
)
```

### Complete Notebook Replacement

**Before (curated-template.Notebook):**
```python
table = Table(filePath)
curatedTableFullName = tablesql.get_table_name(table)

# Manual SQL composition
statement = f'''
SELECT DISTINCT {tablesql.cast_fields(table, fields)}
FROM {table.get_sources()[0].name} src
{tablesql.create_joins(joins)}
{whereclause}
'''
spark.sql(f'CREATE TEMP VIEW temp_{name} AS {statement}')

# Manual null logging
for field in natural_fields:
    spark.sql(log.log_nulls_query(...))

# Manual duplicate logging
spark.sql(log.log_duplicates_query(...))

# Manual merge
spark.sql(sql.merge_query(...))
```

**After (Delta-Gen v2 with plugins and hooks):**
```python
from deltagen.providers import YamlConfigProvider
from deltagen.runner import PlanBuilder
from deltagen.runner.writer import DeltaWriter
from deltagen.plugins import create_plugin_context

# Load config
cfg = YamlConfigProvider().load("configs/d_customer.yaml")

# Create context
ctx = create_plugin_context(
    table_name=cfg.name,
    config=cfg,
    load_id=loadID,
    debug=True
)

# Build DataFrame (handles casting, joins, filters automatically)
# Stage plugins are invoked automatically based on YAML extensions
# Pass context to build() for metrics and plugin invocation
df = PlanBuilder(cfg).build(spark, debug=True, context=ctx)

# Write (DeltaWriter handles merge, hooks handle Fabric-specific operations)
writer = DeltaWriter()
result = writer.write(
    spark, df, cfg,
    context=ctx,
    pre_write_hook=fabric_setup_hook,      # Optional: Fabric shortcuts, etc.
    post_write_hook=fabric_logging_hook,   # Optional: Log to tracking tables
)

# Print summary
print(ctx.metrics.complete().get_summary_table())
```

---

## API Reference

### Registry Functions

| Function | Description |
|----------|-------------|
| `register_column(name, **kwargs)` | Decorator to register a column plugin |
| `register_stage(name, **kwargs)` | Decorator to register a stage plugin |
| `get_column_plugin(name)` | Look up column plugin by name |
| `get_stage_plugin(name)` | Look up stage plugin by name |
| `get_plugin_info(name, plugin_type=None)` | Get plugin metadata |
| `list_plugins(plugin_type=None, tags=None)` | List registered plugins |
| `clear_registry(plugin_type=None)` | Clear plugins (for testing) |

### Writer (from deltagen.runner.writer)

| Class/Type | Description |
|------------|-------------|
| `DeltaWriter` | Core writer class with hook support |
| `WriteResult` | Dataclass with write operation details |
| `PreWriteHook` | Type alias for pre-write hook functions |
| `PostWriteHook` | Type alias for post-write hook functions |

### Metrics Classes

| Class | Description |
|-------|-------------|
| `MetricsCollector` | Main class for collecting metrics |
| `RunMetrics` | Container for all metrics from a run |
| `SourceReadMetric` | Metrics for a source read operation |
| `StageMetric` | Metrics for a transformation stage |
| `DataQualityMetric` | Metrics for a DQ issue |
| `SchemaChangeMetric` | Metrics for schema drift |
| `WriteMetric` | Metrics for a write operation |

### Enums

| Enum | Values |
|------|--------|
| `MetricAction` | `REJECTED`, `KEPT_FIRST`, `KEPT_LATEST`, `FILLED_DEFAULT`, `FLAGGED`, `LOGGED` |
| `SchemaChangeType` | `COLUMN_ADDED`, `COLUMN_REMOVED`, `COLUMN_TYPE_CHANGED`, `COLUMN_NULLABLE_CHANGED` |

### Context Functions

| Function | Description |
|----------|-------------|
| `create_plugin_context(table_name, **kwargs)` | Create a new plugin context |
| `create_null_context(debug=False)` | Create a no-op context for testing |

---

## Best Practices

1. **Always use context for metrics** - Pass the context to plugins, don't create new collectors
2. **Record metrics at appropriate granularity** - Stage-level timing, row-level counts
3. **Use structured logging** - Let the context add run_id automatically
4. **Handle errors gracefully** - Use `ctx.metrics.fail(message)` on errors
5. **Tag your plugins** - Makes discovery and filtering easier
6. **Document with docstrings** - They become the plugin description if not provided
7. **Complete the metrics** - Always call `complete()` or `fail()` at the end

---

## Testing Plugins

```python
from deltagen.plugins import create_null_context, clear_registry

# Use null context for unit tests (no metrics overhead)
ctx = create_null_context(debug=True)

# Clear registry between tests
def setup_function():
    clear_registry()

def test_my_plugin():
    @register_stage("test_plugin")
    def test_plugin(df, stage, ctx):
        return df

    plugin = get_stage_plugin("test_plugin")
    assert plugin is not None
```
