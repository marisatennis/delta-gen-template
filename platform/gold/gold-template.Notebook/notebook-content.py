# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ba74f90-b21d-4bc1-90e3-3ac7e8973e7e",
# META       "default_lakehouse_name": "gold",
# META       "default_lakehouse_workspace_id": "10fa994f-aa66-40c6-b370-890f73c87b51",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ba74f90-b21d-4bc1-90e3-3ac7e8973e7e"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Gold Notebook (Delta-Gen v2)
# This notebook uses Delta-Gen v2 to transform data from silver to gold layer.
# **Features:**
# - Automatic metrics persistence to Delta tables via `FabricMetricsAdapter`
# - Plugin-based transformations defined in YAML config
# - Incremental source filtering (watermark or period-based) at load time
# - DQ logging to dedicated tables for rejected records and unexpected duplicates
# - FK re-resolution: clears watermark override to force re-join of unresolved foreign keys

# MARKDOWN ********************

# ### Setup: Load Libraries

# CELL ********************

import os
import sys
import zipfile

LAKEHOUSE_ROOT = "/lakehouse/default/Files"
LIBS_DIR = f"{LAKEHOUSE_ROOT}/libs"
FABRIC_LIBS_DIR = f"{LIBS_DIR}/fabric_libs"
DELTAGEN_DIR = f"{LIBS_DIR}/deltagen"
ZIP_PATH = f"/tmp/libs_{os.getpid()}.zip"
SHARED_ZIP_PATH = f"{LIBS_DIR}/libs.zip"

_orchestrator_run = globals().get('ORCHESTRATOR_RUN', False)

if _orchestrator_run:
    spark.sparkContext.addPyFile(SHARED_ZIP_PATH)
    if LIBS_DIR not in sys.path:
        sys.path.insert(0, LIBS_DIR)
else:
    for mod in list(sys.modules.keys()):
        if mod.startswith(("fabric_libs", "deltagen")):
            del sys.modules[mod]

    if os.path.isdir(FABRIC_LIBS_DIR) and os.path.isdir(DELTAGEN_DIR):
        with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
            for base_dir in (FABRIC_LIBS_DIR, DELTAGEN_DIR):
                if not os.path.isdir(base_dir):
                    continue
                for root, _, files in os.walk(base_dir):
                    if "__pycache__" in root:
                        continue
                    for filename in files:
                        if filename.endswith(".pyc"):
                            continue
                        full_path = os.path.join(root, filename)
                        arcname = os.path.relpath(full_path, LIBS_DIR)
                        zf.write(full_path, arcname)
        spark.sparkContext.addPyFile(ZIP_PATH)
        if LIBS_DIR not in sys.path:
            sys.path.insert(0, LIBS_DIR)
    else:
        print(f"Missing {FABRIC_LIBS_DIR} or {DELTAGEN_DIR}; upload libs first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Import Dependencies

# CELL ********************

import uuid

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider
from deltagen.providers.macros import load_defaults
from deltagen.runner import PlanBuilder
from deltagen.runner.writer import DeltaWriter

from deltagen.fabric import create_fabric_context
import fabric_libs  # noqa: F401  -- registers generic cleaning plugins

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

FILE_PATH = "/lakehouse/default/Files/inputs/gold/dimension/d_date.yaml"
FULL_LOAD = False
DEBUG = True
RUN_ID = None
ORCHESTRATOR_RUN = False
WATERMARK_OVERRIDE = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run Pipeline

# CELL ********************

if RUN_ID is None:
    RUN_ID = str(uuid.uuid4())

NOTEBOOK_NAME = FILE_PATH.split("/")[-1].replace(".yaml", "")

print("\n" + "=" * 80)
print(f"PIPELINE: {NOTEBOOK_NAME}")
print("=" * 80)
print(f"  FILE_PATH: {FILE_PATH}")
print(f"  FULL_LOAD: {FULL_LOAD}")
print(f"  RUN_ID: {RUN_ID}")
print("=" * 80 + "\n")

ctx = None
table_name = "unknown"
target_table = "unknown"

try:
    DEFAULTS_PATH = "/lakehouse/default/Files/inputs/config/gold_defaults.yaml"
    defaults = load_defaults(DEFAULTS_PATH)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    config = provider.load(FILE_PATH)

    table_name = f"{config.layer}_{config.name}" if config.layer else config.name
    if config.target_schema:
        target_table = f"{config.target_schema}.{config.name}"
    elif config.layer:
        target_table = f"{config.layer}.{config.name}"
    else:
        target_table = config.name

    metrics_config = defaults.get("metrics", {})

    ctx = create_fabric_context(
        spark=spark,
        table_name=table_name,
        config=config,
        load_id=RUN_ID,
        schema=metrics_config.get("schema", "logging"),
        prefix=metrics_config.get("prefix", "deltagen"),
        environment=defaults.get("environment", "fabric"),
        auto_persist=metrics_config.get("auto_persist", True),
        log_summary=metrics_config.get("log_summary", True),
        full_load=FULL_LOAD,
    )

    # =========================================================================
    # FK RE-RESOLUTION: Clear watermark to force re-join of unresolved FKs
    # =========================================================================
    # For fact tables with foreign keys, unresolved FKs (sentinel values) from
    # previous runs need to be re-resolved when the dimension data arrives.
    # Setting WATERMARK_OVERRIDE to None when sentinels exist forces a wider
    # reload window so those rows get re-processed with updated dimension joins.
    effective_watermark = WATERMARK_OVERRIDE

    if not FULL_LOAD and hasattr(config, 'stages') and config.stages:
        has_fk_check = any(
            getattr(stage, 'extensions', None)
            and getattr(stage.extensions, 'stage_plugin', None) == 'check_unresolved_fks'
            for stage in config.stages
        )
        if has_fk_check and effective_watermark is None:
            # Check if target table exists and has unresolved FK sentinels
            try:
                if spark.catalog.tableExists(target_table):
                    fk_checks = []
                    for stage in config.stages:
                        ext = getattr(stage, 'extensions', None)
                        if ext and getattr(ext, 'stage_plugin', None) == 'check_unresolved_fks':
                            for check in getattr(ext, 'checks', []):
                                fk_checks.append(check)

                    if fk_checks:
                        conditions = " OR ".join(
                            f"{check['column']} = '{check['sentinel']}'"
                            for check in fk_checks
                        )
                        unresolved_count = spark.sql(
                            f"SELECT COUNT(*) AS cnt FROM {target_table} WHERE {conditions}"
                        ).collect()[0]["cnt"]

                        if unresolved_count > 0:
                            print(f"  Found {unresolved_count:,} unresolved FK rows -- widening reload window")
                            # Pull watermark back to include unresolved rows
                            watermark_col = config.incremental.watermark_column if config.incremental else None
                            if watermark_col:
                                min_wm = spark.sql(
                                    f"SELECT MIN({watermark_col}) AS min_wm "
                                    f"FROM {target_table} WHERE {conditions}"
                                ).collect()[0]["min_wm"]
                                if min_wm is not None:
                                    effective_watermark = str(min_wm)
                                    print(f"  Watermark override set to: {effective_watermark}")
            except Exception as fk_err:
                print(f"  FK re-resolution check skipped: {fk_err}")

    if FULL_LOAD:
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    builder = PlanBuilder(config)
    df = builder.build(spark, debug=DEBUG, context=ctx, watermark_override=effective_watermark)

    output_count = df.count()

    if DEBUG:
        print(f"\nRecords after transformation: {output_count:,}")
        display(df.limit(100))

    if output_count == 0:
        ctx.metrics.complete(status="skipped")
        notebookutils.notebook.exit(">>> EXIT NOTEBOOK: No new records to process")

    if FULL_LOAD:
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        ctx.metrics.record_write(
            target_table=target_table,
            write_mode="overwrite",
            inserted=output_count,
        )
    else:
        writer = DeltaWriter()
        write_result = writer.write(spark, df, config, debug=DEBUG, context=ctx)
        ctx.metrics.record_write(
            target_table=target_table,
            write_mode=write_result.mode,
            inserted=write_result.rows_inserted or 0,
            updated=write_result.rows_updated or 0,
            merge_strategy=getattr(write_result, "strategy", None),
        )

    metrics = ctx.metrics.complete()

    print("\n" + "=" * 80)
    print(f"SUCCESS: {NOTEBOOK_NAME}")
    print(f"  Duration: {metrics.duration_ms}ms")
    print(f"  Rows read: {metrics.total_rows_read:,}")
    print(f"  Rows written: {metrics.total_rows_written:,}")
    print("=" * 80 + "\n")

except Exception as e:
    if ctx:
        ctx.metrics.fail(str(e))

    print("\n" + "=" * 80)
    print(f"FAILED: {NOTEBOOK_NAME}")
    print(f"  Error: {e}")
    print("=" * 80 + "\n")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
