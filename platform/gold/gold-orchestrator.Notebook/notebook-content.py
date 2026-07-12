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

# # Gold Orchestrator
# This script orchestrates the following activities:
# - resolve gold YAML configs into batches
# - ensure target schemas exist
# - run gold notebooks for each config

# MARKDOWN ********************

# ### Import dependencies

# CELL ********************

import json
import os
import sys
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Load fabric_libs + deltagen from Lakehouse
LAKEHOUSE_ROOT = "/lakehouse/default/Files"
LIBS_DIR = f"{LAKEHOUSE_ROOT}/libs"
FABRIC_LIBS_DIR = f"{LIBS_DIR}/fabric_libs"
DELTAGEN_DIR = f"{LIBS_DIR}/deltagen"
ZIP_PATH = f"{LIBS_DIR}/libs.zip"

# Clear cached modules to ensure fresh imports
for mod in list(sys.modules.keys()):
    if mod.startswith(("fabric_libs", "deltagen")):
        del sys.modules[mod]

if os.path.isdir(FABRIC_LIBS_DIR) and os.path.isdir(DELTAGEN_DIR):
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
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

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider
from deltagen.providers.macros import load_defaults
from fabric_libs.orchestration import (
    ensure_schemas,
    run_notebook_with_tracking,
    load_batch_config,
    resolve_yaml_paths,
    load_table_entries,
    collect_schema_list,
    group_by_batch,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

ORCHESTRATION_ID = str(uuid.uuid4())

folder_path = "/lakehouse/default/Files/inputs/"
layer_name = "gold"
debug = True
full_load = False
parallelism = 7
notebook_timeout_seconds = 3600
run_schedule = "all"  # daily | weekly | monthly | all
run_folder = ""
run_files = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run Orchestration

# CELL ********************

# =============================================================================
# INITIALIZATION
# =============================================================================
orchestration_start = time.perf_counter()
run_id = ORCHESTRATION_ID
defaults_path = os.path.join(folder_path, "config/gold_defaults.yaml")
batch_config_path = os.path.join(folder_path, "config/gold_batches.yaml")
config_root = os.path.join(folder_path, "gold")

print("\n" + "=" * 100)
print(f"GOLD ORCHESTRATOR")
print("=" * 100)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Run ID: {run_id}")
print(f"\nParameters:")
print(f"  folder_path: {folder_path}")
print(f"  full_load: {full_load}")
print(f"  parallelism: {parallelism}")
print(f"  run_schedule: {run_schedule}")
print("=" * 100 + "\n")

failed_tasks = []
successful_tasks = []
skipped_tasks = []
batch_timings = {}

# =============================================================================
# RESOLVE WORKSET
# =============================================================================
print("[STEP 1] Resolving workset...")
defaults = load_defaults(defaults_path)
provider = YamlConfigProvider(TableConfig, defaults_path=defaults_path)

batch_config = load_batch_config(batch_config_path)
batch_defs = batch_config.get("batches", {})

yaml_paths = resolve_yaml_paths(config_root, run_folder, run_files)
table_entries, config_errors = load_table_entries(provider, yaml_paths, batch_defs, run_schedule)

print(f"  Found {len(yaml_paths)} YAML config(s)")
print(f"  Matched {len(table_entries)} table(s) for schedule '{run_schedule}'")

if not table_entries:
    print(f"\nNo tables matched schedule '{run_schedule}'. Exiting.")
    notebookutils.notebook.exit(f"No tables for schedule '{run_schedule}'")

# =============================================================================
# ENSURE SCHEMAS
# =============================================================================
print(f"\n[STEP 2] Ensuring schemas exist...")
schema_list = collect_schema_list(defaults, table_entries)
ensure_schemas(spark, schema_list)
print(f"  Schemas ready: {schema_list}")

# =============================================================================
# BUILD EXECUTION BATCHES
# =============================================================================
print(f"\n[STEP 3] Building execution batches...")
params_by_batch = {}
grouped = group_by_batch(table_entries)
for batch_id, items in grouped.items():
    for item in items:
        table = item["table"]
        notebook_name = table.policies.creation.notebook_name or "gold-template"
        params_by_batch.setdefault(str(batch_id), []).append({
            "order": item["order"],
            "notebookName": notebook_name,
            "fileName": os.path.basename(item["path"]),
            "params": {
                "FILE_PATH": item["path"],
                "FULL_LOAD": full_load,
                "DEBUG": debug,
                "RUN_ID": run_id,
                "ORCHESTRATOR_RUN": True,
            },
        })

total_notebooks = sum(len(batch) for batch in params_by_batch.values())
print(f"  Total notebooks: {total_notebooks}")

# =============================================================================
# EXECUTE BATCHES
# =============================================================================
print(f"\n[STEP 4] Executing batches...")
completed_count = 0

for batch in sorted(params_by_batch.keys(), key=int):
    if str(batch) == "0":
        continue
    batch_start = time.perf_counter()
    params_list = sorted(params_by_batch[batch], key=lambda item: item["order"])

    print(f"\n  --- Batch {batch}: {len(params_list)} notebook(s), parallelism={parallelism} ---")

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        future_to_params = {
            executor.submit(
                run_notebook_with_tracking,
                notebook_name=params["fileName"],
                notebook_path=params["notebookName"],
                timeout_seconds=notebook_timeout_seconds,
                parameters=params["params"],
            ): params
            for params in params_list
        }

        for future in as_completed(future_to_params):
            params = future_to_params[future]
            result = future.result()
            completed_count += 1

            status = result.get("status", "UNKNOWN")
            duration = result.get("duration_seconds", 0)
            name = params["fileName"]

            if status == "FAILED":
                failed_tasks.append(result)
            elif status == "SKIPPED":
                skipped_tasks.append(result)
            else:
                successful_tasks.append(result)

            print(f"    [{completed_count}/{total_notebooks}] {name}: {status} ({duration:.1f}s)")

    batch_duration = time.perf_counter() - batch_start
    batch_timings[batch] = batch_duration

# =============================================================================
# FINAL SUMMARY
# =============================================================================
orchestration_duration = time.perf_counter() - orchestration_start

print("\n" + "=" * 100)
print("ORCHESTRATION SUMMARY")
print("=" * 100)
print(f"Duration: {orchestration_duration:.1f}s ({orchestration_duration/60:.1f} min)")
print(f"  Success:  {len(successful_tasks)}")
print(f"  Failed:   {len(failed_tasks)}")
print("=" * 100)

if failed_tasks:
    raise RuntimeError(
        f"Gold orchestration failed: {len(failed_tasks)}/{total_notebooks} notebook(s) failed."
    )
else:
    print("All tasks completed successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
