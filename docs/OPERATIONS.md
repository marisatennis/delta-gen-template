# Operations Guide

Run-book for operating the data platform. Covers scheduled runs, manual triggers, failure investigation, data quality, and monitoring.

---

## Platform Overview

The platform processes data through a sequential pipeline, orchestrated by Fabric Data Pipelines:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     Bronze       │────>│     Silver       │────>│      Gold        │
│  (raw ingest)    │     │  (cleanse/dedup) │     │  (dimensional)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         └───────────────────────┴────────────────────────┘
                                 │
                          ┌──────v──────┐
                          │  Log / Obs  │
                          │ (metrics)   │
                          └─────────────┘
```

Each layer runs in its own Fabric workspace:

| Workspace | Purpose |
|---|---|
| `{env}-bronze` | Raw data landing zone |
| `{env}-silver` | Cleansed, validated data |
| `{env}-gold` | Analytics-ready dimensional model |
| `{env}-log` | Observability, pipeline metrics, DQ results |

---

## Scheduled Runs

The platform runs on automated schedules defined in Fabric Data Pipelines. Typical batch schedules:

### Daily Batch

| Time (UTC) | Layer | What Runs |
|---|---|---|
| 06:00 | Bronze | All source ingestion notebooks |
| 06:30 | Silver | Tables tagged `batch: daily` in YAML configs |
| 07:00 | Gold | Dimensions and facts tagged `batch: daily` |

### Weekly Batch

| Time (UTC) | Layer | What Runs |
|---|---|---|
| Sunday 04:00 | Bronze | Full refresh of weekly sources |
| Sunday 04:30 | Silver | Tables tagged `batch: weekly` |
| Sunday 05:00 | Gold | Tables tagged `batch: weekly` |

### Monthly Batch

| Time (UTC) | Layer | What Runs |
|---|---|---|
| 1st of month 03:00 | Bronze | Full refresh of monthly sources |
| 1st of month 03:30 | Silver | Tables tagged `batch: monthly` |
| 1st of month 04:00 | Gold | Tables tagged `batch: monthly` |

> **Note**: Adjust schedules to match your data source availability and SLAs. Batch tags are defined in `datalake/inputs/config/silver_batches.yaml` and `gold_batches.yaml`.

---

## Triggering a Manual Run

### Full Pipeline (Bronze → Silver → Gold)

1. Open the `{env}-log` workspace in Fabric
2. Find the **orchestrator** Data Pipeline
3. Click **Run** -- this triggers Bronze, then Silver, then Gold sequentially

### Single Layer

1. Open the relevant workspace (`{env}-bronze`, `{env}-silver`, or `{env}-gold`)
2. Open the orchestrator notebook (e.g., `silver-orchestrator`)
3. Click **Run all**
4. The orchestrator will process all tables assigned to the current batch

### Override Batch Selection

To run a specific batch out of schedule, set the `batch_override` parameter on the orchestrator notebook:

```python
# In the orchestrator notebook parameters cell:
batch_override = "daily"   # Force daily batch regardless of schedule
```

---

## Failure Response

When a pipeline run fails, use this symptom-to-cause table to diagnose:

| Symptom | Likely Cause | Resolution |
|---|---|---|
| Bronze orchestrator fails immediately | Fabric capacity paused or Key Vault unreachable | Check capacity status; verify KV access policy for `kv-{project}-{env}` |
| Single Bronze notebook fails | Source system unavailable or auth token expired | Check source connectivity; refresh credentials in Key Vault |
| Silver orchestrator fails on startup | `deltagen` library not found in Files/libs/ | Re-run CI/CD deployment to sync libraries to lakehouse |
| Single Silver table fails | YAML config error or source table missing from Bronze | Check notebook error output; validate YAML syntax; confirm Bronze table exists |
| Gold table fails with lookup error | Referenced dimension not yet loaded | Check dimension `order:` is lower than fact `order:` in batch config |
| Gold orchestrator times out | Too many tables in a single batch or Spark session limit | Split batch into smaller groups; check `gold_batches.yaml` parallelism settings |
| Semantic model refresh fails | Gold tables not yet available or schema change | Verify Gold pipeline completed; check for column renames/drops |

### Checking Logs

Pipeline execution logs are stored in the `{env}-log` lakehouse:

```sql
-- Recent pipeline runs
SELECT * FROM log.pipeline_runs
ORDER BY start_time DESC
LIMIT 20;

-- Failed runs with error details
SELECT * FROM log.pipeline_runs
WHERE status = 'FAILED'
ORDER BY start_time DESC;
```

---

## Reloading a Single Table

To reload a specific table without running the full pipeline:

### Silver Table

1. Open `{env}-silver` workspace
2. Open `silver-template` notebook
3. Set the parameter: `config_path = "inputs/silver/{source}/{table_name}.yaml"`
4. Optionally set `full_reload = True` to ignore watermark and reload all data
5. Run the notebook

### Gold Table

1. Open `{env}-gold` workspace
2. Open `gold-template` notebook
3. Set the parameter: `config_path = "inputs/gold/{type}/{table_name}.yaml"`
4. Optionally set `full_reload = True`
5. Run the notebook

---

## Forcing a Full Reload

A full reload reprocesses all source data, ignoring incremental watermarks. Use this when:

- Source data has been retroactively corrected
- A schema change requires rebuilding the table
- Data quality issues require a clean slate

### Single Table

Set `full_reload = True` on the template notebook (see above).

### Full Layer Reload

Set the `full_reload` parameter on the orchestrator notebook:

```python
full_reload = True    # Reload all tables in this layer from scratch
batch_override = ""   # Leave empty to run all batches, or specify one
```

> **Warning**: Full reloads can be time-consuming for large tables. Schedule during off-peak hours and monitor Fabric capacity utilisation.

---

## Data Quality Investigation

### DQ Tables

Data quality metrics are captured during Silver and Gold processing and stored in the log lakehouse:

| Table | Contents |
|---|---|
| `log.dq_results` | Per-table, per-column DQ check results (null counts, uniqueness, range) |
| `log.dq_summary` | Aggregated pass/fail counts per table per run |
| `log.row_counts` | Row counts at each layer for reconciliation |

### Querying DQ Results

```sql
-- Tables with DQ failures in the last 24 hours
SELECT table_name, check_name, check_result, details
FROM log.dq_results
WHERE run_date >= DATEADD(day, -1, CURRENT_DATE())
  AND check_result = 'FAIL'
ORDER BY table_name;

-- Row count reconciliation: Bronze vs Silver vs Gold
SELECT
  r.table_name,
  r.bronze_count,
  r.silver_count,
  r.gold_count,
  r.bronze_count - r.silver_count AS bronze_silver_diff
FROM log.row_counts r
WHERE r.run_date = CURRENT_DATE()
ORDER BY bronze_silver_diff DESC;
```

### Sentinel Values

The platform uses sentinel records in dimensions to handle missing or unmatched keys gracefully:

| Sentinel | Purpose | Key Value |
|---|---|---|
| `NO_CUSTOMER` | Fact row has no matching customer | `-1` |
| `NO_PRODUCT` | Fact row has no matching product | `-2` |
| `NO_DATE` | Fact row has no valid date | `19000101` |

When investigating unexpected sentinel matches, check:

1. Is the source data arriving in Bronze? Query the Bronze table for the missing key.
2. Is the Silver cleaning dropping the record? Check Silver DQ results for that table.
3. Is the Gold lookup finding no match? Verify the dimension has the expected business key.

---

## Monitoring with Observability Dashboard

The platform includes a Power BI observability dashboard connected to the `{env}-log` lakehouse. Key pages:

| Page | Shows |
|---|---|
| Pipeline Overview | Run history, success/failure rates, duration trends |
| Table Health | Per-table DQ scores, row count trends, freshness |
| Capacity Usage | Spark session durations, CU consumption by layer |
| Alerts | Tables with DQ failures, stale data, or missing runs |

### Setting Up Alerts

Configure Power BI data-driven alerts on key metrics:

1. Open the observability report in Power BI Service
2. Pin tiles for critical metrics (e.g., "Failed Runs Today")
3. Set alert thresholds (e.g., alert if failed runs > 0)
4. Route alerts to Teams channel or email distribution list

---

## Adding a New Data Source

To onboard a new data source into the platform:

### 1. Bronze Ingestion

- Create a new ingestion notebook in `platform/bronze/` (or extend an existing one)
- Add connection details to Key Vault: `kv-{project}-{env}`
- Add the source to the Bronze orchestrator's dependency graph
- Test ingestion in a developer workspace

### 2. Silver Transformation

- Create YAML config files in `datalake/inputs/silver/{source_name}/` (one per table)
- Define columns, cleaning rules, merge keys, and batch assignment
- See [Delta-Gen Integration Guide](DELTA_GEN_INTEGRATION.md) for YAML syntax

### 3. Gold Dimensional Model

- Create YAML config files in `datalake/inputs/gold/dimension/` or `gold/fact/`
- Define dimension lookups, surrogate keys, and fact measures
- Ensure dimension `order:` values are lower than fact `order:` values

### 4. Mapping Documentation

- Add a source-to-target mapping document in `design/mappings/`
- Update `docs/SCHEMAS.md` with the new table schemas

### 5. Testing and Deployment

- Test end-to-end in a developer workspace (Bronze → Silver → Gold)
- Verify row counts, DQ results, and sentinel handling
- Create a PR and follow the [Developer Guide](DEVELOPER-GUIDE.md) process

---

## Known Outstanding Items

> **Placeholder** -- Track known issues, technical debt, and planned improvements here.

| Item | Description | Priority | Status |
|---|---|---|---|
| _Example_ | _Describe the known issue or improvement_ | _High / Medium / Low_ | _Open / In Progress_ |
