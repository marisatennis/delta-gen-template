# Log Layer

Centralized logging and observability for the platform.

## Intended contents

- `observability.Lakehouse` — Delta tables for run metrics, write logs, DQ violations, ingestion metadata
- `*-observability.SemanticModel` — Power BI semantic model over the observability lakehouse
- `Platform Observability.Report` — Power BI report for ops dashboards
- `create-sql-endpoint-views.Notebook` — creates SQL endpoint views over the logging tables
- `orchestrator.DataPipeline` — schedules ingestion/reconciliation of log data

## Tables typically written here

- `logging.deltagen_run_metrics` (from `FabricMetricsAdapter`)
- `logging.deltagen_table_load_log` (from the post-write hook)
- `logging.deltagen_dq_nulls`, `logging.deltagen_dq_invalid` (from DQ plugins)
- `logging.deltagen_unresolved_fks` (from `check_unresolved_fks`)
- `control.*` (ingestion control tables — file metadata, SharePoint delta state, etc.)
