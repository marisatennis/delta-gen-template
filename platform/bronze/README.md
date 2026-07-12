# Bronze Layer

The Bronze layer is the raw landing zone for all ingested data. Data arrives here unmodified -- column names and values are preserved exactly as received from the source system. This layer is append-only by default.

## What's Here

```
platform/bronze/
├── bronze.Lakehouse/                       # Fabric Lakehouse (Delta tables + Files)
├── bronze-env.Environment/                 # Spark environment (Python packages)
├── bronze-orchestrator.Notebook/           # Master orchestrator -- runs all ingestion
├── ingestion-config-file.Notebook/         # Loads file ingestion control table
├── file-ingestion.Notebook/               # Ingests files from SharePoint/ADLS shortcuts
├── sharepoint-lists-ingestion.Notebook/    # Ingests SharePoint Lists via Graph API
├── manual-metadata.Notebook/              # Handles manually-uploaded CSV files
└── salesforce_integration/
    └── salesforce-data-ingestion.Notebook/ # Ingests Salesforce objects via SOQL
```

---

## Orchestration

**`bronze-orchestrator`** is the entry point. It loads `fabric_libs`, sets up schemas, then runs the ingestion notebooks in the correct order using `run_notebooks_parallel` (with dependencies):

```
ingestion-config-file           (setup -- runs first)
        |
        +-- file-ingestion              (parallel)
        +-- sharepoint-lists-ingestion  (parallel)
        +-- salesforce-data-ingestion   (parallel)
        +-- manual-metadata             (parallel, if manual files present)
```

The orchestrator logs start/end times and a summary to the observability lakehouse.

---

## Notebooks

### `ingestion-config-file`

Loads the file ingestion control table (`config.file_ingestion_control_attributes`) from a CSV stored in the lakehouse `Files/` area. This table tells the file ingestion notebook which folders to process, what file format to expect, and what Delta table to write to.

Must run **before** `file-ingestion`.

### `file-ingestion`

Reads CSV, TXT, and Excel files from OneLake shortcuts (SharePoint and ADLS paths) and appends them to Bronze Delta tables. Uses `fabric_libs.ingestion.files` for parallel processing.

Key behaviour:
- Incremental -- only picks up files not seen in the previous run (based on modification timestamp)
- Parallel across folders (8 workers by default)
- Writes to `bronze.{source_schema}.*` tables

### `sharepoint-lists-ingestion`

Fetches SharePoint List items via the Microsoft Graph API delta endpoint. Supports incremental loads -- only changed items are pulled on subsequent runs.

Writes to `bronze.{source_schema}.*` tables.

### `salesforce-data-ingestion`

Pulls Salesforce objects via SOQL using OAuth credentials stored in Azure Key Vault. Supports incremental loads -- filters by `LastModifiedDate` since the last successful pull.

Writes to `bronze.salesforce.*` schema.

### `manual-metadata`

Handles ad-hoc CSV files dropped into `Files/MANUAL-INGESTION/` with an accompanying metadata sidecar. Useful for one-off data loads that don't have a scheduled source feed.

---

## Lakehouse Schemas

| Schema | Purpose |
|--------|---------|
| `config` | Control tables for ingestion (file matching rules) |
| `control` | Metadata/tracking tables (incremental watermarks) |
| `salesforce` | Salesforce CRM tables (account, contact, etc.) |
| `sharepoint` | SharePoint file and list tables |

---

## Environment

**`bronze-env`** defines the Spark environment used by all bronze notebooks. Key packages:

- `simple-salesforce` -- Salesforce REST API client
- `azure-identity`, `azure-keyvault-secrets` -- Key Vault authentication
- `msal` -- Microsoft authentication for Graph API (SharePoint Lists)

---

## Data Flow

```
SharePoint Files  -->  OneLake Shortcuts  -->  file-ingestion            -->  bronze.{schema}.*
SharePoint Lists  -->  Graph API          -->  sharepoint-lists-ingestion -->  bronze.{schema}.*
Salesforce        -->  SOQL API           -->  salesforce-data-ingestion  -->  bronze.salesforce.*
Manual CSVs       -->  Files/MANUAL-INGESTION/ --> manual-metadata        -->  bronze.{schema}.*
```

---

## Related Documentation

- [fabric_libs README](../../datalake/libs/fabric_libs/README.md) -- Library API reference
- [datalake/inputs/config/](../../datalake/inputs/config/) -- YAML configs (shortcuts, batch schedules, Salesforce columns)
- [Silver Layer](../silver/README.md) -- Next layer downstream
