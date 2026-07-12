# Silver Layer

The Silver layer cleanses, deduplicates, and standardises Bronze data into a consistent schema. It is the central, trusted repository consumed by the Gold layer and by analysts needing raw-but-clean data.

Every Silver table is driven by a **YAML configuration file** (`datalake/inputs/silver/*.yaml`) processed by the **Delta-Gen** library. There are no hand-coded transformation notebooks per table -- the template notebook reads the YAML and Delta-Gen executes the plan.

## What's Here

```
platform/silver/
├── silver.Lakehouse/               # Fabric Lakehouse (Delta tables + Files)
├── silver-env.Environment/         # Spark environment (Python packages + deltagen whl)
├── silver-orchestrator.Notebook/   # Master orchestrator -- resolves batches and runs tables
└── silver-template.Notebook/       # Template executed once per YAML config
```

---

## Orchestration

**`silver-orchestrator`** is the entry point. It:

1. Loads `fabric_libs` and `deltagen` from `Files/libs/`
2. Reads `inputs/config/silver_batches.yaml` to determine which batch to run (daily, weekly, monthly)
3. Resolves all `inputs/silver/*.yaml` configs assigned to that batch
4. Groups configs by batch/order and runs `silver-template` in parallel (one call per config)
5. Logs overall execution results to the observability lakehouse

The orchestrator is typically triggered by the Fabric Data Pipeline `orchestrator` in `platform/log/`.

---

## How a Silver Table is Built

Every silver table follows the same path:

```
datalake/inputs/silver/{table}.yaml
         |
         v
silver-orchestrator  (resolves batch, picks yaml)
         |
         v
silver-template  (reads yaml, loads fabric_libs + deltagen)
         |
         v
Delta-Gen PlanBuilder  (executes stages: transformation, dedup, merge)
         |
         v
silver.{schema}.{table}  (upserted via MERGE)
```

### YAML Config Structure

Each `inputs/silver/*.yaml` file defines:

```yaml
name: source_monthly_data
target_schema: your_schema

policies:
  optimisation:
    load_mode: merge              # upsert strategy
    merge_strategy: update_changed
    hash_columns: [model, entity, source_period]
  orchestration:
    batch: 1                      # links to silver_batches.yaml
    order: 1

sources:
  - name: src
    catalog: silver               # reads from bronze shortcut in silver lakehouse
    schema: bronze_source_schema
    table: source_data_table

stages:
  - name: base
    mode: transformation
    columns:
      - name: model
        data_type: string
        natural: true             # part of natural/business key
        inputs:
          - source: src
            column: model_name
```

---

## Lakehouse Schemas

| Schema | Purpose |
|--------|---------|
| `salesforce` | Cleansed Salesforce CRM tables (account, contact, etc.) |
| `sharepoint` | Cleansed SharePoint file tables |
| `transform` | Multi-source consolidated tables (consolidated views, staging) |
| `log` | DQ rejection logs, duplicate logs, deltagen metrics |

Bronze data is accessed via **OneLake shortcuts** mounted at `bronze_{source_schema}` schemas within the Silver lakehouse.

---

## Batch Schedule

Silver tables are grouped into batches defined in `inputs/config/silver_batches.yaml`:

| Batch | Name | Schedule | Days |
|-------|------|----------|------|
| 1 | daily_core | Daily | Mon-Fri |
| 2 | weekly_refresh | Weekly | Saturday |
| 3 | month_end | Monthly | 1st of month |

Each YAML config declares which batch it belongs to (`policies.orchestration.batch`).

---

## Data Quality

Delta-Gen applies DQ checks defined in each YAML config:
- **`not_null`** -- rejects or warns on null values in required columns
- **`in_set`** -- validates column values against an allowed set
- Rejected rows are written to `log.silver_dq_rejected`
- Duplicate rows are written to `log.silver_dq_duplicates`

---

## Environment

**`silver-env`** defines the Spark environment. Key packages:
- `deltagen` (custom wheel -- `datalake/libs/deltagen.zip`)
- `pyyaml` -- YAML config parsing
- `azure-identity`, `azure-keyvault-secrets`

---

## Related Documentation

- [datalake/inputs/silver/](../../datalake/inputs/silver/) -- All YAML table configs
- [deltagen README](../../datalake/libs/deltagen/README.md) -- Delta-Gen library reference
- [Bronze Layer](../bronze/README.md) -- Upstream data source
- [Gold Layer](../gold/README.md) -- Downstream consumer
