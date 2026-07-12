# Gold Layer

The Gold layer contains the analytics-ready dimensional model -- a star schema of facts and dimensions built from Silver data, optimised for Power BI semantic models and business reporting.

Like Silver, every Gold table is driven by a **YAML configuration file** (`datalake/inputs/gold/*.yaml`) executed by the **Delta-Gen** library via the template notebook pattern.

## What's Here

```
platform/gold/
├── gold.Lakehouse/                 # Fabric Lakehouse (Delta tables + Files)
├── gold-env.Environment/           # Spark environment (Python packages + deltagen whl)
├── gold-orchestrator.Notebook/     # Master orchestrator -- resolves batches and runs tables
└── gold-template.Notebook/         # Template executed once per YAML config
```

---

## Orchestration

**`gold-orchestrator`** mirrors the silver orchestrator pattern:

1. Loads `fabric_libs` and `deltagen` from `Files/libs/`
2. Reads `inputs/config/gold_batches.yaml` to determine which batch to run
3. Resolves all `inputs/gold/*.yaml` configs assigned to that batch
4. Groups configs by batch/order -- dimensions run before facts (order matters for FK lookups)
5. Runs `gold-template` in parallel for each config
6. Logs execution results to the observability lakehouse

---

## How a Gold Table is Built

```
datalake/inputs/gold/{table}.yaml
         |
         v
gold-orchestrator  (resolves batch, picks yaml)
         |
         v
gold-template  (reads yaml, loads fabric_libs + deltagen)
         |
         v
Delta-Gen PlanBuilder  (joins Silver tables, applies business logic)
         |
         v
gold.{d_/f_}{table}  (dimension or fact, written as Delta table)
```

### Table Naming

| Prefix | Type | Example |
|--------|------|---------|
| `d_` | Dimension | `gold.d_contact`, `gold.d_product`, `gold.d_date` |
| `f_` | Fact | `gold.f_monthly_data`, `gold.f_sales_interaction` |

### Column Naming

Gold columns use **PascalCase**: `ContactID`, `Amount`, `SourcePeriod`, `InteractionDate`.

---

## Dimensional Model

### Dimensions

| Table | Description |
|---|---|
| `gold.d_date` | Calendar dimension -- days, weeks, months, quarters, fiscal periods |
| `gold.d_contact` | Contacts from CRM (firms, individuals) |
| `gold.d_product` | Products and model identifiers |
| `gold.d_product_family` | Product family groupings |
| `gold.d_interaction_type` | Interaction type classifications |
| `gold.d_sector` | Sector/category classifications |

### Facts

| Table | Description |
|---|---|
| `gold.f_monthly_data` | Monthly data snapshot per product/entity |
| `gold.f_monthly_net_flow` | Monthly net flow aggregated across sources |
| `gold.f_weekly_net_flow` | Weekly net flow |
| `gold.f_monthly_targets` | Monthly targets by contact/team |
| `gold.f_sales_interaction` | Sales team interactions with contacts (CRM) |
| `gold.f_weekly_performance` | Weekly performance metrics |

---

## Silver Access

Gold notebooks access Silver tables via the Silver SQL endpoint, mounted as a shortcut in the Gold lakehouse. YAML sources reference these as:

```yaml
sources:
  - name: silver_contact
    catalog: silver
    schema: salesforce
    table: contact
```

---

## Semantic Models

The Gold lakehouse is consumed by Power BI semantic models in `platform/reporting/`:

- **`main.SemanticModel`** -- primary model for reporting dashboards
- **`silver_view.SemanticModel`** -- direct Silver-layer views for operational reporting

These are DirectLake models connected to the Gold (and Silver) SQL endpoint.

---

## Development

### Gold Template

**`gold-template`** is the single notebook that handles all Gold table builds. It:
1. Reads the YAML config path passed as a parameter
2. Creates a Delta-Gen `FabricContext` for metrics collection
3. Calls `PlanBuilder` to execute the transformation plan
4. Writes the result to the Gold lakehouse via MERGE

### FK Resolution and Sentinels

Delta-Gen resolves foreign keys during Gold processing. When an FK lookup fails to find a match, a sentinel value (e.g., `NO_MATCH`) is used instead. Unresolved FKs are logged to `log.gold_unresolved_fks` for investigation.

---

## Related Documentation

- [datalake/inputs/gold/](../../datalake/inputs/gold/) -- All YAML table configs
- [deltagen README](../../datalake/libs/deltagen/README.md) -- Delta-Gen library reference
- [Silver Layer](../silver/README.md) -- Upstream data source
- [platform/reporting/](../reporting/) -- Power BI semantic models and reports
