# Delta-Gen Integration Guide

This platform uses [Delta-Gen](https://github.com/marisatennis/delta-gen) as its transformation engine for the Silver and Gold layers. Delta-Gen is a declarative YAML-driven engine that compiles table configurations into Spark DataFrame pipelines.

## Architecture

```
datalake/
├── inputs/
│   ├── config/                    # Layer defaults, batch schedules
│   │   ├── silver_defaults.yaml
│   │   ├── gold_defaults.yaml
│   │   ├── silver_batches.yaml
│   │   └── gold_batches.yaml
│   ├── silver/                    # Silver layer YAML table configs
│   │   └── example/customer.yaml
│   └── gold/                      # Gold layer YAML table configs
│       ├── dimension/d_date.yaml
│       └── fact/f_sales.yaml
└── libs/
    ├── deltagen/                      # Delta-Gen core engine (vendored, uploaded to Lakehouse)
    │   └── fabric/                    # Fabric integration layer
    │       ├── adapter.py             # FabricMetricsAdapter
    │       ├── context.py             # create_fabric_context()
    │       └── plugins/               # DQ, dimension, flow, partition, write-hook plugins
    └── fabric_libs/                   # Platform utilities (single top-level package)
        ├── cleaning.py                # Generic column-cleaning plugins (e.g. clean_decimal)
        ├── orchestration/             # Batch orchestration utilities
        │   ├── config_runner.py       # YAML batch scheduling
        │   ├── notebook_runner.py     # Parallel notebook execution
        │   └── tracking.py            # Run metrics persistence
        ├── ingestion/                 # File / Salesforce / SharePoint ingestion
        ├── auth/                      # Azure Key Vault helpers
        └── utils/                     # Schema management utilities
```

## How It Works

1. **YAML configs** define each table: sources, columns, joins, filters, load mode
2. **Orchestrator notebooks** resolve configs into batches and run them in parallel
3. **Template notebooks** load a single YAML, build the DataFrame via PlanBuilder, write via DeltaWriter
4. **Metrics** are auto-persisted to Delta logging tables via FabricMetricsAdapter

## Adding a New Table

1. Create a YAML file under `datalake/inputs/silver/` or `datalake/inputs/gold/`
2. Define sources, columns, joins, and policies
3. Set the batch and order in `policies.orchestration`
4. The orchestrator will pick it up automatically on the next run

Example silver table:

```yaml
name: customer
layer: silver
target_schema: silver

sources:
  - name: src
    table: bronze.raw_customers

natural_key:
  - customer_id

incremental:
  filter_mode: watermark
  watermark_column: modified_date

policies:
  orchestration:
    active: true
    batch: 1
    order: 10
  optimisation:
    load_mode: merge
    merge_strategy: update_changed

columns:
  - name: customer_id
    data_type: string
    nullable: false
    inputs:
      - source: src
        column: id
  - name: customer_name
    data_type: string
    inputs:
      - source: src
        column: name
```

## Defaults and Macros

Layer defaults (`silver_defaults.yaml`, `gold_defaults.yaml`) provide shared configuration. Reference them in table configs with `${defaults.*}`:

```yaml
columns:
  - name: source_modified
    data_type: date
    default: ${defaults.columns.default_date}
```

## Batch Scheduling

Batches control execution order and schedule. Defined in `*_batches.yaml`:

- **Batch 0**: Initialization (seed tables, only runs if missing)
- **Batch 1+**: Regular batches, filtered by `run_schedule` parameter (daily/weekly/monthly/all)

## Plugins

### Built-in (Delta-Gen core)
- `not_null`, `in_set`, `mask_email` (column)
- `dedupe_keep_last`, `distinct`, `filter_latest_file_per_period` (stage)

### Fabric Helpers
- `log_nulls_to_table`, `log_invalid_to_table` (DQ logging)
- `ensure_sentinels` (dimension sentinel rows)
- `check_unresolved_fks` (FK validation)
- `schedule_fk_reresolution` (auto re-processing)
- `self_join_previous_period` (flow calculation)
- `composite_period_replace` (multi-column partition replacement)
- `clean_decimal`, `parse_mixed_date`, `clean_uk_postcode`, `clean_contact_name`, `clean_company_name` (data cleaning)

## Deploying Libraries

Upload `deltagen/` and `fabric_libs/` to:
```
/lakehouse/default/Files/libs/
```

The orchestrator notebook auto-zips and distributes them to Spark workers.
