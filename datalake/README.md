# Datalake

This directory contains the shared data assets that are deployed to all Fabric Lakehouses -- configuration files, YAML table definitions, and reusable Python libraries.

## Structure

```
datalake/
├── inputs/                    # Configuration and table definitions
│   ├── config/                # Platform configuration files
│   │   ├── silver_batches.yaml    # Silver batch schedule definitions
│   │   ├── gold_batches.yaml      # Gold batch schedule definitions
│   │   ├── silver_defaults.yaml   # Default values for silver YAML macros
│   │   ├── gold_defaults.yaml     # Default values for gold YAML macros
│   │   └── shortcuts.yaml         # OneLake shortcut definitions
│   │
│   ├── silver/                # Silver layer YAML table configs
│   │   ├── salesforce/        # Salesforce source tables
│   │   ├── sharepoint/        # SharePoint file source tables
│   │   └── transform/         # Multi-source consolidated tables
│   │
│   └── gold/                  # Gold layer YAML table configs
│       ├── dimensions/        # Dimension table configs (d_*)
│       └── facts/             # Fact table configs (f_*)
│
├── libs/                      # Reusable Python libraries
│   ├── fabric_libs/           # Platform utilities (ingestion, auth, utils, orchestration, cleaning)
│   └── deltagen/              # Vendored Delta-Gen v2 engine
│
└── README.md                  # This file
```

## How It Works

### Inputs

The `inputs/` directory contains everything the platform needs to know about what data to process and how:

- **Config files** define batch schedules, default values, and infrastructure setup
- **Silver YAMLs** define how each Bronze table is cleansed and loaded into Silver (column mappings, data types, DQ rules, merge strategy)
- **Gold YAMLs** define how Silver tables are joined and transformed into the dimensional model (fact/dimension definitions, FK lookups, business logic)

### Libraries

The `libs/` directory contains Python packages that are loaded by Fabric notebooks at runtime. See [libs/README.md](libs/README.md) for details.

## Deployment

All files in `datalake/` are deployed to each Fabric Lakehouse's `Files/` directory:

```
Lakehouse Files/
├── inputs/          # <- from datalake/inputs/
│   ├── config/
│   ├── silver/
│   └── gold/
│
└── libs/            # <- from datalake/libs/
    ├── fabric_libs/
    └── deltagen/
```

This deployment happens via Fabric Git integration or manual upload during development.

## Adding a New Table

To add a new Silver or Gold table:

1. Create a YAML config file in the appropriate `inputs/silver/` or `inputs/gold/` subdirectory
2. Follow the existing YAML structure (see sibling files for examples)
3. Assign it to the correct batch in `policies.orchestration.batch`
4. Deploy to the Lakehouse
5. The orchestrator will automatically pick it up on the next run

See the [Delta-Gen documentation](libs/deltagen/README.md) for the full YAML config specification.
