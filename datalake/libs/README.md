# Libraries

This directory contains the reusable Python libraries deployed to each Fabric Lakehouse for use by notebooks.

## Structure

```
datalake/libs/
├── fabric_libs/                # Platform utilities (single top-level package)
│   ├── auth/                   # Azure Key Vault helpers
│   ├── ingestion/              # Data source ingestion modules
│   │   ├── files/              # File-based ingestion (CSV, Excel, TXT)
│   │   ├── salesforce/         # Salesforce API ingestion
│   │   └── sharepoint_lists/   # SharePoint Graph API ingestion
│   ├── orchestration/          # Notebook orchestration utilities
│   ├── utils/                  # Schema management utilities
│   ├── cleaning.py             # Generic column-cleaning plugins (clean_decimal, ...)
│   └── README.md
│
└── deltagen/                   # Vendored Delta-Gen v2 engine; deltagen.fabric ships
                                # the Fabric integration (FabricMetricsAdapter,
                                # create_fabric_context, dimension/DQ/flow plugins)
```

## How Libraries Are Loaded

Fabric Spark notebooks cannot install packages at runtime in the traditional way. Instead, libraries are:

1. Stored as directories in the Lakehouse `Files/libs/` area
2. Zipped at notebook startup and added to the Spark context
3. Also added to `sys.path` for direct Python imports

Standard loading pattern (used in all orchestrator and template notebooks):

```python
import os, shutil, sys

LAKEHOUSE_ROOT = "/lakehouse/default/Files"

# Load fabric_libs
PKG_DIR = f"{LAKEHOUSE_ROOT}/libs/fabric_libs"
ZIP_PATH = f"{LAKEHOUSE_ROOT}/libs/fabric_libs.zip"
if os.path.isdir(PKG_DIR):
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
    shutil.make_archive(ZIP_PATH.replace('.zip', ''), "zip", f"{LAKEHOUSE_ROOT}/libs", "fabric_libs")
    spark.sparkContext.addPyFile(ZIP_PATH)
    if f"{LAKEHOUSE_ROOT}/libs" not in sys.path:
        sys.path.insert(0, f"{LAKEHOUSE_ROOT}/libs")

# Load deltagen
DELTAGEN_ZIP = f"{LAKEHOUSE_ROOT}/libs/deltagen.zip"
if os.path.exists(DELTAGEN_ZIP):
    spark.sparkContext.addPyFile(DELTAGEN_ZIP)
```

## Library Descriptions

### fabric_libs

Platform-wide utilities for data ingestion, authentication, and schema management.

- **ingestion.files** -- Generic file-based ingestion with parallel processing, control-table matching, incremental tracking, profiling, and reconciliation
- **ingestion.salesforce** -- Salesforce API ingestion with SOQL queries, incremental pulls, and metadata tracking
- **ingestion.sharepoint_lists** -- SharePoint list ingestion via Microsoft Graph API delta queries
- **auth** -- Azure Key Vault secret retrieval
- **utils** -- Lakehouse schema CRUD operations

### fabric_libs.orchestration

Notebook orchestration utilities for running multiple notebooks in parallel with dependency management, logging, and result tracking.

### fabric_libs.cleaning

Generic column-cleaning plugins (e.g., `clean_decimal`) registered with Delta-Gen via `@register_column`. Auto-registered when `fabric_libs` is imported.

### deltagen

Vendored copy of the Delta-Gen v2 transformation engine (see [delta-gen](https://github.com/marisatennis/delta-gen)). Includes `deltagen.fabric` — the Fabric integration layer (`FabricMetricsAdapter`, `create_fabric_context`, dimension/DQ/flow/partition/reresolution plugins, write hooks). Updated by re-syncing from a pinned release.

## Deployment

Libraries are deployed to each Lakehouse's `Files/libs/` directory. This can be done:

1. **Via Fabric Git integration** -- Libraries in `datalake/libs/` are synced to the Lakehouse
2. **Manually** -- Upload the directory to the Lakehouse Files area
3. **Via CI/CD pipeline** -- Automate deployment as part of the release process
