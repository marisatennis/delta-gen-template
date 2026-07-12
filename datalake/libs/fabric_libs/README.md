# fabric_libs - Reusable Fabric/Spark Utilities

Modular library for Azure Fabric data platform operations.

## Structure

```
datalake/libs/
├── fabric_libs/                 # Main package directory
│   ├── ingestion/               # Data ingestion utilities
│   │   ├── files/               # Generic file-based ingestion
│   │   │   ├── file_ingestion.py  # Orchestration, parallel processing
│   │   │   ├── io.py              # CSV/Excel/TXT file readers
│   │   │   ├── matching.py        # Control table matching
│   │   │   ├── profiling.py       # File metadata profiling
│   │   │   ├── reconcile.py       # Data reconciliation
│   │   │   ├── tracking.py        # Incremental load tracking
│   │   │   ├── utils.py           # Utilities, UDFs
│   │   │   └── README.md
│   │   │
│   │   ├── salesforce/          # Salesforce API-based ingestion
│   │   │   ├── client.py        # Authentication
│   │   │   ├── ingestion.py     # SOQL queries & orchestration
│   │   │   ├── metadata.py      # Metadata tracking
│   │   │   └── README.md
│   │   │
│   │   ├── sharepoint_lists/    # SharePoint list ingestion (Graph API)
│   │   │   ├── client.py        # Authentication
│   │   │   ├── ingestion.py     # List fetch & delta queries
│   │   │   ├── metadata.py      # Delta link tracking
│   │   │   ├── pretty.py        # Result formatting
│   │   │   └── storage.py       # Delta table writes
│   │   │
│   │   └── __init__.py
│   │
│   ├── utils/                   # General utilities
│   │   └── schema_management.py # Schema CRUD operations
│   │
│   ├── auth/                    # Authentication helpers
│   │   └── keyvault.py          # Azure Key Vault access
│   │
│   └── __init__.py
│
└── fabric_libs.zip              # Zipped version for Spark distribution
```

## Usage

### Loading the Library

All notebooks should load fabric_libs using this pattern:

```python
import os, shutil, sys
LAKEHOUSE_ROOT = "/lakehouse/default/Files"
PKG_DIR = f"{LAKEHOUSE_ROOT}/libs/fabric_libs"
ZIP_PATH = f"{LAKEHOUSE_ROOT}/libs/fabric_libs.zip"

if os.path.isdir(PKG_DIR):
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
    shutil.make_archive(f"{LAKEHOUSE_ROOT}/libs/fabric_libs", "zip", f"{LAKEHOUSE_ROOT}/libs", "fabric_libs")
    spark.sparkContext.addPyFile(ZIP_PATH)
    if f"{LAKEHOUSE_ROOT}/libs" not in sys.path:
        sys.path.insert(0, f"{LAKEHOUSE_ROOT}/libs")
```

### File-Based Ingestion

For SharePoint or any file-based data source:

```python
from fabric_libs.ingestion import files

# Sequential ingestion
results = files.run_ingestion(
    spark,
    root_path="Files/Your-Source-Data",
    source_name="Your-Source",
    control_table="config.file_ingestion_control_attributes",
    metadata_table="control.file_ingestion_metadata_log",
    materialize=True,
    incremental=True,
)

# Parallel ingestion (faster)
results = files.run_ingestion_parallel(
    spark,
    root_path="Files/Your-Source-Data",
    source_name="Your-Source",
    control_table="config.file_ingestion_control_attributes",
    metadata_table="control.file_ingestion_metadata_log",
    workers=8,
    materialize=True,
    incremental=True,
)

# View results and reconcile
files.print_parallel_results(results)
files.view_metadata_summary(spark)
```

### Salesforce Ingestion

For Salesforce data ingestion:

```python
from fabric_libs.ingestion import salesforce

# Authenticate
sf_client = salesforce.get_salesforce_client(keyvault_uri, sf_domain)

# Run ingestion
results = salesforce.run_ingestion(
    spark=spark,
    sf_client=sf_client,
    column_config=column_config,
    object_to_table_map={
        "Account": "bronze.salesforce.account",
        "Contact": "bronze.salesforce.contact"
    },
    metadata_table="bronze.salesforce.ingestion_metadata",
    run_id=run_id,
)
```

### Orchestration

For orchestrating multiple notebooks (Bronze, Silver, Gold layers):

```python
import uuid
from fabric_libs.orchestration import (
    log_orchestration_start,
    run_notebooks_parallel,
    print_execution_summary,
    log_orchestration_end
)

# Initialize
orchestration_id = str(uuid.uuid4())
start_time = log_orchestration_start(orchestration_id, "Bronze Orchestration", "parallel")

# Define jobs
jobs = [
    {
        'name': 'Config Setup',
        'path': 'ingestion-config-file',
        'timeout': 1800,
        'parameters': None,
        'depends_on': []
    },
    {
        'name': 'File Ingestion',
        'path': 'file-ingestion-main',
        'timeout': 3600,
        'parameters': None,
        'depends_on': ['Config Setup']
    },
    {
        'name': 'Salesforce Ingestion',
        'path': 'salesforce_integration/salesforce_data_ingestion',
        'timeout': 1800,
        'parameters': None,
        'depends_on': []
    }
]

# Execute (parallel mode with dependency management)
results = run_notebooks_parallel(jobs, max_workers=3)

# Print summary and log completion
print_execution_summary(results, orchestration_id, "parallel")
summary = log_orchestration_end(orchestration_id, start_time, results)
```

## Qualified Imports (Recommended Pattern)

Use qualified imports for clarity and namespace disambiguation:

```python
# Import submodules
from fabric_libs.ingestion import files, salesforce
from fabric_libs import orchestration

# Use with qualification
files.run_ingestion_parallel(...)
salesforce.run_ingestion(...)
orchestration.run_notebooks_parallel(...)
```

This pattern:
- Makes code self-documenting (clear which module functions come from)
- Avoids naming conflicts
- Scales well as you add more ingestion sources

## Design Principles

1. **Function-First Organization**: Organized by function (ingestion, orchestration, transformation) rather than by data source
2. **Consistent Naming**: All ingestion modules use `run_ingestion()` - module namespace provides context
3. **Reusability**: Shared utilities (orchestration) used across all layers (Bronze, Silver, Gold)
4. **Scalability**: Easy to add new data sources (Azure SQL, Database, APIs) following existing patterns

## Future Extensions

Planned modules:

```
fabric_libs/
├── ingestion/
│   ├── files/           # Complete
│   ├── salesforce/      # Complete
│   ├── sharepoint_lists/ # Complete
│   ├── azure_sql/       # Future
│   ├── api/             # Future
│   └── databricks/      # Future
│
├── transformation/      # Future
│   ├── common/
│   ├── date_utils/
│   └── business_rules/
│
├── orchestration/       # Complete
│
└── quality/             # Future
    ├── validation/
    └── profiling/
```

## Contributing

When adding new modules:

1. Place them in the appropriate top-level category (ingestion, transformation, etc.)
2. Use consistent naming conventions (`run_ingestion()`, `run_transformation()`, etc.)
3. Include comprehensive docstrings with examples
4. Update the parent `__init__.py` to export the new module
5. Add a README.md with usage examples
6. Follow the qualified import pattern
