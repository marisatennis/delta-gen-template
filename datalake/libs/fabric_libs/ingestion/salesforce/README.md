# Salesforce Ingestion Library

Reusable Salesforce ingestion utilities for Fabric/Spark notebooks.

## Overview

This library provides a modular approach to ingesting Salesforce data into Azure Fabric Lakehouse Delta tables with support for:

- Incremental data pulls based on `LastModifiedDate`
- Metadata tracking for audit and recovery
- Data completeness validation
- Schema management

## Modules

### `client.py`

Handles Salesforce authentication using Azure Key Vault credentials.

```python
from fabric_libs.salesforce import get_salesforce_client

keyvault_uri = "https://kv-your-project.vault.azure.net/"
sf_domain = "yourorg.my"
sf_client = get_salesforce_client(keyvault_uri, sf_domain)
```

### `schema.py`

Schema creation is now centralized in `platform/bronze/schema-setup.Notebook`.
The `salesforce` schema is created automatically by the orchestrator before ingestion runs.
The legacy `schema.py` helper has been removed.

### `metadata.py`

Tracks ingestion metadata for incremental loads.

```python
from fabric_libs.salesforce import ensure_metadata_table, get_latest_timestamp, update_metadata

# Create metadata table
ensure_metadata_table(spark, "bronze.salesforce.ingestion_metadata")

# Get last pull timestamp
last_pull = get_latest_timestamp(spark, "Account", "bronze.salesforce.ingestion_metadata")

# Update metadata after successful pull
update_metadata(spark, "Account", datetime.utcnow(), "bronze.salesforce.ingestion_metadata")
```

### `ingestion.py`

Core ingestion logic including SOQL queries, data retrieval, and orchestration.

```python
from fabric_libs.salesforce import (
    get_data_from_salesforce_object,
    run_ingestion,
)

# Pull data for a specific object
df = get_data_from_salesforce_object(
    spark, sf_client, "Account", ["Id", "Name", "Type"]
)

# Run full ingestion pipeline
results = run_ingestion(
    spark=spark,
    sf_client=sf_client,
    column_config=column_config,
    object_to_table_map=object_to_table_map,
    metadata_table="bronze.salesforce.ingestion_metadata",
    run_id=run_id
)
```

### `storage.py`

The legacy `storage.py` helper has been removed. Writes now happen inline in the ingestion
pipeline using Spark's Delta writer.

## Complete Example

```python
import os, shutil, sys
from pyspark.sql import SparkSession
from datetime import datetime

# Load fabric_libs from Lakehouse
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

# Import Salesforce utilities
from fabric_libs.salesforce import (
    get_salesforce_client,
    run_ingestion,
)

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Configuration
KEYVAULT_URI = "https://kv-your-project.vault.azure.net/"
SF_DOMAIN = "yourorg.my"
METADATA_TABLE = "bronze.salesforce.ingestion_metadata"

SF_OBJECT_TO_LAKEHOUSE_TABLE_MAP = {
    "Account": "bronze.salesforce.account",
    "Contact": "bronze.salesforce.contact",
    "User": "bronze.salesforce.user"
}

# Authenticate
sf_client = get_salesforce_client(KEYVAULT_URI, SF_DOMAIN)

# Column config (or use get_salesforce_config_from_silver_mappings)
column_config = {
    "Account": ["Id", "Name", "Type"],
    "Contact": ["Id", "FirstName", "LastName", "Email"],
}

# Run ingestion
import uuid
run_id = str(uuid.uuid4())
results = run_ingestion(
    spark=spark,
    sf_client=sf_client,
    column_config=column_config,
    object_to_table_map=SF_OBJECT_TO_LAKEHOUSE_TABLE_MAP,
    metadata_table=METADATA_TABLE,
    run_id=run_id
)

print(f"Ingestion complete: {results}")
```

## Configuration File Format

The YAML configuration file (`sf_column_config.yaml`) maps Salesforce objects to the columns to retrieve:

```yaml
Account:
  - Id
  - Name
  - Type
  - Industry
  - BillingCity
  - BillingCountry

Contact:
  - Id
  - FirstName
  - LastName
  - Email
  - Phone
  - AccountId

User:
  - Id
  - Username
  - Email
  - FirstName
  - LastName
```

## Incremental Loading

The library automatically handles incremental loads:

1. Checks metadata table for last successful pull timestamp
2. Queries Salesforce with `WHERE LastModifiedDate > {last_pull}`
3. Only new/modified records are retrieved
4. Metadata is updated after successful ingestion

## Data Completeness

The `check_for_completeness()` function validates that the number of records pulled matches the expected count from Salesforce, preventing silent data loss.
