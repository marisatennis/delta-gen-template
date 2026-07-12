# Microsoft Purview Governance Integration

This module provides scripts to register and manage data assets in Microsoft Purview
based on your Delta-Gen YAML table configurations. It automates the creation of:

- **Collections** - organized by layer (Bronze, Silver, Gold)
- **Custom types** - for Fabric Lakehouse tables
- **Entity definitions** - one per YAML table config
- **Glossary terms** - from column descriptions
- **Lineage** - source-to-target relationships from YAML configs
- **Classifications** - PII, sensitive data tags from column extensions

## Prerequisites

- Python 3.10+
- Azure identity configured (`az login` or service principal)
- Microsoft Purview account with Data Curator role
- `pip install azure-identity azure-purview-catalog azure-purview-scanning pyyaml`

## Quick Start

```bash
# Set environment variables
export PURVIEW_ACCOUNT_NAME="your-purview-account"
export PURVIEW_TENANT_ID="your-tenant-id"

# Register all YAML configs as Purview entities
python register_entities.py \
    --config-path ../../datalake/inputs/ \
    --purview-account $PURVIEW_ACCOUNT_NAME

# Create lineage from YAML source references
python create_lineage.py \
    --config-path ../../datalake/inputs/ \
    --purview-account $PURVIEW_ACCOUNT_NAME

# Sync glossary terms from column descriptions
python sync_glossary.py \
    --config-path ../../datalake/inputs/ \
    --purview-account $PURVIEW_ACCOUNT_NAME

# Dry run (show what would be created without making changes)
python register_entities.py \
    --config-path ../../datalake/inputs/ \
    --purview-account $PURVIEW_ACCOUNT_NAME \
    --dry-run
```

## Scripts

| Script | Purpose |
|--------|---------|
| `purview_client.py` | Shared Purview API client with auth helpers |
| `register_entities.py` | Register/update Purview entities from YAML table configs |
| `create_lineage.py` | Create lineage relationships from YAML source references |
| `sync_glossary.py` | Sync glossary terms from column descriptions and defaults |
| `scan_configs.py` | Scan YAML configs and generate a catalog manifest |

## How It Works

1. **Scans YAML configs** in `datalake/inputs/` to discover table definitions
2. **Maps to Purview types** - each table becomes a Purview entity with columns as schema attributes
3. **Creates lineage** - YAML `sources` are mapped to upstream entities, joins create relationships
4. **Syncs glossary** - column `description` fields and `extensions.classification` tags become glossary terms

## YAML Extensions for Governance

Add governance metadata to your YAML configs:

```yaml
columns:
  - name: email
    data_type: string
    description: "Customer email address"
    extensions:
      classification: PII          # Purview classification
      glossary_term: Email Address  # Link to glossary term
      sensitivity: confidential     # Data sensitivity level

  - name: customer_segment
    data_type: string
    description: "Customer segmentation category"
    extensions:
      glossary_term: Customer Segment
      owner: data-analytics-team
```

## Configuration

Create `purview_config.yaml` in this directory to customize behavior:

```yaml
purview:
  account_name: ${PURVIEW_ACCOUNT_NAME}
  tenant_id: ${PURVIEW_TENANT_ID}

  # Collection hierarchy
  collections:
    root: "Data Platform"
    layers:
      bronze: "Bronze Layer"
      silver: "Silver Layer"
      gold: "Gold Layer"
      log: "Logging & Metrics"

  # Custom type definitions
  type_prefix: "fabric_lakehouse"

  # Classification mappings
  classifications:
    PII: "MICROSOFT.PERSONAL.ALL"
    financial: "MICROSOFT.FINANCIAL.ALL"
    confidential: "MICROSOFT.GENERAL.CONFIDENTIAL"
```
