# Platform

The platform directory contains all Fabric workspace artifacts organized by medallion layer. Each subdirectory maps to a Fabric workspace and contains its notebooks, lakehouses, environments, pipelines, and semantic models.

## Medallion Architecture

```
                    ┌─────────────────────┐
                    │   Log / Observability│
                    │   (orchestrator      │
                    │    pipeline)         │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              v                v                v
     ┌────────────┐   ┌────────────┐   ┌────────────┐
     │   Bronze   │──>│   Silver   │──>│    Gold    │
     │  (raw)     │   │ (cleansed) │   │(analytics) │
     └────────────┘   └────────────┘   └────────────┘
                                              │
                                              v
                                     ┌────────────┐
                                     │ Reporting  │
                                     │ (Power BI) │
                                     └────────────┘
```

### Bronze Layer (`platform/bronze/`)

Raw landing zone. Data arrives unmodified from source systems:
- **File ingestion** -- CSV, TXT, Excel files from SharePoint/ADLS via OneLake shortcuts
- **Salesforce ingestion** -- CRM objects via SOQL API with incremental pulls
- **SharePoint Lists** -- List items via Microsoft Graph API delta queries
- **Manual ingestion** -- Ad-hoc CSV files with metadata sidecars

### Silver Layer (`platform/silver/`)

Cleansed, deduplicated, and standardised data:
- Every table driven by a **YAML config** processed by **Delta-Gen**
- No hand-coded transformation notebooks -- single template pattern
- Batched orchestration (daily, weekly, monthly)
- DQ checks with rejection logging

### Gold Layer (`platform/gold/`)

Analytics-ready dimensional model (star schema):
- Dimensions (`d_*`) and Facts (`f_*`) built from Silver data
- FK resolution with sentinel values for unmatched lookups
- Same Delta-Gen YAML + template pattern as Silver
- Consumed by Power BI DirectLake semantic models

### Log / Observability (`platform/log/`)

Operational control plane:
- Master orchestrator pipeline (Bronze -> Silver -> Gold)
- Run logs, DQ results, and metrics collection
- SQL endpoint views for the platform health dashboard
- Failure alerting via email

---

## Orchestration Flow

The master pipeline in `platform/log/` drives the full refresh:

```
1. Bronze orchestrator
   ├── Load control table (config)
   ├── File ingestion (parallel)
   ├── SharePoint Lists ingestion (parallel)
   ├── Salesforce ingestion (parallel)
   └── Manual metadata (parallel)

2. Silver orchestrator
   ├── Resolve batch schedule (daily/weekly/monthly)
   ├── Load YAML configs for current batch
   └── Run silver-template per config (parallel by order group)

3. Gold orchestrator
   ├── Resolve batch schedule
   ├── Load YAML configs for current batch
   └── Run gold-template per config (dimensions before facts)
```

A shared `ORCHESTRATION_ID` (UUID) is passed through all layers for end-to-end traceability.

---

## Directory Structure

```
platform/
├── bronze/          # Raw ingestion layer
├── silver/          # Cleansed transformation layer
├── gold/            # Dimensional model layer
├── log/             # Orchestration and observability
├── reporting/       # Power BI semantic models and reports
└── README.md        # This file
```

---

## Key Technologies

| Component | Technology |
|-----------|------------|
| Compute | Microsoft Fabric Spark |
| Storage | OneLake (Delta Lake) |
| Orchestration | Fabric Data Pipelines + Notebook orchestration |
| Transformations | Delta-Gen (YAML-driven) |
| Ingestion | fabric_libs (Python) |
| Reporting | Power BI DirectLake |
| Secrets | Azure Key Vault |
| Authentication | Azure AD / Service Principals |

---

## Related Documentation

- [Bronze Layer](bronze/README.md)
- [Silver Layer](silver/README.md)
- [Gold Layer](gold/README.md)
- [fabric_libs](../datalake/libs/fabric_libs/README.md) -- Ingestion and orchestration library
- [Delta-Gen](../datalake/libs/deltagen/README.md) -- YAML-driven transformation engine
