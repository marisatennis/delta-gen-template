# Documentation Hub

Central index for all project documentation. Start here and follow the links to the area you need.

---

## Where to Start

| I want to... | Go to |
|---|---|
| Understand the overall architecture | [Root README](../README.md) |
| Learn how Delta-Gen drives Silver/Gold | [Delta-Gen Integration Guide](DELTA_GEN_INTEGRATION.md) |
| Set up my dev environment and contribute | [Developer Guide](DEVELOPER-GUIDE.md) |
| Monitor, troubleshoot, or operate the platform | [Operations Guide](OPERATIONS.md) |
| Look up table schemas | [Schema Reference](SCHEMAS.md) |
| Understand CI/CD pipelines | [DevOps Pipelines](../devops-pipelines/README.md) |

---

## Architecture & Design

High-level architecture lives in `design/architecture/`. The platform follows the **Medallion Architecture** pattern:

```
Source Systems  -->  Bronze (raw)  -->  Silver (cleansed)  -->  Gold (dimensional)  -->  Power BI
```

Key resources:

| Resource | Location |
|---|---|
| Architecture diagram | `design/architecture/MedallionArchitecture.png` |
| Conceptual data model | `design/conceptual/` |
| Logical data model | `design/logical/` |
| Source-to-target mappings | `design/mappings/` |

---

## Data Mapping Specifications

> **Placeholder** -- Add links to your source-to-target mapping documents here.

Mapping specs define how source system fields translate through Bronze, Silver, and Gold layers. Typically stored in `design/mappings/` as Excel or DrawIO files.

| Source System | Mapping Document | Status |
|---|---|---|
| _Example: CRM_ | `design/mappings/crm_mapping.xlsx` | _Draft_ |
| _Example: ERP_ | `design/mappings/erp_mapping.xlsx` | _Draft_ |

---

## Data Transformation Guide

The Silver and Gold layers are driven by YAML configurations processed by the Delta-Gen engine. See the full integration guide:

- **[Delta-Gen Integration Guide](DELTA_GEN_INTEGRATION.md)** -- How YAML configs drive transformations, plugin system, batch orchestration, and adding new tables.

---

## Schema Reference

Table-level schema documentation for each layer:

- **[Schema Reference](SCHEMAS.md)** -- Bronze, Silver, Gold, and Logging table schemas.

---

## Operations Guide

Run-book for operating the platform day-to-day:

- **[Operations Guide](OPERATIONS.md)** -- Scheduling, manual runs, failure response, data quality investigation, monitoring, and adding new data sources.

---

## Platform Layer READMEs

Each layer has its own README with orchestration details, notebook inventory, and configuration:

| Layer | README |
|---|---|
| Platform (overview) | [`platform/README.md`](../platform/README.md) |
| Bronze | [`platform/bronze/README.md`](../platform/bronze/README.md) |
| Silver | [`platform/silver/README.md`](../platform/silver/README.md) |
| Gold | [`platform/gold/README.md`](../platform/gold/README.md) |

---

## Developer Guide

Everything you need to contribute code to this project:

- **[Developer Guide](DEVELOPER-GUIDE.md)** -- Development workflow, code standards, PR guidelines, documentation standards.

---

## DevOps & Deployment

CI/CD pipelines for deploying and managing Fabric workspaces:

| Resource | Location |
|---|---|
| Pipeline overview | [`devops-pipelines/README.md`](../devops-pipelines/README.md) |
| Dev workspace guide | [`devops-pipelines/README_DEV_WORKSPACES.md`](../devops-pipelines/README_DEV_WORKSPACES.md) |

---

## Semantic Model

> **Placeholder** -- Add documentation for your Power BI semantic model here.

The semantic model lives in `platform/main.SemanticModel/` and defines the business-facing data model consumed by Power BI reports. Document measures, calculated columns, relationships, and RLS rules in this section as the model matures.
