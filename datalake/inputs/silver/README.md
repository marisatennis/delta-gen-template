# Silver Layer Inputs

YAML configurations for each Silver table. One YAML per table.

## Subdirectory conventions

Group YAMLs by **how the data arrives**, not by business domain. This keeps related sources together when you ramp ingestion of a new system.

| Subdir | Use when |
|---|---|
| `sharepoint/` | Source is SharePoint Lists or SharePoint files (Graph API) |
| `salesforce/` | Source is Salesforce (SOQL via the Salesforce API) |
| `adls/` | Source is files landing in ADLS Gen2 (CSV, Excel, Parquet) |
| `transform/` | Multi-source consolidations / aggregations that join several Silver tables |
| `mapping/` | Reference/lookup tables, often hand-curated or refreshed by a separate notebook |
| `example/` | Reference example for new contributors (`customer.yaml`) — keep generic, do not delete |

## Adding a new table

1. Create a YAML file under the appropriate subdirectory.
2. Define `sources`, `columns`, `policies` (merge strategy, batch).
3. Assign to a batch in `datalake/inputs/config/silver_batches.yaml`.
4. The silver orchestrator will pick it up at the next run.

See `example/customer.yaml` for the minimal working shape, and `docs/DELTA_GEN_INTEGRATION.md` for the full schema.
