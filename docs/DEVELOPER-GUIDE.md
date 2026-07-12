# Developer Guide

Everything you need to contribute code to this project. Follow the workflow below, adhere to the code standards, and your PRs will sail through review.

---

## Development Workflow

### 1. Clone the Repository

```bash
git clone https://dev.azure.com/{your-ado-org}/{your-project}/_git/{repo-name}
cd {repo-name}
```

### 2. Create a Feature Branch

Branch from `main` using the naming convention:

```bash
git checkout main
git pull origin main
git checkout -b feature/{work-item-id}-short-description
# Example: feature/1234-add-customer-dimension
```

Branch naming conventions:

| Prefix | Use Case |
|---|---|
| `feature/` | New tables, notebooks, pipeline changes |
| `fix/` | Bug fixes, data corrections |
| `refactor/` | Code restructuring without behaviour changes |
| `docs/` | Documentation-only changes |

### 3. Create a Developer Workspace

Run the **Developer Workspace** pipeline in Azure DevOps to spin up personal Fabric workspaces for testing:

1. Navigate to **Pipelines** in your ADO project
2. Run `developer-workspace-deployment`
3. Enter your branch name when prompted
4. Wait for workspaces to be created: `{your-name}-bronze`, `{your-name}-silver`, `{your-name}-gold`, `{your-name}-log`

See [`devops-pipelines/README_DEV_WORKSPACES.md`](../devops-pipelines/README_DEV_WORKSPACES.md) for full details.

### 4. Develop and Test

- Make changes locally and push to your feature branch
- The dev workspace pipeline deploys your branch automatically
- Test in your personal Fabric workspaces
- Run notebooks manually to validate transformations

### 5. Create a Pull Request

```bash
git push origin feature/{work-item-id}-short-description
```

Then open a PR in Azure DevOps targeting `main`. See [Pull Request Guidelines](#pull-request-guidelines) below.

### 6. Cleanup

After your PR is merged, delete your developer workspaces:

1. Run the `developer-workspace-cleanup` pipeline, or
2. Wait for the automated monthly cleanup (stale workspaces older than 30 days are removed on the 1st of each month)

---

## Code Standards

### General

- **Python 3.10+** -- All notebooks and libraries target Python 3.10 or later.
- **No hardcoded secrets** -- Use Azure Key Vault references: `kv-{project}-{env}`.
- **No hardcoded environment values** -- Use parameters or environment variables for workspace names, lakehouse names, and connection strings.
- **Consistent naming** -- Use `snake_case` for Python variables, functions, and file names. Use `PascalCase` for class names.
- **Type hints** -- Add type hints to all function signatures in library code.

### Bronze Layer

Bronze notebooks ingest raw data without transformation. Standards:

| Rule | Example |
|---|---|
| Preserve source column names exactly | `Account_Name` stays `Account_Name` |
| Append-only by default | Use `mode="append"` unless full reload is required |
| Track ingestion metadata | Add `_ingested_at`, `_source_file` columns |
| One notebook per source type | `file-ingestion.Notebook`, `salesforce-data-ingestion.Notebook` |

### Silver Layer

Silver tables are YAML-driven via Delta-Gen. Standards:

| Rule | Detail |
|---|---|
| One YAML file per table | `datalake/inputs/silver/{source}/{table_name}.yaml` |
| Declare all columns explicitly | Use `columns:` section -- no `SELECT *` |
| Apply cleaning plugins | Use `trim`, `lower`, `null_if_empty` as appropriate |
| Use standard sentinel keys | `NO_PRODUCT`, `NO_CUSTOMER`, `NO_DATE` for missing dimension lookups |
| Set batch assignment | Every config must have a `batch:` tag (daily, weekly, monthly) |

Example YAML structure:

```yaml
name: customer
source:
  lakehouse: bronze
  table: raw_customers
columns:
  - name: customer_id
    source: CustomerID
    type: int
  - name: customer_name
    source: Customer_Name
    type: string
    cleaning:
      - trim
      - null_if_empty
batch: daily
mode: merge
merge_keys:
  - customer_id
```

### Gold Layer

Gold tables build the dimensional model. Standards:

| Rule | Detail |
|---|---|
| Prefix dimensions with `d_` | `d_customer`, `d_product`, `d_date` |
| Prefix facts with `f_` | `f_transactions`, `f_orders` |
| Surrogate keys are `{table}_key` | `d_customer.customer_key` |
| Business keys are `{entity}_id` | `d_customer.customer_id` |
| All facts reference dimension keys | Use `lookup:` plugin in YAML for FK resolution |
| Dimensions run before facts | Use `order:` field -- dimensions at order 1, facts at order 2+ |

### Semantic Model

The Power BI semantic model in `platform/main.SemanticModel/` follows these rules:

- All measures use DAX best practices (variables, `CALCULATE`, `FILTER` patterns).
- Relationship cardinality is explicitly defined (one-to-many, many-to-one).
- Row-level security (RLS) roles are defined where required.
- Formatting strings follow locale-appropriate patterns.

---

## Pull Request Guidelines

### Before Submitting

- [ ] Tested in your developer workspace
- [ ] All new YAML configs are valid (no syntax errors)
- [ ] New tables appear in the lakehouse with expected data
- [ ] No hardcoded secrets or environment-specific values
- [ ] Updated relevant documentation (YAML comments, READMEs)

### PR Title Format

```
{type}: {short description} (#{work-item-id})
```

Examples:
- `feature: add d_product dimension (#1234)`
- `fix: correct null handling in silver customer (#1235)`
- `refactor: consolidate ingestion config loading (#1236)`

### PR Description Template

```markdown
## What

Brief description of the change.

## Why

Business or technical motivation.

## How

Implementation approach -- key decisions, trade-offs.

## Testing

How you validated the change (workspace name, sample queries, row counts).
```

### Review Checklist

Reviewers should verify:

- [ ] YAML configs follow naming conventions and declare all columns
- [ ] No regressions to existing tables (schema changes are additive)
- [ ] Batch assignments are correct (daily vs. weekly vs. monthly)
- [ ] Merge keys are appropriate for the table's grain
- [ ] Documentation is updated if behaviour changes

---

## Documentation Standards

### Where Documentation Lives

| Type | Location |
|---|---|
| Architecture and design | `design/` |
| Data mappings | `design/mappings/` |
| Platform guides | `docs/` |
| Layer-specific READMEs | `platform/{layer}/README.md` |
| DevOps guides | `devops-pipelines/README.md` |
| Inline config docs | YAML comments in `datalake/inputs/` |

### When to Update Docs

- **Adding a new table**: Add a YAML comment header describing the table's purpose and source.
- **Adding a new data source**: Update the Bronze README and add a mapping spec to `design/mappings/`.
- **Changing orchestration**: Update the relevant layer README and `OPERATIONS.md`.
- **Changing CI/CD**: Update `devops-pipelines/README.md`.

### Style

- Use Markdown for all documentation.
- Use tables for structured reference content.
- Use code blocks with language hints for examples.
- Keep sentences concise -- prefer bullet points over paragraphs.
- Use absolute paths from the repo root when referencing files.
