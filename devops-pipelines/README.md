# DevOps Pipelines

Azure DevOps CI/CD pipelines for deploying and managing Microsoft Fabric workspaces.

## Pipelines at a Glance

| Pipeline            | File                                   | Trigger                                    | Purpose                                          |
| ------------------- | -------------------------------------- | ------------------------------------------ | ------------------------------------------------ |
| Main Deployment     | `fabric-workspaces-deployment.yml`     | Push to `main`, `test`, `prod`             | Deploy all Fabric workspaces for an environment  |
| Developer Workspace | `developer-workspace-deployment.yml`   | Manual                                     | Create personal temp workspaces for feature dev  |
| Developer Cleanup   | `developer-workspace-cleanup.yml`      | Manual                                     | Delete personal temp workspaces                  |
| Auto Cleanup        | `automated-temp-workspace-cleanup.yml` | 1st of month, 2 AM UTC                     | Remove stale temp workspaces (30+ days inactive) |
| PNG from DrawIO     | `create-png-from-drawio.yml`           | PR touching `design/**`                    | Auto-export DrawIO diagrams to PNG               |

---

## Main Deployment Pipeline

**File**: [`fabric-workspaces-deployment.yml`](fabric-workspaces-deployment.yml)

Triggered on pushes to `main`, `test`, or `prod` branches (excluding docs/design-only changes). Creates or updates all workspaces for the target environment.

**Workspaces created per environment:**

| Workspace         | Purpose                                 |
| ----------------- | --------------------------------------- |
| `{env}-bronze`    | Raw ingestion lakehouse                 |
| `{env}-silver`    | Cleansed/validated lakehouse            |
| `{env}-gold`      | Curated dimensional model lakehouse     |
| `{env}-log`       | Observability / pipeline logging        |
| `{env}-reporting` | Semantic models (DirectLake)            |
| `{env}-app-*`     | Power BI app workspaces (auto-discovered from `platform/apps/`) |

**Environments:**

| Environment | Branch | Workspace prefix |
| ----------- | ------ | ---------------- |
| Development | `main` | `main-`          |
| Test        | `test` | `test-`          |
| Production  | `prod` | `prod-`          |

**Pipeline stages** (in order):

1. **Derive Context** — determine `ENV_PREFIX` from branch name
2. **Create Workspaces** — create data layer workspaces + app workspaces (auto-discovered from `platform/apps/`)
3. **Assign Capacity** — attach to nonprod or prod capacity
4. **Add Users** — apply dev and admin group permissions
5. **Git Connect** — connect workspaces to Azure DevOps repo (app workspaces use directory from folder structure)
6. **Git Sync** — pull content from branch into workspaces
7. **Publish Environments** — publish Spark environments (bronze/silver/gold)
8. **Copy Files** — upload config files to bronze/silver/gold lakehouses
9. **Apply Shortcuts** — create OneLake shortcuts on bronze (`shortcuts-bronze.yaml`)
10. **Repoint Lakehouse Shortcuts** — repoint silver/gold/log shortcuts to correct source lakehouses
11. **Connect Notebooks** — link notebooks to lakehouses (bronze/silver/gold/log)
12. **Connect Semantic Models** — link DirectLake models to SQL endpoints (gold/log/reporting)
13. **Rebind Reports** — bind reports in app workspaces to semantic models in the reporting workspace
14. **Git Update** — pull latest from remote before committing (prevents conflicts)
15. **Git Commit** — commit workspace metadata back to branch

Stages 7–13 run in two parallel batches after Git Sync. Stages 14–15 wait for all parallel stages.

---

## Developer Workspace Pipelines

Personal isolated workspaces for feature development. Workspaces follow the `temp-{layer}-{suffix}` naming pattern.

See [README_DEV_WORKSPACES.md](README_DEV_WORKSPACES.md) for full details.

### Create

Run `developer-workspace-deployment` with:

| Parameter               | Description                                                                                           | Default     |
| ----------------------- | ----------------------------------------------------------------------------------------------------- | ----------- |
| `devSuffix`             | Your name or feature identifier (letters, numbers, hyphens only)                                      | —           |
| `createBronze`          | Create bronze workspace                                                                               | true        |
| `createSilver`          | Create silver workspace                                                                               | true        |
| `createGold`            | Create gold workspace                                                                                 | true        |
| `createLog`             | Create log/observability workspace                                                                    | false       |
| `createReporting`       | Create reporting workspace (semantic models)                                                          | true        |
| `createApps`            | Create Power BI app workspaces                                                                        | false       |
| `appsList`              | Comma-separated app names to deploy (e.g. `dq-checks,manual-overrides`)                               | —           |
| `appsReportingSource`   | Bind app reports to: `developer` (temp reporting) or `main` (main-reporting)                          | developer   |
| `shortcutSource`        | Where silver/gold shortcuts point: `developer` (your temp workspaces) or `main` (`main-*` workspaces) | developer   |
| `gitBranch`             | Branch to sync (leave empty for current)                                                              | `<current>` |
| `skipWorkspaceCreation` | Reuse existing workspaces without recreating                                                          | false       |

### Delete

Run `developer-workspace-cleanup` with the same `devSuffix`. Select layers to delete and check the confirmation box. Supports bronze, silver, gold, log.

### Auto Cleanup

`automated-temp-workspace-cleanup` runs monthly and deletes `temp-*` workspaces inactive for 30+ days. Runs in **dry run mode by default** — an admin must set `dryRun: false` to enable actual deletion.

---

## Templates

Reusable pipeline templates in [`templates/`](templates/). Each wraps a Bash script from [`templates/scripts/`](templates/scripts/).

| Template                       | Script                       | Purpose                                              |
| ------------------------------ | ---------------------------- | ---------------------------------------------------- |
| `create-workspace.yaml`        | `createws.sh`                | Create Fabric workspace                              |
| `assign-capacity.yaml`         | `assigncap.sh`               | Assign workspace to capacity                         |
| `add-users.yaml`               | `adduser.sh`                 | Add users/groups to workspace                        |
| `git-connect.yaml`             | `gitconnect.sh`              | Connect workspace to Git repository                  |
| `git-sync.yaml`                | `gitsync.sh`                 | Sync workspace from Git                              |
| `git-commit.yaml`              | —                            | Commit workspace metadata to Git                     |
| `apply-shortcuts.yaml`         | `apply-shortcuts.sh`         | Create OneLake shortcuts from YAML                   |
| `update-shortcuts.yaml`        | `update-shortcuts.sh`        | Repoint existing OneLake shortcuts                   |
| `lakehouse-connect-noteb.yaml` | `connect-notebooks.sh`       | Link notebooks to lakehouse + environment            |
| `connect-semantic-models.yaml` | `connect-semantic-models.sh` | Update DirectLake model connections                  |
| `copyfiles-lakehouse.yaml`     | `copyfilestoLH.sh`           | Upload config files to lakehouse                     |
| `publish-environments.yaml`    | —                            | Publish Spark environments                           |
| `rebind-reports.yaml`          | `rebind-reports.sh`          | Rebind reports to semantic models (cross-workspace)  |
| `delete-workspaces.yaml`       | `delete-workspaces.sh`       | Delete workspaces (shared by both cleanup pipelines) |
| _(inline)_                     | `deploy-apps.sh`             | Auto-discover and deploy app workspaces from `platform/apps/` |

---

## Variable Groups

All pipelines use these Azure DevOps variable groups:

| Group                 | Contents                                    |
| --------------------- | ------------------------------------------- |
| `vg-platform-global`  | Global settings (repo ID, organisation URL) |
| `vg-platform-nonprod` | Nonprod capacity ID, dev/admin group IDs    |
| `vg-platform-prod`    | Prod capacity ID, user group IDs            |

Service connection: `svc-azure-terraform-001`

---

## Architecture

See [design/architecture/](../design/architecture/) for:

- `DeploymentsPattern.png` — how Git branches map to Fabric environments
- `DevelopmentPattern.png` — isolated temp workspace per feature pattern
- `DevelopmentProcess.png` — end-to-end developer workflow
