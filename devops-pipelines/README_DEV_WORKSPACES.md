# Developer Workspace Guide

How to create and manage temporary Fabric workspaces for feature development.

---

## Overview

Each developer gets their own isolated Fabric workspaces for a feature branch. Workspaces are:

- **Temporary** — prefixed `temp-` and auto-cleaned after 30 days of inactivity
- **Isolated** — no interference between developers or features
- **Fully configured** — capacity, Git, lakehouses, notebooks, and semantic models all wired up on creation
- **Layer-selective** — create only the layers you need

---

## Workspace Types

| Workspace | Name pattern              | Default | Purpose                                 |
| --------- | ------------------------- | ------- | --------------------------------------- |
| Bronze    | `temp-bronze-{suffix}`    | on      | Raw ingestion lakehouse                 |
| Silver    | `temp-silver-{suffix}`    | on      | Cleansed/validated lakehouse            |
| Gold      | `temp-gold-{suffix}`      | on      | Curated dimensional model lakehouse     |
| Log       | `temp-log-{suffix}`       | **off** | Observability / pipeline logging        |
| Reporting | `temp-reporting-{suffix}` | on      | Semantic models (DirectLake)            |
| Apps      | `temp-app-{name}-{suffix}` | **off** | Power BI app workspaces (from `platform/apps/`) |

**Naming rules:** suffix must contain only letters, numbers, and hyphens. Use your name or feature name — keep it short.

---

## Creating Workspaces

### 1. Navigate to the pipeline

Azure DevOps → Pipelines → `developer-workspace-deployment` → **Run pipeline**

### 2. Set parameters

| Parameter               | Description                                                                                             | Default      |
| ----------------------- | ------------------------------------------------------------------------------------------------------- | ------------ |
| `devSuffix`             | Your name or feature identifier                                                                         | _(required)_ |
| `createBronze`          | Create bronze workspace                                                                                 | true         |
| `createSilver`          | Create silver workspace                                                                                 | true         |
| `createGold`            | Create gold workspace                                                                                   | true         |
| `createLog`             | Create log/observability workspace                                                                      | false        |
| `createReporting`       | Create reporting workspace                                                                              | true         |
| `createApps`            | Create Power BI app workspaces                                                                          | false        |
| `appsList`              | Comma-separated app names (e.g. `dq-checks,manual-overrides`). Required if `createApps` is true         | _(empty)_    |
| `appsReportingSource`   | Bind app reports to: `developer` (temp reporting) or `main` (main-reporting)                            | developer    |
| `shortcutSource`        | Where silver/gold/log shortcuts point: `developer` = your temp workspaces, `main` = `main-*` workspaces | developer    |
| `gitBranch`             | Branch to sync. Leave empty for the pipeline's current branch                                           | `<current>`  |
| `skipWorkspaceCreation` | Set `true` to reuse existing workspaces (skip create/capacity/users stages)                             | false        |

**`shortcutSource`:** use `developer` when you've created all three layers and want a fully isolated stack. Use `main` when you've only created gold/reporting and want to read from the `main-silver` data instead of a local silver workspace.

### 3. What the pipeline does

1. Validate inputs and derive Git branch
2. Check workspace existence (validates against `skipWorkspaceCreation` flag)
3. Create workspaces _(skipped if `skipWorkspaceCreation=true`)_
4. Assign nonprod capacity _(skipped if `skipWorkspaceCreation=true`)_
5. Add dev and admin group access _(skipped if `skipWorkspaceCreation=true`)_
6. Git connect workspaces to repo
7. Git sync from branch
8. Publish Spark environments (bronze/silver/gold) — **parallel**
9. Copy config files to lakehouses — **parallel**
10. Apply OneLake shortcuts on bronze — **parallel**
11. Connect notebooks to lakehouses — **parallel**
12. Connect semantic models to SQL endpoints (sequential after step 11)
13. Repoint silver/gold/log shortcuts based on `shortcutSource`
14. Rebind app reports to semantic models _(if `createApps` is true)_
15. Git update (pull latest before committing)
16. Git commit workspace metadata to branch
17. Summary

**Duration:** ~10–15 minutes for a full setup.

---

## Deleting Workspaces

### Manual cleanup

Azure DevOps → Pipelines → `developer-workspace-cleanup` → **Run pipeline**

| Parameter         | Description                                 |
| ----------------- | ------------------------------------------- |
| `devSuffix`       | Same suffix used when creating              |
| `deleteBronze`    | Delete bronze workspace                     |
| `deleteSilver`    | Delete silver workspace                     |
| `deleteGold`      | Delete gold workspace                       |
| `deleteLog`       | Delete log workspace                        |
| `confirmDeletion` | **Must check this** — deletion is permanent |

> The reporting workspace is not in the cleanup pipeline. Delete `temp-reporting-{suffix}` manually in the Fabric portal if needed.

**Stages:** Validate → List workspaces → Delete → Summary

**Duration:** ~2–3 minutes.

### Automated monthly cleanup

`automated-temp-workspace-cleanup` runs on the 1st of every month at 2 AM UTC.

**Parameters:**

| Parameter        | Default | Description                                                          |
| ---------------- | ------- | -------------------------------------------------------------------- |
| `dryRun`         | `true`  | Preview mode — lists what would be deleted without actually deleting |
| `inactivityDays` | `30`    | Threshold for inactivity                                             |

A workspace is considered **inactive** if no items have been modified within the threshold period, or the workspace is empty.

**Enabling actual deletion:** an admin must edit the pipeline and change `dryRun` default to `false`.

**Admins can also trigger manually** — run the pipeline and set `dryRun: false` and/or adjust `inactivityDays`.

**Keeping a workspace active:** edit any item in the workspace (add a comment to a notebook, etc.) to reset the inactivity timer.

---

## Best Practices

### Developers

- Delete workspaces manually when your feature is merged — don't rely on automated cleanup
- Use `shortcutSource: main` when you only need gold/reporting and don't want to manage a full bronze/silver stack
- Use `skipWorkspaceCreation: true` to re-run the configuration stages on an existing workspace without recreating it

### Admins

- Review the dry run log on the 1st of each month before enabling actual deletion
- Adjust `inactivityDays` if the team needs longer-running workspaces

---

## Troubleshooting

| Symptom                                                  | Cause                                        | Fix                                                                       |
| -------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| Suffix validation error                                  | Special characters in suffix                 | Use only letters, numbers, hyphens                                        |
| `skipWorkspaceCreation=true` but workspace doesn't exist | Mismatch                                     | Set `skipWorkspaceCreation=false` or create the workspace first           |
| Workspace exists but `skipWorkspaceCreation=false`       | Would fail creation                          | Set `skipWorkspaceCreation=true` to reuse it                              |
| Git sync fails                                           | Branch doesn't exist or wrong case           | Check branch name exactly in Azure DevOps                                 |
| Workspace not visible in Fabric                          | Not in dev group or propagation delay        | Verify `dev_group` membership in Entra ID, wait a few minutes             |
| Notebooks not connected                                  | Copy Files or Connect Notebooks stage failed | Check those stage logs; re-run pipeline with `skipWorkspaceCreation=true` |

---

## Architecture Notes

### Shortcut repointing

After Git sync, both pipelines repoint OneLake shortcuts so each workspace reads from the correct lakehouse:

- **`developer` mode:** silver → `temp-bronze-{suffix}`, gold → `temp-silver-{suffix}`, log → `temp-silver-{suffix}` + `temp-gold-{suffix}`
- **`main` mode:** silver → `main-bronze`, gold → `main-silver`, log → `main-silver` + `main-gold`

### Semantic model connections

DirectLake models in the reporting workspace are connected to the gold lakehouse via the Fabric `updateDefinition` API. If gold is not selected, reporting points to `main-gold` instead.

### App workspaces

App workspaces are auto-discovered from `platform/apps/`. The folder convention is:

```
platform/apps/{semanticModel}/{appName}/
```

- `{semanticModel}` = parent folder = name of the semantic model to bind reports to
- `{appName}` = leaf folder = app workspace suffix (`temp-app-{appName}-{suffix}`)

Adding a new app = creating a new folder under `platform/apps/`. No pipeline changes needed.

Use `appsReportingSource: main` to bind reports to `main-reporting` semantic models without deploying a temp reporting workspace.

### Git conflict prevention

Both deployment pipelines run a **Git Update** stage (pull from remote) immediately before the final **Git Commit** stage. This prevents `CommitFailedDueToIncomingChanges` errors when someone pushed to the branch during the pipeline run.

### Shared scripts

Both cleanup pipelines source `templates/scripts/delete-workspaces.sh` which provides:

| Function                       | Description                           |
| ------------------------------ | ------------------------------------- |
| `get_access_token`             | Get Fabric API access token           |
| `find_workspace_id`            | Find workspace ID by name             |
| `delete_workspace_by_id`       | Delete a single workspace             |
| `delete_workspaces_by_names`   | Delete a list of workspaces by name   |
| `discover_inactive_workspaces` | Find inactive `temp-*` workspaces     |
| `list_matching_workspaces`     | Preview matching workspaces (dry-run) |

### Variable groups

Developer pipelines use nonprod variable groups only:

- `vg-platform-global`, `vg-platform-nonprod`

Service connection: `svc-azure-terraform-001`
