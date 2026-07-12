#!/usr/bin/env bash
set -euo pipefail

# Deploy app workspaces by auto-discovering from the platform/apps/ folder structure.
#
# Folder convention:
#   platform/apps/{semanticModel}/{appName}/
#     - {semanticModel} = parent folder = name of the semantic model to bind to
#     - {appName}        = leaf folder   = app workspace name component
#     - semanticModelWorkspace is always "reporting"
#
# Workspace naming:
#   Main pipeline:  {env}-app-{appName}              e.g. main-app-distribution
#   Dev pipeline:   temp-app-{appName}-{devSuffix}    e.g. temp-app-distribution-mib-test
#
# To add a new app: create a new folder. No config files or pipeline changes needed.
#
# All functions take these first 3 args:
#   <env-prefix>  Environment prefix ("main", "test", "prod", or "temp")
#   <apps-list>   "all" or comma-separated app names (e.g. "dq-checks,manual-overrides")
#   <apps-dir>    Root apps folder (e.g. "platform/apps")
#
# Optional 4th arg for dev workspaces:
#   <dev-suffix>  Developer suffix (e.g. "mib-test"). If empty, no suffix appended.

# ---- Dependencies ----
for c in az curl jq; do
  command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; exit 1; }
done

# ---- Helpers ----

# Discover apps from folder structure and return as JSON array
_get_apps() {
  local apps_dir="$1" filter="$2"

  local apps_json="[]"
  for model_dir in "$apps_dir"/*/; do
    [[ -d "$model_dir" ]] || continue
    local semantic_model
    semantic_model=$(basename "$model_dir")

    for app_dir in "$model_dir"*/; do
      [[ -d "$app_dir" ]] || continue
      local app_name
      app_name=$(basename "$app_dir")
      local directory
      directory="${app_dir%/}"

      apps_json=$(echo "$apps_json" | jq --arg name "$app_name" --arg dir "$directory" --arg sm "$semantic_model" \
        '. + [{"name": $name, "directory": $dir, "semanticModel": $sm}]')
    done
  done

  if [[ "$filter" != "all" ]]; then
    local filter_json
    filter_json=$(echo "$filter" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | jq -R . | jq -s .)
    apps_json=$(echo "$apps_json" | jq --argjson names "$filter_json" '[.[] | select(.name as $n | $names | index($n))]')
  fi

  echo "$apps_json"
}

# Build workspace name:
#   _ws_name "main" "distribution" ""         → main-app-distribution
#   _ws_name "temp" "distribution" "mib-test" → temp-app-distribution-mib-test
_ws_name() {
  local prefix="$1" app_name="$2" suffix="${3:-}"
  if [[ -n "$suffix" ]]; then
    echo "${prefix}-app-${app_name}-${suffix}"
  else
    echo "${prefix}-app-${app_name}"
  fi
}

# ---- Stage Functions ----

apps_create_workspaces() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" DEV_SUFFIX="${4:-}"

  echo "[INFO] Creating app workspaces..."
  source devops-pipelines/templates/scripts/createws.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  echo "[INFO] Apps discovered: $count"
  echo "$apps_json" | jq -r '.[] | "  - \(.name) (model: \(.semanticModel), dir: \(.directory))"'

  for i in $(seq 0 $((count - 1))); do
    local name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Creating workspace: $ws_name"
    create_workspace "$ws_name" "App workspace for $name"
  done
}

apps_assign_capacity() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" CAPACITY_NAME="$4" DEV_SUFFIX="${5:-}"

  echo "[INFO] Assigning capacity to app workspaces..."
  source devops-pipelines/templates/scripts/assigncap.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  for i in $(seq 0 $((count - 1))); do
    local name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Assigning capacity to: $ws_name"
    assign_workspace_to_capacity "$ws_name" "$CAPACITY_NAME"
  done
}

apps_add_users() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" DEV_GROUP="$4" ADMIN_GROUP="$5" DEV_SUFFIX="${6:-}"

  echo "[INFO] Adding users to app workspaces..."
  source devops-pipelines/templates/scripts/adduser.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  for i in $(seq 0 $((count - 1))); do
    local name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Adding users to: $ws_name"
    assign_role_to_workspace "$ws_name" "$DEV_GROUP" "Admin"
    assign_role_to_workspace "$ws_name" "$ADMIN_GROUP" "Admin"
  done
}

apps_git_connect() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" BRANCH="$4"
  local ORG="$5" PROJECT="$6" REPO="$7"
  local CONN_NAME="$8" SP_TENANT="$9" SP_CLIENT="${10}" SP_SECRET="${11}" DEV_SUFFIX="${12:-}"

  echo "[INFO] Git connecting app workspaces..."
  source devops-pipelines/templates/scripts/gitconnect.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  local ADO_REPO_URL="https://dev.azure.com/$ORG/$PROJECT/_git/$REPO"

  for i in $(seq 0 $((count - 1))); do
    local name directory
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    directory=$(echo "$apps_json" | jq -r ".[$i].directory")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Git connecting $ws_name to $directory"
    fabric_git_connect_ado_id \
      --workspace-name "$ws_name" \
      --conn-name "$CONN_NAME" \
      --ado-sp-tenant "$SP_TENANT" \
      --ado-sp-client-id "$SP_CLIENT" \
      --ado-sp-secret "$SP_SECRET" \
      --org "$ORG" \
      --project "$PROJECT" \
      --repo "$REPO" \
      --branch "$BRANCH" \
      --directory "$directory" \
      --ado-repo-url "$ADO_REPO_URL"
  done
}

apps_git_sync() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" DEV_SUFFIX="${4:-}"

  echo "[INFO] Git syncing app workspaces..."
  source devops-pipelines/templates/scripts/gitsync.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  for i in $(seq 0 $((count - 1))); do
    local name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Git syncing: $ws_name"
    fabric_git_initialize_then_update_by_name "$ws_name"
  done
}

apps_rebind_reports() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" SM_WS_PREFIX="${4:-$1}" DEV_SUFFIX="${5:-}" SM_SUFFIX="${6:-}"

  # SM_WS_PREFIX + SM_SUFFIX control which reporting workspace to bind to.
  # Main pipeline: SM_WS_PREFIX="main", SM_SUFFIX="" → main-reporting
  # Dev pipeline:  SM_WS_PREFIX="temp", SM_SUFFIX="mib-test" → temp-reporting-mib-test
  #   or:          SM_WS_PREFIX="main", SM_SUFFIX="" → main-reporting (if appsReportingSource=main)

  echo "[INFO] Rebinding reports to semantic models..."
  source devops-pipelines/templates/scripts/rebind-reports.sh

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  # Build reporting workspace name
  local sm_ws_name
  if [[ -n "$SM_SUFFIX" ]]; then
    sm_ws_name="${SM_WS_PREFIX}-reporting-${SM_SUFFIX}"
  else
    sm_ws_name="${SM_WS_PREFIX}-reporting"
  fi
  echo "[INFO] Semantic model workspace: $sm_ws_name"

  for i in $(seq 0 $((count - 1))); do
    local name sm_name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    sm_name=$(echo "$apps_json" | jq -r ".[$i].semanticModel")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Rebinding reports in $ws_name to model '$sm_name' in $sm_ws_name"
    fabric_rebind_reports "$ws_name" "$sm_ws_name" "$sm_name"
  done
}

apps_git_commit() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" DEV_SUFFIX="${4:-}"

  echo "[INFO] Git committing app workspaces..."
  source devops-pipelines/templates/scripts/gitcommit.sh 2>/dev/null || true

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  for i in $(seq 0 $((count - 1))); do
    local name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    local ws_name
    ws_name=$(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX")
    echo "[INFO] Git committing: $ws_name"
    fabric_git_commit_by_name "$ws_name" || echo "[WARN] Git commit skipped for $ws_name"
  done
}

apps_print_summary() {
  local ENV_PREFIX="$1" APPS_LIST="$2" APPS_DIR="$3" DEV_SUFFIX="${4:-}"

  local apps_json
  apps_json=$(_get_apps "$APPS_DIR" "$APPS_LIST")
  local count
  count=$(echo "$apps_json" | jq 'length')

  echo ""
  echo "App Workspaces ($count):"
  for i in $(seq 0 $((count - 1))); do
    local name sm_name
    name=$(echo "$apps_json" | jq -r ".[$i].name")
    sm_name=$(echo "$apps_json" | jq -r ".[$i].semanticModel")
    echo "  ✓ $(_ws_name "$ENV_PREFIX" "$name" "$DEV_SUFFIX") → model: $sm_name"
  done
}
