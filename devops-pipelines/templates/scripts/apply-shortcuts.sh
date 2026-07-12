#!/bin/bash

# Apply OneLake shortcuts to a Fabric Lakehouse from a YAML config file.
# Pure bash implementation using Fabric REST API.
#
# Usage:
#   fabric_apply_shortcuts_by_workspace_name "temp-bronze-maria" "bronze" "datalake/inputs/config/shortcuts-bronze.yaml"
#
# Requires: az, curl, jq, yq (or python for yaml parsing)

fabric_apply_shortcuts_by_workspace_name() {
  set -euo pipefail

  # Verify jq is available
  if ! command -v jq &> /dev/null; then
    echo "❌ Error: jq is not installed. Please install jq to use this script."
    echo "   On Ubuntu/Debian: sudo apt-get install -y jq"
    echo "   On macOS: brew install jq"
    return 1
  fi

  local WORKSPACE_NAME="${1:?workspace name required}"
  local LAKEHOUSE_NAME="${2:?lakehouse name required}"
  local SHORTCUTS_FILE="${3:?shortcuts yaml file required}"
  local API_BASE="https://api.fabric.microsoft.com/v1"

  echo "============================================"
  echo "Applying OneLake Shortcuts"
  echo "============================================"
  echo "Workspace: ${WORKSPACE_NAME}"
  echo "Lakehouse: ${LAKEHOUSE_NAME}"
  echo "Config file: ${SHORTCUTS_FILE}"
  echo "============================================"

  # Check if shortcuts file exists
  if [[ ! -f "${SHORTCUTS_FILE}" ]]; then
    echo "ℹ️  Shortcuts file '${SHORTCUTS_FILE}' not found. Skipping."
    return 0
  fi

  # --- Token ---
  echo "Getting Fabric access token via Azure CLI..."
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv 2>/dev/null || true)"
  [[ -z "${FABRIC_TOKEN}" ]] && { echo "❌ az token failed. Run 'az login'."; return 1; }

  # --- WorkspaceId ---
  echo "Resolving workspace ID for '${WORKSPACE_NAME}'..."
  WORKSPACE_ID="$(curl -sS -X GET "${API_BASE}/workspaces" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" \
    | jq -r --arg name "${WORKSPACE_NAME}" '.value[] | select(.displayName == $name) | .id')"
  [[ -z "${WORKSPACE_ID}" || "${WORKSPACE_ID}" == "null" ]] && { echo "❌ Workspace '${WORKSPACE_NAME}' not found."; return 1; }
  echo "✅ Workspace ID: ${WORKSPACE_ID}"

  # --- LakehouseId ---
  echo "Resolving lakehouse ID for '${LAKEHOUSE_NAME}'..."
  LAKEHOUSE_ID="$(curl -sS -X GET "${API_BASE}/workspaces/${WORKSPACE_ID}/items" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" \
    | jq -r --arg name "${LAKEHOUSE_NAME}" '.value[] | select(.type == "Lakehouse" and (.displayName | ascii_downcase) == ($name | ascii_downcase)) | .id')"
  [[ -z "${LAKEHOUSE_ID}" || "${LAKEHOUSE_ID}" == "null" ]] && { echo "❌ Lakehouse '${LAKEHOUSE_NAME}' not found in workspace."; return 1; }
  echo "✅ Lakehouse ID: ${LAKEHOUSE_ID}"

  # --- Parse YAML and create shortcuts ---
  echo ""
  echo "Parsing shortcuts from YAML..."
  
  # Try yq first (faster), then python with yaml
  local SHORTCUTS_JSON
  if command -v yq &> /dev/null; then
    SHORTCUTS_JSON="$(yq -o=json '.' "${SHORTCUTS_FILE}" 2>/dev/null)"
  else
    # Use python to convert YAML to JSON
    SHORTCUTS_JSON="$(python3 -c "
import sys, json
try:
    import yaml
except ImportError:
    # Install pyyaml if not present
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', 'pyyaml'])
    import yaml
with open('${SHORTCUTS_FILE}', 'r') as f:
    data = yaml.safe_load(f)
print(json.dumps(data))
" 2>/dev/null)"
  fi

  if [[ -z "${SHORTCUTS_JSON}" || "${SHORTCUTS_JSON}" == "null" ]]; then
    echo "ℹ️  No shortcuts defined in '${SHORTCUTS_FILE}'. Skipping."
    return 0
  fi

  local SHORTCUT_COUNT
  SHORTCUT_COUNT="$(echo "${SHORTCUTS_JSON}" | jq 'length')"
  
  if [[ "${SHORTCUT_COUNT}" == "0" ]]; then
    echo "ℹ️  No shortcuts defined in '${SHORTCUTS_FILE}'. Skipping."
    return 0
  fi

  echo "Found ${SHORTCUT_COUNT} shortcut(s) to apply."
  echo ""

  # --- Apply each shortcut ---
  local CREATED_COUNT=0 EXISTED_COUNT=0 FAILED_COUNT=0

  for i in $(seq 0 $((SHORTCUT_COUNT - 1))); do
    local SHORTCUT
    SHORTCUT="$(echo "${SHORTCUTS_JSON}" | jq -c ".[$i]")"

    local NAME SHORTCUT_PATH TARGET
    NAME="$(echo "${SHORTCUT}" | jq -r '.name')"
    SHORTCUT_PATH="$(echo "${SHORTCUT}" | jq -r '.path')"
    TARGET="$(echo "${SHORTCUT}" | jq -c '.target')"

    echo "Creating shortcut: ${SHORTCUT_PATH}/${NAME}"

    # Build the API payload
    local PAYLOAD
    PAYLOAD="$(jq -n \
      --arg name "${NAME}" \
      --arg path "${SHORTCUT_PATH}" \
      --argjson target "${TARGET}" \
      '{name: $name, path: $path, target: $target}')"

    # Call the create shortcut API
    local RESPONSE STATUS_CODE
    local RESPONSE_FILE
    RESPONSE_FILE="$(mktemp)"
    
    STATUS_CODE="$(curl -s -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/items/${LAKEHOUSE_ID}/shortcuts" \
      -H "Authorization: Bearer ${FABRIC_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "${PAYLOAD}" \
      -o "${RESPONSE_FILE}" \
      -w "%{http_code}")"

    if [[ "${STATUS_CODE}" == "201" ]]; then
      echo "  ✅ Created: ${SHORTCUT_PATH}/${NAME}"
      ((CREATED_COUNT++)) || true
    elif [[ "${STATUS_CODE}" == "409" ]]; then
      echo "  ⚠️  Already exists: ${SHORTCUT_PATH}/${NAME}"
      ((EXISTED_COUNT++)) || true
    else
      echo "  ❌ Failed (HTTP ${STATUS_CODE}): ${SHORTCUT_PATH}/${NAME}"
      local ERROR_MSG
      ERROR_MSG="$(jq -r '.message // .error.message // "Unknown error"' "${RESPONSE_FILE}" 2>/dev/null || cat "${RESPONSE_FILE}")"
      echo "     Error: ${ERROR_MSG}"
      ((FAILED_COUNT++)) || true
    fi

    rm -f "${RESPONSE_FILE}"
  done

  echo ""
  echo "============================================"
  echo "Shortcuts Summary"
  echo "============================================"
  echo "Workspace: ${WORKSPACE_NAME}"
  echo "Lakehouse: ${LAKEHOUSE_NAME}"
  echo "Total shortcuts: ${SHORTCUT_COUNT}"
  echo "Created: ${CREATED_COUNT}"
  echo "Already existed: ${EXISTED_COUNT}"
  echo "Failed: ${FAILED_COUNT}"
  echo "============================================"

  if [[ "${FAILED_COUNT}" -gt 0 ]]; then
    echo "⚠️  Some shortcuts failed. Review the logs above."
    return 0  # Don't fail pipeline for shortcut issues
  fi

  echo "✅ All shortcuts applied successfully."
  return 0
}

# Allow direct invocation: ./apply-shortcuts.sh "workspace" "lakehouse" "file.yaml"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] || [[ -z "${BASH_SOURCE[0]:-}" ]]; then
  fabric_apply_shortcuts_by_workspace_name "$@"
fi