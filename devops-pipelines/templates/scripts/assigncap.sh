#!/usr/bin/env bash
set -euo pipefail

# Check prerequisites
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required. Install jq and retry." >&2
  exit 1
fi

if ! command -v az >/dev/null 2>&1; then
  echo "ERROR: Azure CLI is required. Install az and run 'az login'." >&2
  exit 1
fi

# Acquire token once
TOKEN="$(az account get-access-token --scope 'https://api.fabric.microsoft.com/.default' --query accessToken -o tsv)"

# Function to assign workspace to capacity
assign_workspace_to_capacity() {
  local WS_NAME="$1"
  local CAPACITY_NAME="$2"

  echo "Assigning workspace '${WS_NAME}' to capacity '${CAPACITY_NAME}'..."

  # Resolve capacity GUID
  local CAPACITY_ID
  CAPACITY_ID="$(curl -sS 'https://api.fabric.microsoft.com/v1/capacities' \
    -H "Authorization: Bearer ${TOKEN}" -H 'Accept: application/json' \
  | jq -r --arg name "$CAPACITY_NAME" '.value | map(select(.displayName == $name)) | .[0].id // empty')"

  if [[ -z "${CAPACITY_ID}" ]]; then
    echo "ERROR: Capacity '${CAPACITY_NAME}' not found." >&2
    return 1
  fi
  echo "Capacity GUID: ${CAPACITY_ID}"

  # Resolve workspace ID
  local WS_ID
  WS_ID="$(curl -sS 'https://api.fabric.microsoft.com/v1/workspaces' \
    -H "Authorization: Bearer ${TOKEN}" -H 'Accept: application/json' \
  | jq -r --arg name "$WS_NAME" '.value | map(select(.displayName == $name)) | .[0].id // empty')"

  if [[ -z "${WS_ID}" ]]; then
    echo "ERROR: Workspace '${WS_NAME}' not found." >&2
    return 1
  fi
  echo "Workspace ID: ${WS_ID}"

  # Build JSON payload
  local PAYLOAD
  PAYLOAD="$(jq -n --arg cap "${CAPACITY_ID}" '{capacityId: $cap}')"

  # POST request to assign capacity
  local RESPONSE
  RESPONSE="$(
    curl -sS -X POST "https://api.fabric.microsoft.com/v1/workspaces/${WS_ID}/assignToCapacity" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -H "Accept: application/json" \
      --data-binary "${PAYLOAD}" \
      -w "\n%{http_code}"
  )"

  local HTTP_STATUS
  HTTP_STATUS="$(tail -n1 <<< "${RESPONSE}")"
  local BODY
  BODY="$(sed '$d' <<< "${RESPONSE}")"

  echo "HTTP ${HTTP_STATUS}"
  printf "Response: %s\n" "${BODY}"

  if [[ "${HTTP_STATUS}" -ne 200 && "${HTTP_STATUS}" -ne 202 ]]; then
    # Check if workspace is already assigned to this capacity
    local error_code
    error_code="$(jq -r '.errorCode // empty' <<< "${BODY}")"

    if [[ "${error_code}" == "WorkspaceAlreadyAssignedToCapacity" ]]; then
      echo "INFO: Workspace '${WS_NAME}' is already assigned to a capacity. Skipping." >&2
      return 0
    fi

    echo "ERROR: assignToCapacity failed (HTTP ${HTTP_STATUS})." >&2
    return 1
  fi

  echo "Workspace '${WS_NAME}' successfully assigned to capacity '${CAPACITY_NAME}'."
}

