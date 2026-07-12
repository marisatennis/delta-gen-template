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
TOKEN="$(az account get-access-token --scope "https://api.fabric.microsoft.com/.default" --query accessToken -o tsv)"

# Function to create workspace
create_workspace() {
  local WS_NAME="$1"
  local DESCRIPTION="${2:-Created locally via script}"

  echo "Creating Fabric Workspace: ${WS_NAME}..."

  local PAYLOAD
  PAYLOAD="$(jq -n --arg dn "$WS_NAME" --arg desc "$DESCRIPTION" '{displayName: $dn, description: $desc}')"

  local RESPONSE
  RESPONSE="$(curl -sS -X POST "https://api.fabric.microsoft.com/v1/workspaces" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -d "${PAYLOAD}" \
    -w "\n%{http_code}")"

  local HTTP_STATUS
  HTTP_STATUS="$(tail -n1 <<< "${RESPONSE}")"
  local BODY
  BODY="$(sed '$d' <<< "${RESPONSE}")"

  echo "HTTP ${HTTP_STATUS}"
  printf "Response: %s\n" "${BODY}"

  if [[ "${HTTP_STATUS}" -lt 200 || "${HTTP_STATUS}" -ge 300 ]]; then
    echo "ERROR: Workspace creation failed (HTTP ${HTTP_STATUS})." >&2
    # return 1
  fi
}
