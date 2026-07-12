#!/bin/bash

# Publish all Fabric Environments in a workspace via the Fabric REST API.
# This script always attempts to publish - the API is idempotent and returns
# 200 immediately if there's nothing to publish.
#
# Usage:
#   fabric_publish_environments_by_workspace_name "workspace-name"
#
# Requires: az, curl, jq

fabric_publish_environments_by_workspace_name() {
  set -euo pipefail

  local WORKSPACE_NAME="${1:?workspace name required}"
  local API_BASE="https://api.fabric.microsoft.com/v1"
  local RETRIES=30 SLEEP_SECS=10

  # --- Token ---
  echo "Getting Fabric access token via Azure CLI..."
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv 2>/dev/null || true)"
  [[ -z "${FABRIC_TOKEN}" ]] && { echo "❌ az token failed. Run 'az login'."; return 1; }

  # --- WorkspaceId ---
  echo "Resolving workspace ID for '${WORKSPACE_NAME}'..."
  WORKSPACE_ID="$(curl -sS -X GET "${API_BASE}/workspaces" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" \
    | jq -r --arg name "${WORKSPACE_NAME}" '.value[] | select(.displayName == $name) | .id')"
  [[ -z "${WORKSPACE_ID}" || "${WORKSPACE_ID}" == "null" ]] && { echo "❌ Workspace not found."; return 1; }
  echo "✅ Workspace ID: ${WORKSPACE_ID}"

  # --- List all Environments in workspace ---
  echo "Listing environments in workspace '${WORKSPACE_NAME}'..."
  local ENVIRONMENTS_JSON
  ENVIRONMENTS_JSON="$(curl -sS -X GET "${API_BASE}/workspaces/${WORKSPACE_ID}/environments" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Accept: application/json")"

  local ENV_COUNT
  ENV_COUNT="$(echo "${ENVIRONMENTS_JSON}" | jq -r '.value | length')"

  if [[ "${ENV_COUNT}" == "0" || -z "${ENV_COUNT}" ]]; then
    echo "ℹ️  No environments found in workspace '${WORKSPACE_NAME}'. Skipping."
    return 0
  fi

  echo "Found ${ENV_COUNT} environment(s) in workspace."

  # --- Iterate and publish each environment ---
  local PUBLISHED_COUNT=0 FAILED_COUNT=0

  for row in $(echo "${ENVIRONMENTS_JSON}" | jq -c '.value[]'); do
    local ENV_ID ENV_NAME
    ENV_ID="$(echo "${row}" | jq -r '.id')"
    ENV_NAME="$(echo "${row}" | jq -r '.displayName')"

    echo ""
    echo "============================================"
    echo "Publishing environment: ${ENV_NAME}"
    echo "Environment ID: ${ENV_ID}"
    echo "============================================"

    # --- Call publish endpoint ---
    local pub_status pub_headers pub_location pub_retry
    pub_headers="$(mktemp)"

    pub_status="$(curl -s -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/environments/${ENV_ID}/staging/publish" \
      -H "Authorization: Bearer ${FABRIC_TOKEN}" \
      -H "Content-Type: application/json" \
      -H "Accept: application/json" \
      -d '{}' \
      -D "${pub_headers}" -o /tmp/publish_body.json -w "%{http_code}")"

    pub_location="$(grep -i '^location:' "${pub_headers}" | cut -d' ' -f2 | tr -d '\r\n ' || true)"
    pub_retry="$(grep -i '^retry-after:' "${pub_headers}" | cut -d' ' -f2 | tr -d '\r\n ' || true)"
    [[ -z "${pub_retry}" ]] && pub_retry="${SLEEP_SECS}"

    if [[ "${pub_status}" == "202" && -n "${pub_location}" ]]; then
      echo "▶️ Publish accepted (202). Polling ${pub_location} every ${pub_retry}s..."

      for i in $(seq 1 "${RETRIES}"); do
        local op_json op_status
        op_json="$(curl -s -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Accept: application/json" "${pub_location}")"
        op_status="$(jq -r '.status // empty' <<< "${op_json}")"

        if [[ "${op_status}" == "Succeeded" ]]; then
          echo "✅ Environment '${ENV_NAME}' published successfully."
          ((PUBLISHED_COUNT++)) || true
          break
        elif [[ "${op_status}" == "Failed" ]]; then
          echo "❌ Environment '${ENV_NAME}' publish failed."
          echo "${op_json}" | jq .
          ((FAILED_COUNT++)) || true
          break
        fi

        echo "… still running (status=${op_status:-Unknown}). Sleeping ${pub_retry}s"
        sleep "${pub_retry}"
      done

    elif [[ "${pub_status}" == "200" ]]; then
      echo "✅ Environment '${ENV_NAME}' published successfully (200)."
      ((PUBLISHED_COUNT++)) || true

    elif [[ "${pub_status}" == "400" ]]; then
      # Check if it's because there's nothing to publish
      local error_message
      error_message="$(jq -r '.message // .error.message // empty' /tmp/publish_body.json 2>/dev/null || true)"

      if [[ "${error_message}" == *"nothing to publish"* ]] || [[ "${error_message}" == *"No changes"* ]]; then
        echo "ℹ️  Environment '${ENV_NAME}' is already up to date (nothing to publish)."
        ((PUBLISHED_COUNT++)) || true
      else
        echo "⚠️  Environment '${ENV_NAME}' publish returned 400."
        echo "Message: ${error_message}"
        jq . /tmp/publish_body.json 2>/dev/null || cat /tmp/publish_body.json
        ((FAILED_COUNT++)) || true
      fi

    elif [[ "${pub_status}" == "409" ]]; then
      # Conflict - likely already publishing or recently published
      echo "ℹ️  Environment '${ENV_NAME}' has a publish conflict (409). May already be publishing."
      jq . /tmp/publish_body.json 2>/dev/null || cat /tmp/publish_body.json
      ((PUBLISHED_COUNT++)) || true

    else
      echo "⚠️  Unexpected status ${pub_status} for environment '${ENV_NAME}'."
      echo "Headers:"; cat "${pub_headers}"
      echo "Body:"; jq . /tmp/publish_body.json 2>/dev/null || cat /tmp/publish_body.json
      ((FAILED_COUNT++)) || true
    fi

    rm -f "${pub_headers}" /tmp/publish_body.json
  done

  echo ""
  echo "============================================"
  echo "Environment Publishing Summary"
  echo "============================================"
  echo "Workspace: ${WORKSPACE_NAME}"
  echo "Total environments: ${ENV_COUNT}"
  echo "Published/Up-to-date: ${PUBLISHED_COUNT}"
  echo "Failed: ${FAILED_COUNT}"
  echo "============================================"

  if [[ "${FAILED_COUNT}" -gt 0 ]]; then
    echo "⚠️  Some environments failed to publish. Review the logs above."
    # Don't fail the pipeline for this - environments can be manually published
    return 0
  fi

  echo "✅ All environments published successfully."
  return 0
}
