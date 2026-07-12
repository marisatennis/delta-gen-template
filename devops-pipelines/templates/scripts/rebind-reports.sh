#!/usr/bin/env bash
set -euo pipefail

# Rebind all Power BI reports in a workspace to a semantic model in another workspace
# Usage:
#   ./rebind-reports.sh "<report-workspace>" "<semantic-model-workspace>" "<semantic-model-name>"
#
# This script finds all reports in the report workspace and rebinds them
# to the specified semantic model in the semantic model workspace.
# Designed for cross-workspace report-to-model binding when reports are
# deployed in separate app workspaces for Power BI app publishing.

fabric_rebind_reports() {
  # ---- Args ----
  if [[ $# -lt 3 ]]; then
    echo "[ERROR] Usage: fabric_rebind_reports <report-workspace> <semantic-model-workspace> <semantic-model-name>"
    return 1
  fi
  local REPORT_WS_ARG="$1" SM_WS_ARG="$2" SM_NAME="$3"

  # ---- Dependencies ----
  for c in az curl jq; do
    command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; return 1; }
  done

  # ---- Helpers ----
  _trim() { printf "%s" "${1:-}" | tr -d '[:space:]'; }
  _is_guid() { [[ "${1:-}" =~ ^[0-9a-fA-F-]{36}$ ]]; }

  echo "[INFO] Acquiring Fabric token..."
  local FABRIC_TOKEN
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)"

  # ---- Resolve Report Workspace ID ----
  local REPORT_WS_ID=""
  if _is_guid "$REPORT_WS_ARG"; then
    REPORT_WS_ID="$(_trim "$REPORT_WS_ARG")"
    echo "[INFO] Report workspace treated as GUID: $REPORT_WS_ID"
  else
    echo "[INFO] Resolving report workspace by name: $REPORT_WS_ARG"
    local url="https://api.fabric.microsoft.com/v1/workspaces" cont=""
    while : ; do
      local js; js="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "${url}${cont}")"
      REPORT_WS_ID="$(printf '%s' "$js" | jq -r --arg n "$REPORT_WS_ARG" '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
      REPORT_WS_ID="$(_trim "$REPORT_WS_ID")"
      [[ -n "$REPORT_WS_ID" ]] && break
      local token; token="$(printf '%s' "$js" | jq -r '.continuationToken // empty')"
      [[ -z "$token" || "$token" == "null" ]] && break
      cont="?continuationToken=$token"
    done
  fi
  if [[ -z "$REPORT_WS_ID" ]]; then echo "[ERROR] Report workspace not found: $REPORT_WS_ARG"; return 1; fi
  echo "[OK] Report Workspace ID: $REPORT_WS_ID"

  # ---- Resolve Semantic Model Workspace ID ----
  local SM_WS_ID=""
  if [[ "$SM_WS_ARG" == "$REPORT_WS_ARG" ]]; then
    SM_WS_ID="$REPORT_WS_ID"
    echo "[INFO] Semantic model workspace: same as report workspace"
  elif _is_guid "$SM_WS_ARG"; then
    SM_WS_ID="$(_trim "$SM_WS_ARG")"
    echo "[INFO] Semantic model workspace treated as GUID: $SM_WS_ID"
  else
    echo "[INFO] Resolving semantic model workspace by name: $SM_WS_ARG"
    local sm_url="https://api.fabric.microsoft.com/v1/workspaces" sm_cont=""
    while : ; do
      local sm_js; sm_js="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "${sm_url}${sm_cont}")"
      SM_WS_ID="$(printf '%s' "$sm_js" | jq -r --arg n "$SM_WS_ARG" '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
      SM_WS_ID="$(_trim "$SM_WS_ID")"
      [[ -n "$SM_WS_ID" ]] && break
      local sm_token; sm_token="$(printf '%s' "$sm_js" | jq -r '.continuationToken // empty')"
      [[ -z "$sm_token" || "$sm_token" == "null" ]] && break
      sm_cont="?continuationToken=$sm_token"
    done
  fi
  if [[ -z "$SM_WS_ID" ]]; then echo "[ERROR] Semantic model workspace not found: $SM_WS_ARG"; return 1; fi
  echo "[OK] Semantic Model Workspace ID: $SM_WS_ID"

  # ---- Find Semantic Model by Name ----
  echo "[INFO] Looking for semantic model '$SM_NAME' in workspace..."
  local SM_ITEMS_JSON
  SM_ITEMS_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
    "https://api.fabric.microsoft.com/v1/workspaces/$SM_WS_ID/items?type=SemanticModel")"

  local SM_ID
  SM_ID="$(printf '%s' "$SM_ITEMS_JSON" | jq -r --arg n "$SM_NAME" \
    '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
  SM_ID="$(_trim "$SM_ID")"

  if [[ -z "$SM_ID" || "$SM_ID" == "null" ]]; then
    echo "[ERROR] Semantic model '$SM_NAME' not found in workspace '$SM_WS_ARG'"
    echo "[INFO] Available semantic models:"
    printf '%s' "$SM_ITEMS_JSON" | jq -r '.value[].displayName' | sed 's/^/  - /'
    return 1
  fi
  echo "[OK] Semantic Model ID: $SM_ID (name: $SM_NAME)"

  # ---- List Reports in Report Workspace ----
  echo "[INFO] Listing reports in workspace '$REPORT_WS_ARG'..."
  local REPORT_ITEMS_JSON
  REPORT_ITEMS_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
    "https://api.fabric.microsoft.com/v1/workspaces/$REPORT_WS_ID/items?type=Report")"

  local REPORT_COUNT
  REPORT_COUNT="$(printf '%s' "$REPORT_ITEMS_JSON" | jq '.value | length')"
  echo "[OK] Reports found: $REPORT_COUNT"

  if [[ "$REPORT_COUNT" -eq 0 ]]; then
    echo "[INFO] No reports found in workspace. Nothing to rebind."
    return 0
  fi

  echo "[INFO] Report names:"
  printf '%s' "$REPORT_ITEMS_JSON" | jq -r '.value[].displayName' | sed 's/^/  - /'

  # ---- Rebind Each Report ----
  local REBOUND=0 FAILED=0

  while IFS= read -r report_id; do
    [[ -z "$report_id" || "$report_id" == "null" ]] && continue

    local report_name
    report_name="$(printf '%s' "$REPORT_ITEMS_JSON" | jq -r --arg id "$report_id" '.value[] | select(.id==$id) | .displayName')"

    echo "[INFO] Rebinding report: $report_name (ID: $report_id)"
    echo "[INFO]   Target semantic model: $SM_NAME (ID: $SM_ID) in workspace $SM_WS_ARG"

    # Use the Power BI rebind API
    # POST /v1.0/myorg/groups/{groupId}/reports/{reportId}/Rebind
    local REBIND_URL="https://api.powerbi.com/v1.0/myorg/groups/$REPORT_WS_ID/reports/$report_id/Rebind"
    local REBIND_BODY
    REBIND_BODY="$(jq -n --arg dsId "$SM_ID" '{ datasetId: $dsId }')"

    local HTTP_CODE
    HTTP_CODE="$(curl -sS -o /tmp/rebind_resp.json -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$REBIND_URL" \
      -d "$REBIND_BODY" 2>/dev/null || echo "000")"

    if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
      echo "[OK]   Rebound '$report_name' to semantic model '$SM_NAME'"
      REBOUND=$((REBOUND + 1))
    else
      echo "[WARN]   Rebind failed for '$report_name' (HTTP $HTTP_CODE)"
      [[ -f /tmp/rebind_resp.json ]] && cat /tmp/rebind_resp.json && echo

      # Fallback: Try the Fabric Items API rebind
      echo "[INFO]   Attempting fallback: Fabric Items rebind API..."
      local FABRIC_REBIND_URL="https://api.fabric.microsoft.com/v1/workspaces/$REPORT_WS_ID/reports/$report_id/rebind"
      local FABRIC_REBIND_BODY
      FABRIC_REBIND_BODY="$(jq -n --arg smId "$SM_ID" --arg smWsId "$SM_WS_ID" \
        '{ semanticModelId: $smId, workspaceId: $smWsId }')"

      HTTP_CODE="$(curl -sS -o /tmp/rebind_resp2.json -w "%{http_code}" \
        -H "Authorization: Bearer $FABRIC_TOKEN" \
        -H "Content-Type: application/json" \
        -X POST "$FABRIC_REBIND_URL" \
        -d "$FABRIC_REBIND_BODY" 2>/dev/null || echo "000")"

      if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
        echo "[OK]   Rebound '$report_name' via Fabric API fallback"
        REBOUND=$((REBOUND + 1))
      else
        echo "[WARN]   All rebind methods failed for '$report_name'"
        [[ -f /tmp/rebind_resp2.json ]] && cat /tmp/rebind_resp2.json && echo
        FAILED=$((FAILED + 1))
      fi
    fi

  done < <(printf '%s' "$REPORT_ITEMS_JSON" | jq -r '.value[].id')

  # Cleanup
  rm -f /tmp/rebind_resp.json /tmp/rebind_resp2.json 2>/dev/null || true

  # ---- Summary ----
  echo ""
  echo "============================================"
  echo "Report Rebind Summary"
  echo "============================================"
  echo "  Report workspace:          $REPORT_WS_ARG"
  echo "  Semantic model workspace:  $SM_WS_ARG"
  echo "  Semantic model:            $SM_NAME"
  echo "  Total reports:             $REPORT_COUNT"
  echo "  Rebound:                   $REBOUND"
  echo "  Failed:                    $FAILED"
  echo "============================================"

  if [[ $FAILED -gt 0 ]]; then
    echo "[WARN] Some reports failed to rebind. Manual configuration may be required."
    return 0  # Don't fail the pipeline, just warn
  fi

  return 0
}
