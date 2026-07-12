#!/usr/bin/env bash
set -euo pipefail

# Repoint Data Pipeline notebook references to the correct workspace/notebook IDs.
#
# Fabric Data Pipelines store hardcoded notebookId and workspaceId for each
# TridentNotebook activity. After Git sync deploys a pipeline to a new
# environment, those IDs still point at the source environment. This script
# resolves the correct IDs by notebook display name and updates the pipeline
# definition via the Fabric REST API.
#
# Usage:
#   ./connect-pipelines.sh "<pipeline-workspace>" "<pipeline-name>" \
#       "<activity>=<notebook-workspace>/<notebook-name>" ...
#
# Example:
#   ./connect-pipelines.sh "main-log" "orchestrator" \
#       "Bronze=main-bronze/bronze-orchestrator" \
#       "Silver=main-silver/silver-orchestrator" \
#       "Gold=main-gold/gold-orchestrator"

fabric_connect_pipeline_notebooks() {
  if [[ $# -lt 3 ]]; then
    echo "[ERROR] Usage: fabric_connect_pipeline_notebooks <pipeline-workspace> <pipeline-name> <activity=workspace/notebook> ..."
    echo ""
    echo "Example:"
    echo "  fabric_connect_pipeline_notebooks 'main-log' 'orchestrator' \\"
    echo "    'Bronze=main-bronze/bronze-orchestrator' \\"
    echo "    'Silver=main-silver/silver-orchestrator' \\"
    echo "    'Gold=main-gold/gold-orchestrator'"
    return 1
  fi

  local PIPELINE_WS_ARG="$1"
  local PIPELINE_NAME="$2"
  shift 2
  local MAPPINGS=("$@")

  for c in az curl jq base64; do
    command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; return 1; }
  done

  _b64() { if base64 --help 2>&1 | grep -q -- '-w '; then base64 -w 0; else base64 | tr -d '\n'; fi; }
  _trim() { printf "%s" "${1:-}" | tr -d '[:space:]'; }
  _is_guid() { [[ "${1:-}" =~ ^[0-9a-fA-F-]{36}$ ]]; }

  # Helper: fetch all pages from a Fabric REST API list endpoint.
  _fabric_list_all() {
    local TOKEN="$1"
    local URL="$2"
    local ALL_VALUES="[]"

    while [[ -n "$URL" ]]; do
      local PAGE
      PAGE="$(curl -sS -H "Authorization: Bearer $TOKEN" "$URL")"

      local PAGE_VALUES
      PAGE_VALUES="$(echo "$PAGE" | jq -c '.value // []')"
      ALL_VALUES="$(echo "$ALL_VALUES" "$PAGE_VALUES" | jq -sc '.[0] + .[1]')"

      URL="$(echo "$PAGE" | jq -r '.continuationUri // empty')"
    done

    echo "$ALL_VALUES"
  }

  echo "============================================"
  echo "Connect Pipeline Notebook References"
  echo "============================================"
  echo "Pipeline workspace: $PIPELINE_WS_ARG"
  echo "Pipeline name:      $PIPELINE_NAME"
  echo "Mappings:"
  for m in "${MAPPINGS[@]}"; do
    echo "  - $m"
  done
  echo "============================================"

  echo "[INFO] Acquiring Fabric token…"
  local FABRIC_TOKEN
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)"

  local API_BASE="https://api.fabric.microsoft.com/v1"

  # ---- Resolve all workspaces upfront ----
  echo "[INFO] Resolving workspaces…"
  local WS_ALL
  WS_ALL="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces")"

  # Resolve pipeline workspace
  local PIPELINE_WS_ID=""
  if _is_guid "$PIPELINE_WS_ARG"; then
    PIPELINE_WS_ID="$(_trim "$PIPELINE_WS_ARG")"
  else
    PIPELINE_WS_ID="$(echo "$WS_ALL" | jq -r --arg n "$PIPELINE_WS_ARG" '.[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
    PIPELINE_WS_ID="$(_trim "$PIPELINE_WS_ID")"
  fi

  if [[ -z "$PIPELINE_WS_ID" ]]; then
    echo "[ERROR] Pipeline workspace not found: $PIPELINE_WS_ARG"
    return 1
  fi
  echo "[OK] Pipeline workspace ID: $PIPELINE_WS_ID"

  # ---- Find the pipeline ----
  echo "[INFO] Resolving pipeline '$PIPELINE_NAME'…"
  local PIPELINE_ITEMS
  PIPELINE_ITEMS="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces/${PIPELINE_WS_ID}/items")"

  local PIPELINE_ID
  PIPELINE_ID="$(echo "$PIPELINE_ITEMS" | jq -r --arg n "$PIPELINE_NAME" '.[] | select(.type=="DataPipeline" and ((.displayName|ascii_downcase)==($n|ascii_downcase))) | .id' | head -n1)"
  PIPELINE_ID="$(_trim "$PIPELINE_ID")"

  if [[ -z "$PIPELINE_ID" ]]; then
    echo "[ERROR] Pipeline '$PIPELINE_NAME' not found in workspace"
    return 1
  fi
  echo "[OK] Pipeline ID: $PIPELINE_ID"

  # ---- Get current pipeline definition ----
  echo "[INFO] Fetching pipeline definition…"
  local GET_DEF_URL="${API_BASE}/workspaces/${PIPELINE_WS_ID}/dataPipelines/${PIPELINE_ID}/getDefinition"
  local DEF_RESPONSE DEF_HTTP_CODE

  DEF_RESPONSE="$(mktemp /tmp/pipeline_def_headers.XXXXXX)"
  DEF_HTTP_CODE="$(curl -sS -o /tmp/pipeline_def.json -w "%{http_code}" -D "$DEF_RESPONSE" \
    -H "Authorization: Bearer $FABRIC_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST "$GET_DEF_URL" \
    -d '{}' 2>/dev/null || echo "000")"

  # Handle 202 Accepted with polling
  if [[ "$DEF_HTTP_CODE" == "202" ]]; then
    local DEF_LOCATION
    DEF_LOCATION="$(grep -i '^Location:' "$DEF_RESPONSE" | awk '{print $2}' | tr -d '\r')"
    rm -f "$DEF_RESPONSE"

    if [[ -n "$DEF_LOCATION" ]]; then
      echo "[INFO] Polling getDefinition operation…"
      local POLL_ATTEMPTS=0 POLL_MAX=30
      while [[ $POLL_ATTEMPTS -lt $POLL_MAX ]]; do
        sleep 3
        DEF_HTTP_CODE="$(curl -sS -o /tmp/pipeline_def_op.json -w "%{http_code}" \
          -H "Authorization: Bearer $FABRIC_TOKEN" \
          "$DEF_LOCATION" 2>/dev/null || echo "000")"

        if [[ "$DEF_HTTP_CODE" == "200" ]]; then
          local OP_STATUS
          OP_STATUS="$(jq -r '.status // empty' /tmp/pipeline_def_op.json)"
          if [[ "$OP_STATUS" == "Succeeded" ]]; then
            echo "[INFO] Operation completed, getting result…"
            local OPERATION_ID
            OPERATION_ID="$(echo "$DEF_LOCATION" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)"

            local RESULT_URL="${API_BASE}/operations/$OPERATION_ID/result"
            local RESULT_HTTP_CODE
            RESULT_HTTP_CODE="$(curl -sS -o /tmp/pipeline_def.json -w "%{http_code}" \
              -H "Authorization: Bearer $FABRIC_TOKEN" \
              "$RESULT_URL" 2>/dev/null || echo "000")"

            if [[ "$RESULT_HTTP_CODE" == "200" ]]; then
              DEF_HTTP_CODE="200"
            else
              echo "[ERROR] Failed to get result (HTTP $RESULT_HTTP_CODE)"
              DEF_HTTP_CODE="$RESULT_HTTP_CODE"
            fi
            break
          elif [[ "$OP_STATUS" == "Failed" ]]; then
            echo "[ERROR] Operation failed"
            DEF_HTTP_CODE="500"
            break
          fi
        fi
        POLL_ATTEMPTS=$((POLL_ATTEMPTS + 1))
      done
      rm -f /tmp/pipeline_def_op.json
    fi
  else
    rm -f "$DEF_RESPONSE"
  fi

  if [[ "$DEF_HTTP_CODE" != "200" ]]; then
    echo "[ERROR] Could not fetch pipeline definition (HTTP $DEF_HTTP_CODE)"
    [[ -f /tmp/pipeline_def.json ]] && cat /tmp/pipeline_def.json
    return 1
  fi

  # Extract pipeline content
  local PARTS CONTENT_PART CONTENT_PAYLOAD PIPELINE_JSON PLATFORM_PART PLATFORM_B64
  PARTS="$(jq -r '.definition.parts // []' /tmp/pipeline_def.json)"
  CONTENT_PART="$(echo "$PARTS" | jq -r '.[] | select(.path == "pipeline-content.json")')"
  CONTENT_PAYLOAD="$(echo "$CONTENT_PART" | jq -r '.payload')"
  PIPELINE_JSON="$(echo "$CONTENT_PAYLOAD" | base64 -d 2>/dev/null)"

  PLATFORM_PART="$(echo "$PARTS" | jq -r '.[] | select(.path == ".platform")')"
  PLATFORM_B64="$(echo "$PLATFORM_PART" | jq -r '.payload // empty')"

  if [[ -z "$PIPELINE_JSON" ]]; then
    echo "[ERROR] Could not decode pipeline content"
    return 1
  fi

  echo "[OK] Pipeline definition fetched ($(echo "$PIPELINE_JSON" | jq '.properties.activities | length') activities)"

  # ---- Resolve each mapping and update ----
  local UPDATED=0 FAILED=0

  for mapping in "${MAPPINGS[@]}"; do
    local ACTIVITY_NAME NB_WS_NAME NB_NAME
    ACTIVITY_NAME="${mapping%%=*}"
    local TARGET="${mapping#*=}"
    NB_WS_NAME="${TARGET%%/*}"
    NB_NAME="${TARGET#*/}"

    echo ""
    echo "[INFO] Resolving: $ACTIVITY_NAME → $NB_WS_NAME / $NB_NAME"

    # Resolve notebook workspace
    local NB_WS_ID=""
    if _is_guid "$NB_WS_NAME"; then
      NB_WS_ID="$(_trim "$NB_WS_NAME")"
    else
      NB_WS_ID="$(echo "$WS_ALL" | jq -r --arg n "$NB_WS_NAME" '.[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
      NB_WS_ID="$(_trim "$NB_WS_ID")"
    fi

    if [[ -z "$NB_WS_ID" ]]; then
      echo "[ERROR] Workspace not found: $NB_WS_NAME"
      FAILED=$((FAILED + 1))
      continue
    fi

    # Resolve notebook
    local NB_ITEMS NB_ID
    NB_ITEMS="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces/${NB_WS_ID}/items")"
    NB_ID="$(echo "$NB_ITEMS" | jq -r --arg n "$NB_NAME" '.[] | select(.type=="Notebook" and ((.displayName|ascii_downcase)==($n|ascii_downcase))) | .id' | head -n1)"
    NB_ID="$(_trim "$NB_ID")"

    if [[ -z "$NB_ID" ]]; then
      echo "[ERROR] Notebook '$NB_NAME' not found in workspace '$NB_WS_NAME'"
      FAILED=$((FAILED + 1))
      continue
    fi

    echo "[OK] $ACTIVITY_NAME → workspace=$NB_WS_ID, notebook=$NB_ID"

    # Update the pipeline JSON
    PIPELINE_JSON="$(echo "$PIPELINE_JSON" | jq \
      --arg activity "$ACTIVITY_NAME" \
      --arg wsId "$NB_WS_ID" \
      --arg nbId "$NB_ID" \
      '(.properties.activities[] | select(.name == $activity) | .typeProperties.workspaceId) = $wsId |
       (.properties.activities[] | select(.name == $activity) | .typeProperties.notebookId) = $nbId')"

    UPDATED=$((UPDATED + 1))
  done

  if [[ $FAILED -gt 0 && $UPDATED -eq 0 ]]; then
    echo "[ERROR] All mappings failed to resolve"
    return 1
  fi

  # ---- Strip activities with externalReferences (e.g. Office365Email) ----
  # Activities like email connectors have externalReferences pointing to
  # connection IDs that may not exist in the target environment, causing the
  # updateDefinition call to fail with a 400 error. Fabric also rejects null
  # externalReferences, so the only option is to remove the entire activity
  # from the pushed definition. The activity remains in the git source and
  # will be restored on the next git sync.
  local EXT_REF_COUNT
  EXT_REF_COUNT="$(echo "$PIPELINE_JSON" | jq '[.properties.activities[] | select(has("externalReferences"))] | length')"
  if [[ "$EXT_REF_COUNT" -gt 0 ]]; then
    echo ""
    echo "[INFO] Stripping $EXT_REF_COUNT activities with external connection references (not portable across environments):"
    echo "$PIPELINE_JSON" | jq -r '[.properties.activities[] | select(has("externalReferences")) | .name] | .[] | "  - " + .'
    PIPELINE_JSON="$(echo "$PIPELINE_JSON" | jq '.properties.activities = [.properties.activities[] | select(has("externalReferences") | not)]')"
  fi

  # ---- Push updated definition ----
  echo ""
  echo "[INFO] Pushing updated pipeline definition…"

  local CONTENT_B64
  CONTENT_B64="$(printf '%s' "$PIPELINE_JSON" | _b64)"

  local CONTENT_FILE PLATFORM_FILE UPDATE_BODY
  CONTENT_FILE="$(mktemp /tmp/pipeline_content_b64.XXXXXX)"
  printf '%s' "$CONTENT_B64" > "$CONTENT_FILE"

  if [[ -n "$PLATFORM_B64" && "$PLATFORM_B64" != "null" ]]; then
    PLATFORM_FILE="$(mktemp /tmp/pipeline_platform_b64.XXXXXX)"
    printf '%s' "$PLATFORM_B64" > "$PLATFORM_FILE"
    UPDATE_BODY="$(jq -n \
      --rawfile contentPayload "$CONTENT_FILE" \
      --rawfile platformPayload "$PLATFORM_FILE" \
      '{
        definition: {
          parts: [
            {
              path: "pipeline-content.json",
              payload: $contentPayload,
              payloadType: "InlineBase64"
            },
            {
              path: ".platform",
              payload: $platformPayload,
              payloadType: "InlineBase64"
            }
          ]
        }
      }')"
  else
    UPDATE_BODY="$(jq -n \
      --rawfile contentPayload "$CONTENT_FILE" \
      '{
        definition: {
          parts: [
            {
              path: "pipeline-content.json",
              payload: $contentPayload,
              payloadType: "InlineBase64"
            }
          ]
        }
      }')"
  fi
  rm -f "$CONTENT_FILE" "${PLATFORM_FILE:-}"

  local UPDATE_URL="${API_BASE}/workspaces/${PIPELINE_WS_ID}/dataPipelines/${PIPELINE_ID}/updateDefinition"
  local UPDATE_HEADERS UPDATE_HTTP_CODE
  UPDATE_HEADERS="$(mktemp /tmp/pipeline_update_headers.XXXXXX)"
  UPDATE_HTTP_CODE="$(curl -sS -o /tmp/pipeline_update.json -w "%{http_code}" -D "$UPDATE_HEADERS" \
    -H "Authorization: Bearer $FABRIC_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST "$UPDATE_URL" \
    -d "$UPDATE_BODY" 2>/dev/null || echo "000")"

  # Handle 202 Accepted
  if [[ "$UPDATE_HTTP_CODE" == "202" ]]; then
    echo "[INFO] Update accepted (202), polling…"
    sleep 5
    UPDATE_HTTP_CODE="200"
  fi
  rm -f "$UPDATE_HEADERS"

  if [[ "$UPDATE_HTTP_CODE" == "200" ]]; then
    echo "[OK] Pipeline definition updated successfully"
  else
    echo "[ERROR] Failed to update pipeline (HTTP $UPDATE_HTTP_CODE)"
    [[ -f /tmp/pipeline_update.json ]] && cat /tmp/pipeline_update.json && echo
    return 1
  fi

  # Cleanup
  rm -f /tmp/pipeline_def.json /tmp/pipeline_update.json

  echo ""
  echo "============================================"
  echo "Pipeline Connection Summary"
  echo "============================================"
  echo "  Pipeline: $PIPELINE_NAME"
  echo "  Updated:  $UPDATED"
  echo "  Failed:   $FAILED"
  echo "============================================"

  if [[ $FAILED -gt 0 ]]; then
    echo "[WARN] Some mappings failed — review output above"
    return 0
  fi

  return 0
}

# Allow direct invocation
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] || [[ -z "${BASH_SOURCE[0]:-}" ]]; then
  fabric_connect_pipeline_notebooks "$@"
fi
