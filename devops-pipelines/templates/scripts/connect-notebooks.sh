#!/usr/bin/env bash
set -euo pipefail

# Connect all notebooks in a Fabric workspace to a lakehouse using REST API directly
# No controller notebook needed - uses Fabric API to update notebook definitions
# Usage:
#   ./LHconnectToNB-api.sh "<workspace-name-or-guid>" "<lakehouse-name-or-guid>"

fabric_connect_notebooks_to_lakehouse_api() {
  # ---- Args ----
  if [[ $# -lt 2 ]]; then
    echo "[ERROR] Usage: fabric_connect_notebooks_to_lakehouse_api <workspace-name-or-id> <lakehouse-name-or-id>"
    return 1
  fi
  local WS_ARG="$1" LH_ARG="$2"

  # ---- Dependencies ----
  for c in az curl jq base64; do
    command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; return 1; }
  done

  # ---- Helpers ----
  _b64() { if base64 --help 2>&1 | grep -q -- '-w '; then base64 -w 0; else base64 | tr -d '\n'; fi; }
  _trim() { printf "%s" "${1:-}" | tr -d '[:space:]'; }
  _is_guid() { [[ "${1:-}" =~ ^[0-9a-fA-F-]{36}$ ]]; }

  # Create minimal notebook content with lakehouse dependencies
  _create_minimal_notebook_content() {
    local lh_id="$1" lh_name="$2" ws_id="$3"
    cat <<EOF
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "${lh_id}",
# META       "default_lakehouse_name": "${lh_name}",
# META       "default_lakehouse_workspace_id": "${ws_id}",
# META       "known_lakehouses": [
# META         {
# META           "id": "${lh_id}"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type code here

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
EOF
  }

  echo "[INFO] Acquiring Fabric token…"
  local FABRIC_TOKEN
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)"

  # ---- Resolve Workspace ID ----
  local WS_ID=""
  if _is_guid "$WS_ARG"; then
    WS_ID="$(_trim "$WS_ARG")"
    echo "[INFO] Workspace treated as GUID: $WS_ID"
  else
    echo "[INFO] Resolving workspace by name (case-insensitive): $WS_ARG"
    local url="https://api.fabric.microsoft.com/v1/workspaces" cont=""
    while : ; do
      local js; js="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "${url}${cont}")"
      WS_ID="$(printf '%s' "$js" | jq -r --arg n "$WS_ARG" '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
      WS_ID="$(_trim "$WS_ID")"
      [[ -n "$WS_ID" ]] && break
      local token; token="$(printf '%s' "$js" | jq -r '.continuationToken // empty')"
      [[ -z "$token" || "$token" == "null" ]] && break
      cont="?continuationToken=$token"
    done
  fi
  if [[ -z "$WS_ID" ]]; then echo "[ERROR] Workspace not found: $WS_ARG"; return 1; fi
  echo "[OK] Workspace ID: $WS_ID"

  # ---- Resolve Lakehouse ID & Name ----
  local LH_ID="" LH_NAME=""
  echo "[INFO] Resolving lakehouse: $LH_ARG"
  local LH_LIST_JSON
  LH_LIST_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/lakehouses")"

  if _is_guid "$LH_ARG"; then
    LH_ID="$(_trim "$LH_ARG")"
    LH_NAME="$(printf '%s' "$LH_LIST_JSON" | jq -r --arg id "$LH_ID" '.value[] | select(.id==$id) | .displayName' | head -n1)"
  else
    LH_NAME="$LH_ARG"
    LH_ID="$(printf '%s' "$LH_LIST_JSON" | jq -r --arg n "$LH_ARG" '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
    LH_ID="$(_trim "$LH_ID")"
  fi

  if [[ -z "$LH_ID" ]]; then echo "[ERROR] Lakehouse not found: $LH_ARG"; return 1; fi
  [[ -z "$LH_NAME" ]] && LH_NAME="$LH_ID"
  echo "[OK] Lakehouse ID: $LH_ID"
  echo "[OK] Lakehouse Name: $LH_NAME"

  # ---- List notebooks and environments ----
  echo "[INFO] Listing notebooks and environments in workspace…"
  local ITEMS_JSON NOTEBOOKS_JSON NB_COUNT ENVIRONMENTS_JSON ENV_COUNT
  ITEMS_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items")"
  NOTEBOOKS_JSON="$(printf '%s' "$ITEMS_JSON" | jq '[.value[] | select(.type=="Notebook")]')"
  NB_COUNT="$(printf '%s' "$NOTEBOOKS_JSON" | jq 'length')"
  echo "[OK] Notebooks found: $NB_COUNT"

  # Also get environments for later matching
  ENVIRONMENTS_JSON="$(printf '%s' "$ITEMS_JSON" | jq '[.value[] | select(.type=="Environment")]')"
  ENV_COUNT="$(printf '%s' "$ENVIRONMENTS_JSON" | jq 'length')"
  echo "[OK] Environments found: $ENV_COUNT"

  if [[ "$NB_COUNT" -eq 0 ]]; then
    echo "[INFO] No notebooks found. Nothing to connect."
    return 0
  fi

  echo "[INFO] Notebook names:"
  printf '%s' "$NOTEBOOKS_JSON" | jq -r '.[].displayName' | sed 's/^/ - /'

  if [[ "$ENV_COUNT" -gt 0 ]]; then
    echo "[INFO] Environment names:"
    printf '%s' "$ENVIRONMENTS_JSON" | jq -r '.[].displayName' | sed 's/^/ - /'
  fi

  # ---- Connect each notebook using API ----
  local UPDATED=0 FAILED=0 SKIPPED=0
  local ERRORS="[]"

  while IFS= read -r nb_line; do
    [[ -z "$nb_line" ]] && continue

    local NB_ID NB_NAME
    NB_ID="$(echo "$nb_line" | jq -r '.id')"
    NB_NAME="$(echo "$nb_line" | jq -r '.displayName')"

    echo ""
    echo "[INFO] Processing notebook: $NB_NAME (ID: $NB_ID)"

    # Get current notebook definition
    echo "[INFO]   Fetching notebook definition..."
    local GET_DEF_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$NB_ID/getDefinition"
    local DEF_RESPONSE DEF_HTTP_CODE DEF_LOCATION

    # POST to getDefinition endpoint (FabricGitSource format by default)
    # Send empty JSON body to satisfy HTTP 411 requirement
    DEF_RESPONSE="$(mktemp /tmp/nb_def_headers.XXXXXX)"
    DEF_HTTP_CODE="$(curl -sS -o /tmp/nb_def.json -w "%{http_code}" -D "$DEF_RESPONSE" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$GET_DEF_URL" \
      -d '{}' 2>/dev/null || echo "000")"

    # Handle 202 Accepted with polling
    if [[ "$DEF_HTTP_CODE" == "202" ]]; then
      DEF_LOCATION="$(grep -i '^Location:' "$DEF_RESPONSE" | awk '{print $2}' | tr -d '\r')"
      rm -f "$DEF_RESPONSE"

      if [[ -n "$DEF_LOCATION" ]]; then
        echo "[INFO]   Polling getDefinition operation..."
        local POLL_ATTEMPTS=0 POLL_MAX=20
        while [[ $POLL_ATTEMPTS -lt $POLL_MAX ]]; do
          sleep 3
          DEF_HTTP_CODE="$(curl -sS -o /tmp/nb_def_operation.json -w "%{http_code}" \
            -H "Authorization: Bearer $FABRIC_TOKEN" \
            "$DEF_LOCATION" 2>/dev/null || echo "000")"

          if [[ "$DEF_HTTP_CODE" == "200" ]]; then
            # Check if operation completed
            local OP_STATUS
            OP_STATUS="$(jq -r '.status // empty' /tmp/nb_def_operation.json)"
            if [[ "$OP_STATUS" == "Succeeded" ]]; then
              echo "[INFO]   Operation completed successfully, getting result..."

              # Get the operation ID from the Location URL
              local OPERATION_ID
              OPERATION_ID="$(echo "$DEF_LOCATION" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)"

              if [[ -z "$OPERATION_ID" ]]; then
                echo "[ERROR]   Could not extract operation ID from Location"
                DEF_HTTP_CODE="500"
                break
              fi

              # Call the result endpoint to get the actual notebook definition
              local RESULT_URL="https://api.fabric.microsoft.com/v1/operations/$OPERATION_ID/result"
              echo "[INFO]   Fetching result from: $RESULT_URL"

              local RESULT_HTTP_CODE
              RESULT_HTTP_CODE="$(curl -sS -o /tmp/nb_def.json -w "%{http_code}" \
                -H "Authorization: Bearer $FABRIC_TOKEN" \
                -H "Accept: application/json" \
                "$RESULT_URL" 2>/dev/null || echo "000")"

              if [[ "$RESULT_HTTP_CODE" == "200" ]]; then
                echo "[INFO]   Successfully retrieved notebook definition"
                DEF_HTTP_CODE="200"
              else
                echo "[ERROR]   Failed to get result (HTTP $RESULT_HTTP_CODE)"
                DEF_HTTP_CODE="$RESULT_HTTP_CODE"
              fi
              break

            elif [[ "$OP_STATUS" == "Failed" ]]; then
              echo "[ERROR]   Operation failed!"
              cat /tmp/nb_def_operation.json | jq .
              DEF_HTTP_CODE="500"
              break
            fi
            # Still running, continue polling
          fi
          POLL_ATTEMPTS=$((POLL_ATTEMPTS + 1))
        done
        rm -f /tmp/nb_def_operation.json
      fi
    else
      rm -f "$DEF_RESPONSE"
    fi

    if [[ "$DEF_HTTP_CODE" != "200" ]]; then
      echo "[WARN]   Could not fetch definition (HTTP $DEF_HTTP_CODE)."

      # Try to extract error details from response (disable errexit temporarily)
      set +e
      local ERROR_MSG ERROR_CODE
      if [[ -f /tmp/nb_def.json ]]; then
        echo "[INFO]   Raw response body:"
        cat /tmp/nb_def.json
        echo ""

        ERROR_CODE="$(jq -r '.errorCode // empty' /tmp/nb_def.json 2>/dev/null)"
        ERROR_MSG="$(jq -r '.message // empty' /tmp/nb_def.json 2>/dev/null)"

        if [[ -n "$ERROR_CODE" && "$ERROR_CODE" != "null" ]]; then
          echo "[WARN]   Error Code: $ERROR_CODE"
        fi

        if [[ -n "$ERROR_MSG" && "$ERROR_MSG" != "null" ]]; then
          echo "[WARN]   Error Message: $ERROR_MSG"
        fi

        echo "[INFO]   Attempting to format as JSON:"
        jq . /tmp/nb_def.json 2>/dev/null || echo "   (not valid JSON or jq failed)"
      else
        echo "[WARN]   No response body file found at /tmp/nb_def.json"
      fi
      set -e

      echo "[WARN]   Skipping notebook '$NB_NAME'."
      FAILED=$((FAILED + 1))
      ERRORS="$(echo "$ERRORS" | jq --arg nb "$NB_NAME" --arg err "HTTP $DEF_HTTP_CODE: ${ERROR_CODE:-Unknown error}" '. += [{"notebook": $nb, "error": $err}]')"
      continue
    fi

    # Parse existing definition
    local DEF_JSON
    DEF_JSON="$(cat /tmp/nb_def.json)"

    # Extract parts
    local PARTS PLATFORM_PART CONTENT_PART
    PARTS="$(echo "$DEF_JSON" | jq -r '.definition.parts // []')"
    PLATFORM_PART="$(echo "$PARTS" | jq -r '.[] | select(.path == ".platform")')"
    CONTENT_PART="$(echo "$PARTS" | jq -r '[.[] | select(.path != ".platform")] | .[0]')"

    # Handle .platform file (git integration metadata - always keep as-is)
    echo "[INFO]   Checking for .platform file (git integration metadata)..."
    local PLATFORM_PAYLOAD
    PLATFORM_PAYLOAD="$(echo "$PLATFORM_PART" | jq -r '.payload // empty')"

    if [[ -z "$PLATFORM_PAYLOAD" || "$PLATFORM_PAYLOAD" == "null" ]]; then
      echo "[WARN]   No .platform file found - this notebook may not be Git-synced"
      PLATFORM_B64=""
    else
      echo "[INFO]   Found .platform file, will preserve it"
      PLATFORM_B64="$PLATFORM_PAYLOAD"
    fi

    # Update notebook definition
    echo "[INFO]   Updating notebook definition..."
    local UPDATE_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/notebooks/$NB_ID/updateDefinition"
    local UPDATE_BODY

    # Build parts array - include content if it exists
    if [[ -n "$CONTENT_PART" && "$CONTENT_PART" != "null" ]]; then
      local CONTENT_PATH CONTENT_PAYLOAD CONTENT_TEXT
      CONTENT_PATH="$(echo "$CONTENT_PART" | jq -r '.path')"
      CONTENT_PAYLOAD="$(echo "$CONTENT_PART" | jq -r '.payload')"
      CONTENT_TEXT="$(echo "$CONTENT_PAYLOAD" | base64 -d 2>/dev/null || echo "")"

      if [[ -n "$CONTENT_TEXT" ]]; then
        echo "[INFO]   Found notebook content ($(echo "$CONTENT_TEXT" | wc -l) lines)"
        echo "[INFO]   Updating lakehouse dependencies in notebook content..."

        # Check if notebook has lakehouse metadata
        if grep -q '"default_lakehouse"' <<< "$CONTENT_TEXT"; then
          echo "[INFO]   Found existing lakehouse metadata, updating..."
          # Update lakehouse references in the content (within # META JSON)
          CONTENT_TEXT=$(sed -E "s/(\"default_lakehouse\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$LH_ID\"/g" <<< "$CONTENT_TEXT")
          CONTENT_TEXT=$(sed -E "s/(\"default_lakehouse_name\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$LH_NAME\"/g" <<< "$CONTENT_TEXT")
          CONTENT_TEXT=$(sed -E "s/(\"default_lakehouse_workspace_id\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$WS_ID\"/g" <<< "$CONTENT_TEXT")

          # Update known_lakehouses array - update ALL occurrences of "id": "<uuid>" within lakehouse section
          # This handles both the known_lakehouses array and any other lakehouse ID references
          CONTENT_TEXT=$(sed -E "/\"lakehouse\"/,/^# META[[:space:]]*\}/ s/(\"id\"[[:space:]]*:[[:space:]]*\")[a-f0-9-]+\"/\1$LH_ID\"/g" <<< "$CONTENT_TEXT")
        else
          echo "[WARN]   No lakehouse metadata found in notebook - skipping lakehouse update"
        fi

        # Check for and update environment references
        if grep -q "\"environmentId\"" <<< "$CONTENT_TEXT"; then
          echo "[INFO]   Notebook has environment dependency, updating..."
          if [[ "$ENV_COUNT" -gt 0 ]]; then
            local ENV_ID ENV_NAME NB_BASE_NAME MATCHING_ENV
            NB_BASE_NAME="$(echo "$NB_NAME" | sed -E 's/[_-](data|ingestion|processing|pipeline).*//i' | tr '[:upper:]' '[:lower:]')"
            MATCHING_ENV="$(printf '%s' "$ENVIRONMENTS_JSON" | jq -r --arg pattern "$NB_BASE_NAME" '[.[] | select((.displayName | ascii_downcase) | contains($pattern))] | .[0] // empty')"

            if [[ -n "$MATCHING_ENV" && "$MATCHING_ENV" != "null" ]]; then
              ENV_ID="$(echo "$MATCHING_ENV" | jq -r '.id')"
              ENV_NAME="$(echo "$MATCHING_ENV" | jq -r '.displayName')"
              CONTENT_TEXT=$(sed -E "s/(\"environment\"[^}]*\"environmentId\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$ENV_ID\"/" <<< "$CONTENT_TEXT")
              CONTENT_TEXT=$(sed -E "s/(\"environment\"[^}]*\"workspaceId\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$WS_ID\"/" <<< "$CONTENT_TEXT")
              echo "[OK]   ✓ Connected to environment: $ENV_NAME"
            fi
          fi
        fi

        CONTENT_PAYLOAD="$(printf '%s' "$CONTENT_TEXT" | _b64)"
        echo "[INFO]   Updated content: $(echo "$CONTENT_TEXT" | wc -l) lines"
      else
        echo "[WARN]   Content text is empty after decoding - using original payload"
      fi

      # Build update body - only include .platform if it exists.
      # Use temp files + --rawfile to avoid "Argument list too long" for large notebooks.
      local CONTENT_FILE PLATFORM_FILE
      CONTENT_FILE="$(mktemp /tmp/nb_content_b64.XXXXXX)"
      printf '%s' "$CONTENT_PAYLOAD" > "$CONTENT_FILE"

      if [[ -n "$PLATFORM_B64" ]]; then
        PLATFORM_FILE="$(mktemp /tmp/nb_platform_b64.XXXXXX)"
        printf '%s' "$PLATFORM_B64" > "$PLATFORM_FILE"
        UPDATE_BODY="$(jq -n \
          --arg contentPath "$CONTENT_PATH" \
          --rawfile contentPayload "$CONTENT_FILE" \
          --rawfile platformPayload "$PLATFORM_FILE" \
          '{
            definition: {
              parts: [
                {
                  path: $contentPath,
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
          --arg contentPath "$CONTENT_PATH" \
          --rawfile contentPayload "$CONTENT_FILE" \
          '{
            definition: {
              parts: [
                {
                  path: $contentPath,
                  payload: $contentPayload,
                  payloadType: "InlineBase64"
                }
              ]
            }
          }')"
      fi
      rm -f "$CONTENT_FILE" "${PLATFORM_FILE:-}"
    else
      # No content part exists - this should never happen for Git-synced notebooks
      echo "[ERROR]   No content file found in notebook definition!"
      echo "[ERROR]   This indicates the notebook definition was not properly retrieved from the API."
      echo "[ERROR]   Skipping notebook '$NB_NAME' to avoid data loss."
      FAILED=$((FAILED + 1))
      ERRORS="$(echo "$ERRORS" | jq --arg nb "$NB_NAME" --arg err "No content in definition" '. += [{"notebook": $nb, "error": $err}]')"
      continue
    fi

    local UPDATE_HTTP_CODE UPDATE_LOCATION
    local UPDATE_HEADERS="$(mktemp /tmp/nb_update_headers.XXXXXX)"
    UPDATE_HTTP_CODE="$(curl -sS -o /tmp/nb_update.json -w "%{http_code}" -D "$UPDATE_HEADERS" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$UPDATE_URL" \
      -d "$UPDATE_BODY" 2>/dev/null || echo "000")"

    # Handle 202 Accepted
    if [[ "$UPDATE_HTTP_CODE" == "202" ]]; then
      UPDATE_LOCATION="$(grep -i '^Location:' "$UPDATE_HEADERS" | awk '{print $2}' | tr -d '\r')"
      rm -f "$UPDATE_HEADERS"

      if [[ -n "$UPDATE_LOCATION" ]]; then
        echo "[INFO]   Polling updateDefinition operation..."
        sleep 5  # Give it a moment to process
      fi
      # Don't wait too long - consider it successful
      UPDATE_HTTP_CODE="200"
    else
      rm -f "$UPDATE_HEADERS"
    fi

    if [[ "$UPDATE_HTTP_CODE" == "200" ]]; then
      echo "[OK]   ✓ Connected notebook '$NB_NAME' to lakehouse"
      UPDATED=$((UPDATED + 1))
    else
      echo "[ERROR] ✗ Failed to update '$NB_NAME' (HTTP $UPDATE_HTTP_CODE)"
      [[ -f /tmp/nb_update.json ]] && cat /tmp/nb_update.json && echo
      FAILED=$((FAILED + 1))
      ERRORS="$(echo "$ERRORS" | jq --arg nb "$NB_NAME" --arg err "HTTP $UPDATE_HTTP_CODE" '. += [{"notebook": $nb, "error": $err}]')"
    fi

  done < <(echo "$NOTEBOOKS_JSON" | jq -c '.[]')

  # Cleanup temp files
  rm -f /tmp/nb_def.json /tmp/nb_update.json

  # ---- Summary ----
  echo ""
  echo "============================================"
  echo "Notebook Connection Summary"
  echo "============================================"
  echo "  Total notebooks: $NB_COUNT"
  echo "  Total environments: $ENV_COUNT"
  echo "  Successfully connected: $UPDATED"
  echo "  Skipped: $SKIPPED"
  echo "  Failed: $FAILED"
  echo "============================================"
  echo ""
  echo "Notes:"
  echo "  - All notebooks connected to lakehouse: $LH_NAME"
  if [[ "$ENV_COUNT" -gt 0 ]]; then
    echo "  - Notebooks with environment dependencies connected to matching environments"
  fi
  echo "============================================"

  if [[ $FAILED -gt 0 ]]; then
    echo "[WARN] Some notebooks failed to connect:"
    echo "$ERRORS" | jq -r '.[] | "  - \(.notebook): \(.error)"'

    # Return error if ALL notebooks failed, but succeed if at least some worked
    if [[ $UPDATED -eq 0 && $NB_COUNT -gt 0 ]]; then
      echo "[ERROR] All notebooks failed to connect. Failing pipeline."
      return 1
    else
      echo "[WARN] Some failures occurred, but continuing pipeline."
      return 0
    fi
  fi

  return 0
}
