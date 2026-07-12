#!/usr/bin/env bash
set -euo pipefail

# Connect all semantic models in a Fabric workspace to a Lakehouse SQL endpoint
# Usage:
#   ./connect-semantic-models.sh "<model-workspace>" "<lakehouse-name-or-guid>" ["<lakehouse-workspace>"]
#
# The optional 3rd argument allows the lakehouse to live in a different workspace
# from the semantic models (cross-workspace connection). Defaults to the model workspace.

fabric_connect_semantic_models_to_lakehouse() {
  # ---- Args ----
  if [[ $# -lt 2 ]]; then
    echo "[ERROR] Usage: fabric_connect_semantic_models_to_lakehouse <workspace-name-or-id> <lakehouse-name-or-id> [<lakehouse-workspace-name-or-id>]"
    return 1
  fi
  local WS_ARG="$1" LH_ARG="$2" LH_WS_ARG="${3:-$1}"

  # ---- Dependencies ----
  for c in az curl jq; do
    command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; return 1; }
  done

  # ---- Helpers ----
  _trim() { printf "%s" "${1:-}" | tr -d '[:space:]'; }
  _is_guid() { [[ "${1:-}" =~ ^[0-9a-fA-F-]{36}$ ]]; }

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

  # ---- Resolve Lakehouse Workspace ID (may differ from model workspace for cross-workspace connections) ----
  local LH_WS_ID=""
  if [[ "$LH_WS_ARG" == "$WS_ARG" ]]; then
    LH_WS_ID="$WS_ID"
    echo "[INFO] Lakehouse workspace: same as model workspace"
  elif _is_guid "$LH_WS_ARG"; then
    LH_WS_ID="$(_trim "$LH_WS_ARG")"
    echo "[INFO] Lakehouse workspace treated as GUID: $LH_WS_ID"
  else
    echo "[INFO] Resolving lakehouse workspace by name (case-insensitive): $LH_WS_ARG"
    local lh_url="https://api.fabric.microsoft.com/v1/workspaces" lh_cont=""
    while : ; do
      local lh_js; lh_js="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "${lh_url}${lh_cont}")"
      LH_WS_ID="$(printf '%s' "$lh_js" | jq -r --arg n "$LH_WS_ARG" '.value[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
      LH_WS_ID="$(_trim "$LH_WS_ID")"
      [[ -n "$LH_WS_ID" ]] && break
      local lh_token; lh_token="$(printf '%s' "$lh_js" | jq -r '.continuationToken // empty')"
      [[ -z "$lh_token" || "$lh_token" == "null" ]] && break
      lh_cont="?continuationToken=$lh_token"
    done
  fi
  if [[ -z "$LH_WS_ID" ]]; then echo "[ERROR] Lakehouse workspace not found: $LH_WS_ARG"; return 1; fi
  echo "[OK] Lakehouse Workspace ID: $LH_WS_ID"

  # ---- Resolve Lakehouse ID & Get SQL Endpoint ----
  local LH_ID="" LH_NAME="" SQL_ENDPOINT_ID=""
  echo "[INFO] Resolving lakehouse: $LH_ARG"
  local LH_LIST_JSON
  LH_LIST_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "https://api.fabric.microsoft.com/v1/workspaces/$LH_WS_ID/lakehouses")"

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

  # Get SQL Endpoint ID (each lakehouse has an associated SQL endpoint with same name)
  echo "[INFO] Looking for SQL endpoint associated with lakehouse..."
  local ITEMS_JSON
  ITEMS_JSON="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" "https://api.fabric.microsoft.com/v1/workspaces/$LH_WS_ID/items")"

  # SQL endpoints have type "SQLEndpoint" and are automatically created with lakehouses
  SQL_ENDPOINT_ID="$(printf '%s' "$ITEMS_JSON" | jq -r --arg lhName "$LH_NAME" '.value[] | select(.type=="SQLEndpoint" and .displayName==$lhName) | .id' | head -n1)"
  SQL_ENDPOINT_ID="$(_trim "$SQL_ENDPOINT_ID")"

  if [[ -z "$SQL_ENDPOINT_ID" || "$SQL_ENDPOINT_ID" == "null" ]]; then
    echo "[WARN] SQL Endpoint not found for lakehouse '$LH_NAME'. Semantic models cannot be connected."
    echo "[INFO] SQL endpoints are automatically created with lakehouses. Ensure the lakehouse has been properly initialized."
    return 0
  fi
  echo "[OK] SQL Endpoint ID: $SQL_ENDPOINT_ID"

  # Get SQL Endpoint connection string
  echo "[INFO] Getting SQL endpoint connection string..."
  local SQL_ENDPOINT_CONN_STRING
  SQL_ENDPOINT_CONN_STRING="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
    "https://api.fabric.microsoft.com/v1/workspaces/$LH_WS_ID/sqlEndpoints/$SQL_ENDPOINT_ID/connectionString" | \
    jq -r '.connectionString // empty')"

  if [[ -z "$SQL_ENDPOINT_CONN_STRING" || "$SQL_ENDPOINT_CONN_STRING" == "null" ]]; then
    echo "[WARN] Could not get SQL endpoint connection string. Using fallback."
    SQL_ENDPOINT_CONN_STRING="${LH_WS_ID}-${SQL_ENDPOINT_ID}.datawarehouse.fabric.microsoft.com"
  fi
  echo "[OK] SQL Endpoint connection string: $SQL_ENDPOINT_CONN_STRING"

  # ---- List Semantic Models ----
  echo "[INFO] Listing semantic models in workspace…"
  local SEMANTIC_MODELS_JSON SM_COUNT
  SEMANTIC_MODELS_JSON="$(printf '%s' "$ITEMS_JSON" | jq '[.value[] | select(.type=="SemanticModel")]')"
  SM_COUNT="$(printf '%s' "$SEMANTIC_MODELS_JSON" | jq 'length')"
  echo "[OK] Semantic models found: $SM_COUNT"

  if [[ "$SM_COUNT" -eq 0 ]]; then
    echo "[INFO] No semantic models found. Nothing to connect."
    return 0
  fi

  echo "[INFO] Semantic model names:"
  printf '%s' "$SEMANTIC_MODELS_JSON" | jq -r '.[].displayName' | sed 's/^/ - /'

  # ---- Connect Each Semantic Model to SQL Endpoint ----
  local CONNECTED=0 FAILED=0 SKIPPED=0

  while IFS= read -r sm_id; do
    [[ -z "$sm_id" || "$sm_id" == "null" ]] && continue

    local sm_name
    sm_name="$(printf '%s' "$SEMANTIC_MODELS_JSON" | jq -r --arg id "$sm_id" '.[] | select(.id==$id) | .displayName')"

    echo "[INFO] Processing semantic model: $sm_name (ID: $sm_id)"

    # Build connection paths using the SQL Endpoint (not the Lakehouse directly)
    # Direct Lake semantic models must connect via the SQL analytics endpoint
    local ONELAKE_PATH="onelake.dfs.fabric.microsoft.com/$WS_ID/$SQL_ENDPOINT_ID"
    local ONELAKE_HTTPS_PATH="https://onelake.dfs.fabric.microsoft.com/$WS_ID/$SQL_ENDPOINT_ID/"
    local CONNECTION_STRING="Data Source=powerbi://api.powerbi.com/v1.0/myorg/$WS_ID;Initial Catalog=$SQL_ENDPOINT_ID"

    echo "[INFO]   Target lakehouse: $LH_NAME"
    echo "[INFO]   Target SQL Endpoint ID: $SQL_ENDPOINT_ID"
    echo "[INFO]   OneLake path: ${ONELAKE_HTTPS_PATH}"

    # Method 0: Try updateDefinition API (for DirectLake models)
    # This updates the actual TMDL definition to replace old lakehouse IDs
    # Preserves all tables, columns, measures, relationships, etc.
    echo "[INFO]   Attempting Method 0: updateDefinition API (updates TMDL for DirectLake models)..."

    # Get current definition
    local GET_DEF_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/semanticModels/$sm_id/getDefinition"
    local DEF_HTTP_CODE
    DEF_HTTP_CODE="$(curl -sS -X POST "$GET_DEF_URL" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{}' \
      -w "%{http_code}" \
      -D /tmp/sm_getdef_headers.txt \
      -o /tmp/sm_getdef.json 2>/dev/null || echo "000")"

    if [[ "$DEF_HTTP_CODE" == "202" ]]; then
      # Poll for getDefinition completion
      local DEF_LOCATION
      DEF_LOCATION="$(grep -i '^Location:' /tmp/sm_getdef_headers.txt | awk '{print $2}' | tr -d '\r')"

      echo "[INFO]   Waiting for definition retrieval..."
      local poll_attempts=0
      while [[ $poll_attempts -lt 30 ]]; do
        sleep 2
        local poll_status
        poll_status="$(curl -sS -X GET "$DEF_LOCATION" \
          -H "Authorization: Bearer $FABRIC_TOKEN" \
          -o /tmp/sm_getdef_poll.json \
          -w "%{http_code}" 2>/dev/null || echo "000")"

        if [[ "$poll_status" == "200" ]]; then
          local op_status
          op_status="$(jq -r '.status // empty' /tmp/sm_getdef_poll.json)"

          if [[ "$op_status" == "Succeeded" ]]; then
            echo "[INFO]   Definition retrieved successfully"

            # Get result from operations endpoint
            local OPERATION_ID
            OPERATION_ID="$(echo "$DEF_LOCATION" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)"

            curl -sS -X GET "https://api.fabric.microsoft.com/v1/operations/$OPERATION_ID/result" \
              -H "Authorization: Bearer $FABRIC_TOKEN" \
              -o /tmp/sm_definition.json 2>/dev/null

            # Count the parts in the definition for verification
            local PARTS_COUNT
            PARTS_COUNT="$(jq -r '.definition.parts | length' /tmp/sm_definition.json 2>/dev/null || echo "0")"
            echo "[INFO]   Definition has $PARTS_COUNT parts"

            # Find old lakehouse ID from definition (supports both OneLake URLs and SQL Database connections)
            # Check in all parts, not just expressions
            local OLD_LH_ID="" OLD_WS_ID="" OLD_SQL_SERVER="" FOUND_CONNECTION=false
            local ALL_DECODED
            ALL_DECODED="$(jq -r '.definition.parts[].payload' /tmp/sm_definition.json 2>/dev/null | \
              while read -r payload; do
                echo "$payload" | base64 -d 2>/dev/null
              done)"

            # Try to find OneLake URL pattern first
            local OLD_ONELAKE_URL
            OLD_ONELAKE_URL="$(echo "$ALL_DECODED" | grep -oE 'onelake\.dfs\.fabric\.microsoft\.com/[0-9a-f-]{36}/[0-9a-f-]{36}' | head -1 || true)"

            if [[ -n "$OLD_ONELAKE_URL" ]]; then
              OLD_WS_ID="$(echo "$OLD_ONELAKE_URL" | cut -d'/' -f2)"
              OLD_LH_ID="$(echo "$OLD_ONELAKE_URL" | cut -d'/' -f3)"
              echo "[INFO]   Found OneLake reference: $OLD_ONELAKE_URL"
              FOUND_CONNECTION=true
            else
              # Try to find SQL Database connection pattern: Sql.Database("server", "lakehouse-id")
              # Extract both server and lakehouse ID
              local SQL_DB_LINE
              SQL_DB_LINE="$(echo "$ALL_DECODED" | grep -E 'Sql\.Database\(' | head -1 || true)"
              if [[ -n "$SQL_DB_LINE" ]]; then
                OLD_SQL_SERVER="$(echo "$SQL_DB_LINE" | sed -n 's/.*Sql\.Database("\([^"]*\)".*/\1/p')"
                OLD_LH_ID="$(echo "$SQL_DB_LINE" | grep -oE '"[0-9a-f-]{36}"' | tail -1 | tr -d '"' || true)"
                if [[ -n "$OLD_LH_ID" ]]; then
                  echo "[INFO]   Found SQL Database connection"
                  echo "[INFO]   Old SQL server: $OLD_SQL_SERVER"
                  echo "[INFO]   Old lakehouse ID: $OLD_LH_ID"
                  FOUND_CONNECTION=true
                fi
              fi
            fi

            if [[ "$FOUND_CONNECTION" == true && -n "$OLD_LH_ID" ]]; then
              echo "[INFO]   New SQL Endpoint ID: $SQL_ENDPOINT_ID"
              [[ -n "$OLD_WS_ID" ]] && echo "[INFO]   New workspace ID: $WS_ID"
              [[ -n "$OLD_SQL_SERVER" ]] && echo "[INFO]   New SQL server: $SQL_ENDPOINT_CONN_STRING"

              # Check if already pointing to the correct SQL endpoint
              local ALREADY_CORRECT=false
              if [[ "$OLD_LH_ID" == "$SQL_ENDPOINT_ID" ]]; then
                if [[ -z "$OLD_SQL_SERVER" ]] || [[ "$OLD_SQL_SERVER" == "$SQL_ENDPOINT_CONN_STRING" ]]; then
                  if [[ -z "$OLD_WS_ID" ]] || [[ "$OLD_WS_ID" == "$WS_ID" ]]; then
                    ALREADY_CORRECT=true
                  fi
                fi
              fi

              if [[ "$ALREADY_CORRECT" == true ]]; then
                echo "[INFO]   Model already points to the correct lakehouse and SQL endpoint, skipping..."
                CONNECTED=$((CONNECTED + 1))
                continue 3  # Exit poll loop and continue to next semantic model
              fi

              # Update ALL parts - replace old lakehouse/workspace IDs and SQL server with new ones
              # This preserves tables, columns, measures, relationships etc.
              echo "[INFO]   Updating definition with new lakehouse connection..."
              local UPDATED_PARTS
              if [[ -n "$OLD_WS_ID" ]]; then
                # Replace both workspace and item IDs (OneLake format)
                # Use SQL_ENDPOINT_ID since Direct Lake models reference the SQL endpoint, not the lakehouse
                UPDATED_PARTS="$(jq --arg old_ws "$OLD_WS_ID" --arg old_lh "$OLD_LH_ID" --arg new_ws "$WS_ID" --arg new_lh "$SQL_ENDPOINT_ID" '
                  .definition.parts | map(
                    .payload = (.payload | @base64d |
                      gsub($old_ws; $new_ws) |
                      gsub($old_lh; $new_lh) |
                      @base64)
                  )
                ' /tmp/sm_definition.json 2>/dev/null)"
              elif [[ -n "$OLD_SQL_SERVER" ]]; then
                # Replace SQL server and endpoint ID (SQL Database format)
                UPDATED_PARTS="$(jq --arg old_server "$OLD_SQL_SERVER" --arg old_lh "$OLD_LH_ID" --arg new_server "$SQL_ENDPOINT_CONN_STRING" --arg new_lh "$SQL_ENDPOINT_ID" '
                  .definition.parts | map(
                    .payload = (.payload | @base64d |
                      gsub($old_server; $new_server) |
                      gsub($old_lh; $new_lh) |
                      @base64)
                  )
                ' /tmp/sm_definition.json 2>/dev/null)"
              else
                # Replace only item ID (fallback) — use SQL endpoint ID
                UPDATED_PARTS="$(jq --arg old_lh "$OLD_LH_ID" --arg new_lh "$SQL_ENDPOINT_ID" '
                  .definition.parts | map(
                    .payload = (.payload | @base64d |
                      gsub($old_lh; $new_lh) |
                      @base64)
                  )
                ' /tmp/sm_definition.json 2>/dev/null)"
              fi

              if [[ -n "$UPDATED_PARTS" && "$UPDATED_PARTS" != "null" ]]; then
                # Verify the updated parts count matches
                local UPDATED_PARTS_COUNT
                UPDATED_PARTS_COUNT="$(echo "$UPDATED_PARTS" | jq 'length' 2>/dev/null || echo "0")"
                echo "[INFO]   Updated parts count: $UPDATED_PARTS_COUNT (should match $PARTS_COUNT)"

                if [[ "$UPDATED_PARTS_COUNT" != "$PARTS_COUNT" ]]; then
                  echo "[WARN]   Parts count mismatch! Skipping updateDefinition to avoid data loss."
                else
                  # Build update payload with ALL parts (write to file to avoid argument length limits)
                  echo "$UPDATED_PARTS" | jq '{definition: {parts: .}}' > /tmp/sm_update_payload.json

                  # Debug: show payload size
                  echo "[DEBUG]   Update payload size: $(wc -c < /tmp/sm_update_payload.json) bytes"

                  # Send update
                  local UPDATE_DEF_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/semanticModels/$sm_id/updateDefinition"
                  local UPDATE_HTTP_CODE
                  UPDATE_HTTP_CODE="$(curl -sS -X POST "$UPDATE_DEF_URL" \
                    -H "Authorization: Bearer $FABRIC_TOKEN" \
                    -H "Content-Type: application/json" \
                    -d @/tmp/sm_update_payload.json \
                    -w "%{http_code}" \
                    -D /tmp/sm_updatedef_headers.txt \
                    -o /tmp/sm_updatedef.json 2>/dev/null || echo "000")"

                  if [[ "$UPDATE_HTTP_CODE" == "202" ]]; then
                    # Poll for update completion
                    local UPDATE_LOCATION
                    UPDATE_LOCATION="$(grep -i '^Location:' /tmp/sm_updatedef_headers.txt | awk '{print $2}' | tr -d '\r')"

                    echo "[INFO]   Waiting for definition update to complete..."
                    local update_poll=0
                    while [[ $update_poll -lt 60 ]]; do
                      sleep 3
                      local update_status
                      update_status="$(curl -sS -X GET "$UPDATE_LOCATION" \
                        -H "Authorization: Bearer $FABRIC_TOKEN" \
                        -o /tmp/sm_updatedef_poll.json \
                        -w "%{http_code}" 2>/dev/null || echo "000")"

                      if [[ "$update_status" == "200" ]]; then
                        local update_op_status
                        update_op_status="$(jq -r '.status // empty' /tmp/sm_updatedef_poll.json)"

                        if [[ "$update_op_status" == "Succeeded" ]]; then
                          echo "[OK]   ✓ Connected '$sm_name' via updateDefinition"
                          CONNECTED=$((CONNECTED + 1))
                          continue 3  # Skip to next semantic model (exit 2 loops)
                        elif [[ "$update_op_status" == "Failed" ]]; then
                          local fail_msg
                          fail_msg="$(jq -r '.error.message // .message // "Unknown error"' /tmp/sm_updatedef_poll.json)"
                          echo "[WARN]   updateDefinition failed: $fail_msg"
                          break
                        fi
                      fi
                      update_poll=$((update_poll + 1))
                    done
                  else
                    echo "[WARN]   updateDefinition request failed (HTTP $UPDATE_HTTP_CODE)"
                    [[ -f /tmp/sm_updatedef.json ]] && cat /tmp/sm_updatedef.json
                  fi
                fi
              else
                echo "[WARN]   Failed to update parts"
              fi
            else
              echo "[INFO]   No lakehouse connection references found in definition (checked OneLake URLs and SQL Database connections)"
            fi
            break
          elif [[ "$op_status" == "Failed" ]]; then
            echo "[WARN]   getDefinition operation failed"
            break
          fi
        fi
        poll_attempts=$((poll_attempts + 1))
      done
    else
      echo "[INFO]   getDefinition not supported (HTTP $DEF_HTTP_CODE)"
    fi

    # Method 1: Try updateParameters API (for models with existing parameters)
    echo "[INFO]   Attempting Method 1: updateParameters API..."
    local UPDATE_PARAMS_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/semanticModels/$sm_id/updateParameters"
    local PARAMS_BODY
    PARAMS_BODY="$(jq -n \
      --arg server "powerbi://api.powerbi.com/v1.0/myorg/$WS_ID" \
      --arg database "$SQL_ENDPOINT_ID" \
      '{
        updateDetails: [
          {
            name: "Server",
            newValue: $server
          },
          {
            name: "Database",
            newValue: $database
          }
        ]
      }')"

    local HTTP_CODE
    HTTP_CODE="$(curl -sS -o /tmp/sm_params.json -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$UPDATE_PARAMS_URL" \
      -d "$PARAMS_BODY" 2>/dev/null || echo "000")"

    if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
      echo "[OK]   ✓ Connected '$sm_name' via updateParameters"
      CONNECTED=$((CONNECTED + 1))
      continue
    fi

    # Method 2: Try updateDatasources API (for direct data source management)
    echo "[INFO]   Method 1 failed (HTTP $HTTP_CODE). Trying Method 2: updateDatasources API..."
    local UPDATE_DS_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/semanticModels/$sm_id/updateDatasources"
    local DS_BODY
    DS_BODY="$(jq -n \
      --arg connString "$CONNECTION_STRING" \
      '{
        updateDetails: [
          {
            datasourceType: "AnalysisServices",
            connectionDetails: {
              server: "powerbi://api.powerbi.com/v1.0/myorg/'$WS_ID'",
              database: "'$SQL_ENDPOINT_ID'"
            }
          }
        ]
      }')"

    HTTP_CODE="$(curl -sS -o /tmp/sm_ds.json -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$UPDATE_DS_URL" \
      -d "$DS_BODY" 2>/dev/null || echo "000")"

    if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
      echo "[OK]   ✓ Connected '$sm_name' via updateDatasources"
      CONNECTED=$((CONNECTED + 1))
      continue
    fi

    # Method 3: Try rebind API (for models that support rebinding)
    echo "[INFO]   Method 2 failed (HTTP $HTTP_CODE). Trying Method 3: rebind API..."
    local REBIND_URL="https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/semanticModels/$sm_id/rebind"
    local REBIND_BODY
    REBIND_BODY="$(jq -n \
      --arg lhId "$SQL_ENDPOINT_ID" \
      '{
        itemId: $lhId,
        itemType: "SQLEndpoint"
      }')"

    HTTP_CODE="$(curl -sS -o /tmp/sm_rebind.json -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -X POST "$REBIND_URL" \
      -d "$REBIND_BODY" 2>/dev/null || echo "000")"

    if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
      echo "[OK]   ✓ Connected '$sm_name' via rebind"
      CONNECTED=$((CONNECTED + 1))
      continue
    fi

    # All methods failed
    echo "[WARN] ⚠ All connection methods failed for '$sm_name'"
    echo "[INFO]   Last attempt: rebind API returned HTTP $HTTP_CODE"
    [[ -f /tmp/sm_rebind.json ]] && echo "[INFO]   Response:" && cat /tmp/sm_rebind.json && echo
    echo "[INFO]   Manual configuration may be required:"
    echo "[INFO]   1. Open semantic model '$sm_name' in Fabric"
    echo "[INFO]   2. Go to Settings > Data source credentials"
    echo "[INFO]   3. Select SQL Endpoint: $LH_NAME"
    echo "[INFO]   4. Enter workspace connection: powerbi://api.powerbi.com/v1.0/myorg/$WS_ID"
    SKIPPED=$((SKIPPED + 1))

  done < <(printf '%s' "$SEMANTIC_MODELS_JSON" | jq -r '.[].id')

  # Cleanup temp files
  rm -f /tmp/sm_params.json /tmp/sm_ds.json /tmp/sm_rebind.json /tmp/sm_getdef*.json /tmp/sm_definition.json /tmp/sm_updatedef*.json /tmp/sm_getdef_headers.txt /tmp/sm_updatedef_headers.txt 2>/dev/null || true

  # ---- Summary ----
  echo ""
  echo "============================================"
  echo "Semantic Model Connection Summary"
  echo "============================================"
  echo "  Total models: $SM_COUNT"
  echo "  Connected: $CONNECTED"
  echo "  Skipped: $SKIPPED"
  echo "  Failed: $FAILED"
  echo "============================================"

  if [[ $FAILED -gt 0 ]]; then
    echo "[WARN] Some semantic models failed to connect. Manual configuration may be required."
    return 0  # Don't fail the pipeline, just warn
  fi

  return 0
}