#!/usr/bin/env bash
set -euo pipefail

# Update OneLake shortcuts in a target lakehouse to point at a source lakehouse.
# Usage:
#   fabric_update_shortcuts_to_lakehouse "<target-ws>" "<target-lh>" "<source-ws>" "<source-lh>"
#
# Requires: az, curl, jq

# Helper: fetch all pages from a Fabric REST API list endpoint.
# Accumulates the ".value" arrays across pages into a single JSON array.
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

    # Fabric uses continuationUri for pagination
    URL="$(echo "$PAGE" | jq -r '.continuationUri // empty')"
  done

  echo "$ALL_VALUES"
}

fabric_update_shortcuts_to_lakehouse() {
  if [[ $# -lt 4 ]]; then
    echo "[ERROR] Usage: fabric_update_shortcuts_to_lakehouse <target-workspace> <target-lakehouse> <source-workspace> <source-lakehouse> [shortcut-name]"
    return 1
  fi

  local TARGET_WS_NAME="$1"
  local TARGET_LH_NAME="$2"
  local SOURCE_WS_NAME="$3"
  local SOURCE_LH_NAME="$4"
  local SHORTCUT_NAME_FILTER="${5:-}"  # Optional: only update shortcuts with this exact name
  local API_BASE="https://api.fabric.microsoft.com/v1"

  for c in az curl jq; do
    command -v "$c" >/dev/null 2>&1 || { echo "[ERROR] Missing '$c'"; return 1; }
  done

  echo "============================================"
  echo "Update OneLake Shortcuts"
  echo "============================================"
  echo "Target workspace: ${TARGET_WS_NAME}"
  echo "Target lakehouse: ${TARGET_LH_NAME}"
  echo "Source workspace: ${SOURCE_WS_NAME}"
  echo "Source lakehouse: ${SOURCE_LH_NAME}"
  echo "============================================"

  echo "[INFO] Acquiring Fabric token…"
  local FABRIC_TOKEN
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)"

  echo "[INFO] Resolving workspaces (fetching all pages)…"
  local WS_ALL
  WS_ALL="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces")"

  local TARGET_WS_ID SOURCE_WS_ID
  TARGET_WS_ID="$(echo "$WS_ALL" | jq -r --arg n "$TARGET_WS_NAME" '.[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
  SOURCE_WS_ID="$(echo "$WS_ALL" | jq -r --arg n "$SOURCE_WS_NAME" '.[] | select((.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"

  if [[ -z "$TARGET_WS_ID" || "$TARGET_WS_ID" == "null" ]]; then
    echo "[ERROR] Target workspace not found: $TARGET_WS_NAME"
    return 1
  fi
  if [[ -z "$SOURCE_WS_ID" || "$SOURCE_WS_ID" == "null" ]]; then
    echo "[ERROR] Source workspace not found: $SOURCE_WS_NAME"
    return 1
  fi

  echo "[OK] Target workspace ID: $TARGET_WS_ID"
  echo "[OK] Source workspace ID: $SOURCE_WS_ID"

  echo "[INFO] Resolving lakehouses (fetching all pages)…"
  local TARGET_ITEMS_ALL SOURCE_ITEMS_ALL
  TARGET_ITEMS_ALL="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces/${TARGET_WS_ID}/items")"
  SOURCE_ITEMS_ALL="$(_fabric_list_all "$FABRIC_TOKEN" "${API_BASE}/workspaces/${SOURCE_WS_ID}/items")"

  local TARGET_LH_ID SOURCE_LH_ID
  TARGET_LH_ID="$(echo "$TARGET_ITEMS_ALL" | jq -r --arg n "$TARGET_LH_NAME" '.[] | select(.type=="Lakehouse" and (.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"
  SOURCE_LH_ID="$(echo "$SOURCE_ITEMS_ALL" | jq -r --arg n "$SOURCE_LH_NAME" '.[] | select(.type=="Lakehouse" and (.displayName|ascii_downcase)==($n|ascii_downcase)) | .id' | head -n1)"

  if [[ -z "$TARGET_LH_ID" || "$TARGET_LH_ID" == "null" ]]; then
    echo "[ERROR] Target lakehouse not found: $TARGET_LH_NAME"
    return 1
  fi
  if [[ -z "$SOURCE_LH_ID" || "$SOURCE_LH_ID" == "null" ]]; then
    echo "[ERROR] Source lakehouse not found: $SOURCE_LH_NAME"
    return 1
  fi

  echo "[OK] Target lakehouse ID: $TARGET_LH_ID"
  echo "[OK] Source lakehouse ID: $SOURCE_LH_ID"

  echo "[INFO] Fetching shortcuts from target lakehouse (fetching all pages)…"
  local SHORTCUTS_ALL
  SHORTCUTS_ALL="$(_fabric_list_all "$FABRIC_TOKEN" \
    "${API_BASE}/workspaces/${TARGET_WS_ID}/items/${TARGET_LH_ID}/shortcuts")"

  local SHORTCUT_COUNT
  SHORTCUT_COUNT="$(echo "$SHORTCUTS_ALL" | jq 'length')"

  if [[ "$SHORTCUT_COUNT" -eq 0 ]]; then
    echo "[INFO] No shortcuts found; nothing to update."
    return 0
  fi

  echo "[INFO] Found $SHORTCUT_COUNT shortcut(s)"

  local UPDATED=0 SKIPPED=0 FAILED=0

  for i in $(seq 0 $((SHORTCUT_COUNT - 1))); do
    local SC NAME SHORTCUT_PATH TYPE CURRENT_WS CURRENT_ITEM
    SC="$(echo "$SHORTCUTS_ALL" | jq -c ".[$i]")"
    NAME="$(echo "$SC" | jq -r '.name')"
    SHORTCUT_PATH="$(echo "$SC" | jq -r '.path')"
    TYPE="$(echo "$SC" | jq -r '.target.type')"

    if [[ -n "$SHORTCUT_NAME_FILTER" && "$NAME" != "$SHORTCUT_NAME_FILTER" ]]; then
      echo "[INFO] Skipping shortcut (name filter): ${SHORTCUT_PATH}/${NAME}"
      ((SKIPPED++)) || true
      continue
    fi

    if [[ "$TYPE" != "OneLake" ]]; then
      echo "[INFO] Skipping non-OneLake shortcut: ${SHORTCUT_PATH}/${NAME}"
      ((SKIPPED++)) || true
      continue
    fi

    CURRENT_WS="$(echo "$SC" | jq -r '.target.oneLake.workspaceId')"
    CURRENT_ITEM="$(echo "$SC" | jq -r '.target.oneLake.itemId')"

    if [[ "$CURRENT_WS" == "$SOURCE_WS_ID" && "$CURRENT_ITEM" == "$SOURCE_LH_ID" ]]; then
      echo "[INFO] Already mapped: ${SHORTCUT_PATH}/${NAME}"
      ((SKIPPED++)) || true
      continue
    fi

    echo "[INFO] Updating shortcut: ${SHORTCUT_PATH}/${NAME} (ws: ${CURRENT_WS} -> ${SOURCE_WS_ID}, item: ${CURRENT_ITEM} -> ${SOURCE_LH_ID})"

    # Build the create payload — strip the "type" field from target since the
    # POST endpoint infers the type from the target key (oneLake, adlsGen2, etc.)
    local CREATE_PAYLOAD
    CREATE_PAYLOAD="$(echo "$SC" | jq -c \
      --arg ws "$SOURCE_WS_ID" \
      --arg lh "$SOURCE_LH_ID" \
      '{name: .name, path: .path, target: {oneLake: (.target.oneLake | .workspaceId = $ws | .itemId = $lh)}}')"

    # To update an existing shortcut we must delete then recreate it.
    # Build the delete URL: DELETE .../shortcuts/{path}/{name}
    # The path from the API starts with "/" (e.g. "/Tables"), so strip the leading slash.
    local PATH_FOR_URL
    PATH_FOR_URL="$(echo "$SHORTCUT_PATH" | sed 's|^/||')"
    local DELETE_URL="${API_BASE}/workspaces/${TARGET_WS_ID}/items/${TARGET_LH_ID}/shortcuts/${PATH_FOR_URL}/${NAME}"

    local DELETE_RESPONSE DELETE_STATUS
    DELETE_RESPONSE="$(mktemp)"
    DELETE_STATUS="$(curl -sS -X DELETE -o "$DELETE_RESPONSE" -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      "$DELETE_URL")"
    rm -f "$DELETE_RESPONSE"

    if [[ "$DELETE_STATUS" != "200" && "$DELETE_STATUS" != "202" && "$DELETE_STATUS" != "204" && "$DELETE_STATUS" != "404" ]]; then
      echo "[ERROR] Delete failed for ${SHORTCUT_PATH}/${NAME} (HTTP $DELETE_STATUS)"
      ((FAILED++)) || true
      continue
    fi

    if [[ "$DELETE_STATUS" == "404" ]]; then
      echo "[INFO] Shortcut not found for delete (404); will attempt create anyway."
    fi

    local CREATE_RESPONSE CREATE_STATUS
    CREATE_RESPONSE="$(mktemp)"
    CREATE_STATUS="$(curl -sS -X POST -o "$CREATE_RESPONSE" -w "%{http_code}" \
      -H "Authorization: Bearer $FABRIC_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$CREATE_PAYLOAD" \
      "${API_BASE}/workspaces/${TARGET_WS_ID}/items/${TARGET_LH_ID}/shortcuts")"

    if [[ "$CREATE_STATUS" == "200" || "$CREATE_STATUS" == "201" || "$CREATE_STATUS" == "202" ]]; then
      echo "[OK] Updated: ${SHORTCUT_PATH}/${NAME}"
      ((UPDATED++)) || true
    else
      local ERROR_MSG
      ERROR_MSG="$(jq -r '.message // .error.message // "Unknown error"' "$CREATE_RESPONSE" 2>/dev/null || cat "$CREATE_RESPONSE")"
      echo "[ERROR] Recreate failed for ${SHORTCUT_PATH}/${NAME} (HTTP $CREATE_STATUS): $ERROR_MSG"
      ((FAILED++)) || true
    fi
    rm -f "$CREATE_RESPONSE"
  done

  echo ""
  echo "============================================"
  echo "Shortcut Update Summary"
  echo "============================================"
  echo "Target: ${TARGET_WS_NAME}/${TARGET_LH_NAME}"
  echo "Source: ${SOURCE_WS_NAME}/${SOURCE_LH_NAME}"
  [[ -n "$SHORTCUT_NAME_FILTER" ]] && echo "Filter: ${SHORTCUT_NAME_FILTER}"
  echo "Updated: $UPDATED"
  echo "Skipped: $SKIPPED"
  echo "Failed: $FAILED"
  echo "============================================"

  if [[ $FAILED -gt 0 ]]; then
    return 1
  fi

  return 0
}

# Allow direct invocation
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] || [[ -z "${BASH_SOURCE[0]:-}" ]]; then
  fabric_update_shortcuts_to_lakehouse "$@"
fi
