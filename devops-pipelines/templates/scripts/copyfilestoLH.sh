
#!/usr/bin/env bash
# Purpose: Upload a local folder (from checked-out repo) to Fabric OneLake (Lakehouse)
# Requires: az, curl, jq, azcopy
# Auth: Use AzureCLI@2 service connection; derive OAuth tokens via az CLI

set -euo pipefail

fabric_upload_local_folder_to_onelake() {
  # Usage:
  # fabric_upload_local_folder_to_onelake "<ws-name>" "<lh-name>" "<src-rel-path>" "<dest-subfolder-under-Files>" [--use-abfs=true|false]
  #
  # Examples:
  # fabric_upload_local_folder_to_onelake "Silver-WS-7" "Silver_Lakehouse" "datalake/inputs" "inputs" --use-abfs=true
  # fabric_upload_local_folder_to_onelake "Silver-WS-7" "Silver_Lakehouse" "datalake/inputs" "inputs" --use-abfs=false

  if [[ $# -lt 4 ]]; then
    echo "Usage: fabric_upload_local_folder_to_onelake <ws-name> <lh-name> <src-rel-path> <dest-subfolder> [--use-abfs=true|false]"
    return 1
  fi

  local WS_NAME="$1" LH_NAME="$2" SRC_REL="$3" DEST_SUB="$4"
  shift 4

  local USE_ABFS="false"
  if [[ $# -gt 0 ]]; then
    case "$1" in
      --use-abfs=true)  USE_ABFS="true" ;;
      --use-abfs=false) USE_ABFS="false" ;;
      *) echo "ERROR: Unknown option '$1'"; return 1 ;;
    esac
  fi

  # Resolve local source from checkout root
  # Build.SourcesDirectory can come in different casings; prefer Build_SourcesDirectory, then BUILD_SOURCESDIRECTORY.
  local SRC_ROOT="${Build_SourcesDirectory:-${BUILD_SOURCESDIRECTORY:-}}"
  [[ -z "${SRC_ROOT:-}" ]] && { echo "ERROR: Build.SourcesDirectory env not found"; return 1; }

  # Normalize path joins
  local SRC_DIR="${SRC_ROOT%/}/${SRC_REL#./}"
  [[ -d "$SRC_DIR" ]] || { echo "ERROR: Source folder not found: $SRC_DIR"; return 1; }

  echo "Local source folder: $SRC_DIR"

  # Acquire tokens via Azure CLI (hosted agents: use access token; MSI is not available)
  local FABRIC_TOKEN STORAGE_TOKEN
  FABRIC_TOKEN="$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)"
  STORAGE_TOKEN="$(az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv)"

  # Resolve Workspace ID
  local WS_ID
  WS_ID="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
            "https://api.fabric.microsoft.com/v1/workspaces" \
          | jq -r --arg n "$WS_NAME" '.value[] | select(.displayName==$n) | .id' | head -n1)"
  [[ -n "${WS_ID:-}" && "$WS_ID" != "null" ]] || { echo "ERROR: Workspace not found: $WS_NAME"; return 1; }

  # Resolve Lakehouse ID
  local LH_ID
  LH_ID="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
            "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/lakehouses" \
          | jq -r --arg n "$LH_NAME" '.value[] | select(.displayName==$n) | .id' | head -n1)"
  [[ -n "${LH_ID:-}" && "$LH_ID" != "null" ]] || { echo "ERROR: Lakehouse not found: $LH_NAME"; return 1; }

  # Destination URI (ABFS vs DFS HTTPS)
  local DEST_URI ABFS_FILES_PATH
  if [[ "$USE_ABFS" == "true" ]]; then
    # Get OneLake ABFS path from Lakehouse properties
    ABFS_FILES_PATH="$(curl -sS -H "Authorization: Bearer $FABRIC_TOKEN" \
                        "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/lakehouses/$LH_ID" \
                      | jq -r '.properties.oneLakeFilesPath')"
    [[ -n "${ABFS_FILES_PATH:-}" && "$ABFS_FILES_PATH" != "null" ]] || { echo "ERROR: oneLakeFilesPath not found"; return 1; }
    DEST_URI="${ABFS_FILES_PATH%/}/${DEST_SUB#/}"
  else
    # DFS HTTPS path (no ABFS resolution)
    # https://onelake.dfs.fabric.microsoft.com/{workspaceId}/{lakehouseId}.lakehouse/Files/{subfolder}
    DEST_URI="https://onelake.dfs.fabric.microsoft.com/${WS_ID}/${LH_ID}.lakehouse/Files/${DEST_SUB#/}"
  fi

  echo "Destination OneLake URI: $DEST_URI"

  # AzCopy auth using the Storage OAuth token
  #azcopy login --access-token "$STORAGE_TOKEN"

  # Upload folder recursively
  # Use quoted glob to expand files; trust Fabric domain.
  azcopy copy "${SRC_DIR}/*" "$DEST_URI" \
    --recursive=true \
    --trusted-microsoft-suffixes "fabric.microsoft.com"

  echo "SUCCESS: Uploaded contents of '$SRC_DIR' to '$DEST_URI'"
}
