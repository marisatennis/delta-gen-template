
#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Microsoft Fabric: Assign an Entra Group to a Workspace Role
# Usage:
#   assign_role_to_workspace "<workspace_name>" "<group_identifier>" "<role>"
# Where:
#   <group_identifier> is either the group's Object ID (GUID) or its Display Name
#   <role> is one of: Admin | Member | Contributor | Viewer
# ------------------------------------------------------------

FABRIC_SCOPE="https://api.fabric.microsoft.com/.default"
FABRIC_BASE="https://api.fabric.microsoft.com/v1"

err() { echo "ERROR: $*" >&2; }

get_fabric_token() {
  az account get-access-token --scope "$FABRIC_SCOPE" --query accessToken -o tsv
}

# Get Workspace ID by display name
get_workspace_id() {
  local ws_name="$1"
  local token; token="$(get_fabric_token)"

  local ws_id
  ws_id="$(
    curl -fsSL "${FABRIC_BASE}/workspaces" \
      -H "Authorization: Bearer ${token}" \
      -H "Accept: application/json" \
    | jq -r --arg name "$ws_name" '.value | map(select(.displayName == $name)) | .[0].id // empty'
  )"

  if [[ -z "${ws_id}" ]]; then
    err "Workspace '${ws_name}' not found."
    return 1
  fi
  echo "${ws_id}"
}

# Resolve Entra group object ID from either GUID or Display Name
resolve_group_id() {
  local identifier="$1"
  local gid=""

  if [[ "$identifier" =~ ^[0-9a-fA-F-]{36}$ ]]; then
    # GUID provided
    gid="$(az ad group show --group "$identifier" --query id -o tsv || true)"
  else
    # Display name provided
    gid="$(az ad group show --group "$identifier" --query id -o tsv || true)"
  fi

  [[ -n "$gid" ]] || { err "Group not found: '$identifier'. Provide display name or object ID (GUID)."; return 1; }
  echo "$gid"
}

# Assign role to GROUP in workspace
assign_role_to_workspace() {
  local workspace_name="$1"
  local group_identifier="$2"  # GUID or display name
  local role="$3"              # Admin | Member | Contributor | Viewer

  # Basic validations
  if [[ -z "${workspace_name}" || -z "${group_identifier}" || -z "${role}" ]]; then
    echo "Usage: assign_role_to_workspace <workspace_name> <group_identifier> <role>"
    return 1
  fi

  case "$role" in
    Admin|Member|Contributor|Viewer) ;;
    *) err "Invalid role '$role'. Use one of: Admin|Member|Contributor|Viewer"; return 1 ;;
  esac

  echo "Resolving workspace '${workspace_name}'..."
  local workspace_id; workspace_id="$(get_workspace_id "$workspace_name")"
  echo "Workspace ID: ${workspace_id}"

  echo "Resolving group '${group_identifier}'..."
  local group_id; group_id="$(resolve_group_id "$group_identifier")"
  echo "Group Object ID: ${group_id}"

  echo "Acquiring Fabric token..."
  local token; token="$(get_fabric_token)"

  local url="${FABRIC_BASE}/workspaces/${workspace_id}/roleAssignments"

  # Build JSON body safely
  local body
  body="$(jq -n --arg id "$group_id" --arg role "$role" \
             '{principal:{id:$id,type:"Group"},role:$role}')"

  echo "Assigning role '${role}' to Group (${group_id}) in workspace '${workspace_name}' (${workspace_id})..."
  # Capture HTTP status and body
  local resp http_code body_json
  resp="$(curl -sS -X POST "$url" \
            -H "Authorization: Bearer ${token}" \
            -H "Content-Type: application/json" \
            -d "$body" \
            -w "\n%{http_code}")"

  http_code="$(tail -n1 <<< "$resp")"
  body_json="$(sed '$d' <<< "$resp")"

  echo "HTTP status: ${http_code}"

  if [[ "$http_code" == "201" ]]; then
    echo "✅ Role assigned successfully."
    # Pretty print success payload (if API returns one)
    echo "$body_json" | jq . 2>/dev/null || echo "$body_json"
  else
    echo "❌ Failed to assign role."
    # Show raw/body
    echo "$body_json" | jq . 2>/dev/null || echo "$body_json"

    # Extract common error fields
    local err_msg err_code
    err_msg="$(jq -r '.error.message // .message // .Error.Message // empty' <<< "$body_json" 2>/dev/null || true)"
    err_code="$(jq -r '.error.code // .code // .Error.Code // empty' <<< "$body_json" 2>/dev/null || true)"

    if [[ -n "$err_msg" || -n "$err_code" ]]; then
      echo "— Error code: ${err_code:-N/A}"
      echo "— Error message: ${err_msg:-N/A}"
    fi
    #return 1
  fi
}

