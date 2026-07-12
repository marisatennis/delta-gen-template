
#!/usr/bin/env bash
set -euo pipefail

FABRIC_BASE="https://api.fabric.microsoft.com/v1"
# Prefer resource audience; scope also works if you've standardized on it.
TOKEN="$(az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv)"

if [[ -z "${TOKEN}" ]]; then
  echo "ERROR: Failed to acquire Fabric access token." >&2
  exit 1
fi

http_post_json() {
  local url="$1" json="$2"
  curl -sS -X POST "${url}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -d "${json}" \
    -w "\n%{http_code}"
}

http_get() {
  local url="$1"
  curl -sS -X GET "${url}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Accept: application/json" \
    -w "\n%{http_code}"
}

poll_operation() {
  local op_id="$1" max_attempts="${2:-120}" sleep_secs="${3:-5}"
  local attempt=0
  while [[ "${attempt}" -lt "${max_attempts}" ]]; do
    local resp body http_status status err_msg
    resp="$(http_get "${FABRIC_BASE}/operations/${op_id}")"
    http_status="$(tail -n1 <<< "${resp}")"
    body="$(sed '$d' <<< "${resp}")"

    if [[ "${http_status}" -lt 200 || "${http_status}" -ge 300 ]]; then
      echo "ERROR: LRO polling failed (HTTP ${http_status}). Body: ${body}" >&2
      return 1
    fi

    status="$(jq -r '.status // empty' <<< "${body}")"
    err_msg="$(jq -r '.error.message // empty' <<< "${body}")"
    echo "Operation status: ${status}" >&2

    case "${status}" in
      Succeeded) return 0 ;;
      Failed|Cancelled)
        echo "ERROR: Operation ${status}. ${err_msg}" >&2
        return 1
        ;;
    esac

    sleep "${sleep_secs}"
    attempt=$((attempt+1))
  done

  echo "ERROR: Operation ${op_id} timed out." >&2
  return 1
}

resolve_workspace_id_by_name() {
  local name="$1"
  echo "Resolving workspace ID for displayName: '${name}'..." >&2

  local resp body http_status
  resp="$(http_get "${FABRIC_BASE}/workspaces")"
  http_status="$(tail -n1 <<< "${resp}")"
  body="$(sed '$d' <<< "${resp}")"

  if [[ "${http_status}" -lt 200 || "${http_status}" -ge 300 ]]; then
    echo "ERROR: List workspaces failed (HTTP ${http_status}). Body: ${body}" >&2
    return 1
  fi

  local id
  id="$(jq -r ".value[] | select(.displayName==\"${name}\") | .id" <<< "${body}")"
  if [[ -z "${id}" || "${id}" == "null" ]]; then
    echo "ERROR: Workspace '${name}' not found or insufficient permissions." >&2
    return 1
  fi

  printf "%s" "${id}"
}

has_uncommitted_changes() {
  local ws_id="$1"
  local resp body http_status
  resp="$(http_get "${FABRIC_BASE}/workspaces/${ws_id}/git/status")"
  http_status="$(tail -n1 <<< "${resp}")"
  body="$(sed '$d' <<< "${resp}")"

  if [[ "${http_status}" -lt 200 || "${http_status}" -ge 300 ]]; then
    echo "WARN: Git status check failed (HTTP ${http_status}). Proceeding to commit anyway." >&2
    echo "true"
    return 0
  fi

  local uncommitted_count
  uncommitted_count="$(jq -r '.uncommittedChanges | length' <<< "${body}")"
  if [[ "${uncommitted_count}" -gt 0 ]]; then
    echo "true"
  else
    echo "false"
  fi
}

# Public function: commit by workspace name
commit_fabric_workspace_to_git() {
  local workspace_name="$1"
  local comment="${2:-Git Commit from DevOps Pipeline}"
  local skip_if_no_changes="${3:-true}"

  # Resolve ID cleanly
  local ws_id
  ws_id="$(resolve_workspace_id_by_name "${workspace_name}")"
  ws_id="$(printf "%s" "${ws_id}" | tr -d '\r\n')"
  echo "Workspace ID: ${ws_id}" >&2

  # Optional: skip if no changes
  if [[ "${skip_if_no_changes}" == "true" ]]; then
    echo "Checking for uncommitted changes..." >&2
    if [[ "$(has_uncommitted_changes "${ws_id}")" == "false" ]]; then
      echo "No uncommitted changes. Skipping commit." >&2
      return 0
    fi
  fi

  # REQUIRED FIELDS: mode and comment (per API spec). Use mode="All" to commit everything.
  # For selective commits, set mode="Selective" and provide "items" from /git/status.
  local payload
  payload="$(jq -n --arg c "${comment}" --arg m "All" '{mode: $m, comment: $c}')"

  echo "Submitting commit to Git for workspace: ${ws_id}" >&2
  local resp body http_status
  resp="$(http_post_json "${FABRIC_BASE}/workspaces/${ws_id}/git/commitToGit" "${payload}")"
  http_status="$(tail -n1 <<< "${resp}")"
  body="$(sed '$d' <<< "${resp}")"

  echo "HTTP ${http_status}" >&2
  printf "Response: %s\n" "${body}" >&2

  if [[ "${http_status}" -lt 200 || "${http_status}" -ge 300 ]]; then
    # Check if error is "NoChangesToCommit" - this is not a failure
    local error_code
    error_code="$(jq -r '.errorCode // empty' <<< "${body}")"
    if [[ "${error_code}" == "NoChangesToCommit" ]]; then
      echo "INFO: No changes to commit. This is expected." >&2
      return 0
    fi

    echo "ERROR: commitToGit failed (HTTP ${http_status})." >&2
    return 1
  fi

  local op_id
  op_id="$(jq -r '.operationId // empty' <<< "${body}")"
  if [[ -n "${op_id}" && "${op_id}" != "null" ]]; then
    echo "Polling commit long-running operation: ${op_id}" >&2
    poll_operation "${op_id}"
  else
    echo "No operationId returned; commit completed synchronously." >&2
  fi

  echo "Commit finished." >&2
}
