#!/bin/bash
# delete-workspaces.sh
# Reusable functions for deleting Microsoft Fabric workspaces
# Used by: developer-workspace-cleanup.yml, automated-temp-workspace-cleanup.yml

set -euo pipefail

# Initialize API settings
API_BASE="${API_BASE:-https://api.fabric.microsoft.com/v1}"

# Get access token for Fabric API
get_access_token() {
  az account get-access-token --scope "https://api.fabric.microsoft.com/.default" --query accessToken -o tsv
}

# List all workspaces and return JSON
list_workspaces() {
  local token="$1"
  curl -s -X GET "${API_BASE}/workspaces" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json"
}

# Find workspace ID by exact name
# Returns workspace ID or empty string if not found
find_workspace_id() {
  local token="$1"
  local ws_name="$2"
  local workspaces_json
  
  workspaces_json=$(list_workspaces "$token")
  echo "$workspaces_json" | jq -r --arg name "$ws_name" '.value[] | select(.displayName == $name) | .id // empty'
}

# Delete a single workspace by ID
# Returns: 0 on success, 1 on failure
delete_workspace_by_id() {
  local token="$1"
  local ws_id="$2"
  local ws_name="${3:-$ws_id}"  # Optional name for logging
  
  echo "Deleting workspace '$ws_name' (ID: $ws_id)..."
  
  local response http_status body
  response=$(curl -s -X DELETE "${API_BASE}/workspaces/${ws_id}" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -w "\nHTTP_STATUS:%{http_code}")
  
  http_status=$(echo "$response" | sed -n 's/.*HTTP_STATUS:\([0-9]\+\).*/\1/p')
  body=$(echo "$response" | sed 's/HTTP_STATUS:[0-9]\+$//')
  
  if [[ "$http_status" -ge 200 && "$http_status" -lt 300 ]]; then
    echo "✅ Successfully deleted '$ws_name'"
    return 0
  else
    echo "❌ Failed to delete '$ws_name' — HTTP $http_status"
    [[ -n "$body" ]] && echo "Response: $body"
    return 1
  fi
}

# Delete workspaces by name list
# Arguments: space-separated list of workspace names
# Returns: prints summary and sets exit code
delete_workspaces_by_names() {
  local token="$1"
  shift
  local workspace_names=("$@")
  
  local success_count=0
  local fail_count=0
  local not_found_count=0
  
  for ws_name in "${workspace_names[@]}"; do
    echo "Looking for workspace: $ws_name"
    
    local ws_id
    ws_id=$(find_workspace_id "$token" "$ws_name")
    
    if [ -z "$ws_id" ]; then
      echo "⚠️  Workspace '$ws_name' not found. Skipping."
      not_found_count=$((not_found_count + 1))
      echo ""
      continue
    fi
    
    if delete_workspace_by_id "$token" "$ws_id" "$ws_name"; then
      success_count=$((success_count + 1))
    else
      fail_count=$((fail_count + 1))
    fi
    echo ""
  done
  
  echo "============================================"
  echo "Deletion Summary:"
  echo "  Successful: $success_count"
  echo "  Failed: $fail_count"
  echo "  Not Found: $not_found_count"
  echo "============================================"
  
  # Return non-zero if any deletions failed
  [ "$fail_count" -eq 0 ]
}

# Delete workspaces from a JSON array
# Input: JSON array with objects containing "id" and "name" fields
delete_workspaces_from_json() {
  local token="$1"
  local json_list="$2"
  
  local delete_count
  delete_count=$(echo "$json_list" | jq 'length')
  
  if [ "$delete_count" -eq 0 ]; then
    echo "No workspaces to delete."
    return 0
  fi
  
  local success_count=0
  local fail_count=0
  
  while IFS= read -r workspace; do
    local ws_id ws_name ws_reason
    ws_id=$(echo "$workspace" | jq -r '.id')
    ws_name=$(echo "$workspace" | jq -r '.name')
    ws_reason=$(echo "$workspace" | jq -r '.reason // empty')
    
    if [ -n "$ws_reason" ]; then
      echo "Deleting: $ws_name ($ws_reason)"
    fi
    
    if delete_workspace_by_id "$token" "$ws_id" "$ws_name"; then
      success_count=$((success_count + 1))
    else
      fail_count=$((fail_count + 1))
    fi
    echo ""
  done < <(echo "$json_list" | jq -c '.[]')
  
  echo "============================================"
  echo "Deletion Summary:"
  echo "  Successful: $success_count"
  echo "  Failed: $fail_count"
  echo "============================================"
  
  [ "$fail_count" -eq 0 ]
}

# Discover inactive temp workspaces
# Returns: JSON array of workspaces to delete with id, name, reason
# Sets: DISCOVER_RESULT variable with the JSON array
discover_inactive_workspaces() {
  local token="$1"
  local prefix="$2"
  local inactivity_days="$3"
  
  local threshold_seconds=$((inactivity_days * 86400))
  local current_time
  current_time=$(date +%s)
  
  echo "Discovering workspaces with prefix '$prefix'..." >&2
  echo "Inactivity threshold: $inactivity_days days" >&2
  echo "" >&2
  
  local workspaces_json temp_workspaces total_temp
  workspaces_json=$(list_workspaces "$token")
  
  temp_workspaces=$(echo "$workspaces_json" | jq --arg prefix "$prefix" '
    .value | map(select(.displayName | startswith($prefix)))
  ')
  total_temp=$(echo "$temp_workspaces" | jq 'length')
  
  echo "Found $total_temp workspace(s) matching prefix '$prefix'" >&2
  echo "" >&2
  
  if [ "$total_temp" -eq 0 ]; then
    echo "No temporary workspaces found. Nothing to do." >&2
    DISCOVER_RESULT="[]"
    echo "$DISCOVER_RESULT"
    return 0
  fi
  
  local delete_list="[]"
  local active_count=0
  local inactive_count=0
  
  while IFS= read -r workspace; do
    local ws_name ws_id
    ws_name=$(echo "$workspace" | jq -r '.displayName')
    ws_id=$(echo "$workspace" | jq -r '.id')
    
    # Check Git integration for last sync time (primary activity indicator)
    # Note: lastSyncTime updates when:
    #   1. Workspace pulls changes from Git (Update from Git)
    #   2. Workspace commits changes to Git AND syncs back (Commit to Git + auto-sync)
    # This covers both deployment (Git -> Fabric) and development (Fabric -> Git) workflows
    local git_connection_response last_modified
    git_connection_response=$(curl -s -X GET "${API_BASE}/workspaces/${ws_id}/git/connection" \
      -H "Authorization: Bearer $token" \
      -H "Content-Type: application/json")

    # Extract lastSyncTime from Git connection
    last_modified=$(echo "$git_connection_response" | jq -r '.gitSyncDetails.lastSyncTime // empty')

    # If no Git sync time, check if workspace has any items (fallback)
    if [ -z "$last_modified" ] || [ "$last_modified" == "null" ]; then
      echo "  ⚠️  $ws_name - No Git integration found, checking workspace items..." >&2

      local items_response item_count
      items_response=$(curl -s -X GET "${API_BASE}/workspaces/${ws_id}/items" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json")

      item_count=$(echo "$items_response" | jq '.value | length' 2>/dev/null || echo "0")

      if [ "$item_count" -eq 0 ]; then
        echo "  ⚠️  $ws_name - No activity detected (empty workspace, no Git integration)" >&2
        delete_list=$(echo "$delete_list" | jq --arg id "$ws_id" --arg name "$ws_name" \
          '. += [{"id": $id, "name": $name, "reason": "No activity detected (empty)"}]')
        inactive_count=$((inactive_count + 1))
        continue
      else
        # Workspace has items but no Git sync and no modification dates
        # Mark as active to be safe (cannot determine true inactivity)
        echo "  ⚠️  $ws_name - Has $item_count items but no Git sync (cannot determine activity, marking as active)" >&2
        active_count=$((active_count + 1))
        continue
      fi
    fi

    # We have a last_modified date (from Git sync) - parse and check inactivity
    # Convert ISO 8601 timestamp to Unix timestamp
    local last_modified_ts
    # Try Linux date format first (for Ubuntu pipeline agents)
    last_modified_ts=$(date -d "${last_modified}" +%s 2>/dev/null || echo "0")

    if [ "$last_modified_ts" -eq 0 ]; then
      # Fallback for macOS date format
      last_modified_ts=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${last_modified%.*}" +%s 2>/dev/null || echo "0")
    fi

    if [ "$last_modified_ts" -eq 0 ]; then
      echo "  ⚠️  $ws_name - Could not parse Git sync date: $last_modified" >&2
      # Mark as active to be safe
      active_count=$((active_count + 1))
      continue
    fi

    local time_diff days_inactive
    time_diff=$((current_time - last_modified_ts))
    days_inactive=$((time_diff / 86400))

    if [ "$time_diff" -gt "$threshold_seconds" ]; then
      echo "  🗑️  $ws_name - Inactive for $days_inactive days (last Git sync: $last_modified)" >&2
      delete_list=$(echo "$delete_list" | jq --arg id "$ws_id" --arg name "$ws_name" --arg days "$days_inactive" \
        '. += [{"id": $id, "name": $name, "reason": "Inactive for \($days) days (last Git sync)"}]')
      inactive_count=$((inactive_count + 1))
    else
      echo "  ✅ $ws_name - Active ($days_inactive days old, last Git sync: $last_modified)" >&2
      active_count=$((active_count + 1))
    fi
  done < <(echo "$temp_workspaces" | jq -c '.[]')
  
  echo "" >&2
  echo "============================================" >&2
  echo "Discovery Summary:" >&2
  echo "  Total temp workspaces: $total_temp" >&2
  echo "  Active workspaces: $active_count" >&2
  echo "  Inactive workspaces: $inactive_count" >&2
  echo "============================================" >&2
  
  # Return the delete list as JSON (stdout only)
  DISCOVER_RESULT="$delete_list"
  echo "$delete_list"
}

# List workspaces that match a pattern (for preview/dry-run)
list_matching_workspaces() {
  local token="$1"
  shift
  local workspace_names=("$@")
  
  echo "Fetching all workspaces to verify which ones will be deleted..."
  local workspaces_json
  workspaces_json=$(list_workspaces "$token")
  
  echo ""
  echo "Workspaces that will be DELETED:"
  echo "================================="
  
  local found=0
  
  for ws_name in "${workspace_names[@]}"; do
    if echo "$workspaces_json" | jq -e --arg name "$ws_name" '.value[] | select(.displayName == $name)' > /dev/null 2>&1; then
      local ws_id
      ws_id=$(echo "$workspaces_json" | jq -r --arg name "$ws_name" '.value[] | select(.displayName == $name) | .id')
      echo "  ✓ Found: $ws_name (ID: $ws_id)"
      found=$((found + 1))
    else
      echo "  ⚠ Not found: $ws_name (will be skipped)"
    fi
  done
  
  echo "================================="
  echo ""
  
  if [ "$found" -eq 0 ]; then
    echo "⚠️  No matching workspaces found. Nothing to delete."
    return 1
  fi
  
  echo "Found $found workspace(s) to delete."
  return 0
}
