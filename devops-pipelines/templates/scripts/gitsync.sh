
# Initialize (PreferRemote) and then auto Update From Git for a Fabric workspace (by name),
# including the required JSON body with remoteCommitHash to avoid HTTP 411.
#
# Usage:
#   fabric_git_initialize_then_update_by_name "Silver-WS-6"
#
# Requires: az, curl, jq
fabric_git_initialize_then_update_by_name() {
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

  local STATUS_INCOMING_COUNT STATUS_UNCOMMITTED_COUNT STATUS_WORKSPACE_HEAD STATUS_REMOTE_HASH

  get_git_status() {
    curl -sS -X GET "${API_BASE}/workspaces/${WORKSPACE_ID}/git/status" \
      -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Accept: application/json"
  }

  parse_git_status() {
    local status_json="$1"
    STATUS_INCOMING_COUNT="$(jq -r '[.changes[]? | select(.remoteChange != null and .remoteChange != "None")] | length' <<< "${status_json}")"
    STATUS_UNCOMMITTED_COUNT="$(jq -r 'if (.uncommittedChanges | type) == "array"
      then (.uncommittedChanges | length)
      else ([.changes[]? | select(.workspaceChange != null and .workspaceChange != "None")] | length)
      end' <<< "${status_json}")"
    STATUS_WORKSPACE_HEAD="$(jq -r '.workspaceHead // "null"' <<< "${status_json}")"
    STATUS_REMOTE_HASH="$(jq -r '.remoteCommitHash // empty' <<< "${status_json}")"
  }

  build_update_body() {
    local remote_hash="$1"
    local workspace_head="$2"
    if [[ "${workspace_head}" == "null" || -z "${workspace_head}" ]]; then
      jq -n --arg hash "${remote_hash}" '{
        remoteCommitHash: $hash,
        workspaceHead: null,
        options: {
          allowOverrideItems: true
        },
        conflictResolution: {
          conflictResolutionType: "Workspace",
          conflictResolutionPolicy: "PreferRemote"
        }
      }'
    else
      jq -n --arg remote "${remote_hash}" --arg workspace "${workspace_head}" '{
        remoteCommitHash: $remote,
        workspaceHead: $workspace,
        options: {
          allowOverrideItems: true
        },
        conflictResolution: {
          conflictResolutionType: "Workspace",
          conflictResolutionPolicy: "PreferRemote"
        }
      }'
    fi
  }

  set_required_action_from_status() {
    local status_json="$1"
    local quiet="${2:-false}"
    parse_git_status "${status_json}"
    WORKSPACE_HEAD="${STATUS_WORKSPACE_HEAD}"
    REMOTE_COMMIT_HASH="${STATUS_REMOTE_HASH}"

    if [[ "${quiet}" != "true" ]]; then
      echo "Incoming changes (remoteChange != null): ${STATUS_INCOMING_COUNT}" >&2
      echo "Uncommitted changes (workspaceChange != null): ${STATUS_UNCOMMITTED_COUNT}" >&2
      echo "Workspace HEAD: ${WORKSPACE_HEAD}" >&2
      echo "Remote commit hash: ${REMOTE_COMMIT_HASH}" >&2
    fi

    # Conditions that require an UpdateFromGit:
    #   1. Incoming changes flagged on the status payload.
    #   2. Uncommitted (workspace) changes flagged.
    #   3. workspaceHead != remoteCommitHash — heads have drifted regardless
    #      of what the changes array says. This catches the case where a
    #      transient/partial git/status response listed no changes but the
    #      heads clearly differ (was previously leaving prod-reporting stale
    #      for days because the script silently concluded NoAction).
    local heads_differ="false"
    if [[ -n "${WORKSPACE_HEAD}" && "${WORKSPACE_HEAD}" != "null" \
          && -n "${REMOTE_COMMIT_HASH}" && "${REMOTE_COMMIT_HASH}" != "null" \
          && "${WORKSPACE_HEAD}" != "${REMOTE_COMMIT_HASH}" ]]; then
      heads_differ="true"
    fi

    if [[ "${STATUS_INCOMING_COUNT}" -gt 0 \
          || "${STATUS_UNCOMMITTED_COUNT}" -gt 0 \
          || "${heads_differ}" == "true" ]]; then
      REQUIRED_ACTION="UpdateFromGit"
    else
      REQUIRED_ACTION="NoAction"
    fi
  }

  poll_operation_status() {
    local op_url="$1"
    local retry_secs="${2:-${SLEEP_SECS}}"
    local max_attempts="${3:-${RETRIES}}"
    local op_json op_status

    for i in $(seq 1 "${max_attempts}"); do
      op_json="$(curl -s -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Accept: application/json" "${op_url}")"
      op_status="$(jq -r '.status // empty' <<< "${op_json}")"
      if [[ "${op_status}" == "Succeeded" ]]; then
        echo "${op_json}"
        return 0
      elif [[ "${op_status}" == "Failed" ]]; then
        echo "${op_json}"
        return 1
      fi
      echo "… still running (status=${op_status:-Unknown}). Sleeping ${retry_secs}s" >&2
      sleep "${retry_secs}"
    done

    echo "${op_json}"
    return 1
  }

  wait_for_git_ready() {
    local attempts="${1:-15}" status_response remote_hash
    for i in $(seq 1 "${attempts}"); do
      status_response="$(get_git_status)"
      remote_hash="$(jq -r '.remoteCommitHash // empty' <<< "${status_response}")"
      if [[ -n "${remote_hash}" && "${remote_hash}" != "null" ]]; then
        echo "${status_response}"
        return 0
      fi
      echo "… Git status not ready (remoteCommitHash empty). Sleeping ${SLEEP_SECS}s" >&2
      sleep "${SLEEP_SECS}"
    done
    echo "${status_response}"
    return 1
  }

  # Post-updateFromGit sanity check. updateFromGit returning Succeeded doesn't
  # always guarantee the workspace actually advanced — re-fetch git/status and
  # confirm workspaceHead == remoteCommitHash. Returns 0 if synced, 1 if not.
  verify_post_update_sync() {
    local status_response ws_head remote_hash
    status_response="$(get_git_status)"
    ws_head="$(jq -r '.workspaceHead // empty' <<< "${status_response}")"
    remote_hash="$(jq -r '.remoteCommitHash // empty' <<< "${status_response}")"
    if [[ -z "${remote_hash}" || "${remote_hash}" == "null" ]]; then
      echo "❌ Post-update verify: remote commit hash missing in git/status." >&2
      return 1
    fi
    if [[ "${ws_head}" != "${remote_hash}" ]]; then
      echo "❌ Post-update verify: heads still differ after updateFromGit." >&2
      echo "   workspaceHead=${ws_head} remoteCommitHash=${remote_hash}" >&2
      return 1
    fi
    echo "✅ Post-update verify: workspaceHead == remoteCommitHash (${remote_hash})"
    return 0
  }

  # Items where remote=Deleted but workspace=Modified (or workspace=Added) are
  # unresolvable by updateFromGit — Fabric returns 400 UnknownError even with
  # PreferRemote, because git says the item shouldn't exist (no remote version
  # to revert to) yet the workspace has uncommitted edits. The only way to
  # make the workspace match git here is to DELETE the workspace item.
  #
  # GUARDRAILS (defense in depth, all must be true for an item to be deleted):
  #
  #   1. Caller has set DISCARD_LOCAL_ON_REMOTE_DELETE=true. Default off, so
  #      dev/test workspaces (humans actively editing) don't silently lose
  #      uncommitted work — they fail with a diagnostic instead.
  #   2. The conflicting item's itemType is "Report". Reports are the only
  #      Fabric item type where rename/replace is a routine pipeline event,
  #      and where the underlying data store (the Lakehouse/Delta tables)
  #      is unaffected by deletion. NEVER auto-deletes Lakehouses, Notebooks,
  #      DataPipelines, SemanticModels, etc. — those need human attention
  #      because the blast radius is much larger.
  #   3. We snapshot the report's full definition into the build log
  #      (base64-encoded) BEFORE deleting, so the discarded local edits
  #      can be reconstructed from pipeline output if necessary.
  #
  # If non-Report conflicts exist alongside Reports, Reports still get processed
  # but the function returns 1 at the end so the pipeline fails — the non-Report
  # cases need explicit human review.
  #
  # Returns 0 if no such conflicts (nothing to do) or if all conflicts were
  # Reports that got resolved cleanly. Returns 1 in any other case
  # (opt-in flag off, non-Report conflict present, delete API failure).
  resolve_remote_deleted_conflicts() {
    local status_json="$1"
    local opt_in="${DISCARD_LOCAL_ON_REMOTE_DELETE:-false}"

    # Two pre-filtered lists: reports we can auto-handle, others we cannot.
    local reports others i objectId displayName itemType
    local reports_count others_count
    reports="$(jq -c '[
      .changes[]?
      | select(.remoteChange == "Deleted" and (.workspaceChange == "Modified" or .workspaceChange == "Added"))
      | select((.itemMetadata.itemType // "" | ascii_downcase) == "report")
    ]' <<< "${status_json}")"
    others="$(jq -c '[
      .changes[]?
      | select(.remoteChange == "Deleted" and (.workspaceChange == "Modified" or .workspaceChange == "Added"))
      | select((.itemMetadata.itemType // "" | ascii_downcase) != "report")
    ]' <<< "${status_json}")"
    reports_count="$(jq 'length' <<< "${reports}")"
    others_count="$(jq 'length' <<< "${others}")"

    if [[ "${reports_count}" -eq 0 && "${others_count}" -eq 0 ]]; then
      return 0
    fi

    # Non-Report conflicts are NEVER auto-resolved. Surface them loudly.
    if [[ "${others_count}" -gt 0 ]]; then
      echo "❌ ${others_count} non-Report item(s) have Modified-vs-Deleted conflicts." >&2
      echo "   This script will NOT auto-delete non-Report items (lakehouses, notebooks," >&2
      echo "   pipelines, semantic models, etc.) because the blast radius is too large." >&2
      echo "   These need human review:" >&2
      for i in $(seq 0 $((others_count - 1))); do
        objectId="$(jq -r ".[$i].itemMetadata.itemIdentifier.objectId // empty" <<< "${others}")"
        displayName="$(jq -r ".[$i].itemMetadata.displayName // empty" <<< "${others}")"
        itemType="$(jq -r ".[$i].itemMetadata.itemType // empty" <<< "${others}")"
        echo "     - ${itemType} '${displayName}' (objectId: ${objectId:-<unknown>})" >&2
      done
      echo "" >&2
    fi

    if [[ "${reports_count}" -gt 0 ]]; then
      if [[ "${opt_in}" != "true" ]]; then
        echo "❌ ${reports_count} Report(s) have local edits (Modified/Added) but were Deleted on remote." >&2
        echo "   Fabric's updateFromGit cannot auto-resolve this; the workspace items would" >&2
        echo "   have to be deleted to honor the PreferRemote policy, which would discard the" >&2
        echo "   local edits." >&2
        echo "" >&2
        echo "   Affected reports:" >&2
        for i in $(seq 0 $((reports_count - 1))); do
          objectId="$(jq -r ".[$i].itemMetadata.itemIdentifier.objectId // empty" <<< "${reports}")"
          displayName="$(jq -r ".[$i].itemMetadata.displayName // empty" <<< "${reports}")"
          echo "     - Report '${displayName}' (objectId: ${objectId:-<unknown>})" >&2
        done
        echo "" >&2
        echo "   To resolve, EITHER discard the local edits in the Fabric workspace UI, OR" >&2
        echo "   rerun this pipeline stage with DISCARD_LOCAL_ON_REMOTE_DELETE=true to have" >&2
        echo "   the script delete them automatically (definitions snapshotted to log first)." >&2
        return 1
      fi

      echo "⚠️  ${reports_count} Report(s) Modified/Added locally but Deleted on remote — opt-in flag set, discarding local edits"
      local snap_body del_body del_http snap_http snap_b64
      for i in $(seq 0 $((reports_count - 1))); do
        objectId="$(jq -r ".[$i].itemMetadata.itemIdentifier.objectId // empty" <<< "${reports}")"
        displayName="$(jq -r ".[$i].itemMetadata.displayName // empty" <<< "${reports}")"
        if [[ -z "${objectId}" ]]; then
          echo "  ⚠️  Skipping Report '${displayName}' — no objectId in status payload (cannot delete by API; manual cleanup needed)"
          continue
        fi

        # Snapshot definition first so the local state is recoverable from build logs.
        echo "  📸 Snapshotting Report '${displayName}' (objectId: ${objectId}) before delete..."
        snap_body="$(mktemp)"
        snap_http="$(curl -sS -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/items/${objectId}/getDefinition" \
          -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" \
          -d '{}' -o "${snap_body}" -w "%{http_code}")"
        if [[ "${snap_http}" == "200" || "${snap_http}" == "202" ]]; then
          # 202 = LRO; for snapshot purposes we just dump whatever body we got.
          snap_b64="$(base64 < "${snap_body}" | tr -d '\n')"
          echo "  --- BEGIN SNAPSHOT (objectId=${objectId}, base64) ---"
          # Wrap at 200 chars per line so build log viewers don't truncate horribly.
          echo "${snap_b64}" | fold -w 200
          echo "  --- END SNAPSHOT (objectId=${objectId}) ---"
        else
          echo "  ⚠️  Snapshot failed (HTTP ${snap_http}). Proceeding with delete anyway since opt-in flag is set."
          echo "     Response: $(cat "${snap_body}" 2>/dev/null || echo '<empty>')"
        fi
        rm -f "${snap_body}"

        echo "  🗑️  Deleting Report '${displayName}' (objectId: ${objectId})..."
        del_body="$(mktemp)"
        del_http="$(curl -sS -X DELETE "${API_BASE}/workspaces/${WORKSPACE_ID}/items/${objectId}" \
          -H "Authorization: Bearer ${FABRIC_TOKEN}" -o "${del_body}" -w "%{http_code}")"
        case "${del_http}" in
          200|204|404)
            echo "  ✅ Deleted (HTTP ${del_http})"
            rm -f "${del_body}"
            ;;
          *)
            echo "  ❌ Delete failed (HTTP ${del_http})."
            echo "     Response: $(cat "${del_body}" 2>/dev/null || echo '<empty>')"
            rm -f "${del_body}"
            return 1
            ;;
        esac
      done
    fi

    # If any non-Report conflicts remained, fail so the operator notices.
    if [[ "${others_count}" -gt 0 ]]; then
      return 1
    fi
    return 0
  }

  # --- Check connection state before attempting initializeConnection ---
  # initializeConnection requires personal Git credentials from the calling user.
  # When running as a service principal, this returns 400 GitCredentialsNotConfigured.
  # Workaround: if the workspace is already ConnectedAndInitialized, skip
  # initializeConnection entirely and go straight to git/status + updateFromGit.
  echo "Checking Git connection state for workspace '${WORKSPACE_ID}'..."
  local REMOTE_COMMIT_HASH WORKSPACE_HEAD REQUIRED_ACTION
  local CONN_RESP GIT_CONN_STATE
  CONN_RESP="$(curl -sS -X GET "${API_BASE}/workspaces/${WORKSPACE_ID}/git/connection" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json")"
  GIT_CONN_STATE="$(jq -r '.gitConnectionState // empty' <<< "${CONN_RESP}")"
  echo "Git connection state: ${GIT_CONN_STATE}"

  if [[ "${GIT_CONN_STATE}" == "ConnectedAndInitialized" ]]; then
    echo "✅ Workspace already ConnectedAndInitialized — skipping initializeConnection."
    local status_response
    status_response="$(get_git_status)"
    echo "Git Status:" >&2
    echo "${status_response}" | jq . >&2
    set_required_action_from_status "${status_response}" "false"
    REMOTE_COMMIT_HASH="${STATUS_REMOTE_HASH}"
    WORKSPACE_HEAD="${STATUS_WORKSPACE_HEAD}"
  else
    # Not yet initialized — call initializeConnection
    echo "Initializing Git connection (PreferRemote) for workspace '${WORKSPACE_ID}'..."
    local INIT_BODY='{"initializationStrategy":"PreferRemote"}'
    local init_status init_headers location retry_after
    init_headers="$(mktemp)"

    init_status="$(curl -s -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/git/initializeConnection" \
      -H "Authorization: Bearer ${FABRIC_TOKEN}" \
      -H "Content-Type: application/json" -H "Accept: application/json" \
      -d "${INIT_BODY}" -D "${init_headers}" -o /tmp/init_body.json -w "%{http_code}")"

    location="$(awk -v IGNORECASE=1 -F': ' '$1=="Location"{print $2}' "${init_headers}" | tr -d '\r')"
    retry_after="$(awk -v IGNORECASE=1 -F': ' '$1=="Retry-After"{print $2}' "${init_headers}" | tr -d '\r')"
    [[ -z "${retry_after}" ]] && retry_after="${SLEEP_SECS}"

    REMOTE_COMMIT_HASH=""
    if [[ "${init_status}" == "202" && -n "${location}" ]]; then
      echo "▶️ Initialize accepted. Polling ${location} every ${retry_after}s..."
      op_json="$(poll_operation_status "${location}" "${retry_after}" "${RETRIES}")" || {
        echo "❌ Initialize failed."
        echo "${op_json}" | jq .
        rm -f "${init_headers}" /tmp/init_body.json
        return 1
      }
      echo "✅ Initialize succeeded."
      echo "${op_json}" | jq .
      REQUIRED_ACTION="$(jq -r '.result.requiredAction // empty' <<< "${op_json}")"
      REMOTE_COMMIT_HASH="$(jq -r '.result.remoteCommitHash // empty' <<< "${op_json}")"
      WORKSPACE_HEAD="$(jq -r '.result.workspaceHead // "null"' <<< "${op_json}")"

    elif [[ "${init_status}" == "200" ]]; then
      echo "✅ Initialize completed (200)."
      jq . /tmp/init_body.json
      REQUIRED_ACTION="$(jq -r '.requiredAction // empty' /tmp/init_body.json)"
      REMOTE_COMMIT_HASH="$(jq -r '.remoteCommitHash // empty' /tmp/init_body.json)"
      WORKSPACE_HEAD="$(jq -r '.workspaceHead // "null"' /tmp/init_body.json)"

    elif [[ "${init_status}" == "409" ]]; then
      local error_code
      error_code="$(jq -r '.errorCode // empty' /tmp/init_body.json)"
      if [[ "${error_code}" == "WorkspaceGitConnectionAlreadyInitialized" ]]; then
        echo "✅ Workspace connection already initialized. Checking for incoming changes..."
        local status_response
        status_response="$(get_git_status)"
        echo "Git Status:" >&2
        echo "${status_response}" | jq . >&2
        set_required_action_from_status "${status_response}" "false"
      else
        echo "❌ Unexpected 409 error: ${error_code}"
        jq . /tmp/init_body.json
        rm -f "${init_headers}" /tmp/init_body.json
        return 1
      fi

    else
      echo "❌ Unexpected initialize status: ${init_status}"
      echo "Headers:"; cat "${init_headers}"
      echo "Body:"; cat /tmp/init_body.json
      rm -f "${init_headers}" /tmp/init_body.json
      return 1
    fi

    rm -f "${init_headers}" /tmp/init_body.json
  fi  # end ConnectedAndInitialized check

  echo "RequiredAction: ${REQUIRED_ACTION:-<none>}"
  echo "RemoteCommitHash: ${REMOTE_COMMIT_HASH:-<none>}"
  echo "WorkspaceHead: ${WORKSPACE_HEAD:-<none>}"

  # Re-check status to ensure we always take remote if local changes exist.
  status_response="$(get_git_status)"
  set_required_action_from_status "${status_response}" "true"
  if [[ "${REQUIRED_ACTION}" == "UpdateFromGit" ]]; then
    echo "⚠️  Forcing UpdateFromGit to ensure remote state wins." >&2
    # Pre-flight: resolve Modified-vs-Deleted conflicts updateFromGit can't auto-handle.
    resolve_remote_deleted_conflicts "${status_response}" || return 1
    status_response="$(get_git_status)"
    set_required_action_from_status "${status_response}" "true"
  fi

  # --- Update From Git (only if required) ---
  # Check if update is needed based on RequiredAction from initializeConnection
  if [[ "${REQUIRED_ACTION}" == "UpdateFromGit" ]]; then
    echo "🚀 Update required. Calling Update From Git for workspace '${WORKSPACE_ID}'..."

    # Ensure Git connection is ready and refresh hashes
    status_response="$(wait_for_git_ready 15)" || {
      echo "❌ Git connection not ready; remoteCommitHash unavailable."
      return 1
    }
    set_required_action_from_status "${status_response}" "true"

    # Validate we have the commit hash
    if [[ -z "${REMOTE_COMMIT_HASH}" ]]; then
      echo "⚠️ Could not get remoteCommitHash from Initialize. Aborting."
      return 1
    fi

    local UPDATE_ATTEMPTS=6
    for attempt in $(seq 1 "${UPDATE_ATTEMPTS}"); do
      # Build update body with both hashes (workspaceHead can be null for first sync)
      # Include options.allowOverrideItems to allow overwriting workspace items with Git content
      local UPDATE_BODY
      UPDATE_BODY="$(build_update_body "${REMOTE_COMMIT_HASH}" "${WORKSPACE_HEAD}")"

      local upd_status upd_headers upd_location upd_retry
      upd_headers="$(mktemp)"

      echo "Sending UpdateFromGit request with body (attempt ${attempt}/${UPDATE_ATTEMPTS}):"
      echo "${UPDATE_BODY}" | jq .

      upd_status="$(curl -s -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/git/updateFromGit" \
        -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Accept: application/json" \
        -H "Content-Type: application/json" -d "${UPDATE_BODY}" \
        -D "${upd_headers}" -o /tmp/update_body.json -w "%{http_code}")"

      echo "UpdateFromGit response status: ${upd_status}"
      echo "UpdateFromGit response body:"
      cat /tmp/update_body.json | jq . 2>/dev/null || cat /tmp/update_body.json

      upd_location="$(grep -i '^location:' "${upd_headers}" | sed -e 's/^[Ll]ocation:[[:space:]]*//' -e 's/\r$//')"
      upd_retry="$(grep -i '^retry-after:' "${upd_headers}" | sed -e 's/^[Rr]etry-[Aa]fter:[[:space:]]*//' -e 's/\r$//')"
      upd_op_id="$(grep -i '^x-ms-operation-id:' "${upd_headers}" | sed -e 's/^[Xx]-ms-operation-id:[[:space:]]*//' -e 's/\r$//')"
      if [[ -z "${upd_location}" && -n "${upd_op_id}" ]]; then
        upd_location="${API_BASE}/operations/${upd_op_id}"
      fi
      [[ -z "${upd_retry}" ]] && upd_retry="${SLEEP_SECS}"

      if [[ "${upd_status}" == "202" && -n "${upd_location}" ]]; then
        echo "▶️ UpdateFromGit accepted. Polling ${upd_location} every ${upd_retry}s..."
        uop_json="$(poll_operation_status "${upd_location}" "${upd_retry}" "${RETRIES}")" || {
          echo "❌ UpdateFromGit operation failed."
          echo "${uop_json}" | jq .
          rm -f "${upd_headers}" /tmp/update_body.json
          return 1
        }
        echo "✅ UpdateFromGit operation succeeded."
        echo "${uop_json}" | jq .
        rm -f "${upd_headers}" /tmp/update_body.json
        verify_post_update_sync || return 1
        echo "✅ Flow complete: Initialize (PreferRemote) → Update From Git."
        return 0

      elif [[ "${upd_status}" == "200" ]]; then
        echo "✅ UpdateFromGit completed (200)."
        jq . /tmp/update_body.json
        rm -f "${upd_headers}" /tmp/update_body.json
        verify_post_update_sync || return 1
        echo "✅ Flow complete: Initialize (PreferRemote) → Update From Git."
        return 0

      elif [[ "${upd_status}" == "400" && "${attempt}" -lt "${UPDATE_ATTEMPTS}" ]]; then
        echo "⚠️ UpdateFromGit returned 400. Retrying after ${upd_retry}s..."
        rm -f "${upd_headers}" /tmp/update_body.json
        sleep "${upd_retry}"
        status_response="$(get_git_status)"
        set_required_action_from_status "${status_response}" "true"
        continue
      else
        echo "❌ Unexpected UpdateFromGit status: ${upd_status}"
        echo "Headers:"; cat "${upd_headers}"
        echo "Body:"; cat /tmp/update_body.json
        rm -f "${upd_headers}" /tmp/update_body.json
        return 1
      fi
    done

    echo "❌ UpdateFromGit failed after ${UPDATE_ATTEMPTS} attempts."
    return 1
  elif [[ "${REQUIRED_ACTION}" == "NoAction" || "${REQUIRED_ACTION}" == "None" ]]; then
    # Defense in depth: REQUIRED_ACTION=NoAction is only safe to declare success
    # on if we actually have a remote commit hash AND the workspace head matches.
    # set_required_action_from_status now upgrades head-mismatch to UpdateFromGit,
    # but if git/status returned an empty/error response we'd still land here with
    # a null hash — refuse to silently report success in that case.
    if [[ -z "${REMOTE_COMMIT_HASH}" || "${REMOTE_COMMIT_HASH}" == "null" ]]; then
      echo "❌ ERROR: Remote commit hash is null/empty but REQUIRED_ACTION evaluated to NoAction." >&2
      echo "   The git/status response was missing or incomplete, so we can't safely" >&2
      echo "   conclude the workspace is in sync. Check that:" >&2
      echo "   1. The Git repository has content in the branch configured for this workspace" >&2
      echo "   2. The workspace Git connection points to the correct branch/directory" >&2
      echo "   3. The workspace has been initialised from Git at least once" >&2
      return 1
    fi
    if [[ -n "${WORKSPACE_HEAD}" && "${WORKSPACE_HEAD}" != "null" \
          && "${WORKSPACE_HEAD}" != "${REMOTE_COMMIT_HASH}" ]]; then
      echo "❌ ERROR: Heads differ but REQUIRED_ACTION evaluated to NoAction." >&2
      echo "   workspaceHead=${WORKSPACE_HEAD} remoteCommitHash=${REMOTE_COMMIT_HASH}" >&2
      echo "   This indicates a stale or partial git/status response. Refusing to claim success." >&2
      return 1
    fi
    echo "✅ No update needed - workspace is already in sync with Git."
    echo "   WorkspaceHead: ${WORKSPACE_HEAD}"
    echo "   RemoteCommitHash: ${REMOTE_COMMIT_HASH}"
  else
    echo "⚠️  Unknown RequiredAction: ${REQUIRED_ACTION}. No update performed."
  fi
}
