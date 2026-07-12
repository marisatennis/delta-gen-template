fabric_git_connect_ado_id() {
  set -euo pipefail

  # --- Args ---
  local WORKSPACE_NAME="" CONN_NAME="" ADO_SP_TENANT="" ADO_SP_CLIENT_ID="" ADO_SP_SECRET=""
  local ORG="" PROJECT="" REPO="" BRANCH="" DIRECTORY="" ADO_REPO_URL=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workspace-name)   WORKSPACE_NAME="$2"; shift 2 ;;
      --conn-name)        CONN_NAME="$2"; shift 2 ;;
      --ado-sp-tenant)    ADO_SP_TENANT="$2"; shift 2 ;;
      --ado-sp-client-id) ADO_SP_CLIENT_ID="$2"; shift 2 ;;
      --ado-sp-secret)    ADO_SP_SECRET="$2"; shift 2 ;;
      --org)              ORG="$2"; shift 2 ;;
      --project)          PROJECT="$2"; shift 2 ;;
      --repo)             REPO="$2"; shift 2 ;;
      --branch)           BRANCH="$2"; shift 2 ;;
      --directory)        DIRECTORY="$2"; shift 2 ;;
      --ado-repo-url)     ADO_REPO_URL="$2"; shift 2 ;;
      *) echo "Unknown arg: $1"; return 1 ;;
    esac
  done

  # --- Validate ---
  : "${WORKSPACE_NAME:?--workspace-name is required}"
  : "${CONN_NAME:?--conn-name is required}"
  : "${ADO_SP_TENANT:?--ado-sp-tenant is required}"
  : "${ADO_SP_CLIENT_ID:?--ado-sp-client-id is required}"
  : "${ADO_SP_SECRET:?--ado-sp-secret is required}"
  : "${ORG:?--org is required}"
  : "${PROJECT:?--project is required}"
  : "${REPO:?--repo is required}"
  : "${BRANCH:?--branch is required}"
  : "${DIRECTORY:?--directory is required}"

  local API_BASE="https://api.fabric.microsoft.com/v1"

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

  # --- Check existing git connection ---
  echo "Checking existing Git connection for workspace '${WORKSPACE_ID}'..."
  EXISTING_CONN_RESP="$(curl -sS -X GET "${API_BASE}/workspaces/${WORKSPACE_ID}/git/connection" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json")"
  EXISTING_PROVIDER="$(jq -r '.gitProviderDetails.gitProviderType // empty' <<< "${EXISTING_CONN_RESP}")"
  if [[ -n "${EXISTING_PROVIDER}" ]]; then
    EXISTING_ORG="$(jq -r '.gitProviderDetails.organizationName // empty' <<< "${EXISTING_CONN_RESP}")"
    EXISTING_PROJECT="$(jq -r '.gitProviderDetails.projectName // empty' <<< "${EXISTING_CONN_RESP}")"
    EXISTING_REPO="$(jq -r '.gitProviderDetails.repositoryName // empty' <<< "${EXISTING_CONN_RESP}")"
    EXISTING_BRANCH="$(jq -r '.gitProviderDetails.branchName // empty' <<< "${EXISTING_CONN_RESP}")"
    EXISTING_DIR="$(jq -r '.gitProviderDetails.directoryName // empty' <<< "${EXISTING_CONN_RESP}")"

    # Normalize for comparison: ADO org/project/repo names are case-insensitive at the URL
    # level (Fabric may have stored an earlier-cased value), and Fabric prepends a leading
    # slash to directoryName when storing. Branch stays case-sensitive — Git branches are.
    local DIRECTORY_NORMALIZED="${DIRECTORY#/}"
    EXISTING_DIR_NORMALIZED="${EXISTING_DIR#/}"

    if [[ "${EXISTING_ORG,,}" == "${ORG,,}" && "${EXISTING_PROJECT,,}" == "${PROJECT,,}" && \
          "${EXISTING_REPO,,}" == "${REPO,,}" && "${EXISTING_BRANCH}" == "${BRANCH}" && \
          "${EXISTING_DIR_NORMALIZED}" == "${DIRECTORY_NORMALIZED}" ]]; then
      echo "✅ Workspace already connected to target repo/branch/directory. Skipping connect."
      return 0
    fi

    echo "❌ Workspace already connected to a different Git repo/branch/directory."
    echo "   Existing: ${EXISTING_ORG}/${EXISTING_PROJECT}/${EXISTING_REPO} @ ${EXISTING_BRANCH} (${EXISTING_DIR})"
    echo "   Target:   ${ORG}/${PROJECT}/${REPO} @ ${BRANCH} (${DIRECTORY})"
    echo "   Disconnect or update connection before continuing."
    return 1
  fi

  # --- Find existing connection by displayName ---
  echo "Looking for connection '${CONN_NAME}'..."
  EXISTING_CONN_JSON="$(curl -sS -X GET "${API_BASE}/connections" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json")"
  CONNECTION_ID="$(jq -r --arg name "${CONN_NAME}" '.value[] | select(.displayName == $name) | .id' <<< "${EXISTING_CONN_JSON}")"

  # --- Create if not found ---
  if [[ -z "${CONNECTION_ID}" || "${CONNECTION_ID}" == "null" ]]; then
    echo "Creating Fabric Configured Connection '${CONN_NAME}' (AzureDevOpsSourceControl, ServicePrincipal)..."
    CREATE_CONN_PAYLOAD="$(jq -n \
      --arg name "${CONN_NAME}" \
      --arg t "${ADO_SP_TENANT}" \
      --arg cid "${ADO_SP_CLIENT_ID}" \
      --arg sec "${ADO_SP_SECRET}" \
      --arg url "${ADO_REPO_URL}" \
      '{
        connectivityType: "ShareableCloud",
        displayName: $name,
        connectionDetails: {
          type: "AzureDevOpsSourceControl",
          creationMethod: "AzureDevOpsSourceControl.Contents",
          parameters: (if ($url|length)>0 then [ {dataType:"Text", name:"url", value:$url} ] else [] end)
        },
        credentialDetails: {
          credentials: {
            credentialType: "ServicePrincipal",
            tenantId: $t,
            servicePrincipalClientId: $cid,
            servicePrincipalSecret: $sec
          }
        }
      }')"

    CREATE_RESP="$(curl -sS -X POST "${API_BASE}/connections" \
      -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" -d "${CREATE_CONN_PAYLOAD}")"

    # Capture id from create response (or handle duplicate)
    if [[ "$(jq -r '.errorCode // empty' <<< "${CREATE_RESP}")" == "DuplicateConnectionName" ]]; then
      echo "⚠️ Connection name already exists. Re-using existing connection."
      CONNECTION_ID="$(jq -r --arg name "${CONN_NAME}" '.value[] | select(.displayName == $name) | .id' <<< "${EXISTING_CONN_JSON}")"
    else
      CONNECTION_ID="$(jq -r '.id // empty' <<< "${CREATE_RESP}")"
    fi
  fi

  [[ -z "${CONNECTION_ID}" || "${CONNECTION_ID}" == "null" ]] && { echo "❌ Could not resolve connectionId."; return 1; }
  echo "✅ Using connectionId: ${CONNECTION_ID}"

  # --- git/connect (use connectionId!) ---
  echo "Connecting workspace to Azure DevOps repo '${REPO}' (branch '${BRANCH}', directory '${DIRECTORY}')..."
  CONNECT_BODY="$(jq -n \
    --arg org "${ORG}" --arg proj "${PROJECT}" --arg repo "${REPO}" \
    --arg branch "${BRANCH}" --arg dir "${DIRECTORY}" --arg connId "${CONNECTION_ID}" \
    '{
      gitProviderDetails: {
        gitProviderType: "AzureDevOps",
        organizationName: $org,
        projectName: $proj,
        repositoryName: $repo,
        branchName: $branch,
        directoryName: $dir
      },
      myGitCredentials: {
        source: "ConfiguredConnection",
        connectionId:  $connId 
      }
    }')"

  CONNECT_RESP="$(curl -sS -X POST "${API_BASE}/workspaces/${WORKSPACE_ID}/git/connect" \
    -H "Authorization: Bearer ${FABRIC_TOKEN}" -H "Content-Type: application/json" -d "${CONNECT_BODY}")"

  echo "${CONNECT_RESP}" | jq .

  # Check for errors
  local error_code
  error_code="$(jq -r '.errorCode // empty' <<< "${CONNECT_RESP}")"

  if [[ -n "${error_code}" ]]; then
    # Check if it's "already connected" error - this is acceptable
    if [[ "${error_code}" == "WorkspaceAlreadyConnectedToGit" ]]; then
      echo "✅ Workspace '${WORKSPACE_NAME}' is already connected to Git. Skipping."
      return 0
    fi

    echo "❌ git/connect failed with error: ${error_code}"
    return 1
  fi

  echo "✅ git/connect succeeded for workspace '${WORKSPACE_NAME}'."
}
