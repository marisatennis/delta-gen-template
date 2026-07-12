"""SharePoint authentication helpers."""


def get_sharepoint_access_token(tenant_id, client_id, client_secret, scope):
    """
    Gets an OAuth access token for SharePoint or Microsoft Graph using client credentials.

    Parameters:
    -----------
    tenant_id : str
        Azure AD tenant ID (GUID)
    client_id : str
        App registration (service principal) client ID
    client_secret : str
        Client secret for the app registration
    scope : str
        OAuth scope (e.g., 'https://graph.microsoft.com/.default'
        or 'https://{tenant}.sharepoint.com/.default')

    Returns:
    --------
    str
        Access token
    """
    import requests

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
        "grant_type": "client_credentials",
    }
    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()
    return response.json().get("access_token")


def get_sharepoint_access_token_from_keyvault(
    keyvault_uri,
    tenant_id,
    client_id_secret_name,
    client_secret_secret_name,
    scope,
    tenant_id_secret_name=None,
):
    """
    Fetches SharePoint client credentials from Key Vault and returns an access token.

    Parameters:
    -----------
    keyvault_uri : str
        Azure Key Vault URI
    tenant_id : str
        Azure AD tenant ID (GUID)
    client_id_secret_name : str
        Key Vault secret name containing the client ID
    client_secret_secret_name : str
        Key Vault secret name containing the client secret
    scope : str
        OAuth scope (e.g., 'https://graph.microsoft.com/.default')

    Returns:
    --------
    str
        Access token
    """
    from fabric_libs.auth import get_secret

    if tenant_id_secret_name:
        tenant_id = get_secret(keyvault_uri, tenant_id_secret_name)

    client_id = get_secret(keyvault_uri, client_id_secret_name)
    client_secret = get_secret(keyvault_uri, client_secret_secret_name)
    return get_sharepoint_access_token(tenant_id, client_id, client_secret, scope)
