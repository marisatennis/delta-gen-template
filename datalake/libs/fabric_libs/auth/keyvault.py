"""Azure Key Vault helpers."""


def get_secret(keyvault_uri, secret_name):
    """
    Fetch a secret value from Azure Key Vault.

    Parameters:
    -----------
    keyvault_uri : str
        Azure Key Vault URI (e.g., 'https://kv-your-project.vault.azure.net/')
    secret_name : str
        Name of the secret stored in Key Vault

    Returns:
    --------
    str
        Secret value
    """
    from notebookutils.mssparkutils.credentials import getSecret

    return getSecret(keyvault_uri, secret_name)
