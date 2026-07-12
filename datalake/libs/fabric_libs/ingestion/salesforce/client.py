"""Salesforce client authentication and connection management."""


def get_salesforce_client(keyvault_uri, sf_domain):
    """
    Creates an authenticated Salesforce client using credentials from Azure Key Vault.

    Parameters:
    -----------
    keyvault_uri : str
        Azure Key Vault URI (e.g., 'https://kv-your-project.vault.azure.net/')
    sf_domain : str
        Salesforce domain (e.g., 'yourorg.my')

    Returns:
    --------
    Salesforce
        Authenticated Salesforce client instance

    Example:
    --------
    >>> keyvault_uri = "https://kv-your-project.vault.azure.net/"
    >>> sf_domain = "yourorg.my"
    >>> sf_client = get_salesforce_client(keyvault_uri, sf_domain)
    """
    # Lazy imports - only load when function is called
    from simple_salesforce import Salesforce
    from fabric_libs.auth import get_secret

    sf_client_id = get_secret(keyvault_uri, "ExternalClient-Consumer-Key")
    sf_client_secret = get_secret(keyvault_uri, "ExternalClient-Consumer-Secret")

    sf_client = Salesforce(
        consumer_key=sf_client_id,
        consumer_secret=sf_client_secret,
        domain=sf_domain
    )
    return sf_client
