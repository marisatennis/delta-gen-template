"""SharePoint list ingestion utilities for Fabric/Spark."""

from .client import (
    get_sharepoint_access_token,
    get_sharepoint_access_token_from_keyvault,
)
from .ingestion import (
    build_delta_url,
    fetch_sharepoint_list_items,
    fetch_sharepoint_list_delta_items,
    list_items_to_dataframe,
    normalize_delta_items,
    normalize_list_items,
    resolve_graph_list_items_url,
    run_ingestion,
)
from .metadata import ensure_delta_table, get_latest_delta_link, update_delta_link
from .pretty import format_results, print_parallel_results
from .storage import write_dataframe_to_lakehouse

__all__ = [
    "get_sharepoint_access_token",
    "get_sharepoint_access_token_from_keyvault",
    "build_delta_url",
    "fetch_sharepoint_list_items",
    "fetch_sharepoint_list_delta_items",
    "list_items_to_dataframe",
    "normalize_delta_items",
    "normalize_list_items",
    "resolve_graph_list_items_url",
    "run_ingestion",
    "ensure_delta_table",
    "get_latest_delta_link",
    "update_delta_link",
    "format_results",
    "print_parallel_results",
    "write_dataframe_to_lakehouse",
]
