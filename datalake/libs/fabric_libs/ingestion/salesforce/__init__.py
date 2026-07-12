"""Reusable Salesforce ingestion utilities for Fabric/Spark."""

from .client import get_salesforce_client
from .ingestion import (
    check_for_completeness,
    get_salesforce_config_from_silver_mappings,
    get_data_from_salesforce_object,
    get_object_count,
    run_ingestion,
)
from .metadata import (
    get_latest_timestamp,
    update_metadata,
)

__all__ = [
    "check_for_completeness",
    "get_salesforce_config_from_silver_mappings",
    "get_data_from_salesforce_object",
    "get_latest_timestamp",
    "get_object_count",
    "get_salesforce_client",
    "run_ingestion",
    "update_metadata",
]
