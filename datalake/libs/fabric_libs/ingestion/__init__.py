"""Reusable ingestion utilities for Fabric/Spark.

This module provides ingestion capabilities for different data sources:
- files: Generic file-based ingestion (CSV, Excel, TXT) for SharePoint and other file sources
- salesforce: Salesforce API-based ingestion using SOQL queries
- sharepoint_lists: SharePoint list ingestion using REST or Graph APIs

Usage:
------
# File-based ingestion
from fabric_libs.ingestion import files
results = files.run_ingestion(control_table, ...)
results = files.run_ingestion_parallel(control_table, workers=8, ...)

# Salesforce ingestion
from fabric_libs.ingestion import salesforce
results = salesforce.run_ingestion(spark, sf_client, config, ...)
"""

# Lazy imports to avoid dependency issues
# Only import submodules when they're actually accessed
def __getattr__(name):
    """Lazy import submodules to avoid loading unnecessary dependencies."""
    if name == "files":
        import importlib
        files = importlib.import_module('.files', __name__)
        globals()[name] = files
        return files
    elif name == "salesforce":
        import importlib
        salesforce = importlib.import_module('.salesforce', __name__)
        globals()[name] = salesforce
        return salesforce
    elif name == "sharepoint_lists":
        import importlib
        sharepoint_lists = importlib.import_module('.sharepoint_lists', __name__)
        globals()[name] = sharepoint_lists
        return sharepoint_lists
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    "files",
    "salesforce",
    "sharepoint_lists",
]
