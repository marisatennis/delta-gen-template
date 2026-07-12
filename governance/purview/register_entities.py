"""Register Delta-Gen YAML table configs as Purview entities.

Reads YAML configs, maps them to Purview entity definitions, and
creates/updates them via the Purview REST API.

Usage:
    python register_entities.py --config-path ../../datalake/inputs/ --purview-account myaccount
    python register_entities.py --config-path ../../datalake/inputs/ --purview-account myaccount --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

from scan_configs import scan_yaml_configs

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

FABRIC_TABLE_TYPE = "fabric_lakehouse_table"
FABRIC_COLUMN_TYPE = "fabric_lakehouse_column"


def ensure_custom_types(client) -> None:
    """Ensure custom Purview type definitions exist for Fabric Lakehouse."""
    existing = client.get_type_def(FABRIC_TABLE_TYPE)
    if existing:
        logger.info(f"Type '{FABRIC_TABLE_TYPE}' already exists")
        return

    type_defs = {
        "entityDefs": [
            {
                "name": FABRIC_TABLE_TYPE,
                "superTypes": ["DataSet"],
                "description": "Microsoft Fabric Lakehouse Delta table managed by Delta-Gen",
                "attributeDefs": [
                    {"name": "layer", "typeName": "string", "isOptional": True},
                    {"name": "target_schema", "typeName": "string", "isOptional": True},
                    {"name": "load_mode", "typeName": "string", "isOptional": True},
                    {"name": "merge_strategy", "typeName": "string", "isOptional": True},
                    {"name": "natural_key", "typeName": "string", "isOptional": True},
                    {"name": "config_file", "typeName": "string", "isOptional": True},
                    {"name": "column_count", "typeName": "int", "isOptional": True},
                ],
            }
        ]
    }

    logger.info(f"Creating custom type '{FABRIC_TABLE_TYPE}'")
    client.create_or_update_type_defs(type_defs)


def build_entity(table: dict) -> dict:
    """Convert a scanned table config to a Purview entity definition."""
    qualified_name = table["qualified_name"]

    # Build column schema
    columns = []
    for col in table.get("columns", []):
        columns.append({
            "qualifiedName": f"{qualified_name}#{col['name']}",
            "name": col["name"],
            "type": col.get("data_type", "string"),
            "isNullable": col.get("nullable", True),
            "description": col.get("description", ""),
        })

    entity = {
        "typeName": FABRIC_TABLE_TYPE,
        "attributes": {
            "qualifiedName": qualified_name,
            "name": table["name"],
            "description": table.get("description", ""),
            "layer": table.get("layer"),
            "target_schema": table.get("target_schema"),
            "natural_key": ", ".join(table.get("natural_key", [])),
            "config_file": table.get("config_file"),
            "column_count": table.get("column_count", 0),
        },
        "relationshipAttributes": {
            "schema": columns,
        },
    }

    return entity


def register_entities(
    config_path: str,
    purview_account: str,
    dry_run: bool = False,
) -> None:
    """Register all YAML table configs as Purview entities."""
    tables = scan_yaml_configs(config_path)
    logger.info(f"Scanned {len(tables)} table configs from {config_path}")

    if not tables:
        logger.warning("No table configs found")
        return

    if dry_run:
        print(f"\n=== DRY RUN: Would register {len(tables)} entities ===\n")
        for t in tables:
            print(f"  {t['qualified_name']}")
            print(f"    Layer: {t.get('layer')} | Schema: {t.get('target_schema')}")
            print(f"    Columns: {t.get('column_count')} | Sources: {t.get('source_count')}")
            print(f"    Key: {', '.join(t.get('natural_key', []))}")
            print()
        return

    from purview_client import PurviewClient

    client = PurviewClient(account_name=purview_account)

    # Ensure custom types exist
    ensure_custom_types(client)

    # Build and register entities
    entities = [build_entity(t) for t in tables]

    logger.info(f"Registering {len(entities)} entities...")
    result = client.create_or_update_entities(entities)

    created = result.get("mutatedEntities", {}).get("CREATE", [])
    updated = result.get("mutatedEntities", {}).get("UPDATE", [])
    logger.info(f"Created: {len(created)}, Updated: {len(updated)}")


def main():
    parser = argparse.ArgumentParser(description="Register Delta-Gen configs as Purview entities")
    parser.add_argument("--config-path", required=True, help="Root path to YAML configs")
    parser.add_argument("--purview-account", required=True, help="Purview account name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    args = parser.parse_args()

    register_entities(args.config_path, args.purview_account, args.dry_run)


if __name__ == "__main__":
    main()
