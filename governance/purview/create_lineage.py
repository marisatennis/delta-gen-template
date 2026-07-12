"""Create Purview lineage relationships from Delta-Gen YAML configs.

Maps YAML source references to upstream-downstream lineage in Purview.
Each table's sources become inputs, and the table itself is the output.

Usage:
    python create_lineage.py --config-path ../../datalake/inputs/ --purview-account myaccount
    python create_lineage.py --config-path ../../datalake/inputs/ --purview-account myaccount --dry-run
"""
from __future__ import annotations

import argparse
import logging

from scan_configs import scan_yaml_configs

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def build_lineage_process(table: dict) -> dict | None:
    """Build a Purview process entity representing a Delta-Gen pipeline."""
    sources = table.get("sources", [])
    if not sources:
        return None

    # Skip generated sources (no upstream lineage)
    real_sources = [s for s in sources if not s.get("generated")]
    if not real_sources:
        return None

    target_qn = table["qualified_name"]
    process_qn = f"{target_qn}__pipeline"

    inputs = []
    for src in real_sources:
        source_table = src.get("table")
        if source_table:
            schema, name = source_table.split(".", 1) if "." in source_table else ("default", source_table)
            inputs.append({
                "typeName": "fabric_lakehouse_table",
                "uniqueAttributes": {
                    "qualifiedName": f"fabric://{schema}/{name}",
                },
            })

    if not inputs:
        return None

    return {
        "typeName": "Process",
        "attributes": {
            "qualifiedName": process_qn,
            "name": f"Delta-Gen: {table['name']}",
            "description": f"Delta-Gen pipeline for {table['name']} ({table.get('layer', '?')} layer)",
        },
        "relationshipAttributes": {
            "inputs": inputs,
            "outputs": [
                {
                    "typeName": "fabric_lakehouse_table",
                    "uniqueAttributes": {"qualifiedName": target_qn},
                }
            ],
        },
    }


def create_lineage(config_path: str, purview_account: str, dry_run: bool = False) -> None:
    """Create lineage for all YAML table configs."""
    tables = scan_yaml_configs(config_path)
    logger.info(f"Scanned {len(tables)} table configs")

    processes = []
    for t in tables:
        proc = build_lineage_process(t)
        if proc:
            processes.append((t, proc))

    if dry_run:
        print(f"\n=== DRY RUN: Would create {len(processes)} lineage relationships ===\n")
        for t, proc in processes:
            sources = [s.get("table", "?") for s in t.get("sources", []) if s.get("table")]
            print(f"  {' + '.join(sources)} --> {t['qualified_name']}")
        return

    from purview_client import PurviewClient

    client = PurviewClient(account_name=purview_account)

    logger.info(f"Creating {len(processes)} lineage relationships...")
    for t, proc in processes:
        try:
            client.create_lineage(proc)
            logger.info(f"  Created lineage for {t['name']}")
        except Exception as e:
            logger.error(f"  Failed lineage for {t['name']}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Create Purview lineage from Delta-Gen configs")
    parser.add_argument("--config-path", required=True, help="Root path to YAML configs")
    parser.add_argument("--purview-account", required=True, help="Purview account name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    args = parser.parse_args()

    create_lineage(args.config_path, args.purview_account, args.dry_run)


if __name__ == "__main__":
    main()
