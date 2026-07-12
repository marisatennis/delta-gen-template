"""Sync glossary terms from Delta-Gen YAML column descriptions to Purview.

Extracts column descriptions and governance extensions from YAML configs
and creates/updates corresponding glossary terms in Purview.

Usage:
    python sync_glossary.py --config-path ../../datalake/inputs/ --purview-account myaccount
    python sync_glossary.py --config-path ../../datalake/inputs/ --purview-account myaccount --dry-run
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict

from scan_configs import scan_yaml_configs

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def extract_glossary_terms(tables: list[dict]) -> dict[str, dict]:
    """Extract unique glossary terms from column metadata."""
    terms: dict[str, dict] = {}

    for table in tables:
        for col in table.get("columns", []):
            term_name = col.get("glossary_term")
            if not term_name:
                continue

            if term_name not in terms:
                terms[term_name] = {
                    "name": term_name,
                    "description": col.get("description", ""),
                    "tables_using": [],
                    "classifications": set(),
                }

            terms[term_name]["tables_using"].append(
                f"{table.get('target_schema', '?')}.{table['name']}.{col['name']}"
            )

            if col.get("classification"):
                terms[term_name]["classifications"].add(col["classification"])

    # Convert sets to lists for JSON serialization
    for t in terms.values():
        t["classifications"] = sorted(t["classifications"])

    return terms


def sync_glossary(config_path: str, purview_account: str, dry_run: bool = False) -> None:
    """Sync glossary terms from YAML configs to Purview."""
    tables = scan_yaml_configs(config_path)
    terms = extract_glossary_terms(tables)

    logger.info(f"Found {len(terms)} unique glossary terms across {len(tables)} tables")

    if dry_run:
        print(f"\n=== DRY RUN: Would sync {len(terms)} glossary terms ===\n")
        for name, info in sorted(terms.items()):
            print(f"  {name}")
            print(f"    Description: {info['description'][:80]}")
            print(f"    Used in: {len(info['tables_using'])} column(s)")
            if info["classifications"]:
                print(f"    Classifications: {', '.join(info['classifications'])}")
            print()
        return

    from purview_client import PurviewClient

    client = PurviewClient(account_name=purview_account)

    glossary = client.get_glossary()
    glossary_guid = glossary[0]["guid"] if isinstance(glossary, list) else glossary.get("guid")

    for name, info in terms.items():
        try:
            term_body = {
                "name": name,
                "qualifiedName": f"{name}@Glossary",
                "longDescription": info["description"],
                "anchor": {"glossaryGuid": glossary_guid},
            }
            client.create_glossary_term(term_body)
            logger.info(f"  Synced term: {name}")
        except Exception as e:
            logger.error(f"  Failed to sync term '{name}': {e}")


def main():
    parser = argparse.ArgumentParser(description="Sync glossary terms from Delta-Gen configs")
    parser.add_argument("--config-path", required=True, help="Root path to YAML configs")
    parser.add_argument("--purview-account", required=True, help="Purview account name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    args = parser.parse_args()

    sync_glossary(args.config_path, args.purview_account, args.dry_run)


if __name__ == "__main__":
    main()
