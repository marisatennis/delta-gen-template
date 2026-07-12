"""Scan Delta-Gen YAML configs and generate a catalog manifest.

Reads all YAML table configs and produces a JSON manifest suitable for
registering with Purview or reviewing before applying changes.

Usage:
    python scan_configs.py --config-path ../../datalake/inputs/ --output manifest.json
    python scan_configs.py --config-path ../../datalake/inputs/ --format table
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import yaml


def scan_yaml_configs(config_path: str) -> list[dict]:
    """Scan a directory tree for Delta-Gen YAML configs and extract metadata."""
    tables = []
    config_root = Path(config_path)

    for yaml_file in sorted(config_root.rglob("*.yaml")):
        # Skip config/defaults/batch files
        if yaml_file.parent.name == "config":
            continue

        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
        except Exception as e:
            print(f"  Warning: Could not parse {yaml_file}: {e}", file=sys.stderr)
            continue

        if not isinstance(data, dict) or "name" not in data:
            continue

        table_info = _extract_table_info(data, yaml_file, config_root)
        tables.append(table_info)

    return tables


def _extract_table_info(data: dict, yaml_file: Path, config_root: Path) -> dict:
    """Extract governance-relevant metadata from a YAML table config."""
    columns = []
    for col in data.get("columns", []):
        col_info = {
            "name": col.get("name"),
            "data_type": col.get("data_type"),
            "nullable": col.get("nullable", True),
            "description": col.get("description", ""),
        }
        # Extract governance extensions
        extensions = col.get("extensions", {})
        if extensions:
            if "classification" in extensions:
                col_info["classification"] = extensions["classification"]
            if "glossary_term" in extensions:
                col_info["glossary_term"] = extensions["glossary_term"]
            if "sensitivity" in extensions:
                col_info["sensitivity"] = extensions["sensitivity"]
        columns.append(col_info)

    sources = []
    for src in data.get("sources", []):
        src_info = {"name": src.get("name")}
        if src.get("table"):
            src_info["table"] = src["table"]
        if src.get("generated"):
            src_info["generated"] = True
        sources.append(src_info)

    return {
        "name": data.get("name"),
        "layer": data.get("layer"),
        "target_schema": data.get("target_schema"),
        "description": data.get("description", ""),
        "qualified_name": _build_qualified_name(data),
        "config_file": str(yaml_file.relative_to(config_root)),
        "natural_key": data.get("natural_key", []),
        "columns": columns,
        "sources": sources,
        "column_count": len(columns),
        "source_count": len(sources),
    }


def _build_qualified_name(data: dict) -> str:
    """Build a Purview-style qualified name from table config."""
    schema = data.get("target_schema") or data.get("layer", "default")
    name = data.get("name", "unknown")
    return f"fabric://{schema}/{name}"


def print_table_summary(tables: list[dict]) -> None:
    """Print a formatted summary table."""
    print(f"\n{'Layer':<10} {'Schema':<15} {'Table':<35} {'Cols':<6} {'Sources':<8} {'Key'}")
    print("-" * 100)
    for t in tables:
        layer = t.get("layer", "?")
        schema = t.get("target_schema", "?")
        name = t.get("name", "?")
        cols = t.get("column_count", 0)
        srcs = t.get("source_count", 0)
        key = ", ".join(t.get("natural_key", []))
        print(f"{layer:<10} {schema:<15} {name:<35} {cols:<6} {srcs:<8} {key}")

    # Summary stats
    classified = sum(
        1 for t in tables for c in t.get("columns", []) if c.get("classification")
    )
    total_cols = sum(t.get("column_count", 0) for t in tables)
    print(f"\nTotal: {len(tables)} tables, {total_cols} columns, {classified} classified columns")


def main():
    parser = argparse.ArgumentParser(description="Scan Delta-Gen YAML configs for catalog manifest")
    parser.add_argument("--config-path", required=True, help="Root path to scan for YAML configs")
    parser.add_argument("--output", help="Output JSON file (default: stdout)")
    parser.add_argument("--format", choices=["json", "table"], default="json", help="Output format")
    args = parser.parse_args()

    tables = scan_yaml_configs(args.config_path)

    if args.format == "table":
        print_table_summary(tables)
    else:
        output = json.dumps(tables, indent=2, default=str)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Written {len(tables)} table definitions to {args.output}")
        else:
            print(output)


if __name__ == "__main__":
    main()
