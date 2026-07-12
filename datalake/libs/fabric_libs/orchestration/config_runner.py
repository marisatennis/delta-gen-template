"""Helpers for YAML-driven orchestration."""
from __future__ import annotations

import glob
import os
from datetime import datetime, timezone
from typing import Iterable

import yaml
from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider


def load_batch_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def batch_allows(batch_id: int, batch_defs: dict, run_schedule: str) -> bool:
    batch_info = batch_defs.get(str(batch_id), {})
    if batch_info and not batch_info.get("active", True):
        return False
    if run_schedule == "all":
        return True
    schedules = batch_info.get("schedules", [])
    if isinstance(schedules, str):
        schedules = [schedules]
    return run_schedule in schedules


def schedule_time_allows(batch_info: dict) -> bool:
    now = datetime.now(timezone.utc)
    allowed_days = batch_info.get("days_of_week")
    allowed_dom = batch_info.get("days_of_month")
    allowed_months = batch_info.get("months")
    allowed_hours = batch_info.get("hours_utc")

    if allowed_days:
        day_name = now.strftime("%a").lower()[:3]
        if day_name not in [d.lower()[:3] for d in allowed_days]:
            return False
    if allowed_dom and now.day not in allowed_dom:
        return False
    if allowed_months and now.month not in allowed_months:
        return False
    if allowed_hours and now.hour not in allowed_hours:
        return False
    return True


def resolve_yaml_paths(
    config_root: str,
    run_folder: str | None,
    run_files: Iterable[str] | None,
) -> list[str]:
    scan_root = os.path.join(config_root, run_folder) if run_folder else config_root
    target_files = list(run_files or [])
    target_paths = set()
    target_basenames = set()

    for entry in target_files:
        entry_norm = os.path.normpath(entry)
        has_wildcard = any(ch in entry_norm for ch in ["*", "?", "["])
        has_sep = os.path.sep in entry_norm or "/" in entry_norm
        if has_wildcard:
            pattern = entry_norm if os.path.isabs(entry_norm) else os.path.join(config_root, entry_norm)
            target_paths.update(glob.glob(pattern, recursive=True))
        elif has_sep:
            target_paths.add(entry_norm if os.path.isabs(entry_norm) else os.path.join(config_root, entry_norm))
        else:
            target_basenames.add(entry_norm)

    yaml_paths: list[str] = []
    for root, _, files in os.walk(scan_root):
        for filename in files:
            if not filename.endswith(".yaml"):
                continue
            full_path = os.path.join(root, filename)
            if target_files and full_path not in target_paths and filename not in target_basenames:
                continue
            yaml_paths.append(full_path)

    return yaml_paths


def load_table_entries(
    provider: YamlConfigProvider,
    yaml_paths: Iterable[str],
    batch_defs: dict,
    run_schedule: str,
) -> tuple[list[tuple[int, int, TableConfig, str]], list[dict]]:
    """Load table configs, skipping invalid ones but collecting errors."""
    entries: list[tuple[int, int, TableConfig, str]] = []
    errors: list[dict] = []
    for path in yaml_paths:
        try:
            table = provider.load(path)
        except Exception as exc:
            errors.append({"path": path, "error": str(exc)})
            continue
        if not table.policies.orchestration.active:
            continue
        batch_id = table.policies.orchestration.batch
        if not batch_allows(batch_id, batch_defs, run_schedule):
            continue
        if run_schedule != "all" and not schedule_time_allows(batch_defs.get(str(batch_id), {})):
            continue
        entries.append((batch_id, table.policies.orchestration.order, table, path))
    return entries, errors


def collect_schema_list(
    defaults: dict,
    entries: Iterable[tuple[int, int, TableConfig, str]],
    layer: str = None,
) -> set[str]:
    schemas_config = defaults.get("schemas", [])

    if isinstance(schemas_config, dict):
        if layer:
            schema_list = set(schemas_config.get(layer, []))
        else:
            schema_list = set()
            for layer_schemas in schemas_config.values():
                if isinstance(layer_schemas, list):
                    schema_list.update(layer_schemas)
    else:
        schema_list = set(schemas_config)

    for _, _, table, _ in entries:
        schema_name = table.target_schema or table.layer
        if schema_name:
            schema_list.add(schema_name)
    return schema_list


def group_by_batch(entries: Iterable[tuple[int, int, TableConfig, str]]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for batch_id, order, table, full_path in entries:
        grouped.setdefault(str(batch_id), []).append({
            "order": order,
            "table": table,
            "path": full_path,
        })
    return grouped
