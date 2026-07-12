"""Macro expansion and defaults handling for YAML configurations."""
from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml


def load_defaults(defaults_path: str | Path | None = None) -> dict[str, Any]:
    """Load defaults from a YAML file.

    Args:
        defaults_path: Path to defaults.yaml. If None, looks for defaults.yaml
                      in the same directory as the config being loaded.

    Returns:
        Dictionary containing default values

    Raises:
        FileNotFoundError: If defaults file doesn't exist
    """
    if defaults_path is None:
        return {}

    path = Path(defaults_path)
    if not path.exists():
        raise FileNotFoundError(f"Defaults file not found: {path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return data if data is not None else {}


def expand_macros(config: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Recursively expand ${...} macros in configuration.

    Supports dot-notation paths like ${defaults.policies.load_mode}.

    Args:
        config: Configuration dictionary with potential macros
        defaults: Defaults dictionary to resolve macro values from

    Returns:
        Configuration with all macros expanded

    Raises:
        ValueError: If a macro references a non-existent path

    Examples:
        >>> defaults = {"policies": {"load_mode": "merge"}}
        >>> config = {"policies": {"optimisation": {"load_mode": "${defaults.policies.load_mode}"}}}
        >>> expand_macros(config, defaults)
        {"policies": {"optimisation": {"load_mode": "merge"}}}
    """
    # Combine defaults and config for resolution context
    resolution_context = {"defaults": defaults}
    resolution_context.update(config)

    def resolve_value(value: Any, path: str = "") -> Any:
        """Recursively resolve macros in a value."""
        if isinstance(value, str):
            return _expand_string_macros(value, resolution_context, path)
        elif isinstance(value, dict):
            return {k: resolve_value(v, f"{path}.{k}" if path else k) for k, v in value.items()}
        elif isinstance(value, list):
            return [resolve_value(item, f"{path}[{i}]") for i, item in enumerate(value)]
        else:
            return value

    return resolve_value(config)


def _expand_string_macros(value: str, context: dict[str, Any], path: str) -> Any:
    """Expand ${...} macros in a string value.

    Args:
        value: String that may contain macros
        context: Dictionary context for resolving macro paths
        path: Current path in config (for error messages)

    Returns:
        Expanded value (may be string, int, bool, etc.)

    Raises:
        ValueError: If macro path doesn't exist
    """
    # Pattern to match ${path.to.value}
    macro_pattern = r"\$\{([^}]+)\}"

    def replace_macro(match: re.Match) -> str:
        macro_path = match.group(1).strip()
        try:
            resolved_value = _resolve_path(macro_path, context)
            # Convert to string for interpolation inside a larger string
            return str(resolved_value)
        except KeyError as e:
            raise ValueError(
                f"Macro expansion failed at '{path}': "
                f"Could not resolve '${{{macro_path}}}'. "
                f"Available paths: {_get_available_paths(context)}"
            ) from e

    # Check if entire value is a single macro (preserve type)
    single_macro_match = re.fullmatch(macro_pattern, value)
    if single_macro_match:
        macro_path = single_macro_match.group(1).strip()
        try:
            return _resolve_path(macro_path, context)
        except KeyError as e:
            raise ValueError(
                f"Macro expansion failed at '{path}': "
                f"Could not resolve '${{{macro_path}}}'. "
                f"Available paths: {_get_available_paths(context)}"
            ) from e

    # Otherwise, do string replacement
    result = re.sub(macro_pattern, replace_macro, value)
    return result


def _resolve_path(path: str, context: dict[str, Any]) -> Any:
    """Resolve a dot-notation path in a nested dictionary.

    Args:
        path: Dot-separated path like "defaults.policies.load_mode"
        context: Nested dictionary to resolve from

    Returns:
        Value at the specified path

    Raises:
        KeyError: If path doesn't exist
    """
    parts = path.split(".")
    current = context

    for part in parts:
        if not isinstance(current, dict):
            raise KeyError(f"Cannot navigate into non-dict at '{part}'")
        if part not in current:
            raise KeyError(f"Key '{part}' not found in path '{path}'")
        current = current[part]

    return current


def _get_available_paths(context: dict[str, Any], prefix: str = "", max_depth: int = 3) -> list[str]:
    """Get list of available paths in context for error messages.

    Args:
        context: Dictionary to extract paths from
        prefix: Current path prefix
        max_depth: Maximum depth to traverse

    Returns:
        List of dot-notation paths
    """
    if max_depth == 0:
        return []

    paths = []
    for key, value in context.items():
        current_path = f"{prefix}.{key}" if prefix else key
        paths.append(current_path)
        if isinstance(value, dict):
            paths.extend(_get_available_paths(value, current_path, max_depth - 1))

    return paths[:20]  # Limit for readability


def merge_column_templates(config: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Merge column templates from defaults.common_columns into config columns.

    For each column in the config, if a template exists in defaults.common_columns
    with the same name, merge it in (config column values take precedence).

    Args:
        config: Configuration dictionary
        defaults: Defaults dictionary with optional common_columns section
                 (may be nested under "defaults" key)

    Returns:
        Config with column templates merged

    Examples:
        >>> defaults = {
        ...     "common_columns": {
        ...         "is_active": {"data_type": "boolean", "nullable": False, "default": True}
        ...     }
        ... }
        >>> config = {
        ...     "stages": [{
        ...         "columns": [
        ...             {"name": "is_active", "temporary": False}
        ...         ]
        ...     }]
        ... }
        >>> result = merge_column_templates(config, defaults)
        >>> result["stages"][0]["columns"][0]
        {'name': 'is_active', 'data_type': 'boolean', 'nullable': False, 'default': True, 'temporary': False}
    """
    # Handle nested defaults structure (defaults.yaml has top-level "defaults" key)
    if "defaults" in defaults and "common_columns" in defaults["defaults"]:
        common_columns = defaults["defaults"]["common_columns"]
    else:
        common_columns = defaults.get("common_columns", {})

    if not common_columns:
        return config

    # Deep copy to avoid mutating original
    result = copy.deepcopy(config)

    # Process stages if they exist (TableConfig structure)
    if "stages" in result:
        for stage in result["stages"]:
            if "columns" in stage and isinstance(stage["columns"], list):
                for column in stage["columns"]:
                    if isinstance(column, dict) and "name" in column:
                        _merge_column_template(column, common_columns)

    # Also handle top-level columns if they exist (other config types)
    if "columns" in result and isinstance(result["columns"], list):
        for column in result["columns"]:
            if isinstance(column, dict) and "name" in column:
                _merge_column_template(column, common_columns)

    return result


def _merge_column_template(column: dict[str, Any], common_columns: dict[str, Any]) -> None:
    """Merge template for a single column (in-place).

    Args:
        column: Column dictionary with at least a 'name' field
        common_columns: Dictionary of common column templates
    """
    column_name = column.get("name")
    if column_name and column_name in common_columns:
        template = common_columns[column_name]
        # Merge template values, but column config takes precedence
        for key, value in template.items():
            if key not in column:
                column[key] = value


def merge_defaults(config: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Deep merge defaults into config (config takes precedence).

    Also handles smart column template matching:
    - For columns named X, auto-merges defaults.common_columns.X if it exists
    - Column-specific values in config override template values

    Args:
        config: Configuration dictionary
        defaults: Defaults dictionary

    Returns:
        Merged dictionary with config values taking precedence

    Examples:
        >>> defaults = {
        ...     "policies": {"optimisation": {"load_mode": "merge"}},
        ...     "common_columns": {"is_active": {"data_type": "boolean", "nullable": False}}
        ... }
        >>> config = {
        ...     "name": "test",
        ...     "stages": [{"columns": [{"name": "is_active", "temporary": False}]}]
        ... }
        >>> result = merge_defaults(config, defaults)
        >>> # Policies merged + column template applied
    """
    def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
        """Recursively merge overlay into base."""
        result = base.copy()
        for key, value in overlay.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                # Config value takes precedence over defaults
                result[key] = value
        return result

    # Don't merge the 'defaults' key itself
    config_without_defaults = {k: v for k, v in config.items() if k != "defaults"}

    # First, do general defaults merging
    merged = deep_merge(defaults, config_without_defaults)

    # Then, apply smart column template matching
    merged = merge_column_templates(merged, defaults)

    return merged
