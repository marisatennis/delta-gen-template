"""Top-level package for reusable Fabric utilities.

Importing this package registers generic column-cleaning plugins
(`clean_decimal`, ...) with Delta-Gen so YAML configs can reference
them via `extensions.transform`.
"""
from . import cleaning  # noqa: F401  -- registers @register_column plugins
