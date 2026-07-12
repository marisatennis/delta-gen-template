"""Smoke test for the cleaning plugin registry.

Verifies that importing `fabric_libs` registers `clean_decimal` with
Delta-Gen's column-plugin registry. This catches the most common breakage
(import errors, decorator mistakes) without needing a Spark session.
"""
from deltagen.plugins.registry import get_column_plugin


def test_import_fabric_libs_registers_clean_decimal():
    import fabric_libs  # noqa: F401  -- side-effect import for plugin registration

    plugin = get_column_plugin("clean_decimal")
    assert plugin is not None, "clean_decimal should be registered after importing fabric_libs"
    assert callable(plugin)
