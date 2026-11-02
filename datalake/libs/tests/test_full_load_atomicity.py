"""Source contract for safe full-load notebook replacement.

These notebooks run in Fabric, so this fast test protects the critical ordering
in CI without pretending to replace the required live Delta verification.
"""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
TEMPLATES = (
    ROOT / "platform/silver/silver-template.Notebook/notebook-content.py",
    ROOT / "platform/gold/gold-template.Notebook/notebook-content.py",
)


@pytest.mark.parametrize("template", TEMPLATES, ids=("silver", "gold"))
def test_full_load_never_drops_the_current_table_before_replacement(template: Path):
    source = template.read_text(encoding="utf-8")

    assert "DROP TABLE IF EXISTS" not in source
    assert '.mode("overwrite")' in source
    assert '"overwriteSchema", "true"' in source
    assert source.index("builder.build(") < source.index('.mode("overwrite")')
