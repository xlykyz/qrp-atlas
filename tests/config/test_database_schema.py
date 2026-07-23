from __future__ import annotations

from pathlib import Path


def test_legacy_init_script_uses_package_initializer():
    repository_root = Path(__file__).resolve().parents[2]
    source = (repository_root / "scripts" / "init_db.py").read_text(encoding="utf-8")
    assert "from qrp_atlas.database import create_empty_database" in source
    assert "CREATE TABLE" not in source
