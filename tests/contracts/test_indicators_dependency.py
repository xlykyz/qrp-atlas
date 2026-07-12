"""模块依赖方向架构测试。"""

from __future__ import annotations

from pathlib import Path


def test_contracts_do_not_import_indicators() -> None:
    """contracts 必须保持为 indicators 的单向上游。"""
    project_root = Path(__file__).resolve().parents[2]
    contracts_root = project_root / "src" / "qrp_atlas" / "contracts"

    for path in contracts_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "qrp_atlas.indicators" not in text, (
            f"contracts 不得依赖 indicators: {path.relative_to(project_root)}"
        )
