"""loader.py - 回测结果文件读取层。

从 BACKTEST_RUNS_DIR 读取每个 run 目录下的 JSON 文件。
不依赖数据库，未来切换 DuckDB 时替换此层即可。

文件契约（每个 run 目录下）:
- run_meta.json
- summary.json
- equity.json
- trades.json
- skipped.json
- config.json

run_id 仅允许 [A-Za-z0-9_-]+，避免路径穿越。
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

from qrp_atlas.config.paths import BACKTEST_RUNS_DIR, PROJECT_ROOT


class RunNotFoundError(Exception):
    """指定的 run_id 不存在。"""

    def __init__(self, run_id: str):
        super().__init__(f"backtest run not found: {run_id}")
        self.run_id = run_id


class ResultFileMissingError(Exception):
    """run 存在但某个结果文件缺失。"""

    def __init__(self, run_id: str, filename: str):
        super().__init__(f"result file missing: {run_id}/{filename}")
        self.run_id = run_id
        self.filename = filename


_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_run_id(run_id: str) -> str:
    """白名单校验 run_id，拒绝非法字符。"""
    if not run_id or not _RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"invalid run_id: {run_id!r}")
    return run_id


def _default_search_roots() -> list[Path]:
    """Resolve result roots: explicit env first, then product + fixture defaults."""

    roots: list[Path] = []
    env = os.getenv("QRP_ATLAS_BACKTEST_RUNS_DIR")
    if env:
        roots.append(Path(env))
    product = PROJECT_ROOT / "data" / "backtest_runs"
    if product not in roots:
        roots.append(product)
    if BACKTEST_RUNS_DIR not in roots:
        roots.append(BACKTEST_RUNS_DIR)
    # Deduplicate while preserving order.
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root.resolve()) if root.exists() else str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


class BacktestRunsLoader:
    """从本地 JSON 文件读取回测结果。

    Supports one or more search roots so product runs under data/backtest_runs
    and fixture runs under tests/fixtures/backtest_runs can coexist.
    """

    def __init__(self, root: Path | Iterable[Path] | None = None) -> None:
        if root is None:
            self.roots = _default_search_roots()
        elif isinstance(root, (str, Path)):
            self.roots = [Path(root)]
        else:
            self.roots = [Path(item) for item in root]
        # Backward-compatible single-root attribute used by existing tests.
        self.root = self.roots[0]

    def _run_dir(self, run_id: str) -> Path:
        _validate_run_id(run_id)
        for root in self.roots:
            path = root / run_id
            if path.is_dir():
                return path
        raise RunNotFoundError(run_id)

    def list_run_ids(self) -> list[str]:
        """列出所有 run_id，按字母序。Product roots take precedence for duplicates."""
        ids: set[str] = set()
        for root in self.roots:
            if not root.is_dir():
                continue
            for path in root.iterdir():
                if path.is_dir() and _RUN_ID_PATTERN.match(path.name):
                    ids.add(path.name)
        return sorted(ids)

    def _load_json(self, run_id: str, filename: str) -> Any:
        run_dir = self._run_dir(run_id)
        path = run_dir / filename
        if not path.is_file():
            raise ResultFileMissingError(run_id, filename)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def load_run_meta(self, run_id: str) -> dict[str, Any]:
        return self._load_json(run_id, "run_meta.json")

    def load_summary(self, run_id: str) -> dict[str, Any]:
        return self._load_json(run_id, "summary.json")

    def load_equity(self, run_id: str) -> list[dict[str, Any]]:
        data = self._load_json(run_id, "equity.json")
        return data if isinstance(data, list) else []

    def load_trades(self, run_id: str) -> list[dict[str, Any]]:
        data = self._load_json(run_id, "trades.json")
        return data if isinstance(data, list) else []

    def load_skipped(self, run_id: str) -> list[dict[str, Any]]:
        data = self._load_json(run_id, "skipped.json")
        return data if isinstance(data, list) else []

    def load_config(self, run_id: str) -> dict[str, Any]:
        data = self._load_json(run_id, "config.json")
        return data if isinstance(data, dict) else {}
