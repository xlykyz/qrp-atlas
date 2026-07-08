"""service.py - 回测结果业务层。

调用 loader 读取 JSON，转换为 schema 对象返回。
router 层只调用本模块的函数。
"""

from __future__ import annotations

from typing import List

from .loader import BacktestRunsLoader, ResultFileMissingError, RunNotFoundError
from .schemas import (
    BacktestConfigSnapshot,
    BacktestRunMeta,
    BacktestSummary,
    BacktestTrade,
    EquityPoint,
    SkippedTrade,
)


_loader = BacktestRunsLoader()


def list_runs() -> List[BacktestRunMeta]:
    """列出所有 run，按 run_id 排序。

    缺失 run_meta.json 的 run 会被跳过，不抛错。
    """
    runs: List[BacktestRunMeta] = []
    for run_id in _loader.list_run_ids():
        try:
            meta = _loader.load_run_meta(run_id)
        except ResultFileMissingError:
            continue
        runs.append(BacktestRunMeta.model_validate(meta))
    return runs


def get_run_meta(run_id: str) -> BacktestRunMeta:
    meta = _loader.load_run_meta(run_id)
    return BacktestRunMeta.model_validate(meta)


def get_summary(run_id: str) -> BacktestSummary:
    data = _loader.load_summary(run_id)
    if "run_id" not in data:
        data = {"run_id": run_id, **data}
    return BacktestSummary.model_validate(data)


def get_equity(run_id: str) -> List[EquityPoint]:
    data = _loader.load_equity(run_id)
    return [EquityPoint.model_validate(p) for p in data]


def get_trades(run_id: str) -> List[BacktestTrade]:
    data = _loader.load_trades(run_id)
    return [BacktestTrade.model_validate(t) for t in data]


def get_skipped(run_id: str) -> List[SkippedTrade]:
    data = _loader.load_skipped(run_id)
    return [SkippedTrade.model_validate(s) for s in data]


def get_config(run_id: str) -> BacktestConfigSnapshot:
    data = _loader.load_config(run_id)
    return BacktestConfigSnapshot(run_id=run_id, config=data)


__all__ = [
    "RunNotFoundError",
    "ResultFileMissingError",
    "list_runs",
    "get_run_meta",
    "get_summary",
    "get_equity",
    "get_trades",
    "get_skipped",
    "get_config",
]
