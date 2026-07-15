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
    CostBreakdown,
    DailyReturnPoint,
    EquityPoint,
    RollingPerformancePoint,
    RunCompareResponse,
    RunDiagnostics,
    SkippedTrade,
)


_loader: BacktestRunsLoader | None = None


def get_loader() -> BacktestRunsLoader:
    global _loader
    if _loader is None:
        _loader = BacktestRunsLoader()
    return _loader


def set_loader_for_tests(loader: BacktestRunsLoader | None) -> None:
    """Test helper to inject/reset the process-wide results loader."""
    global _loader
    _loader = loader


def run_belongs_to(run_id: str, owner_user_id: str, *, allow_legacy: bool = False) -> bool:
    meta = get_loader().load_run_meta(run_id)
    stored_owner = meta.get("owner_user_id")
    if stored_owner is None:
        return allow_legacy
    return str(stored_owner) == str(owner_user_id)


def list_runs(
    *, owner_user_id: str | None = None, allow_legacy: bool = False
) -> List[BacktestRunMeta]:
    """列出所有 run，按 run_id 排序。

    缺失 run_meta.json 的 run 会被跳过，不抛错。
    """
    runs: List[BacktestRunMeta] = []
    for run_id in get_loader().list_run_ids():
        try:
            meta = get_loader().load_run_meta(run_id)
        except ResultFileMissingError:
            continue
        stored_owner = meta.get("owner_user_id")
        if owner_user_id is not None:
            if stored_owner is None and not allow_legacy:
                continue
            if stored_owner is not None and str(stored_owner) != str(owner_user_id):
                continue
        runs.append(BacktestRunMeta.model_validate(meta))
    return runs


def get_run_meta(run_id: str) -> BacktestRunMeta:
    meta = get_loader().load_run_meta(run_id)
    return BacktestRunMeta.model_validate(meta)


def get_summary(run_id: str) -> BacktestSummary:
    data = get_loader().load_summary(run_id)
    if "run_id" not in data:
        data = {"run_id": run_id, **data}
    return BacktestSummary.model_validate(data)


def get_equity(run_id: str) -> List[EquityPoint]:
    data = get_loader().load_equity(run_id)
    return [EquityPoint.model_validate(p) for p in data]


def get_trades(run_id: str) -> List[BacktestTrade]:
    data = get_loader().load_trades(run_id)
    return [BacktestTrade.model_validate(t) for t in data]


def get_skipped(run_id: str) -> List[SkippedTrade]:
    data = get_loader().load_skipped(run_id)
    return [SkippedTrade.model_validate(s) for s in data]


def get_config(run_id: str) -> BacktestConfigSnapshot:
    data = get_loader().load_config(run_id)
    return BacktestConfigSnapshot(run_id=run_id, config=data)


def get_orders(run_id: str) -> list[dict]:
    return get_loader().load_orders(run_id)


def get_fills(run_id: str) -> list[dict]:
    return get_loader().load_fills(run_id)


def get_snapshots(run_id: str) -> list[dict]:
    return get_loader().load_snapshots(run_id)


def get_daily_returns(run_id: str) -> list[DailyReturnPoint]:
    rows = get_loader().load_daily_returns(run_id)
    return [DailyReturnPoint.model_validate(row) for row in rows]


def get_rolling_performance(run_id: str) -> list[RollingPerformancePoint]:
    rows = get_loader().load_rolling_performance(run_id)
    return [RollingPerformancePoint.model_validate(row) for row in rows]


def get_costs(run_id: str) -> CostBreakdown | None:
    data = get_loader().load_costs(run_id)
    if data is None:
        # derive from summary when older packages lack costs.json
        summary = get_loader().load_summary(run_id)
        data = {
            "commission": summary.get("commission"),
            "stamp_tax": summary.get("stamp_tax"),
            "slippage_cost": summary.get("slippage_cost"),
            "total_cost": summary.get("total_cost"),
            "turnover": summary.get("turnover"),
            "final_equity": summary.get("final_equity"),
            "total_return_pct": summary.get("total_return_pct"),
        }
    return CostBreakdown.model_validate(data)


def get_diagnostics(run_id: str) -> RunDiagnostics | None:
    data = get_loader().load_diagnostics(run_id)
    if data is None:
        return None
    return RunDiagnostics.model_validate(data)



def get_benchmark(run_id: str) -> BenchmarkArtifact | None:
    data = get_loader().load_benchmark(run_id)
    if not data:
        return None
    return BenchmarkArtifact.model_validate(data)


def get_exposures(run_id: str) -> ExposureArtifact | None:
    data = get_loader().load_exposures(run_id)
    if not data:
        return None
    return ExposureArtifact.model_validate(data)


def get_reproducibility(run_id: str) -> dict | None:
    return get_loader().load_reproducibility(run_id)


def compare_runs(run_ids: list[str]) -> RunCompareResponse:
    """Compare multiple product runs by meta/summary/config snapshots."""

    runs: list[BacktestRunMeta] = []
    summaries: list[BacktestSummary] = []
    configs: list[BacktestConfigSnapshot] = []
    missing: list[str] = []
    for run_id in run_ids:
        try:
            runs.append(get_run_meta(run_id))
            summaries.append(get_summary(run_id))
            configs.append(get_config(run_id))
        except Exception:  # noqa: BLE001
            missing.append(run_id)
    return RunCompareResponse(runs=runs, summaries=summaries, configs=configs, missing=missing)


__all__ = [
    "RunNotFoundError",
    "ResultFileMissingError",
    "list_runs",
    "run_belongs_to",
    "get_run_meta",
    "get_summary",
    "get_equity",
    "get_trades",
    "get_skipped",
    "get_config",
    "get_orders",
    "get_fills",
    "get_snapshots",
    "get_daily_returns",
    "get_rolling_performance",
    "get_costs",
    "get_diagnostics",
    "compare_runs",
    "get_loader",
    "set_loader_for_tests",
]
