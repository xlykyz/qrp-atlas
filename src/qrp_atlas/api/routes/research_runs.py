"""研究区回测结果路由（只读）。

把独立研究区 `QRP_RESEARCH_RUNS_DIR`（默认 ~/projects/backtest-research/results）
产出的 run 暴露给前端，**与 /api/backtest/* 完全隔离**：

- 独立命名空间 `/api/research/*`，不改动 /api/backtest/* 的任何行为与返回结构。
- 只读：不提供任何写入 / 删除 / 触发回测的接口。
- 归属：不做 owner 过滤（研究区是个人研究沙盒，只读，对已登录用户开放）。
- 数据来源：只扫研究区根，与生产根互不干扰，避免 run_id 撞名。

复用 BacktestRunsLoader + results/schemas.py 的 Pydantic 模型，契约与生产一致
（result_package_version 1.1，17 个 JSON）。
"""

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from qrp_atlas.auth.dependencies import CurrentUser

from qrp_atlas.backtest.results import (
    BacktestConfigSnapshot,
    BacktestRunMeta,
    BacktestSummary,
    BacktestTrade,
    EquityPoint,
    ResultFileMissingError,
    RunNotFoundError,
    SkippedTrade,
)
from qrp_atlas.backtest.results.schemas import (
    CostBreakdown,
    DailyReturnPoint,
    RollingPerformancePoint,
    RunCompareResponse,
    RunDiagnostics,
)
from qrp_atlas.backtest.results.service import (
    compare_runs,
    get_benchmark,
    get_config,
    get_costs,
    get_daily_returns,
    get_diagnostics,
    get_equity,
    get_exposures,
    get_fills,
    get_orders,
    get_reproducibility,
    get_research_loader,
    get_rolling_performance,
    get_run_meta,
    get_skipped,
    get_snapshots,
    get_summary,
    get_trades,
    list_runs,
)

router = APIRouter(prefix="/api/research", tags=["研究区回测"])


def _map_errors(exc: Exception) -> HTTPException:
    if isinstance(exc, RunNotFoundError):
        return HTTPException(status_code=404, detail=f"backtest run not found: {exc.run_id}")
    if isinstance(exc, ResultFileMissingError):
        return HTTPException(status_code=404, detail=f"result file missing: {exc.filename}")
    return HTTPException(status_code=500, detail=str(exc))


def _loader():
    """研究区 loader（只扫 QRP_RESEARCH_RUNS_DIR）。"""
    return get_research_loader()


@router.get("/runs", response_model=List[BacktestRunMeta])
def api_research_list_runs(user: CurrentUser):
    """列出研究区所有 run（不做 owner 过滤）。

    研究区根不存在或为空时返回 []，不报错。
    缺 run_meta.json 的 run 会被跳过。
    """
    return list_runs(loader=_loader())


@router.get("/runs/{run_id}", response_model=BacktestRunMeta)
def api_research_get_run(run_id: str, user: CurrentUser):
    try:
        return get_run_meta(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/summary", response_model=BacktestSummary)
def api_research_get_summary(run_id: str, user: CurrentUser):
    try:
        return get_summary(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/equity", response_model=List[EquityPoint])
def api_research_get_equity(run_id: str, user: CurrentUser):
    try:
        return get_equity(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/trades", response_model=List[BacktestTrade])
def api_research_get_trades(run_id: str, user: CurrentUser):
    try:
        return get_trades(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/skipped", response_model=List[SkippedTrade])
def api_research_get_skipped(run_id: str, user: CurrentUser):
    try:
        return get_skipped(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/config", response_model=BacktestConfigSnapshot)
def api_research_get_config(run_id: str, user: CurrentUser):
    try:
        return get_config(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/costs", response_model=Optional[CostBreakdown])
def api_research_get_costs(run_id: str, user: CurrentUser):
    try:
        return get_costs(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/diagnostics", response_model=Optional[RunDiagnostics])
def api_research_get_diagnostics(run_id: str, user: CurrentUser):
    try:
        return get_diagnostics(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/daily-returns", response_model=List[DailyReturnPoint])
def api_research_get_daily_returns(run_id: str, user: CurrentUser):
    try:
        return get_daily_returns(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/rolling", response_model=List[RollingPerformancePoint])
def api_research_get_rolling(run_id: str, user: CurrentUser):
    try:
        return get_rolling_performance(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


# ── 以下返回结构沿用 /api/backtest/* 的原始契约（object / object[] / null） ──


@router.get("/runs/{run_id}/orders")
def api_research_get_orders(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        return get_orders(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/fills")
def api_research_get_fills(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        return get_fills(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/snapshots")
def api_research_get_snapshots(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        return get_snapshots(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/benchmark")
def api_research_get_benchmark(run_id: str, user: CurrentUser):
    try:
        return get_benchmark(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/exposures")
def api_research_get_exposures(run_id: str, user: CurrentUser):
    try:
        return get_exposures(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/reproducibility")
def api_research_get_reproducibility(run_id: str, user: CurrentUser):
    try:
        return get_reproducibility(run_id, loader=_loader())
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/compare", response_model=RunCompareResponse)
def api_research_compare_runs_get(user: CurrentUser, run_ids: list[str] = Query(default=[])):
    if not run_ids:
        raise HTTPException(status_code=400, detail="run_ids required")
    if len(run_ids) > 10:
        raise HTTPException(status_code=400, detail="compare supports at most 10 runs")
    return compare_runs(run_ids, loader=_loader())