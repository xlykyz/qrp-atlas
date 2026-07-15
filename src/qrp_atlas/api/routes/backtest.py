"""回测分析路由。

薄路由层，只负责 HTTP 接口和异常映射，业务逻辑在 qrp_atlas.backtest.results.service。
"""

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from qrp_atlas.auth.dependencies import CurrentUser
from qrp_atlas.config.auth import DEFAULT_LOCAL_USER_ID

from qrp_atlas.backtest.results import (
    BacktestConfigSnapshot,
    BacktestRunMeta,
    BacktestSummary,
    BacktestTrade,
    EquityPoint,
    SkippedTrade,
    ResultFileMissingError,
    RunNotFoundError,
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
    get_exposures,
    get_reproducibility,
    get_equity,
    get_fills,
    get_orders,
    get_rolling_performance,
    get_run_meta,
    get_skipped,
    get_snapshots,
    get_summary,
    get_trades,
    list_runs,
    run_belongs_to,
)

router = APIRouter(prefix="/api/backtest", tags=["回测分析"])


class CompareBody(BaseModel):
    run_ids: list[str]


def _map_errors(exc: Exception) -> HTTPException:
    if isinstance(exc, RunNotFoundError):
        return HTTPException(status_code=404, detail=f"backtest run not found: {exc.run_id}")
    if isinstance(exc, ResultFileMissingError):
        return HTTPException(status_code=404, detail=f"result file missing: {exc.filename}")
    return HTTPException(status_code=500, detail=str(exc))


def _ensure_run_owner(run_id: str, user: CurrentUser) -> None:
    allow_legacy = str(user.user_id) == str(DEFAULT_LOCAL_USER_ID)
    try:
        owned = run_belongs_to(run_id, str(user.user_id), allow_legacy=allow_legacy)
    except (RunNotFoundError, ResultFileMissingError) as exc:
        raise _map_errors(exc)
    if not owned:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")


@router.get("/runs", response_model=List[BacktestRunMeta])
def api_list_runs(user: CurrentUser):
    """列出所有可查看的回测 run。"""
    return list_runs(
        owner_user_id=str(user.user_id),
        allow_legacy=str(user.user_id) == str(DEFAULT_LOCAL_USER_ID),
    )


@router.get("/runs/{run_id}", response_model=BacktestRunMeta)
def api_get_run(run_id: str, user: CurrentUser):
    """返回单个 run 的元信息。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_run_meta(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/summary", response_model=BacktestSummary)
def api_get_summary(run_id: str, user: CurrentUser):
    """返回单个 run 的汇总指标。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_summary(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/equity", response_model=List[EquityPoint])
def api_get_equity(run_id: str, user: CurrentUser):
    """返回净值和回撤曲线。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_equity(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/trades", response_model=List[BacktestTrade])
def api_get_trades(run_id: str, user: CurrentUser):
    """返回交易明细。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_trades(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/skipped", response_model=List[SkippedTrade])
def api_get_skipped(run_id: str, user: CurrentUser):
    """返回被跳过的信号记录。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_skipped(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/config", response_model=BacktestConfigSnapshot)
def api_get_config(run_id: str, user: CurrentUser):
    """返回当前 run 的配置 JSON。"""
    try:
        _ensure_run_owner(run_id, user)
        return get_config(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/orders")
def api_get_orders(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        _ensure_run_owner(run_id, user)
        return get_orders(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/fills")
def api_get_fills(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        _ensure_run_owner(run_id, user)
        return get_fills(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/snapshots")
def api_get_snapshots(run_id: str, user: CurrentUser) -> list[dict[str, Any]]:
    try:
        _ensure_run_owner(run_id, user)
        return get_snapshots(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/daily-returns", response_model=List[DailyReturnPoint])
def api_get_daily_returns(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_daily_returns(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/rolling", response_model=List[RollingPerformancePoint])
def api_get_rolling(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_rolling_performance(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/costs", response_model=Optional[CostBreakdown])
def api_get_costs(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_costs(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/diagnostics", response_model=Optional[RunDiagnostics])
def api_get_diagnostics(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_diagnostics(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/benchmark")
def api_get_benchmark(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_benchmark(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/exposures")
def api_get_exposures(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_exposures(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.get("/runs/{run_id}/reproducibility")
def api_get_reproducibility(run_id: str, user: CurrentUser):
    try:
        _ensure_run_owner(run_id, user)
        return get_reproducibility(run_id)
    except (RunNotFoundError, ResultFileMissingError) as e:
        raise _map_errors(e)


@router.post("/compare", response_model=RunCompareResponse)
def api_compare_runs(body: CompareBody, user: CurrentUser):
    """Compare two or more product runs using locked snapshots."""
    if not body.run_ids:
        raise HTTPException(status_code=400, detail="run_ids required")
    if len(body.run_ids) > 10:
        raise HTTPException(status_code=400, detail="compare supports at most 10 runs")
    for run_id in body.run_ids:
        _ensure_run_owner(run_id, user)
    return compare_runs(body.run_ids)


@router.get("/compare", response_model=RunCompareResponse)
def api_compare_runs_get(user: CurrentUser, run_ids: list[str] = Query(default=[])):
    if not run_ids:
        raise HTTPException(status_code=400, detail="run_ids required")
    if len(run_ids) > 10:
        raise HTTPException(status_code=400, detail="compare supports at most 10 runs")
    for run_id in run_ids:
        _ensure_run_owner(run_id, user)
    return compare_runs(run_ids)


class ReplayBody(BaseModel):
    new_run_id: str | None = None


@router.post("/runs/{run_id}/replay")
def api_replay_run(run_id: str, user: CurrentUser, body: ReplayBody | None = None):
    """Re-execute a product run from its locked reproducibility snapshot."""

    from qrp_atlas.backtest.product.service import (
        BacktestTaskExecutionError,
        get_product_service,
        replay_product_run,
    )

    service = get_product_service()
    payload = body or ReplayBody()
    try:
        _ensure_run_owner(run_id, user)
        return replay_product_run(
            run_id,
            runs_dir=service.runs_dir,
            db_path=service.db_path,
            new_run_id=payload.new_run_id,
            owner_user_id=str(user.user_id),
        )
    except BacktestTaskExecutionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise _map_errors(exc)
