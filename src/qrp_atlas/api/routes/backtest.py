"""回测分析路由。

薄路由层，只负责 HTTP 接口和异常映射，业务逻辑在 qrp_atlas.backtest.results.service。
"""

from typing import List

from fastapi import APIRouter, HTTPException

from qrp_atlas.backtest.results import (
    BacktestConfigSnapshot,
    BacktestRunMeta,
    BacktestSummary,
    BacktestTrade,
    EquityPoint,
    SkippedTrade,
    ResultFileMissingError,
    RunNotFoundError,
    get_config,
    get_equity,
    get_run_meta,
    get_skipped,
    get_summary,
    get_trades,
    list_runs,
)

router = APIRouter(prefix="/api/backtest", tags=["回测分析"])


@router.get("/runs", response_model=List[BacktestRunMeta])
def api_list_runs():
    """列出所有可查看的回测 run。"""
    return list_runs()


@router.get("/runs/{run_id}", response_model=BacktestRunMeta)
def api_get_run(run_id: str):
    """返回单个 run 的元信息。"""
    try:
        return get_run_meta(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")


@router.get("/runs/{run_id}/summary", response_model=BacktestSummary)
def api_get_summary(run_id: str):
    """返回单个 run 的汇总指标。"""
    try:
        return get_summary(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")


@router.get("/runs/{run_id}/equity", response_model=List[EquityPoint])
def api_get_equity(run_id: str):
    """返回净值和回撤曲线。"""
    try:
        return get_equity(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")


@router.get("/runs/{run_id}/trades", response_model=List[BacktestTrade])
def api_get_trades(run_id: str):
    """返回交易明细。"""
    try:
        return get_trades(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")


@router.get("/runs/{run_id}/skipped", response_model=List[SkippedTrade])
def api_get_skipped(run_id: str):
    """返回被跳过的信号记录。"""
    try:
        return get_skipped(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")


@router.get("/runs/{run_id}/config", response_model=BacktestConfigSnapshot)
def api_get_config(run_id: str):
    """返回当前 run 的配置 JSON。"""
    try:
        return get_config(run_id)
    except RunNotFoundError:
        raise HTTPException(status_code=404, detail=f"backtest run not found: {run_id}")
    except ResultFileMissingError as e:
        raise HTTPException(status_code=500, detail=f"result file missing: {e.filename}")
