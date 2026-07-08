"""schemas.py - 回测结果 API 响应模型。

Pydantic v2 模型，对应 /api/backtest/* 响应体。
字段契约与 tests/fixtures/backtest_runs/<run_id>/*.json 对齐。
"""

from typing import Any, Optional

from pydantic import BaseModel


class BacktestRunMeta(BaseModel):
    """单次回测运行的元信息。"""

    run_id: str
    name: str
    strategy_name: str
    universe: str
    start_date: str
    end_date: str
    created_at: str
    status: str


class BacktestSummary(BaseModel):
    """单次回测的汇总指标。

    百分比字段统一使用 pct 后缀，数值单位为百分数（5.25 = 5.25%）。
    缺失字段允许 None。
    """

    run_id: str
    total_return_pct: Optional[float] = None
    annual_return_pct: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    win_rate_pct: Optional[float] = None
    profit_loss_ratio: Optional[float] = None
    trade_count: int = 0
    avg_holding_days: Optional[float] = None
    max_trade_loss_pct: Optional[float] = None
    max_trade_profit_pct: Optional[float] = None
    skipped_count: int = 0


class EquityPoint(BaseModel):
    """净值曲线单个点。"""

    date: str
    equity: float
    drawdown_pct: float


class BacktestTrade(BaseModel):
    """单笔交易明细。"""

    trade_id: str
    asset_id: str
    signal_date: str
    entry_date: str
    entry_price: float
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    holding_days: Optional[int] = None
    return_pct: Optional[float] = None
    mae_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    exit_reason: Optional[str] = None
    status: str


class SkippedTrade(BaseModel):
    """被跳过的信号记录。"""

    asset_id: Optional[str] = None
    signal_date: Optional[str] = None
    reason: str
    detail: Optional[str] = None


class BacktestConfigSnapshot(BaseModel):
    """回测配置 JSON 快照。"""

    run_id: str
    config: dict[str, Any]
