"""schemas.py - 回测结果 API 响应模型。

Pydantic v2 模型，对应 /api/backtest/* 响应体。
字段契约与 tests/fixtures/backtest_runs/<run_id>/*.json 对齐。
"""

from typing import Any, Optional

from pydantic import BaseModel


class BacktestRunMeta(BaseModel):
    """单次回测运行的元信息。"""

    run_id: str
    owner_user_id: str | None = None
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
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    win_rate_pct: Optional[float] = None
    profit_loss_ratio: Optional[float] = None
    trade_count: int = 0
    avg_holding_days: Optional[float] = None
    max_trade_loss_pct: Optional[float] = None
    max_trade_profit_pct: Optional[float] = None
    skipped_count: int = 0
    turnover: Optional[float] = None
    commission: Optional[float] = None
    stamp_tax: Optional[float] = None
    slippage_cost: Optional[float] = None
    total_cost: Optional[float] = None
    final_equity: Optional[float] = None
    benchmark_id: Optional[str] = None
    benchmark_total_return_pct: Optional[float] = None
    portfolio_total_return_pct: Optional[float] = None
    excess_percentage_point_pct: Optional[float] = None
    relative_return_pct: Optional[float] = None
    excess_total_return_pct: Optional[float] = None
    full_range_excess_available: Optional[bool] = None
    benchmark_sharpe: Optional[float] = None
    excess_sharpe: Optional[float] = None
    daily_active_sharpe: Optional[float] = None


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


class DailyReturnPoint(BaseModel):
    date: str
    daily_return: Optional[float] = None
    equity: Optional[float] = None


class CostBreakdown(BaseModel):
    commission: Optional[float] = None
    stamp_tax: Optional[float] = None
    slippage_cost: Optional[float] = None
    total_cost: Optional[float] = None
    turnover: Optional[float] = None
    final_equity: Optional[float] = None
    total_return_pct: Optional[float] = None


class RollingPerformancePoint(BaseModel):
    model_config = {"extra": "allow"}

    date: str
    equity: Optional[float] = None
    drawdown: Optional[float] = None
    return_w20: Optional[float] = None
    volatility_w20: Optional[float] = None
    sharpe_w20: Optional[float] = None
    drawdown_w20: Optional[float] = None
    return_w60: Optional[float] = None
    volatility_w60: Optional[float] = None
    sharpe_w60: Optional[float] = None
    drawdown_w60: Optional[float] = None


class BenchmarkPoint(BaseModel):
    model_config = {"extra": "allow"}

    date: str
    benchmark_level: float | None = None
    benchmark_return: float | None = None
    benchmark_cumulative_return: float | None = None
    portfolio_return: float | None = None
    portfolio_cumulative_return: float | None = None
    daily_active_return: float | None = None
    excess_return: float | None = None
    excess_percentage_point: float | None = None
    relative_return: float | None = None


class BenchmarkArtifact(BaseModel):
    benchmark_id: str | None = None
    points: list[BenchmarkPoint] = []
    summary: dict[str, Any] = {}
    diagnostics: list[str] = []


class ExposureArtifact(BaseModel):
    model_config = {"extra": "allow"}

    available: bool = False
    industry_available: bool = False
    market_cap_available: bool = False
    reason: str | None = None
    industry: list[dict[str, Any]] = []
    market_cap: list[dict[str, Any]] = []
    position_concentration: list[dict[str, Any]] = []
    note: str | None = None


class ReproducibilityArtifact(BaseModel):
    model_config = {"extra": "allow"}

    locked_to_run_snapshot: bool = False
    snapshot_hash: str | None = None
    strategy_code: str | None = None
    strategy_version: str | None = None
    benchmark_id: str | None = None
    note: str | None = None


class RunDiagnostics(BaseModel):
    result_package_version: Optional[str] = None
    artifact_set: list[str] = []
    has_orders: bool = False
    has_fills: bool = False
    has_snapshots: bool = False
    has_rolling_performance: bool = False
    has_daily_returns: bool = False
    has_benchmark: bool = False
    has_exposures: bool = False
    benchmark_diagnostics: list[str] = []
    full_loss: bool = False
    snapshot_count: int = 0
    order_count: int = 0
    fill_count: int = 0
    trade_count: int = 0
    skipped_count: int = 0


class RunCompareRequest(BaseModel):
    run_ids: list[str]


class RunCompareResponse(BaseModel):
    runs: list[BacktestRunMeta]
    summaries: list[BacktestSummary]
    configs: list[BacktestConfigSnapshot]
    missing: list[str] = []
