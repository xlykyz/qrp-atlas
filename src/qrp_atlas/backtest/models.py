"""
models.py - 回测引擎数据结构

定义规则配置对象（EntryRule / ExitRule / PositionRule / CostRule / BacktestConfig）
和结果对象（Trade / Skipped / BacktestResult）。

约定:
- 所有日期字段统一使用 ISO 字符串 "YYYY-MM-DD"。
- 收益率统一用小数表示（0.05 表示 5%）。
- v0.1 只支持 long 方向；非 long 信号进入 skipped，不在这里报错。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class EntryRule:
    """入场规则。

    Attributes:
        timing: 入场时机，v0.1 支持 signal_close / next_open / next_close。
        price_field: 入场取价字段，如 "open" / "close"。
    """

    timing: str
    price_field: str


@dataclass(frozen=True)
class ExitRule:
    """出场规则。

    Attributes:
        type: 出场类型，v0.1 只支持 hold_n_bars。
        bars: 入场后持有 N 根 bar 后出场。
        price_field: 出场取价字段，如 "close"。
    """

    type: str
    bars: int
    price_field: str


@dataclass(frozen=True)
class PositionRule:
    """仓位规则。

    v0.1 中仓位规则主要用于记录和后续扩展，不参与组合资金曲线计算。

    Attributes:
        initial_cash: 初始资金。
        position_pct: 单笔交易占用资金比例。
        max_positions: 最大同时持仓数。
        allow_overlap: 同一资产是否允许重复开仓。
        compound: 是否复利。
    """

    initial_cash: float
    position_pct: float
    max_positions: int
    allow_overlap: bool
    compound: bool


@dataclass(frozen=True)
class CostRule:
    """成本规则。

    成本计算方式:
        buy_cost  = commission_rate + slippage_bps / 10000
        sell_cost = commission_rate + stamp_tax_rate + slippage_bps / 10000
        net_return = gross_return - buy_cost - sell_cost

    Attributes:
        commission_rate: 双边佣金费率（小数）。
        stamp_tax_rate: 卖出印花税税率（小数）。
        slippage_bps: 滑点（基点 bps），1 bps = 0.0001。
    """

    commission_rate: float
    stamp_tax_rate: float
    slippage_bps: float


@dataclass(frozen=True)
class BacktestConfig:
    """回测总配置。

    Attributes:
        name: 配置名称（标签）。
        entry: 入场规则。
        exit: 出场规则。
        position: 仓位规则。
        cost: 成本规则。
    """

    name: str
    entry: EntryRule
    exit: ExitRule
    position: PositionRule
    cost: CostRule


@dataclass(frozen=True)
class Trade:
    """单笔交易结果。

    收益率统一用小数表示：0.05 = 5%，-0.03 = -3%。
    MAE / MFE 区间包含 entry bar 到 exit bar。

    Attributes:
        asset_id: 资产代码。
        asset_name: 资产名称。
        asset_type: 资产类型（stock / index / ...）。
        signal_date: 信号触发日期（ISO 字符串）。
        signal_name: 信号名称（仅标签）。
        direction: 方向，v0.1 固定 "long"。
        entry_date: 入场日期（ISO 字符串）。
        entry_price: 入场价。
        exit_date: 出场日期（ISO 字符串）。
        exit_price: 出场价。
        holding_bars: 持有 bar 数。
        gross_return: 毛收益率（不含成本）。
        net_return: 净收益率（扣除成本）。
        mae: 最大不利偏移，min(low / entry_price - 1) over 区间。
        mfe: 最大有利偏移，max(high / entry_price - 1) over 区间。
        meta: 透传外部信号附带的元信息。
    """

    asset_id: str
    asset_name: Optional[str]
    asset_type: Optional[str]
    signal_date: str
    signal_name: Optional[str]
    direction: str
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    holding_bars: int
    gross_return: float
    net_return: float
    mae: float
    mfe: float
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "asset_type": self.asset_type,
            "signal_date": self.signal_date,
            "signal_name": self.signal_name,
            "direction": self.direction,
            "entry_date": self.entry_date,
            "entry_price": self.entry_price,
            "exit_date": self.exit_date,
            "exit_price": self.exit_price,
            "holding_bars": self.holding_bars,
            "gross_return": self.gross_return,
            "net_return": self.net_return,
            "mae": self.mae,
            "mfe": self.mfe,
            "meta": dict(self.meta),
        }


@dataclass(frozen=True)
class Skipped:
    """被跳过的信号记录。

    Attributes:
        asset_id: 资产代码（可缺失）。
        signal_date: 信号日期（可缺失）。
        reason: 跳过原因码，见 validators/broker 中的常量。
        detail: 人类可读的详细信息。
    """

    asset_id: Optional[str]
    signal_date: Optional[str]
    reason: str
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "signal_date": self.signal_date,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class BacktestResult:
    """回测总结果。

    Attributes:
        config: 本次回测使用的配置。
        summary: 汇总指标 dict，参见 metrics.summarize_trades。
        trades: 成交交易列表。
        skipped: 被跳过的信号列表。
        equity_curve: 组合资金曲线，v0.1 不实现，固定为空 list。
    """

    config: BacktestConfig
    summary: Dict[str, Any]
    trades: List[Trade]
    skipped: List[Skipped]
    equity_curve: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict

        return {
            "config": asdict(self.config),
            "summary": dict(self.summary),
            "trades": [t.to_dict() for t in self.trades],
            "skipped": [s.to_dict() for s in self.skipped],
            "equity_curve": list(self.equity_curve),
        }
