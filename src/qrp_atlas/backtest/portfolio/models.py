"""Public models for shared-cash portfolio backtests."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..models import CostRule

ORDER_FILLED = "FILLED"
ORDER_PARTIALLY_FILLED = "PARTIALLY_FILLED"
ORDER_REJECTED = "REJECTED"

REASON_NO_PRICE_DATA = "NO_PRICE_DATA"
REASON_INVALID_PRICE = "INVALID_PRICE"
REASON_SUSPENDED = "SUSPENDED"
REASON_LIMIT_UP_BUY_BLOCKED = "LIMIT_UP_BUY_BLOCKED"
REASON_LIMIT_DOWN_SELL_BLOCKED = "LIMIT_DOWN_SELL_BLOCKED"
REASON_T_PLUS_ONE_BLOCKED = "T_PLUS_ONE_BLOCKED"
REASON_INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
REASON_BELOW_LOT_SIZE = "BELOW_LOT_SIZE"
REASON_MAX_POSITIONS_REACHED = "MAX_POSITIONS_REACHED"


@dataclass(frozen=True)
class PortfolioExecutionRule:
    """A-share execution and valuation rules."""

    price_field: str = "close"
    mark_price_field: str = "close"
    lot_size: int = 100
    minimum_commission: float = 5.0
    enforce_t_plus_one: bool = True
    enforce_price_limits: bool = True
    enforce_suspension: bool = True


@dataclass(frozen=True)
class PortfolioBacktestConfig:
    """Configuration for a shared-cash long-only portfolio run."""

    name: str
    initial_cash: float
    max_positions: int
    max_weight_per_asset: float
    cost: CostRule
    execution: PortfolioExecutionRule = field(default_factory=PortfolioExecutionRule)


@dataclass(frozen=True)
class PortfolioOrder:
    order_id: str
    trade_date: str
    asset_id: str
    side: str
    target_weight: float
    requested_quantity: int
    filled_quantity: int
    status: str
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioFill:
    fill_id: str
    order_id: str
    trade_date: str
    asset_id: str
    side: str
    quantity: int
    reference_price: float
    execution_price: float
    gross_amount: float
    commission: float
    stamp_tax: float
    slippage_cost: float
    cash_flow: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PositionSnapshot:
    asset_id: str
    quantity: int
    available_quantity: int
    last_price: float
    market_value: float
    weight: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioSnapshot:
    trade_date: str
    cash: float
    market_value: float
    equity: float
    daily_return: float
    drawdown: float
    turnover: float
    commission: float
    stamp_tax: float
    slippage_cost: float
    cumulative_cost: float
    positions: tuple[PositionSnapshot, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["positions"] = [position.to_dict() for position in self.positions]
        return payload


@dataclass(frozen=True)
class PortfolioBacktestResult:
    config: PortfolioBacktestConfig
    summary: dict[str, Any]
    orders: tuple[PortfolioOrder, ...]
    fills: tuple[PortfolioFill, ...]
    snapshots: tuple[PortfolioSnapshot, ...]
    equity_curve: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": asdict(self.config),
            "summary": dict(self.summary),
            "orders": [order.to_dict() for order in self.orders],
            "fills": [fill.to_dict() for fill in self.fills],
            "snapshots": [snapshot.to_dict() for snapshot in self.snapshots],
            "equity_curve": [dict(point) for point in self.equity_curve],
        }
