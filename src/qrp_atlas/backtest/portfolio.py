"""Shared-cash long-only portfolio backtest with A-share execution constraints."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from .models import CostRule
from .validators import validate_price_df

TARGET_WEIGHT_REQUIRED_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "asset_id",
    "target_weight",
)

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


@dataclass(frozen=True)
class PortfolioExecutionRule:
    """Execution rules for target-weight portfolio rebalances."""

    price_field: str = "close"
    mark_price_field: str = "close"
    lot_size: int = 100
    minimum_commission: float = 5.0
    enforce_t_plus_one: bool = True
    enforce_price_limits: bool = True
    enforce_suspension: bool = True


@dataclass(frozen=True)
class PortfolioBacktestConfig:
    """Configuration for a shared-cash long-only portfolio backtest."""

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


@dataclass
class _Position:
    quantity: int = 0
    available_quantity: int = 0
    last_price: float = 0.0


def validate_portfolio_config(config: PortfolioBacktestConfig) -> None:
    if not isinstance(config, PortfolioBacktestConfig):
        raise ValueError("config must be a PortfolioBacktestConfig instance")
    if config.initial_cash <= 0:
        raise ValueError("initial_cash must be positive")
    if config.max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if not 0 < config.max_weight_per_asset <= 1:
        raise ValueError("max_weight_per_asset must be in (0, 1]")
    if config.cost.commission_rate < 0:
        raise ValueError("commission_rate must be >= 0")
    if config.cost.stamp_tax_rate < 0:
        raise ValueError("stamp_tax_rate must be >= 0")
    if config.cost.slippage_bps < 0:
        raise ValueError("slippage_bps must be >= 0")
    execution = config.execution
    if not execution.price_field or not execution.mark_price_field:
        raise ValueError("execution price fields must be non-empty")
    if not isinstance(execution.lot_size, int) or execution.lot_size <= 0:
        raise ValueError("lot_size must be a positive int")
    if execution.minimum_commission < 0:
        raise ValueError("minimum_commission must be >= 0")


def validate_target_weights(
    target_weights_df: pd.DataFrame,
    config: PortfolioBacktestConfig,
) -> None:
    if not isinstance(target_weights_df, pd.DataFrame):
        raise ValueError("target_weights_df must be a pandas DataFrame")
    missing = [
        column
        for column in TARGET_WEIGHT_REQUIRED_COLUMNS
        if column not in target_weights_df.columns
    ]
    if missing:
        raise ValueError(f"target_weights_df missing required columns: {missing}")
    if target_weights_df.empty:
        return
    if target_weights_df.duplicated(["trade_date", "asset_id"], keep=False).any():
        raise ValueError("target_weights_df has duplicate (trade_date, asset_id) pairs")
    dates = pd.to_datetime(target_weights_df["trade_date"], errors="coerce")
    if dates.isna().any():
        raise ValueError("target_weights_df contains invalid trade_date values")
    assets = target_weights_df["asset_id"].astype(str).str.strip()
    if assets.eq("").any() or assets.isin({"nan", "None"}).any():
        raise ValueError("target_weights_df contains missing asset_id values")
    weights = pd.to_numeric(target_weights_df["target_weight"], errors="coerce")
    if weights.isna().any() or not weights.map(math.isfinite).all():
        raise ValueError("target_weight values must be finite numbers")
    if (weights < 0).any():
        raise ValueError("target_weight values must be >= 0")
    if (weights > config.max_weight_per_asset + 1e-12).any():
        raise ValueError("target_weight exceeds max_weight_per_asset")
    normalized = target_weights_df.assign(
        trade_date=dates,
        target_weight=weights,
    )
    weight_sums = normalized.groupby("trade_date")["target_weight"].sum()
    if (weight_sums > 1.0 + 1e-9).any():
        raise ValueError("target weights must sum to <= 1 on each trade_date")
    positive_counts = normalized[normalized["target_weight"] > 0].groupby("trade_date").size()
    if (positive_counts > config.max_positions).any():
        raise ValueError("positive target count exceeds max_positions")


def _iso(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _safe_price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    return price


def _flag(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _is_suspended(row: pd.Series) -> bool:
    if _flag(row.get("is_suspended")):
        return True
    suspend_type = row.get("suspend_type")
    if suspend_type is None:
        return False
    try:
        if pd.isna(suspend_type):
            return False
    except (TypeError, ValueError):
        pass
    return str(suspend_type).strip() != ""


def _round_lot(quantity: float, lot_size: int) -> int:
    if quantity <= 0:
        return 0
    return int(math.floor(quantity / lot_size + 1e-12) * lot_size)


def _commission(gross_amount: float, config: PortfolioBacktestConfig) -> float:
    if gross_amount <= 0:
        return 0.0
    return max(
        config.execution.minimum_commission,
        gross_amount * config.cost.commission_rate,
    )


class PortfolioBacktestEngine:
    """Execute complete target-weight snapshots against one shared cash account.

    A date present in ``target_weights_df`` is a full rebalance snapshot: held
    assets omitted from that date receive a zero target. Dates without targets
    only mark the existing portfolio to market.
    """

    def run(
        self,
        price_df: pd.DataFrame,
        target_weights_df: pd.DataFrame,
        config: PortfolioBacktestConfig,
    ) -> PortfolioBacktestResult:
        validate_price_df(price_df)
        validate_portfolio_config(config)
        validate_target_weights(target_weights_df, config)

        prices = price_df.copy()
        prices["trade_date"] = pd.to_datetime(prices["trade_date"], errors="coerce")
        if prices["trade_date"].isna().any():
            raise ValueError("price_df contains invalid trade_date values")
        prices["asset_id"] = prices["asset_id"].astype(str)
        prices = prices.sort_values(["trade_date", "asset_id"], kind="mergesort")

        targets = target_weights_df.copy()
        if not targets.empty:
            targets["trade_date"] = pd.to_datetime(targets["trade_date"], errors="raise")
            targets["asset_id"] = targets["asset_id"].astype(str)
            targets["target_weight"] = pd.to_numeric(targets["target_weight"])
            if "priority" not in targets:
                targets["priority"] = 0.0
            targets["priority"] = pd.to_numeric(targets["priority"], errors="coerce").fillna(0.0)
            missing_dates = set(targets["trade_date"]) - set(prices["trade_date"])
            if missing_dates:
                rendered = sorted(_iso(date) for date in missing_dates)
                raise ValueError(f"target trade_date values absent from price calendar: {rendered}")

        target_groups = {
            date: group.copy()
            for date, group in targets.groupby("trade_date", sort=False)
        }
        price_groups = {
            date: group.set_index("asset_id", drop=False)
            for date, group in prices.groupby("trade_date", sort=True)
        }

        cash = float(config.initial_cash)
        positions: dict[str, _Position] = {}
        orders: list[PortfolioOrder] = []
        fills: list[PortfolioFill] = []
        snapshots: list[PortfolioSnapshot] = []
        order_seq = 1
        fill_seq = 1
        peak_equity = float(config.initial_cash)
        previous_equity = float(config.initial_cash)
        cumulative_cost = 0.0

        for trade_date, day_prices in price_groups.items():
            if config.execution.enforce_t_plus_one:
                for position in positions.values():
                    position.available_quantity = position.quantity

            for asset_id, row in day_prices.iterrows():
                mark_price = _safe_price(row.get(config.execution.mark_price_field))
                if mark_price is not None and asset_id in positions:
                    positions[asset_id].last_price = mark_price

            pre_trade_equity = cash + sum(
                position.quantity * position.last_price
                for position in positions.values()
            )
            day_commission = 0.0
            day_stamp_tax = 0.0
            day_slippage = 0.0
            day_traded = 0.0

            target_group = target_groups.get(trade_date)
            if target_group is not None:
                target_by_asset = {
                    str(row.asset_id): float(row.target_weight)
                    for row in target_group.itertuples(index=False)
                }
                all_assets = set(positions) | set(target_by_asset)
                target_quantities: dict[str, int] = {}
                reference_prices: dict[str, float] = {}

                for asset_id in sorted(all_assets):
                    row = day_prices.loc[asset_id] if asset_id in day_prices.index else None
                    price = (
                        _safe_price(row.get(config.execution.price_field))
                        if row is not None
                        else None
                    )
                    if price is None:
                        current_quantity = positions.get(asset_id, _Position()).quantity
                        requested = current_quantity if target_by_asset.get(asset_id, 0.0) == 0 else 0
                        orders.append(
                            PortfolioOrder(
                                order_id=f"O{order_seq:08d}",
                                trade_date=_iso(trade_date),
                                asset_id=asset_id,
                                side="SELL" if current_quantity else "BUY",
                                target_weight=target_by_asset.get(asset_id, 0.0),
                                requested_quantity=requested,
                                filled_quantity=0,
                                status=ORDER_REJECTED,
                                reason=REASON_NO_PRICE_DATA if row is None else REASON_INVALID_PRICE,
                            )
                        )
                        order_seq += 1
                        continue
                    reference_prices[asset_id] = price
                    target_value = pre_trade_equity * target_by_asset.get(asset_id, 0.0)
                    target_quantities[asset_id] = _round_lot(
                        target_value / price,
                        config.execution.lot_size,
                    )

                for asset_id in sorted(all_assets):
                    current = positions.get(asset_id)
                    if current is None or asset_id not in target_quantities:
                        continue
                    requested_quantity = max(0, current.quantity - target_quantities[asset_id])
                    if requested_quantity <= 0:
                        continue
                    order_id = f"O{order_seq:08d}"
                    order_seq += 1
                    row = day_prices.loc[asset_id]
                    rejection = self._execution_rejection(row, "SELL", config)
                    sell_quantity = requested_quantity
                    if rejection is None and config.execution.enforce_t_plus_one:
                        sell_quantity = min(sell_quantity, current.available_quantity)
                        if sell_quantity <= 0:
                            rejection = REASON_T_PLUS_ONE_BLOCKED
                    if rejection is not None:
                        orders.append(
                            PortfolioOrder(
                                order_id,
                                _iso(trade_date),
                                asset_id,
                                "SELL",
                                target_by_asset.get(asset_id, 0.0),
                                requested_quantity,
                                0,
                                ORDER_REJECTED,
                                rejection,
                            )
                        )
                        continue

                    reference_price = reference_prices[asset_id]
                    execution_price = reference_price * (
                        1.0 - config.cost.slippage_bps / 10000.0
                    )
                    gross = execution_price * sell_quantity
                    commission = _commission(gross, config)
                    stamp_tax = gross * config.cost.stamp_tax_rate
                    slippage_cost = (reference_price - execution_price) * sell_quantity
                    cash_flow = gross - commission - stamp_tax
                    cash += cash_flow
                    current.quantity -= sell_quantity
                    current.available_quantity -= sell_quantity
                    if current.quantity == 0:
                        positions.pop(asset_id, None)
                    day_commission += commission
                    day_stamp_tax += stamp_tax
                    day_slippage += slippage_cost
                    day_traded += gross
                    cumulative_cost += commission + stamp_tax + slippage_cost
                    fill_id = f"F{fill_seq:08d}"
                    fill_seq += 1
                    fills.append(
                        PortfolioFill(
                            fill_id,
                            order_id,
                            _iso(trade_date),
                            asset_id,
                            "SELL",
                            sell_quantity,
                            reference_price,
                            execution_price,
                            gross,
                            commission,
                            stamp_tax,
                            slippage_cost,
                            cash_flow,
                        )
                    )
                    status = ORDER_FILLED if sell_quantity == requested_quantity else ORDER_PARTIALLY_FILLED
                    reason = None if status == ORDER_FILLED else REASON_T_PLUS_ONE_BLOCKED
                    orders.append(
                        PortfolioOrder(
                            order_id,
                            _iso(trade_date),
                            asset_id,
                            "SELL",
                            target_by_asset.get(asset_id, 0.0),
                            requested_quantity,
                            sell_quantity,
                            status,
                            reason,
                        )
                    )

                buy_assets = sorted(
                    (
                        (
                            str(row.asset_id),
                            float(row.target_weight),
                            float(row.priority),
                        )
                        for row in target_group.itertuples(index=False)
                        if float(row.target_weight) > 0
                    ),
                    key=lambda item: (-item[2], -item[1], item[0]),
                )
                for asset_id, target_weight, _priority in buy_assets:
                    if asset_id not in target_quantities:
                        continue
                    current = positions.get(asset_id, _Position())
                    requested_quantity = max(0, target_quantities[asset_id] - current.quantity)
                    if requested_quantity <= 0:
                        continue
                    order_id = f"O{order_seq:08d}"
                    order_seq += 1
                    row = day_prices.loc[asset_id]
                    rejection = self._execution_rejection(row, "BUY", config)
                    if rejection is not None:
                        orders.append(
                            PortfolioOrder(
                                order_id,
                                _iso(trade_date),
                                asset_id,
                                "BUY",
                                target_weight,
                                requested_quantity,
                                0,
                                ORDER_REJECTED,
                                rejection,
                            )
                        )
                        continue
                    reference_price = reference_prices[asset_id]
                    execution_price = reference_price * (
                        1.0 + config.cost.slippage_bps / 10000.0
                    )
                    buy_quantity = self._affordable_quantity(
                        requested_quantity,
                        execution_price,
                        cash,
                        config,
                    )
                    if buy_quantity <= 0:
                        reason = (
                            REASON_BELOW_LOT_SIZE
                            if requested_quantity < config.execution.lot_size
                            else REASON_INSUFFICIENT_CASH
                        )
                        orders.append(
                            PortfolioOrder(
                                order_id,
                                _iso(trade_date),
                                asset_id,
                                "BUY",
                                target_weight,
                                requested_quantity,
                                0,
                                ORDER_REJECTED,
                                reason,
                            )
                        )
                        continue
                    gross = execution_price * buy_quantity
                    commission = _commission(gross, config)
                    slippage_cost = (execution_price - reference_price) * buy_quantity
                    cash_flow = -(gross + commission)
                    cash += cash_flow
                    position = positions.setdefault(asset_id, _Position())
                    position.quantity += buy_quantity
                    if not config.execution.enforce_t_plus_one:
                        position.available_quantity += buy_quantity
                    position.last_price = (
                        _safe_price(row.get(config.execution.mark_price_field))
                        or reference_price
                    )
                    day_commission += commission
                    day_slippage += slippage_cost
                    day_traded += gross
                    cumulative_cost += commission + slippage_cost
                    fill_id = f"F{fill_seq:08d}"
                    fill_seq += 1
                    fills.append(
                        PortfolioFill(
                            fill_id,
                            order_id,
                            _iso(trade_date),
                            asset_id,
                            "BUY",
                            buy_quantity,
                            reference_price,
                            execution_price,
                            gross,
                            commission,
                            0.0,
                            slippage_cost,
                            cash_flow,
                        )
                    )
                    status = ORDER_FILLED if buy_quantity == requested_quantity else ORDER_PARTIALLY_FILLED
                    orders.append(
                        PortfolioOrder(
                            order_id,
                            _iso(trade_date),
                            asset_id,
                            "BUY",
                            target_weight,
                            requested_quantity,
                            buy_quantity,
                            status,
                            None if status == ORDER_FILLED else REASON_INSUFFICIENT_CASH,
                        )
                    )

            market_value = sum(
                position.quantity * position.last_price
                for position in positions.values()
            )
            equity = cash + market_value
            daily_return = equity / previous_equity - 1.0 if previous_equity else 0.0
            peak_equity = max(peak_equity, equity)
            drawdown = equity / peak_equity - 1.0 if peak_equity else 0.0
            turnover = day_traded / pre_trade_equity if pre_trade_equity else 0.0
            position_snapshots = tuple(
                PositionSnapshot(
                    asset_id=asset_id,
                    quantity=position.quantity,
                    available_quantity=position.available_quantity,
                    last_price=position.last_price,
                    market_value=position.quantity * position.last_price,
                    weight=(position.quantity * position.last_price / equity) if equity else 0.0,
                )
                for asset_id, position in sorted(positions.items())
            )
            snapshots.append(
                PortfolioSnapshot(
                    trade_date=_iso(trade_date),
                    cash=float(cash),
                    market_value=float(market_value),
                    equity=float(equity),
                    daily_return=float(daily_return),
                    drawdown=float(drawdown),
                    turnover=float(turnover),
                    commission=float(day_commission),
                    stamp_tax=float(day_stamp_tax),
                    slippage_cost=float(day_slippage),
                    cumulative_cost=float(cumulative_cost),
                    positions=position_snapshots,
                )
            )
            previous_equity = equity

        final_equity = snapshots[-1].equity if snapshots else config.initial_cash
        total_return = final_equity / config.initial_cash - 1.0
        max_drawdown = min((snapshot.drawdown for snapshot in snapshots), default=0.0)
        total_turnover = sum(snapshot.turnover for snapshot in snapshots)
        total_commission = sum(fill.commission for fill in fills)
        total_stamp_tax = sum(fill.stamp_tax for fill in fills)
        total_slippage = sum(fill.slippage_cost for fill in fills)
        rejected_count = sum(order.status == ORDER_REJECTED for order in orders)
        sell_count = sum(fill.side == "SELL" for fill in fills)
        summary = {
            "initial_cash": float(config.initial_cash),
            "final_equity": float(final_equity),
            "total_return": float(total_return),
            "total_return_pct": float(total_return * 100.0),
            "max_drawdown": float(max_drawdown),
            "max_drawdown_pct": float(max_drawdown * 100.0),
            "turnover": float(total_turnover),
            "order_count": len(orders),
            "fill_count": len(fills),
            "trade_count": sell_count,
            "skipped_count": rejected_count,
            "commission": float(total_commission),
            "stamp_tax": float(total_stamp_tax),
            "slippage_cost": float(total_slippage),
            "total_cost": float(total_commission + total_stamp_tax + total_slippage),
        }
        equity_curve = tuple(
            {
                "date": snapshot.trade_date,
                "equity": snapshot.equity / config.initial_cash,
                "drawdown_pct": snapshot.drawdown * 100.0,
            }
            for snapshot in snapshots
        )
        return PortfolioBacktestResult(
            config=config,
            summary=summary,
            orders=tuple(orders),
            fills=tuple(fills),
            snapshots=tuple(snapshots),
            equity_curve=equity_curve,
        )

    @staticmethod
    def _execution_rejection(
        row: pd.Series,
        side: str,
        config: PortfolioBacktestConfig,
    ) -> str | None:
        if config.execution.enforce_suspension and _is_suspended(row):
            return REASON_SUSPENDED
        if config.execution.enforce_price_limits:
            if side == "BUY" and _flag(row.get("is_limit_up")):
                return REASON_LIMIT_UP_BUY_BLOCKED
            if side == "SELL" and _flag(row.get("is_limit_down")):
                return REASON_LIMIT_DOWN_SELL_BLOCKED
        return None

    @staticmethod
    def _affordable_quantity(
        requested_quantity: int,
        execution_price: float,
        cash: float,
        config: PortfolioBacktestConfig,
    ) -> int:
        lot_size = config.execution.lot_size
        quantity = _round_lot(requested_quantity, lot_size)
        while quantity > 0:
            gross = execution_price * quantity
            if gross + _commission(gross, config) <= cash + 1e-9:
                return quantity
            quantity -= lot_size
        return 0


__all__ = [
    "ORDER_FILLED",
    "ORDER_PARTIALLY_FILLED",
    "ORDER_REJECTED",
    "PortfolioBacktestConfig",
    "PortfolioBacktestEngine",
    "PortfolioBacktestResult",
    "PortfolioExecutionRule",
    "PortfolioFill",
    "PortfolioOrder",
    "PortfolioSnapshot",
    "PositionSnapshot",
    "REASON_BELOW_LOT_SIZE",
    "REASON_INSUFFICIENT_CASH",
    "REASON_INVALID_PRICE",
    "REASON_LIMIT_DOWN_SELL_BLOCKED",
    "REASON_LIMIT_UP_BUY_BLOCKED",
    "REASON_NO_PRICE_DATA",
    "REASON_SUSPENDED",
    "REASON_T_PLUS_ONE_BLOCKED",
    "validate_portfolio_config",
    "validate_target_weights",
]
