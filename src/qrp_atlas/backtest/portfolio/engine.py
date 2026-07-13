"""Shared-cash target-weight portfolio engine."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..validators import validate_price_df
from .execution import (
    affordable_quantity,
    commission,
    execution_rejection,
    iso_date,
    round_lot,
    safe_price,
)
from .models import (
    ORDER_FILLED,
    ORDER_PARTIALLY_FILLED,
    ORDER_REJECTED,
    PortfolioBacktestConfig,
    PortfolioBacktestResult,
    PortfolioFill,
    PortfolioOrder,
    PortfolioSnapshot,
    PositionSnapshot,
    REASON_BELOW_LOT_SIZE,
    REASON_INSUFFICIENT_CASH,
    REASON_INVALID_PRICE,
    REASON_MAX_POSITIONS_REACHED,
    REASON_NO_PRICE_DATA,
    REASON_T_PLUS_ONE_BLOCKED,
)
from .validators import validate_portfolio_config, validate_target_weights


@dataclass
class _Position:
    quantity: int = 0
    available_quantity: int = 0
    last_price: float = 0.0


class PortfolioBacktestEngine:
    """Execute complete target-weight snapshots against one shared cash account.

    A date present in ``target_weights_df`` is a full target snapshot. Held
    assets omitted on that date receive a zero target. Dates without a target
    snapshot only mark the existing portfolio to market.
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

        prices = self._normalize_prices(price_df)
        targets = self._normalize_targets(target_weights_df, prices)
        price_groups = {
            date: group.set_index("asset_id", drop=False)
            for date, group in prices.groupby("trade_date", sort=True)
        }
        target_groups = {
            date: group.copy()
            for date, group in targets.groupby("trade_date", sort=False)
        }

        cash = float(config.initial_cash)
        positions: dict[str, _Position] = {}
        orders: list[PortfolioOrder] = []
        fills: list[PortfolioFill] = []
        snapshots: list[PortfolioSnapshot] = []
        order_seq = 1
        fill_seq = 1
        previous_equity = float(config.initial_cash)
        peak_equity = float(config.initial_cash)
        cumulative_cost = 0.0

        for trade_date, day_prices in price_groups.items():
            if config.execution.enforce_t_plus_one:
                for position in positions.values():
                    position.available_quantity = position.quantity

            target_group = target_groups.get(trade_date)
            valuation_field = (
                config.execution.price_field
                if target_group is not None
                else config.execution.mark_price_field
            )
            self._mark_positions(positions, day_prices, valuation_field)
            pre_trade_equity = cash + self._market_value(positions)
            day_commission = 0.0
            day_stamp_tax = 0.0
            day_slippage = 0.0
            day_traded = 0.0

            if target_group is not None:
                target_by_asset = {
                    str(row.asset_id): float(row.target_weight)
                    for row in target_group.itertuples(index=False)
                }
                target_quantities, reference_prices, order_seq = self._target_quantities(
                    trade_date,
                    day_prices,
                    positions,
                    target_by_asset,
                    pre_trade_equity,
                    config,
                    orders,
                    order_seq,
                )

                cash, order_seq, fill_seq, sell_stats = self._execute_sells(
                    trade_date,
                    day_prices,
                    positions,
                    target_by_asset,
                    target_quantities,
                    reference_prices,
                    cash,
                    config,
                    orders,
                    fills,
                    order_seq,
                    fill_seq,
                )
                day_commission += sell_stats["commission"]
                day_stamp_tax += sell_stats["stamp_tax"]
                day_slippage += sell_stats["slippage"]
                day_traded += sell_stats["traded"]

                cash, order_seq, fill_seq, buy_stats = self._execute_buys(
                    trade_date,
                    day_prices,
                    positions,
                    target_group,
                    target_quantities,
                    reference_prices,
                    cash,
                    config,
                    orders,
                    fills,
                    order_seq,
                    fill_seq,
                )
                day_commission += buy_stats["commission"]
                day_slippage += buy_stats["slippage"]
                day_traded += buy_stats["traded"]

            cumulative_cost += day_commission + day_stamp_tax + day_slippage
            self._mark_positions(positions, day_prices, config.execution.mark_price_field)
            market_value = self._market_value(positions)
            equity = cash + market_value
            daily_return = equity / previous_equity - 1.0 if previous_equity else 0.0
            peak_equity = max(peak_equity, equity)
            drawdown = equity / peak_equity - 1.0 if peak_equity else 0.0
            turnover = day_traded / pre_trade_equity if pre_trade_equity else 0.0
            snapshots.append(
                self._snapshot(
                    trade_date,
                    cash,
                    market_value,
                    equity,
                    daily_return,
                    drawdown,
                    turnover,
                    day_commission,
                    day_stamp_tax,
                    day_slippage,
                    cumulative_cost,
                    positions,
                )
            )
            previous_equity = equity

        return self._result(config, orders, fills, snapshots)

    @staticmethod
    def _normalize_prices(price_df: pd.DataFrame) -> pd.DataFrame:
        prices = price_df.copy()
        prices["trade_date"] = pd.to_datetime(prices["trade_date"], errors="coerce")
        if prices["trade_date"].isna().any():
            raise ValueError("price_df contains invalid trade_date values")
        prices["asset_id"] = prices["asset_id"].astype(str)
        return prices.sort_values(["trade_date", "asset_id"], kind="mergesort")

    @staticmethod
    def _normalize_targets(
        target_weights_df: pd.DataFrame,
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        targets = target_weights_df.copy()
        if targets.empty:
            return targets
        targets["trade_date"] = pd.to_datetime(targets["trade_date"], errors="raise")
        targets["asset_id"] = targets["asset_id"].astype(str)
        targets["target_weight"] = pd.to_numeric(targets["target_weight"])
        if "priority" not in targets:
            targets["priority"] = 0.0
        targets["priority"] = pd.to_numeric(
            targets["priority"], errors="coerce"
        ).fillna(0.0)
        missing_dates = set(targets["trade_date"]) - set(prices["trade_date"])
        if missing_dates:
            rendered = sorted(iso_date(date) for date in missing_dates)
            raise ValueError(f"target trade_date values absent from price calendar: {rendered}")
        return targets

    @staticmethod
    def _mark_positions(
        positions: dict[str, _Position],
        day_prices: pd.DataFrame,
        field: str,
    ) -> None:
        for asset_id, row in day_prices.iterrows():
            price = safe_price(row.get(field))
            if price is not None and asset_id in positions:
                positions[asset_id].last_price = price

    @staticmethod
    def _market_value(positions: dict[str, _Position]) -> float:
        return sum(position.quantity * position.last_price for position in positions.values())

    @staticmethod
    def _reject(
        orders: list[PortfolioOrder],
        order_id: str,
        trade_date: pd.Timestamp,
        asset_id: str,
        side: str,
        target_weight: float,
        requested_quantity: int,
        reason: str,
    ) -> None:
        orders.append(
            PortfolioOrder(
                order_id=order_id,
                trade_date=iso_date(trade_date),
                asset_id=asset_id,
                side=side,
                target_weight=target_weight,
                requested_quantity=requested_quantity,
                filled_quantity=0,
                status=ORDER_REJECTED,
                reason=reason,
            )
        )

    @classmethod
    def _target_quantities(
        cls,
        trade_date: pd.Timestamp,
        day_prices: pd.DataFrame,
        positions: dict[str, _Position],
        target_by_asset: dict[str, float],
        equity: float,
        config: PortfolioBacktestConfig,
        orders: list[PortfolioOrder],
        order_seq: int,
    ) -> tuple[dict[str, int], dict[str, float], int]:
        target_quantities: dict[str, int] = {}
        reference_prices: dict[str, float] = {}
        for asset_id in sorted(set(positions) | set(target_by_asset)):
            row = day_prices.loc[asset_id] if asset_id in day_prices.index else None
            price = (
                safe_price(row.get(config.execution.price_field))
                if row is not None
                else None
            )
            if price is None:
                current_quantity = positions.get(asset_id, _Position()).quantity
                cls._reject(
                    orders,
                    f"O{order_seq:08d}",
                    trade_date,
                    asset_id,
                    "SELL" if current_quantity else "BUY",
                    target_by_asset.get(asset_id, 0.0),
                    current_quantity if target_by_asset.get(asset_id, 0.0) == 0 else 0,
                    REASON_NO_PRICE_DATA if row is None else REASON_INVALID_PRICE,
                )
                order_seq += 1
                continue
            reference_prices[asset_id] = price
            target_quantities[asset_id] = round_lot(
                equity * target_by_asset.get(asset_id, 0.0) / price,
                config.execution.lot_size,
            )
        return target_quantities, reference_prices, order_seq

    @classmethod
    def _execute_sells(
        cls,
        trade_date: pd.Timestamp,
        day_prices: pd.DataFrame,
        positions: dict[str, _Position],
        target_by_asset: dict[str, float],
        target_quantities: dict[str, int],
        reference_prices: dict[str, float],
        cash: float,
        config: PortfolioBacktestConfig,
        orders: list[PortfolioOrder],
        fills: list[PortfolioFill],
        order_seq: int,
        fill_seq: int,
    ) -> tuple[float, int, int, dict[str, float]]:
        stats = {
            "commission": 0.0,
            "stamp_tax": 0.0,
            "slippage": 0.0,
            "traded": 0.0,
        }
        for asset_id in sorted(set(positions) | set(target_by_asset)):
            position = positions.get(asset_id)
            if position is None or asset_id not in target_quantities:
                continue
            requested = max(0, position.quantity - target_quantities[asset_id])
            if requested <= 0:
                continue
            order_id = f"O{order_seq:08d}"
            order_seq += 1
            rejection = execution_rejection(day_prices.loc[asset_id], "SELL", config)
            quantity = requested
            if rejection is None and config.execution.enforce_t_plus_one:
                quantity = min(quantity, position.available_quantity)
                if quantity <= 0:
                    rejection = REASON_T_PLUS_ONE_BLOCKED
            if rejection is not None:
                cls._reject(
                    orders,
                    order_id,
                    trade_date,
                    asset_id,
                    "SELL",
                    target_by_asset.get(asset_id, 0.0),
                    requested,
                    rejection,
                )
                continue

            reference_price = reference_prices[asset_id]
            execution_price = reference_price * (1 - config.cost.slippage_bps / 10000)
            gross = execution_price * quantity
            fee = commission(gross, config)
            stamp_tax = gross * config.cost.stamp_tax_rate
            slippage = (reference_price - execution_price) * quantity
            cash_flow = gross - fee - stamp_tax
            cash += cash_flow
            position.quantity -= quantity
            position.available_quantity -= quantity
            if position.quantity == 0:
                positions.pop(asset_id)
            fills.append(
                PortfolioFill(
                    fill_id=f"F{fill_seq:08d}",
                    order_id=order_id,
                    trade_date=iso_date(trade_date),
                    asset_id=asset_id,
                    side="SELL",
                    quantity=quantity,
                    reference_price=reference_price,
                    execution_price=execution_price,
                    gross_amount=gross,
                    commission=fee,
                    stamp_tax=stamp_tax,
                    slippage_cost=slippage,
                    cash_flow=cash_flow,
                )
            )
            fill_seq += 1
            status = ORDER_FILLED if quantity == requested else ORDER_PARTIALLY_FILLED
            orders.append(
                PortfolioOrder(
                    order_id,
                    iso_date(trade_date),
                    asset_id,
                    "SELL",
                    target_by_asset.get(asset_id, 0.0),
                    requested,
                    quantity,
                    status,
                    None if status == ORDER_FILLED else REASON_T_PLUS_ONE_BLOCKED,
                )
            )
            stats["commission"] += fee
            stats["stamp_tax"] += stamp_tax
            stats["slippage"] += slippage
            stats["traded"] += gross
        return cash, order_seq, fill_seq, stats

    @classmethod
    def _execute_buys(
        cls,
        trade_date: pd.Timestamp,
        day_prices: pd.DataFrame,
        positions: dict[str, _Position],
        target_group: pd.DataFrame,
        target_quantities: dict[str, int],
        reference_prices: dict[str, float],
        cash: float,
        config: PortfolioBacktestConfig,
        orders: list[PortfolioOrder],
        fills: list[PortfolioFill],
        order_seq: int,
        fill_seq: int,
    ) -> tuple[float, int, int, dict[str, float]]:
        stats = {"commission": 0.0, "slippage": 0.0, "traded": 0.0}
        candidates = sorted(
            (
                (str(row.asset_id), float(row.target_weight), float(row.priority))
                for row in target_group.itertuples(index=False)
                if float(row.target_weight) > 0
            ),
            key=lambda item: (-item[2], -item[1], item[0]),
        )
        for asset_id, target_weight, _priority in candidates:
            if asset_id not in target_quantities:
                continue
            current = positions.get(asset_id, _Position())
            requested = max(0, target_quantities[asset_id] - current.quantity)
            if requested <= 0:
                if target_weight > 0 and current.quantity == 0:
                    cls._reject(
                        orders,
                        f"O{order_seq:08d}",
                        trade_date,
                        asset_id,
                        "BUY",
                        target_weight,
                        0,
                        REASON_BELOW_LOT_SIZE,
                    )
                    order_seq += 1
                continue

            order_id = f"O{order_seq:08d}"
            order_seq += 1
            rejection = execution_rejection(day_prices.loc[asset_id], "BUY", config)
            if (
                rejection is None
                and asset_id not in positions
                and len(positions) >= config.max_positions
            ):
                rejection = REASON_MAX_POSITIONS_REACHED
            if rejection is not None:
                cls._reject(
                    orders,
                    order_id,
                    trade_date,
                    asset_id,
                    "BUY",
                    target_weight,
                    requested,
                    rejection,
                )
                continue

            reference_price = reference_prices[asset_id]
            execution_price = reference_price * (1 + config.cost.slippage_bps / 10000)
            quantity = affordable_quantity(requested, execution_price, cash, config)
            if quantity <= 0:
                cls._reject(
                    orders,
                    order_id,
                    trade_date,
                    asset_id,
                    "BUY",
                    target_weight,
                    requested,
                    REASON_INSUFFICIENT_CASH,
                )
                continue

            gross = execution_price * quantity
            fee = commission(gross, config)
            slippage = (execution_price - reference_price) * quantity
            cash_flow = -(gross + fee)
            cash += cash_flow
            position = positions.setdefault(asset_id, _Position())
            position.quantity += quantity
            if not config.execution.enforce_t_plus_one:
                position.available_quantity += quantity
            position.last_price = reference_price
            fills.append(
                PortfolioFill(
                    fill_id=f"F{fill_seq:08d}",
                    order_id=order_id,
                    trade_date=iso_date(trade_date),
                    asset_id=asset_id,
                    side="BUY",
                    quantity=quantity,
                    reference_price=reference_price,
                    execution_price=execution_price,
                    gross_amount=gross,
                    commission=fee,
                    stamp_tax=0.0,
                    slippage_cost=slippage,
                    cash_flow=cash_flow,
                )
            )
            fill_seq += 1
            status = ORDER_FILLED if quantity == requested else ORDER_PARTIALLY_FILLED
            orders.append(
                PortfolioOrder(
                    order_id,
                    iso_date(trade_date),
                    asset_id,
                    "BUY",
                    target_weight,
                    requested,
                    quantity,
                    status,
                    None if status == ORDER_FILLED else REASON_INSUFFICIENT_CASH,
                )
            )
            stats["commission"] += fee
            stats["slippage"] += slippage
            stats["traded"] += gross
        return cash, order_seq, fill_seq, stats

    @staticmethod
    def _snapshot(
        trade_date: pd.Timestamp,
        cash: float,
        market_value: float,
        equity: float,
        daily_return: float,
        drawdown: float,
        turnover: float,
        commission_value: float,
        stamp_tax: float,
        slippage: float,
        cumulative_cost: float,
        positions: dict[str, _Position],
    ) -> PortfolioSnapshot:
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
        return PortfolioSnapshot(
            trade_date=iso_date(trade_date),
            cash=float(cash),
            market_value=float(market_value),
            equity=float(equity),
            daily_return=float(daily_return),
            drawdown=float(drawdown),
            turnover=float(turnover),
            commission=float(commission_value),
            stamp_tax=float(stamp_tax),
            slippage_cost=float(slippage),
            cumulative_cost=float(cumulative_cost),
            positions=position_snapshots,
        )

    @staticmethod
    def _result(
        config: PortfolioBacktestConfig,
        orders: list[PortfolioOrder],
        fills: list[PortfolioFill],
        snapshots: list[PortfolioSnapshot],
    ) -> PortfolioBacktestResult:
        final_equity = snapshots[-1].equity if snapshots else config.initial_cash
        total_return = final_equity / config.initial_cash - 1.0
        max_drawdown = min((snapshot.drawdown for snapshot in snapshots), default=0.0)
        fees = sum(fill.commission for fill in fills)
        stamp_tax = sum(fill.stamp_tax for fill in fills)
        slippage = sum(fill.slippage_cost for fill in fills)
        summary = {
            "initial_cash": float(config.initial_cash),
            "final_equity": float(final_equity),
            "total_return": float(total_return),
            "total_return_pct": float(total_return * 100),
            "max_drawdown": float(max_drawdown),
            "max_drawdown_pct": float(max_drawdown * 100),
            "turnover": float(sum(snapshot.turnover for snapshot in snapshots)),
            "order_count": len(orders),
            "fill_count": len(fills),
            "trade_count": sum(fill.side == "SELL" for fill in fills),
            "skipped_count": sum(order.status == ORDER_REJECTED for order in orders),
            "commission": float(fees),
            "stamp_tax": float(stamp_tax),
            "slippage_cost": float(slippage),
            "total_cost": float(fees + stamp_tax + slippage),
        }
        equity_curve = tuple(
            {
                "date": snapshot.trade_date,
                "equity": snapshot.equity / config.initial_cash,
                "drawdown_pct": snapshot.drawdown * 100,
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
