import pandas as pd
import pytest

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    ORDER_REJECTED,
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
    REASON_BELOW_LOT_SIZE,
    REASON_LIMIT_DOWN_SELL_BLOCKED,
    REASON_MAX_POSITIONS_REACHED,
)


def _prices(rows):
    raw = pd.DataFrame(rows)
    optional = [
        column
        for column in ("is_limit_up", "is_limit_down", "is_suspended")
        if column in raw.columns
    ]
    return raw.assign(asset_name="x", asset_type="stock")[[
        "trade_date",
        "asset_id",
        "asset_name",
        "asset_type",
        "open",
        "high",
        "low",
        "close",
        *optional,
    ]]


def test_positive_target_below_one_lot_is_rejected():
    config = PortfolioBacktestConfig(
        name="below-lot",
        initial_cash=10_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(minimum_commission=0.0),
    )
    price_df = _prices([
        {
            "trade_date": "2024-01-02",
            "asset_id": "A",
            "open": 1000,
            "high": 1000,
            "low": 1000,
            "close": 1000,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, config)

    assert result.orders[0].status == ORDER_REJECTED
    assert result.orders[0].reason == REASON_BELOW_LOT_SIZE
    assert result.fills == ()


def test_blocked_exit_does_not_allow_position_count_to_overrun():
    config = PortfolioBacktestConfig(
        name="one-position",
        initial_cash=20_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(minimum_commission=0.0),
    )
    price_df = _prices([
        {
            "trade_date": "2024-01-02",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
        },
        {
            "trade_date": "2024-01-02",
            "asset_id": "B",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
        },
        {
            "trade_date": "2024-01-03",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
            "is_limit_down": True,
        },
        {
            "trade_date": "2024-01-03",
            "asset_id": "B",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-03", "asset_id": "B", "target_weight": 0.5},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, config)

    assert [order.reason for order in result.orders if order.status == ORDER_REJECTED] == [
        REASON_LIMIT_DOWN_SELL_BLOCKED,
        REASON_MAX_POSITIONS_REACHED,
    ]
    assert [position.asset_id for position in result.snapshots[-1].positions] == ["A"]


def test_open_execution_sizes_from_open_without_using_same_day_close():
    config = PortfolioBacktestConfig(
        name="open-execution",
        initial_cash=10_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="open",
            mark_price_field="close",
            minimum_commission=0.0,
        ),
    )
    price_df = _prices([
        {
            "trade_date": "2024-01-02",
            "asset_id": "A",
            "open": 10,
            "high": 20,
            "low": 10,
            "close": 20,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 1.0},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, config)

    assert result.fills[0].quantity == 1000
    assert result.snapshots[0].equity == pytest.approx(20_000.0)
