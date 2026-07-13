import math

import pandas as pd
import pytest

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    ORDER_PARTIALLY_FILLED,
    ORDER_REJECTED,
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
    REASON_LIMIT_DOWN_SELL_BLOCKED,
    REASON_LIMIT_UP_BUY_BLOCKED,
    REASON_SUSPENDED,
)


def _config(**execution_overrides):
    execution = PortfolioExecutionRule(
        price_field="close",
        mark_price_field="close",
        lot_size=100,
        minimum_commission=5.0,
        **execution_overrides,
    )
    return PortfolioBacktestConfig(
        name="portfolio",
        initial_cash=10_000.0,
        max_positions=2,
        max_weight_per_asset=0.5,
        cost=CostRule(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
        execution=execution,
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


def test_shared_cash_and_lot_size_make_second_order_partial():
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
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-02", "asset_id": "B", "target_weight": 0.5},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, _config())

    assert [(fill.asset_id, fill.quantity) for fill in result.fills] == [
        ("A", 500),
        ("B", 400),
    ]
    assert result.orders[-1].status == ORDER_PARTIALLY_FILLED
    assert math.isclose(result.snapshots[-1].cash, 990.0)
    assert math.isclose(result.snapshots[-1].equity, 9990.0)
    assert result.summary["commission"] == 10.0


def test_limit_and_suspension_rejections_are_stable():
    price_df = _prices([
        {
            "trade_date": "2024-01-02",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
            "is_limit_up": True,
        },
        {
            "trade_date": "2024-01-03",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
            "is_limit_up": False,
        },
        {
            "trade_date": "2024-01-04",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
            "is_limit_down": True,
        },
        {
            "trade_date": "2024-01-05",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
            "is_suspended": True,
        },
        {
            "trade_date": "2024-01-08",
            "asset_id": "A",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-04", "asset_id": "A", "target_weight": 0.0},
        {"trade_date": "2024-01-05", "asset_id": "A", "target_weight": 0.0},
        {"trade_date": "2024-01-08", "asset_id": "A", "target_weight": 0.0},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, _config())

    rejected = [order for order in result.orders if order.status == ORDER_REJECTED]
    assert [order.reason for order in rejected] == [
        REASON_LIMIT_UP_BUY_BLOCKED,
        REASON_LIMIT_DOWN_SELL_BLOCKED,
        REASON_SUSPENDED,
    ]
    assert result.fills[-1].side == "SELL"
    assert result.snapshots[-1].positions == ()


def test_t_plus_one_locks_new_shares_and_equity_is_marked_daily():
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
            "trade_date": "2024-01-03",
            "asset_id": "A",
            "open": 12,
            "high": 12,
            "low": 12,
            "close": 12,
        },
        {
            "trade_date": "2024-01-04",
            "asset_id": "A",
            "open": 11,
            "high": 11,
            "low": 11,
            "close": 11,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-04", "asset_id": "A", "target_weight": 0.0},
    ])

    result = PortfolioBacktestEngine().run(price_df, targets, _config())

    day1 = result.snapshots[0]
    day2 = result.snapshots[1]
    assert day1.positions[0].quantity == 500
    assert day1.positions[0].available_quantity == 0
    assert day2.positions[0].available_quantity == 500
    assert result.equity_curve[0]["equity"] == pytest.approx(0.9995)
    assert result.equity_curve[1]["equity"] == pytest.approx(1.0995)
    assert result.summary["trade_count"] == 1
    assert result.summary["stamp_tax"] > 0


def test_target_weight_validation_rejects_overallocation():
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
            "trade_date": "2024-01-02",
            "asset_id": "C",
            "open": 10,
            "high": 10,
            "low": 10,
            "close": 10,
        },
    ])
    targets = pd.DataFrame([
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
        {"trade_date": "2024-01-02", "asset_id": "B", "target_weight": 0.5},
        {"trade_date": "2024-01-02", "asset_id": "C", "target_weight": 0.1},
    ])

    with pytest.raises(ValueError, match="sum"):
        PortfolioBacktestEngine().run(price_df, targets, _config())
