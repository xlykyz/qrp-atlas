"""测试公用 fixtures。"""

import pandas as pd
import pytest

from qrp_atlas.backtest.models import (
    BacktestConfig,
    CostRule,
    EntryRule,
    ExitRule,
    PositionRule,
)


@pytest.fixture
def default_config() -> BacktestConfig:
    """signal_close 入场 + hold 5 bars 出场的默认配置。"""
    return BacktestConfig(
        name="test_hold_5_bars",
        entry=EntryRule(timing="signal_close", price_field="close"),
        exit=ExitRule(type="hold_n_bars", bars=5, price_field="close"),
        position=PositionRule(
            initial_cash=1_000_000,
            position_pct=1.0,
            max_positions=999_999,
            allow_overlap=True,
            compound=False,
        ),
        cost=CostRule(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
    )


@pytest.fixture
def default_price_df() -> pd.DataFrame:
    """6 个交易日的 000001.SZ 行情，足够 hold 5 bars。"""
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 10.7, 11.2, 10.6, 11.0),
    ]
    return pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")


def make_signals(
    asset_id: str = "000001.SZ",
    signal_date: str = "2024-01-01",
    direction: str = "long",
    signal_name: str = "manual_test",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "signal_date": signal_date,
                "asset_id": asset_id,
                "direction": direction,
                "signal_name": signal_name,
            }
        ]
    )
