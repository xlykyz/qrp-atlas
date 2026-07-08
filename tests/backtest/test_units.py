"""
test_units.py - validators / metrics / data 适配层单元测试
"""

import math

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.backtest.data import _build_where, normalize_price_frame
from qrp_atlas.backtest.metrics import summarize_trades
from qrp_atlas.backtest.models import (
    BacktestConfig,
    CostRule,
    EntryRule,
    ExitRule,
    PositionRule,
    Skipped,
    Trade,
)
from qrp_atlas.backtest.validators import (
    validate_config,
    validate_price_df,
    validate_signals_df,
)


# ────────────────────────────────────────────────────────────
# validators
# ────────────────────────────────────────────────────────────
def test_validate_price_df_missing_columns():
    df = pd.DataFrame({"trade_date": [], "asset_id": []})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_price_df(df)


def test_validate_price_df_not_dataframe():
    with pytest.raises(ValueError, match="must be a pandas DataFrame"):
        validate_price_df([1, 2, 3])


def test_validate_signals_df_missing_columns():
    df = pd.DataFrame({"signal_date": [], "asset_id": []})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_signals_df(df)


def test_validate_signals_df_not_dataframe():
    with pytest.raises(ValueError, match="must be a pandas DataFrame"):
        validate_signals_df("not a df")


def _make_valid_config(**overrides) -> BacktestConfig:
    base = dict(
        name="t",
        entry=EntryRule(timing="signal_close", price_field="close"),
        exit=ExitRule(type="hold_n_bars", bars=5, price_field="close"),
        position=PositionRule(
            initial_cash=1_000_000,
            position_pct=1.0,
            max_positions=10,
            allow_overlap=True,
            compound=False,
        ),
        cost=CostRule(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
    )
    base.update(overrides)
    return BacktestConfig(**base)


def test_validate_config_ok():
    validate_config(_make_valid_config())  # not raise


def test_validate_config_invalid_timing():
    cfg = _make_valid_config(
        entry=EntryRule(timing="wrong", price_field="close")
    )
    with pytest.raises(ValueError, match="entry.timing"):
        validate_config(cfg)


def test_validate_config_invalid_exit_type():
    cfg = _make_valid_config(
        exit=ExitRule(type="trailing_stop", bars=5, price_field="close")
    )
    with pytest.raises(ValueError, match="exit.type"):
        validate_config(cfg)


def test_validate_config_non_positive_bars():
    cfg = _make_valid_config(
        exit=ExitRule(type="hold_n_bars", bars=0, price_field="close")
    )
    with pytest.raises(ValueError, match="exit.bars"):
        validate_config(cfg)


def test_validate_config_non_positive_cash():
    cfg = _make_valid_config(
        position=PositionRule(
            initial_cash=0,
            position_pct=1.0,
            max_positions=10,
            allow_overlap=True,
            compound=False,
        )
    )
    with pytest.raises(ValueError, match="initial_cash"):
        validate_config(cfg)


def test_validate_config_position_pct_out_of_range():
    cfg = _make_valid_config(
        position=PositionRule(
            initial_cash=1_000_000,
            position_pct=2.0,
            max_positions=10,
            allow_overlap=True,
            compound=False,
        )
    )
    with pytest.raises(ValueError, match="position_pct"):
        validate_config(cfg)


def test_validate_config_negative_commission():
    cfg = _make_valid_config(
        cost=CostRule(commission_rate=-0.001, stamp_tax_rate=0.0, slippage_bps=0)
    )
    with pytest.raises(ValueError, match="commission_rate"):
        validate_config(cfg)


# ────────────────────────────────────────────────────────────
# metrics
# ────────────────────────────────────────────────────────────
def _make_trade(net_return: float, mae: float = 0.0, mfe: float = 0.0, holding_bars: int = 5) -> Trade:
    return Trade(
        asset_id="X",
        asset_name=None,
        asset_type="stock",
        signal_date="2024-01-01",
        signal_name=None,
        direction="long",
        entry_date="2024-01-01",
        entry_price=10.0,
        exit_date="2024-01-06",
        exit_price=10.0 * (1 + net_return),
        holding_bars=holding_bars,
        gross_return=net_return,
        net_return=net_return,
        mae=mae,
        mfe=mfe,
        meta={},
    )


def test_summarize_empty_trades():
    s = summarize_trades([], [])
    assert s["trade_count"] == 0
    assert s["skipped_count"] == 0
    assert s["win_rate"] is None
    assert s["avg_return"] is None
    assert s["avg_win"] is None
    assert s["avg_loss"] is None
    assert s["profit_loss_ratio"] is None


def test_summarize_empty_trades_with_skipped():
    s = summarize_trades([], [Skipped("X", "2024-01-01", "NO_PRICE_DATA", "")])
    assert s["trade_count"] == 0
    assert s["skipped_count"] == 1


def test_summarize_all_wins():
    trades = [_make_trade(0.05), _make_trade(0.10), _make_trade(0.08)]
    s = summarize_trades(trades, [])
    assert s["trade_count"] == 3
    assert s["win_count"] == 3
    assert s["loss_count"] == 0
    assert math.isclose(s["win_rate"], 1.0)
    assert s["avg_loss"] is None
    assert s["profit_loss_ratio"] is None  # avg_loss is None


def test_summarize_mixed():
    trades = [_make_trade(0.10), _make_trade(-0.05), _make_trade(0.20), _make_trade(-0.10)]
    s = summarize_trades(trades, [])
    assert s["trade_count"] == 4
    assert s["win_count"] == 2
    assert s["loss_count"] == 2
    assert math.isclose(s["win_rate"], 0.5)
    # avg_win = (0.10 + 0.20) / 2 = 0.15
    assert math.isclose(s["avg_win"], 0.15)
    # avg_loss = (-0.05 + -0.10) / 2 = -0.075
    assert math.isclose(s["avg_loss"], -0.075)
    # profit_loss_ratio = 0.15 / 0.075 = 2.0
    assert math.isclose(s["profit_loss_ratio"], 2.0)
    # median_return of [0.10, -0.05, 0.20, -0.10] sorted = [-0.10, -0.05, 0.10, 0.20], median = (-0.05 + 0.10)/2 = 0.025
    assert math.isclose(s["median_return"], 0.025)
    assert math.isclose(s["max_return"], 0.20)
    assert math.isclose(s["min_return"], -0.10)


def test_summarize_mae_mfe():
    trades = [
        _make_trade(0.05, mae=-0.03, mfe=0.08, holding_bars=5),
        _make_trade(0.10, mae=-0.05, mfe=0.12, holding_bars=3),
    ]
    s = summarize_trades(trades, [])
    assert math.isclose(s["avg_mae"], -0.04)
    assert math.isclose(s["avg_mfe"], 0.10)
    assert math.isclose(s["worst_mae"], -0.05)
    assert math.isclose(s["best_mfe"], 0.12)
    assert math.isclose(s["avg_holding_bars"], 4.0)


def test_summarize_zero_return_is_loss():
    # net_return = 0 → loss
    trades = [_make_trade(0.0), _make_trade(0.05)]
    s = summarize_trades(trades, [])
    assert s["win_count"] == 1
    assert s["loss_count"] == 1


# ────────────────────────────────────────────────────────────
# data.normalize_price_frame
# ────────────────────────────────────────────────────────────
def test_normalize_price_frame_basic():
    df = pd.DataFrame(
        [
            ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
            ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ],
        columns=["trade_date", "ticker", "name", "open", "high", "low", "close"],
    )
    out = normalize_price_frame(df, asset_type="stock", id_col="ticker", name_col="name")
    assert list(out.columns)[:8] == [
        "trade_date",
        "asset_id",
        "asset_name",
        "asset_type",
        "open",
        "high",
        "low",
        "close",
    ]
    # 排序后 01-01 在前
    assert out.iloc[0]["trade_date"] == pd.Timestamp("2024-01-01")
    assert out.iloc[0]["asset_id"] == "000001.SZ"
    assert out.iloc[0]["asset_name"] == "平安银行"
    assert out.iloc[0]["asset_type"] == "stock"
    assert math.isclose(out.iloc[0]["close"], 10.2)


def test_normalize_price_frame_empty():
    out = normalize_price_frame(
        pd.DataFrame(), asset_type="index", id_col="index_code", name_col="index_name"
    )
    assert list(out.columns) == [
        "trade_date",
        "asset_id",
        "asset_name",
        "asset_type",
        "open",
        "high",
        "low",
        "close",
    ]
    assert len(out) == 0


def test_normalize_price_frame_optional_columns():
    df = pd.DataFrame(
        [
            {
                "trade_date": "2024-01-01",
                "ticker": "000001.SZ",
                "name": "平安银行",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000,
                "amount": 10000.0,
                "turnover": 0.01,
                "market_cap": 1e10,
                "float_cap": 5e9,
                "is_st": False,
                "is_limit_up": False,
                "is_limit_down": False,
            }
        ]
    )
    out = normalize_price_frame(df, asset_type="stock", id_col="ticker", name_col="name")
    assert "volume" in out.columns
    assert "is_st" in out.columns
    assert out.iloc[0]["volume"] == 1000


def test_normalize_price_frame_drops_invalid_dates():
    df = pd.DataFrame(
        [
            ("not-a-date", "000001.SZ", "X", 10.0, 10.5, 9.8, 10.2),
            ("2024-01-01", "000001.SZ", "X", 10.0, 10.5, 9.8, 10.2),
        ],
        columns=["trade_date", "ticker", "name", "open", "high", "low", "close"],
    )
    out = normalize_price_frame(df, asset_type="stock", id_col="ticker", name_col="name")
    assert len(out) == 1


# ────────────────────────────────────────────────────────────
# validators: slippage_bps < 0
# ────────────────────────────────────────────────────────────
def test_validate_config_negative_slippage():
    cfg = _make_valid_config(
        cost=CostRule(commission_rate=0.00025, stamp_tax_rate=0.0005, slippage_bps=-1.0)
    )
    with pytest.raises(ValueError, match="slippage_bps"):
        validate_config(cfg)


# ────────────────────────────────────────────────────────────
# validators: price_df 重复 asset_id + trade_date
# ────────────────────────────────────────────────────────────
def test_validate_price_df_duplicate_rows():
    df = pd.DataFrame(
        [
            ("2024-01-01", "000001.SZ", "X", "stock", 10.0, 10.5, 9.8, 10.2),
            ("2024-01-01", "000001.SZ", "X", "stock", 10.0, 10.5, 9.8, 10.2),
        ],
        columns=["trade_date", "asset_id", "asset_name", "asset_type", "open", "high", "low", "close"],
    )
    with pytest.raises(ValueError, match="duplicate"):
        validate_price_df(df)


def test_validate_price_df_no_duplicate_across_assets():
    # 不同 asset_id 同一日期不算重复
    df = pd.DataFrame(
        [
            ("2024-01-01", "000001.SZ", "X", "stock", 10.0, 10.5, 9.8, 10.2),
            ("2024-01-01", "000002.SZ", "Y", "stock", 5.0, 5.2, 4.9, 5.1),
        ],
        columns=["trade_date", "asset_id", "asset_name", "asset_type", "open", "high", "low", "close"],
    )
    validate_price_df(df)  # not raise


# ────────────────────────────────────────────────────────────
# data._build_where: pandas Index / numpy array
# ────────────────────────────────────────────────────────────
def test_build_where_with_pandas_index():
    idx = pd.Index(["000001.SZ", "000002.SZ"])
    where_sql, params = _build_where(
        column_values=idx, column_name="ticker", start_date=None, end_date=None
    )
    assert "ticker IN (?, ?)" in where_sql
    assert params == ["000001.SZ", "000002.SZ"]


def test_build_where_with_numpy_array():
    arr = np.array(["000001.SZ", "000002.SZ"])
    where_sql, params = _build_where(
        column_values=arr, column_name="ticker", start_date=None, end_date=None
    )
    assert "ticker IN (?, ?)" in where_sql
    assert params == ["000001.SZ", "000002.SZ"]


def test_build_where_with_none_no_in_clause():
    # column_values=None 时不生成 IN 子句
    where_sql, params = _build_where(
        column_values=None, column_name="ticker", start_date="2024-01-01", end_date=None
    )
    assert "IN" not in where_sql
    assert "trade_date >=" in where_sql
    assert params == ["2024-01-01"]
