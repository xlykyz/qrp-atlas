"""
test_engine_scenarios.py - 引擎端到端场景测试

覆盖提示词"十三、测试要求"中的 10 个场景:
1. 正常 signal_close 入场
2. next_open 入场
3. next_close 入场
4. 缺少行情 → NO_PRICE_DATA
5. signal_date 不存在 → SIGNAL_DATE_NOT_FOUND
6. next_open 但没有下一根 bar → NO_NEXT_BAR_FOR_ENTRY
7. 出场 bar 不足 → NO_EXIT_BAR
8. 非 long 信号 → INVALID_DIRECTION
9. 价格非法 → INVALID_PRICE
10. 空输入（空 price_df / 空 signals_df）
"""

import math

import pandas as pd
import pytest

from qrp_atlas.backtest.engine import BacktestEngine
from qrp_atlas.backtest.models import EntryRule, ExitRule

from .conftest import make_signals


def _approx(a: float, b: float, rel: float = 1e-9) -> bool:
    return math.isclose(a, b, rel_tol=rel, abs_tol=1e-12)


# ────────────────────────────────────────────────────────────
# 场景 1: 正常 signal_close 入场
# ────────────────────────────────────────────────────────────
def test_signal_close_entry(default_config, default_price_df):
    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 1
    assert result.summary["skipped_count"] == 0
    assert len(result.trades) == 1
    assert len(result.skipped) == 0

    t = result.trades[0]
    assert t.asset_id == "000001.SZ"
    assert t.asset_name == "平安银行"
    assert t.asset_type == "stock"
    assert t.signal_date == "2024-01-01"
    assert t.signal_name == "manual_test"
    assert t.direction == "long"
    # signal_close: 入场日 = 信号日，入场价 = 当日 close
    assert t.entry_date == "2024-01-01"
    assert _approx(t.entry_price, 10.2)
    # hold 5 bars: 出场日 = 信号日后第 5 个交易日 = 2024-01-06
    assert t.exit_date == "2024-01-06"
    assert _approx(t.exit_price, 11.0)
    assert t.holding_bars == 5
    # gross_return = 11.0 / 10.2 - 1
    assert _approx(t.gross_return, 11.0 / 10.2 - 1.0)
    # net_return = gross - buy_cost - sell_cost
    # buy_cost  = 0.00025 + 0
    # sell_cost = 0.00025 + 0.0005 + 0
    assert _approx(t.net_return, t.gross_return - 0.00025 - (0.00025 + 0.0005))
    # mae = min(low)/entry - 1, lows 含 entry..exit
    # lows = [9.8, 10.0, 10.3, 10.5, 10.4, 10.6], min = 9.8
    assert _approx(t.mae, 9.8 / 10.2 - 1.0)
    # mfe = max(high)/entry - 1, highs = [10.5, 10.8, 11.0, 11.0, 10.9, 11.2]
    assert _approx(t.mfe, 11.2 / 10.2 - 1.0)
    assert t.meta == {}


# ────────────────────────────────────────────────────────────
# 场景 2: next_open 入场
# ────────────────────────────────────────────────────────────
def test_next_open_entry_happy_path(default_config, default_price_df):
    """next_open happy path: 信号日提前到能让 hold 5 bars 完整成交。

    信号日 = 2023-12-29（不在数据中），改为：让数据 8 行 + 信号日在第 0 行。
    """
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 10.7, 11.2, 10.6, 11.0),
        ("2024-01-07", "000001.SZ", "平安银行", 11.0, 11.4, 10.9, 11.3),
        ("2024-01-08", "000001.SZ", "平安银行", 11.3, 11.6, 11.1, 11.5),
    ]
    price_df = pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")

    new_config = type(default_config)(
        name=default_config.name,
        entry=EntryRule(timing="next_open", price_field="open"),
        exit=default_config.exit,
        position=default_config.position,
        cost=default_config.cost,
    )
    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(price_df, signals_df, new_config)

    assert result.summary["trade_count"] == 1
    t = result.trades[0]
    # 入场日 = 2024-01-02（信号日的下一根 bar），入场价 = open = 10.2
    assert t.entry_date == "2024-01-02"
    assert _approx(t.entry_price, 10.2)
    # hold 5 bars: 入场 idx=1，+5 = idx=6 = 2024-01-07
    assert t.exit_date == "2024-01-07"
    assert _approx(t.exit_price, 11.3)
    assert t.holding_bars == 5


# ────────────────────────────────────────────────────────────
# 场景 3: next_close 入场
# ────────────────────────────────────────────────────────────
def test_next_close_entry(default_config):
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 10.7, 11.2, 10.6, 11.0),
        ("2024-01-07", "000001.SZ", "平安银行", 11.0, 11.4, 10.9, 11.3),
        ("2024-01-08", "000001.SZ", "平安银行", 11.3, 11.6, 11.1, 11.5),
    ]
    price_df = pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")

    new_config = type(default_config)(
        name=default_config.name,
        entry=EntryRule(timing="next_close", price_field="close"),
        exit=default_config.exit,
        position=default_config.position,
        cost=default_config.cost,
    )
    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(price_df, signals_df, new_config)

    assert result.summary["trade_count"] == 1
    t = result.trades[0]
    # 入场日 = 2024-01-02（信号日的下一根 bar），入场价 = close = 10.5
    assert t.entry_date == "2024-01-02"
    assert _approx(t.entry_price, 10.5)
    # hold 5 bars: 入场 idx=1，+5 = idx=6 = 2024-01-07，出场价 = close = 11.3
    assert t.exit_date == "2024-01-07"
    assert _approx(t.exit_price, 11.3)


# ────────────────────────────────────────────────────────────
# 场景 4: 缺少行情 → NO_PRICE_DATA
# ────────────────────────────────────────────────────────────
def test_no_price_data(default_config, default_price_df):
    signals_df = make_signals(asset_id="999999.SZ")
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    s = result.skipped[0]
    assert s.reason == "NO_PRICE_DATA"
    assert s.asset_id == "999999.SZ"
    assert s.signal_date == "2024-01-01"


# ────────────────────────────────────────────────────────────
# 场景 5: signal_date 不存在 → SIGNAL_DATE_NOT_FOUND
# ────────────────────────────────────────────────────────────
def test_signal_date_not_found(default_config, default_price_df):
    signals_df = make_signals(signal_date="2023-12-15")
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    s = result.skipped[0]
    assert s.reason == "SIGNAL_DATE_NOT_FOUND"
    assert s.asset_id == "000001.SZ"
    assert s.signal_date == "2023-12-15"


# ────────────────────────────────────────────────────────────
# 场景 6: next_open 但没有下一根 bar → NO_NEXT_BAR_FOR_ENTRY
# ────────────────────────────────────────────────────────────
def test_no_next_bar_for_entry(default_config, default_price_df):
    new_config = type(default_config)(
        name=default_config.name,
        entry=EntryRule(timing="next_open", price_field="open"),
        exit=default_config.exit,
        position=default_config.position,
        cost=default_config.cost,
    )
    # 信号日 = 最后一根 bar (2024-01-06)
    signals_df = make_signals(signal_date="2024-01-06")
    result = BacktestEngine().run(default_price_df, signals_df, new_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    s = result.skipped[0]
    assert s.reason == "NO_NEXT_BAR_FOR_ENTRY"
    assert s.asset_id == "000001.SZ"
    assert s.signal_date == "2024-01-06"


# ────────────────────────────────────────────────────────────
# 场景 7: 出场 bar 不足 → NO_EXIT_BAR
# ────────────────────────────────────────────────────────────
def test_no_exit_bar(default_config, default_price_df):
    # 信号日 = 2024-01-03 (idx=2)，+5 = idx=7，但 len=6 → NO_EXIT_BAR
    signals_df = make_signals(signal_date="2024-01-03")
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    s = result.skipped[0]
    assert s.reason == "NO_EXIT_BAR"
    assert s.asset_id == "000001.SZ"
    assert s.signal_date == "2024-01-03"
    # detail 应包含 hold_n_bars 信息
    assert "hold_n_bars=5" in (s.detail or "")


def test_no_exit_bar_with_large_bars(default_config, default_price_df):
    # bars=20，肯定不足
    new_config = type(default_config)(
        name=default_config.name,
        entry=default_config.entry,
        exit=ExitRule(type="hold_n_bars", bars=20, price_field="close"),
        position=default_config.position,
        cost=default_config.cost,
    )
    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(default_price_df, signals_df, new_config)

    assert result.summary["skipped_count"] == 1
    assert result.skipped[0].reason == "NO_EXIT_BAR"
    assert "hold_n_bars=20" in (result.skipped[0].detail or "")


# ────────────────────────────────────────────────────────────
# 场景 8: 非 long 信号 → INVALID_DIRECTION
# ────────────────────────────────────────────────────────────
def test_invalid_direction(default_config, default_price_df):
    signals_df = make_signals(direction="short")
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    s = result.skipped[0]
    assert s.reason == "INVALID_DIRECTION"
    assert s.asset_id == "000001.SZ"


# ────────────────────────────────────────────────────────────
# 场景 9: 价格非法 → INVALID_PRICE
# ────────────────────────────────────────────────────────────
def test_invalid_price_entry(default_config):
    # entry close = 0 → INVALID_PRICE
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 0.0, 0.0, 0.0, 0.0),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 10.7, 11.2, 10.6, 11.0),
    ]
    price_df = pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")

    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(price_df, signals_df, default_config)

    assert result.summary["skipped_count"] == 1
    assert result.skipped[0].reason == "INVALID_PRICE"


def test_invalid_price_exit(default_config):
    # exit close = 0 → INVALID_PRICE
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 0.0, 0.0, 0.0, 0.0),
    ]
    price_df = pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")

    signals_df = make_signals(signal_date="2024-01-01")
    result = BacktestEngine().run(price_df, signals_df, default_config)

    assert result.summary["skipped_count"] == 1
    assert result.skipped[0].reason == "INVALID_PRICE"


# ────────────────────────────────────────────────────────────
# 场景 10: 空输入
# ────────────────────────────────────────────────────────────
def test_empty_price_df(default_config):
    price_df = pd.DataFrame(
        columns=["trade_date", "asset_id", "asset_name", "asset_type", "open", "high", "low", "close"]
    )
    signals_df = make_signals()
    result = BacktestEngine().run(price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 1
    assert result.skipped[0].reason == "NO_PRICE_DATA"
    assert result.equity_curve == []


def test_empty_signals_df(default_config, default_price_df):
    signals_df = pd.DataFrame(columns=["signal_date", "asset_id", "direction"])
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 0
    assert result.summary["win_rate"] is None
    assert result.summary["avg_return"] is None
    assert result.trades == []
    assert result.skipped == []
    assert result.equity_curve == []


def test_both_empty(default_config):
    price_df = pd.DataFrame(
        columns=["trade_date", "asset_id", "asset_name", "asset_type", "open", "high", "low", "close"]
    )
    signals_df = pd.DataFrame(columns=["signal_date", "asset_id", "direction"])
    result = BacktestEngine().run(price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 0
    assert result.summary["skipped_count"] == 0
    assert result.equity_curve == []


# ────────────────────────────────────────────────────────────
# 额外: meta 透传
# ────────────────────────────────────────────────────────────
def test_meta_passthrough(default_config, default_price_df):
    signals_df = pd.DataFrame(
        [
            {
                "signal_date": "2024-01-01",
                "asset_id": "000001.SZ",
                "direction": "long",
                "signal_name": "manual_test",
                "meta": {"tag": "abc", "score": 0.9},
            }
        ]
    )
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 1
    assert result.trades[0].meta == {"tag": "abc", "score": 0.9}


# ────────────────────────────────────────────────────────────
# 额外: 多信号混合（成交 + skipped）
# ────────────────────────────────────────────────────────────
def test_mixed_signals(default_config, default_price_df):
    signals_df = pd.DataFrame(
        [
            {"signal_date": "2024-01-01", "asset_id": "000001.SZ", "direction": "long"},
            {"signal_date": "2024-01-01", "asset_id": "999999.SZ", "direction": "long"},
            {"signal_date": "2024-01-01", "asset_id": "000001.SZ", "direction": "short"},
        ]
    )
    result = BacktestEngine().run(default_price_df, signals_df, default_config)

    assert result.summary["trade_count"] == 1
    assert result.summary["skipped_count"] == 2
    reasons = {s.reason for s in result.skipped}
    assert "NO_PRICE_DATA" in reasons
    assert "INVALID_DIRECTION" in reasons
