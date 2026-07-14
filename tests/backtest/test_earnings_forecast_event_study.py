"""Tests for 05-B earnings forecast event study."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    compute_event_forward_returns,
    run_earnings_forecast_event_study,
)


def _calendar() -> list[date]:
    # 10 open days from 2024-03-18
    start = date(2024, 3, 18)
    return [start + timedelta(days=i) for i in range(0, 20) if (start + timedelta(days=i)).weekday() < 5][:12]


def _prices(tickers=("000001.SZ", "600519.SH")) -> pd.DataFrame:
    rows = []
    for i, day in enumerate(_calendar()):
        for j, t in enumerate(tickers):
            # deterministic rising path
            px = 10 + i + j
            rows.append(
                {
                    "trade_date": day.isoformat(),
                    "asset_id": t,
                    "ticker": t,
                    "open": float(px),
                    "high": float(px + 1),
                    "low": float(px - 1),
                    "close": float(px + 0.5),
                }
            )
    return pd.DataFrame(rows)


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "event_type": "earnings_forecast",
                "event_series_id": "s1",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 10,
                "profit_change_max": 20,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "source_record_id": "r1",
                "revision_id": "v1",
            },
            {
                "ticker": "600519.SH",
                "event_type": "earnings_forecast",
                "event_series_id": "s2",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预减",
                "profit_change_min": -20,
                "profit_change_max": -10,
                "net_profit_min": -50,
                "net_profit_max": -40,
                "source_record_id": "r2",
                "revision_id": "v2",
            },
        ]
    )


def test_forward_return_horizons_and_missing():
    events = _events()
    prices = _prices()
    labeled, diag = compute_event_forward_returns(
        events,
        prices,
        trading_days=_calendar(),
        horizons=[1, 5, 10, 20],
    )
    # entry open on day0 = 10 for 000001, day1 close = 10.5? wait day0 close=10.5, horizon1 exit day0 close
    row = labeled[labeled.ticker == "000001.SZ"].iloc[0]
    assert abs(row["forward_return_1d"] - (10.5 / 10.0 - 1)) < 1e-12
    # horizon 5: day index 4 close = 10 + 4 + 0.5 = 14.5
    assert abs(row["forward_return_5d"] - (14.5 / 10.0 - 1)) < 1e-12
    # horizon 20 incomplete => nan
    assert math.isnan(row["forward_return_20d"])
    assert any("incomplete_future_window" in d for d in diag)

    # missing open
    bad_prices = prices.copy()
    bad_prices.loc[
        (bad_prices.asset_id == "000001.SZ") & (bad_prices.trade_date == "2024-03-18"),
        "open",
    ] = math.nan
    labeled2, diag2 = compute_event_forward_returns(
        events,
        bad_prices,
        trading_days=_calendar(),
        horizons=[1],
    )
    assert math.isnan(labeled2.loc[labeled2.ticker == "000001.SZ", "forward_return_1d"].iloc[0])
    assert any("missing_entry_open" in d for d in diag2)


def test_group_stats_and_input_immutability():
    events = _events()
    original = events.copy()
    result = run_earnings_forecast_event_study(
        events,
        _prices(),
        trading_days=_calendar(),
        horizons=[1, 5],
    )
    assert events.equals(original)
    assert not result.group_stats.empty
    assert set(["sample_count", "valid_return_count", "mean_return", "median_return", "win_rate", "std_return"]).issubset(
        result.group_stats.columns
    )
    # direction group exists
    assert DIRECTION_SCORE_PRESENT(result)


def DIRECTION_SCORE_PRESENT(result) -> bool:
    return "direction_score" in set(result.group_stats["group_dim"])


def test_midpoint_buckets_param_removed_and_fixed_bins():
    import inspect
    from qrp_atlas.backtest.research.event_study import run_earnings_forecast_event_study as study

    sig = inspect.signature(study)
    assert "midpoint_buckets" not in sig.parameters
    result = run_earnings_forecast_event_study(
        _events(),
        _prices(),
        trading_days=_calendar(),
        horizons=[1],
    )
    assert "profit_change_midpoint_bucket" in set(result.group_stats["group_dim"])
    buckets = set(
        result.group_stats.loc[
            result.group_stats["group_dim"] == "profit_change_midpoint_bucket",
            "profit_change_midpoint_bucket",
        ].astype(str)
    )
    # sample midpoints 15 and -15 map to fixed research bins
    assert "(10,50]" in buckets
    assert "(-50,-10]" in buckets
