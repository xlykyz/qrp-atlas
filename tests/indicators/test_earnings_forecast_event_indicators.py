"""Tests for 05-B earnings-forecast event indicators."""

from __future__ import annotations

import math
from datetime import date

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    DIRECTION_SCORE_BY_FORECAST_TYPE,
    attach_earnings_forecast_indicators,
    compute_direction_score,
    compute_event_age,
    compute_event_window,
    compute_net_profit_midpoint,
    compute_profit_change_midpoint,
    compute_profit_change_range,
    get_indicator,
    map_forecast_type_direction,
)


def test_midpoint_range_missing_is_nan():
    assert math.isnan(compute_profit_change_midpoint(None, 10))
    assert math.isnan(compute_profit_change_midpoint(10, None))
    assert math.isnan(compute_profit_change_range(math.nan, 3))
    assert compute_profit_change_midpoint(10, 20) == 15
    assert compute_profit_change_range(10, 20) == 10
    assert math.isnan(compute_net_profit_midpoint(1, None))
    assert compute_net_profit_midpoint(100, 200) == 150


def test_forecast_type_mapping_known_and_unknown():
    assert DIRECTION_SCORE_BY_FORECAST_TYPE["预增"] == 1
    assert DIRECTION_SCORE_BY_FORECAST_TYPE["预减"] == -1
    assert DIRECTION_SCORE_BY_FORECAST_TYPE["不确定"] == 0
    assert compute_direction_score("扭亏") == 1
    assert compute_direction_score("首亏") == -1
    score, unknown = map_forecast_type_direction("从未见过的类型")
    assert score == 0
    assert unknown is True


def test_event_age_weekend_and_holiday():
    open_dates = [
        date(2024, 3, 15),  # Fri
        date(2024, 3, 18),  # Mon
        date(2024, 3, 19),
        date(2024, 3, 20),
        date(2024, 4, 1),
        date(2024, 4, 2),
        date(2024, 4, 3),
        date(2024, 4, 5),  # skip 4/4 holiday
    ]
    # available Mon, age 0 same day
    assert compute_event_age("2024-03-18", "2024-03-18", open_dates=open_dates) == 0
    assert compute_event_age("2024-03-19", "2024-03-18", open_dates=open_dates) == 1
    # before availability
    assert math.isnan(compute_event_age("2024-03-15", "2024-03-18", open_dates=open_dates))
    # holiday gap still counts open days only
    assert compute_event_age("2024-04-05", "2024-04-03", open_dates=open_dates) == 1


def test_event_window_bounds():
    open_dates = [
        "2024-03-18",
        "2024-03-19",
        "2024-03-20",
        "2024-03-21",
        "2024-03-22",
        "2024-03-25",
    ]
    assert compute_event_window("2024-03-18", "2024-03-18", open_dates=open_dates, window_days=5) is True
    # age=4 still inside [0,5)
    assert compute_event_window("2024-03-22", "2024-03-18", open_dates=open_dates, window_days=5) is True
    # age=5 outside
    assert compute_event_window("2024-03-25", "2024-03-18", open_dates=open_dates, window_days=5) is False


def test_attach_does_not_mutate_and_unknown_diagnostics():
    events = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "forecast_type": "神秘类型",
                "profit_change_min": 10,
                "profit_change_max": 20,
                "net_profit_min": 1,
                "net_profit_max": 2,
                "available_trade_date": "2024-03-18",
            }
        ]
    )
    original = events.copy()
    out, diag = attach_earnings_forecast_indicators(
        events,
        trade_date="2024-03-19",
        open_dates=["2024-03-18", "2024-03-19"],
        window_days=5,
    )
    assert events.equals(original)
    assert out.loc[0, "direction_score"] == 0
    assert any("unknown_forecast_type" in d for d in diag)
    assert get_indicator("event_age").code == "event_age"
