"""Tests for shared PIT financial factor panel preparation."""

from __future__ import annotations

import math
from datetime import date, datetime

import pandas as pd
import pytest

from qrp_atlas.backtest import prepare_financial_factor_panel
from qrp_atlas.indicators import build_historical_universe, normalize_trade_date


def _day(value: str):
    return normalize_trade_date(value)


def _financials() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "A",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 8),
                "published_at": datetime(2024, 1, 6, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 7, 1, 0, 0),
                "revision_id": "a_old",
                "roe": 0.10,
                "bps": 5.0,
            },
            {
                "ticker": "A",
                "report_period": date(2023, 12, 31),
                "available_trade_date": date(2024, 1, 11),
                "published_at": datetime(2024, 1, 10, 18, 0, 0),
                "ingested_at": datetime(2024, 1, 11, 1, 0, 0),
                "revision_id": "a_new",
                "roe": 0.15,
                "bps": 6.0,
            },
        ]
    )


def test_prepare_panel_columns_and_pit_visibility() -> None:
    universe = build_historical_universe(
        ["2024-01-07", "2024-01-08", "2024-01-11"],
        asset_ids=["A"],
        source="explicit",
    )
    panel = prepare_financial_factor_panel(universe, financials=_financials())
    assert list(panel.columns) == ["trade_date", "asset_id", "roe", "bps"]
    by = panel.set_index("trade_date")
    assert math.isnan(by.loc[_day("2024-01-07"), "roe"])
    assert math.isnan(by.loc[_day("2024-01-07"), "bps"])
    assert by.loc[_day("2024-01-08"), "roe"] == pytest.approx(0.10)
    assert by.loc[_day("2024-01-08"), "bps"] == pytest.approx(5.0)
    assert by.loc[_day("2024-01-11"), "roe"] == pytest.approx(0.15)
    assert by.loc[_day("2024-01-11"), "bps"] == pytest.approx(6.0)


def test_one_query_per_trade_date_for_shared_fields() -> None:
    calls: list[object] = []

    def fake_query(*, as_of_date, table, tickers=None, **kwargs):
        calls.append(as_of_date)
        return _financials()

    universe = build_historical_universe(
        ["2024-01-08", "2024-01-11"],
        asset_ids=["A"],
        source="explicit",
    )
    panel = prepare_financial_factor_panel(universe, financial_query=fake_query)
    assert len(calls) == 2
    assert set(panel["roe"].dropna()) == {0.10, 0.15}
