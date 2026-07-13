"""Tests for prepared cross-section exposure panel (task 04-C)."""

from __future__ import annotations

import math
from datetime import date, datetime

import pandas as pd
import pytest

from qrp_atlas.backtest import prepare_cross_section_exposure_panel
from qrp_atlas.indicators import build_historical_universe, normalize_trade_date


def _day(v: str):
    return normalize_trade_date(v)


def test_exposure_panel_columns_and_same_day_size() -> None:
    universe = build_historical_universe(
        ["2024-01-02", "2024-01-03"],
        asset_ids=["A", "B"],
        source="explicit",
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "market_cap": 0.0},
            {"trade_date": "2024-01-03", "asset_id": "A", "market_cap": 200.0},
            # B missing on day2 -> NaN, no forward fill from day1
        ]
    )
    industry = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "industry_code": "I1"},
            {"trade_date": "2024-01-02", "asset_id": "B", "industry_code": "I2"},
            {"trade_date": "2024-01-03", "asset_id": "A", "industry_code": "I1"},
            {"trade_date": "2024-01-03", "asset_id": "B", "industry_code": "I3"},
        ]
    )
    panel = prepare_cross_section_exposure_panel(
        universe, size_panel=size, industry_panel=industry
    )
    assert list(panel.columns) == ["trade_date", "asset_id", "industry_code", "log_market_cap"]
    by = panel.set_index(["trade_date", "asset_id"])
    assert by.loc[(_day("2024-01-02"), "A"), "log_market_cap"] == pytest.approx(math.log(100.0))
    assert math.isnan(by.loc[(_day("2024-01-02"), "B"), "log_market_cap"])
    assert by.loc[(_day("2024-01-03"), "A"), "log_market_cap"] == pytest.approx(math.log(200.0))
    assert math.isnan(by.loc[(_day("2024-01-03"), "B"), "log_market_cap"])
    assert by.loc[(_day("2024-01-03"), "B"), "industry_code"] == "I3"


def test_industry_query_once_per_date_and_pit_semantics() -> None:
    calls: list[object] = []

    def fake_query(*, as_of_date, asset_ids=None, classification_system=None, industry_level=None, **kwargs):
        calls.append((as_of_date, classification_system, industry_level, tuple(asset_ids or ())))
        # future membership only after 2024-01-10 for A
        as_of = pd.Timestamp(as_of_date)
        rows = []
        for asset in asset_ids or []:
            if asset == "A":
                if as_of >= pd.Timestamp("2024-01-10"):
                    code = "NEW"
                else:
                    code = "OLD"
            else:
                code = "OTH"
            rows.append({"asset_id": asset, "industry_code": code})
        return pd.DataFrame(rows)

    universe = build_historical_universe(
        ["2024-01-08", "2024-01-11"],
        asset_ids=["A", "B"],
        source="explicit",
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-08", "asset_id": "A", "market_cap": 10.0},
            {"trade_date": "2024-01-08", "asset_id": "B", "market_cap": 20.0},
            {"trade_date": "2024-01-11", "asset_id": "A", "market_cap": 30.0},
            {"trade_date": "2024-01-11", "asset_id": "B", "market_cap": 40.0},
        ]
    )
    panel = prepare_cross_section_exposure_panel(
        universe,
        size_panel=size,
        industry_query=fake_query,
        classification_system="sw2021",
        industry_level=1,
    )
    assert len(calls) == 2
    by = panel.set_index(["trade_date", "asset_id"])
    assert by.loc[(_day("2024-01-08"), "A"), "industry_code"] == "OLD"
    assert by.loc[(_day("2024-01-11"), "A"), "industry_code"] == "NEW"
    # later change does not pollute old date
    assert by.loc[(_day("2024-01-08"), "A"), "industry_code"] != "NEW"


def test_duplicate_size_keys_raise() -> None:
    universe = build_historical_universe(["2024-01-02"], asset_ids=["A"], source="explicit")
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 10.0},
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 20.0},
        ]
    )
    with pytest.raises(Exception, match="duplicate"):
        prepare_cross_section_exposure_panel(universe, size_panel=size, industry_panel=pd.DataFrame())


def test_empty_universe_stable() -> None:
    panel = prepare_cross_section_exposure_panel(
        build_historical_universe([], asset_ids=["A"], source="explicit")
    )
    assert panel.empty
    assert list(panel.columns) == ["trade_date", "asset_id", "industry_code", "log_market_cap"]

def test_empty_industry_panel_yields_missing_industry() -> None:
    universe = build_historical_universe(
        ["2024-01-02"], asset_ids=["A", "B"], source="explicit"
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 10.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "market_cap": 20.0},
        ]
    )
    for empty in (pd.DataFrame(), pd.DataFrame(columns=["trade_date", "asset_id", "industry_code"])):
        panel = prepare_cross_section_exposure_panel(
            universe, size_panel=size, industry_panel=empty
        )
        assert len(panel) == 2
        assert list(panel.columns) == ["trade_date", "asset_id", "industry_code", "log_market_cap"]
        assert panel["industry_code"].isna().all() or panel["industry_code"].tolist() == [None, None]


def test_industry_query_duplicate_assets_raise() -> None:
    from qrp_atlas.backtest.exposure_data import ExposurePanelError

    def bad_query(*, as_of_date, asset_ids=None, **kwargs):
        return pd.DataFrame(
            [
                {"asset_id": "A", "industry_code": "I1"},
                {"asset_id": "A", "industry_code": "I2"},
                {"asset_id": "B", "industry_code": "I3"},
            ]
        )

    universe = build_historical_universe(
        ["2024-01-02"], asset_ids=["A", "B"], source="explicit"
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 10.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "market_cap": 20.0},
        ]
    )
    with pytest.raises(ExposurePanelError, match="duplicate"):
        prepare_cross_section_exposure_panel(
            universe, size_panel=size, industry_query=bad_query
        )


def test_pd_na_industry_normalized_in_panel() -> None:
    universe = build_historical_universe(["2024-01-02"], asset_ids=["A"], source="explicit")
    size = pd.DataFrame([{"trade_date": "2024-01-02", "asset_id": "A", "market_cap": 10.0}])
    industry = pd.DataFrame(
        [{"trade_date": "2024-01-02", "asset_id": "A", "industry_code": pd.NA}]
    )
    panel = prepare_cross_section_exposure_panel(
        universe, size_panel=size, industry_panel=industry
    )
    val = panel.loc[0, "industry_code"]
    assert val is None or (isinstance(val, float) and math.isnan(val)) or pd.isna(val)
    assert str(val) != "<NA>" or val is None or pd.isna(val)
    assert "<NA>" not in panel["industry_code"].astype(str).tolist()
