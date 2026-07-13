"""Tests for cross-sectional industry/size neutralization (task 04-C)."""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.backtest import prepare_cross_section_exposure_panel
from qrp_atlas.indicators import (
    NeutralizationError,
    build_historical_universe,
    neutralize_factor_frame,
    normalize_trade_date,
    process_cross_section,
)


def _day(value: str) -> pd.Timestamp:
    return normalize_trade_date(value)


def _factors() -> pd.DataFrame:
    # Two dates, multi-industry, multi-size synthetic factors.
    rows = [
        # day1
        {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 1.0, "roe": 0.10},
        {"trade_date": "2024-01-02", "asset_id": "A2", "momentum": 3.0, "roe": 0.30},
        {"trade_date": "2024-01-02", "asset_id": "B1", "momentum": 2.0, "roe": 0.20},
        {"trade_date": "2024-01-02", "asset_id": "B2", "momentum": 4.0, "roe": 0.40},
        {"trade_date": "2024-01-02", "asset_id": "C1", "momentum": 5.0, "roe": 0.50},
        # day2 - independent values
        {"trade_date": "2024-01-03", "asset_id": "A1", "momentum": 10.0, "roe": 1.0},
        {"trade_date": "2024-01-03", "asset_id": "A2", "momentum": 12.0, "roe": 1.2},
        {"trade_date": "2024-01-03", "asset_id": "B1", "momentum": 11.0, "roe": 1.1},
        {"trade_date": "2024-01-03", "asset_id": "B2", "momentum": 13.0, "roe": 1.3},
        {"trade_date": "2024-01-03", "asset_id": "C1", "momentum": 14.0, "roe": 1.4},
    ]
    return pd.DataFrame(rows)


def _industry_panel() -> pd.DataFrame:
    rows = []
    for d in ("2024-01-02", "2024-01-03"):
        rows.extend(
            [
                {"trade_date": d, "asset_id": "A1", "industry_code": "IND_A"},
                {"trade_date": d, "asset_id": "A2", "industry_code": "IND_A"},
                {"trade_date": d, "asset_id": "B1", "industry_code": "IND_B"},
                {"trade_date": d, "asset_id": "B2", "industry_code": "IND_B"},
                {"trade_date": d, "asset_id": "C1", "industry_code": "IND_C"},
            ]
        )
    return pd.DataFrame(rows)


def _size_panel() -> pd.DataFrame:
    # same-day market caps; day2 different levels
    rows = []
    caps_d1 = {"A1": 100.0, "A2": 400.0, "B1": 200.0, "B2": 800.0, "C1": 300.0}
    caps_d2 = {"A1": 110.0, "A2": 440.0, "B1": 220.0, "B2": 880.0, "C1": 330.0}
    for asset, cap in caps_d1.items():
        rows.append({"trade_date": "2024-01-02", "asset_id": asset, "market_cap": cap})
    for asset, cap in caps_d2.items():
        rows.append({"trade_date": "2024-01-03", "asset_id": asset, "market_cap": cap})
    return pd.DataFrame(rows)


def _exposure() -> pd.DataFrame:
    universe = build_historical_universe(
        ["2024-01-02", "2024-01-03"],
        asset_ids=["A1", "A2", "B1", "B2", "C1"],
        source="explicit",
    )
    return prepare_cross_section_exposure_panel(
        universe,
        size_panel=_size_panel(),
        industry_panel=_industry_panel(),
    )


def test_industry_neutral_industry_means_near_zero() -> None:
    out = neutralize_factor_frame(
        _factors(),
        exposure_panel=_exposure(),
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=(),
    )
    day = out[out["trade_date"] == _day("2024-01-02")].copy()
    industry = _industry_panel().copy()
    industry["trade_date"] = industry["trade_date"].map(normalize_trade_date)
    day = day.merge(
        industry[industry["trade_date"] == _day("2024-01-02")],
        on=["trade_date", "asset_id"],
    )
    for code, group in day.groupby("industry_code"):
        if len(group) >= 2:
            assert group["momentum_neutral"].mean() == pytest.approx(0.0, abs=1e-10)


def test_size_neutral_orthogonal_to_log_market_cap() -> None:
    exp = _exposure()
    out = neutralize_factor_frame(
        _factors(),
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=(),
        numeric_exposures=["log_market_cap"],
    )
    day = out[out["trade_date"] == _day("2024-01-02")].merge(
        exp[exp["trade_date"] == _day("2024-01-02")][["asset_id", "log_market_cap"]],
        on="asset_id",
    )
    resid = day["momentum_neutral"].to_numpy(dtype=float)
    size = day["log_market_cap"].to_numpy(dtype=float)
    # demeaned covariance ~ 0 under intercept + size OLS
    resid_dm = resid - resid.mean()
    size_dm = size - size.mean()
    cov = float(np.dot(resid_dm, size_dm) / len(resid))
    assert cov == pytest.approx(0.0, abs=1e-10)


def test_joint_industry_size_and_multi_factor() -> None:
    out = neutralize_factor_frame(
        _factors(),
        exposure_panel=_exposure(),
        factor_columns=["momentum", "roe"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    assert "momentum_neutral" in out.columns
    assert "roe_neutral" in out.columns
    # original factors preserved
    assert "momentum" in out.columns and "roe" in out.columns
    # residuals finite for complete rows
    day = out[out["trade_date"] == _day("2024-01-02")]
    assert day["momentum_neutral"].notna().all()
    assert day["roe_neutral"].notna().all()


def test_multi_date_isolation() -> None:
    factors = _factors()
    # mutate day2 only
    factors.loc[factors["trade_date"] == "2024-01-03", "momentum"] = [
        100, 200, 300, 400, 500
    ]
    out = neutralize_factor_frame(
        factors,
        exposure_panel=_exposure(),
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=(),
    )
    d1 = out[out["trade_date"] == _day("2024-01-02")].set_index("asset_id")["momentum_neutral"]
    # day1 residual for A1 in industry A with values 1 and 3 -> residual -1 and +1
    assert d1.loc["A1"] == pytest.approx(-1.0)
    assert d1.loc["A2"] == pytest.approx(1.0)
    d2 = out[out["trade_date"] == _day("2024-01-03")].set_index("asset_id")["momentum_neutral"]
    # day2 independent, A means of 100 and 200 -> residuals -50 / +50
    assert d2.loc["A1"] == pytest.approx(-50.0)
    assert d2.loc["A2"] == pytest.approx(50.0)


def test_missing_industry_size_factor_keep_rows() -> None:
    factors = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "momentum": math.nan},
            {"trade_date": "2024-01-02", "asset_id": "B1", "momentum": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "MISS", "momentum": 3.0},
        ]
    )
    industry = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "A2", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "B1", "industry_code": None},
        ]
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "market_cap": 200.0},
            {"trade_date": "2024-01-02", "asset_id": "B1", "market_cap": 300.0},
            # MISS missing size
        ]
    )
    universe = build_historical_universe(
        ["2024-01-02"], asset_ids=["A1", "A2", "B1", "MISS"], source="explicit"
    )
    exp = prepare_cross_section_exposure_panel(
        universe, size_panel=size, industry_panel=industry
    )
    out = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    assert set(out["asset_id"]) == {"A1", "A2", "B1", "MISS"}
    by = out.set_index("asset_id")
    assert math.isnan(by.loc["A2", "momentum_neutral"])  # missing factor
    assert math.isnan(by.loc["B1", "momentum_neutral"])  # missing industry
    assert math.isnan(by.loc["MISS", "momentum_neutral"])  # missing exposures


def test_single_industry_and_small_sample_nan() -> None:
    factors = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "momentum": 2.0},
        ]
    )
    industry = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "industry_code": "ONLY"},
            {"trade_date": "2024-01-02", "asset_id": "A2", "industry_code": "ONLY"},
        ]
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "market_cap": 100.0},  # constant size
        ]
    )
    uni = build_historical_universe(["2024-01-02"], asset_ids=["A1", "A2"], source="explicit")
    exp = prepare_cross_section_exposure_panel(uni, size_panel=size, industry_panel=industry)
    # industry-only with single industry reduces to demean via intercept
    out_ind = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=(),
    )
    assert out_ind.set_index("asset_id").loc["A1", "momentum_neutral"] == pytest.approx(-0.5)
    # joint with constant size -> singular design (intercept + constant size) => NaN
    out_joint = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    assert out_joint["momentum_neutral"].isna().all()


def test_constant_factor_residuals_zero_or_nan_when_unusable() -> None:
    factors = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 5.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "momentum": 5.0},
            {"trade_date": "2024-01-02", "asset_id": "B1", "momentum": 5.0},
        ]
    )
    industry = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "A2", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "B1", "industry_code": "IND_B"},
        ]
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "market_cap": 200.0},
            {"trade_date": "2024-01-02", "asset_id": "B1", "market_cap": 300.0},
        ]
    )
    uni = build_historical_universe(["2024-01-02"], asset_ids=["A1", "A2", "B1"], source="explicit")
    exp = prepare_cross_section_exposure_panel(uni, size_panel=size, industry_panel=industry)
    out = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    # constant y is perfectly fit by intercept => residuals ~ 0
    assert out["momentum_neutral"].abs().max() == pytest.approx(0.0, abs=1e-10)


def test_duplicate_keys_and_reserved_output() -> None:
    factors = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": 2.0},
        ]
    )
    with pytest.raises(Exception):
        neutralize_factor_frame(
            factors,
            exposure_panel=_exposure(),
            factor_columns=["momentum"],
            categorical_exposures=["industry_code"],
            numeric_exposures=(),
        )
    with pytest.raises(NeutralizationError, match="reserved|collides"):
        neutralize_factor_frame(
            _factors(),
            exposure_panel=_exposure(),
            factor_columns=["momentum"],
            categorical_exposures=["industry_code"],
            numeric_exposures=(),
            output_columns={"momentum": "trade_date"},
        )
    with pytest.raises(NeutralizationError, match="collides"):
        neutralize_factor_frame(
            _factors(),
            exposure_panel=_exposure(),
            factor_columns=["momentum"],
            categorical_exposures=["industry_code"],
            numeric_exposures=(),
            output_columns={"momentum": "momentum"},
        )


def test_input_immutability_and_process_cross_section_chain() -> None:
    factors = _factors()
    exp = _exposure()
    f_before = factors.copy(deep=True)
    e_before = exp.copy(deep=True)
    neutral = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum", "roe"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    pd.testing.assert_frame_equal(factors, f_before)
    pd.testing.assert_frame_equal(exp, e_before)
    processed = process_cross_section(
        neutral,
        feature_columns=["momentum_neutral", "roe_neutral"],
        operators=("rank", "zscore"),
    )
    assert "momentum_neutral_rank" in processed.columns
    assert "roe_neutral_zscore" in processed.columns
    assert not processed.duplicated(subset=["trade_date", "asset_id"]).any()


def test_empty_universe_stable_structure() -> None:
    empty = pd.DataFrame(columns=["trade_date", "asset_id", "momentum"])
    exp = prepare_cross_section_exposure_panel(
        build_historical_universe([], asset_ids=["A"], source="explicit"),
        size_panel=_size_panel(),
        industry_panel=_industry_panel(),
    )
    out = neutralize_factor_frame(
        empty,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=["log_market_cap"],
    )
    assert out.empty
    assert "momentum_neutral" in out.columns


def test_non_finite_inputs_become_nan() -> None:
    factors = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "momentum": math.inf},
            {"trade_date": "2024-01-02", "asset_id": "A2", "momentum": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "B1", "momentum": 3.0},
            {"trade_date": "2024-01-02", "asset_id": "B2", "momentum": 4.0},
        ]
    )
    industry = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "A2", "industry_code": "IND_A"},
            {"trade_date": "2024-01-02", "asset_id": "B1", "industry_code": "IND_B"},
            {"trade_date": "2024-01-02", "asset_id": "B2", "industry_code": "IND_B"},
        ]
    )
    size = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A1", "market_cap": 100.0},
            {"trade_date": "2024-01-02", "asset_id": "A2", "market_cap": 200.0},
            {"trade_date": "2024-01-02", "asset_id": "B1", "market_cap": 300.0},
            {"trade_date": "2024-01-02", "asset_id": "B2", "market_cap": 400.0},
        ]
    )
    uni = build_historical_universe(
        ["2024-01-02"], asset_ids=["A1", "A2", "B1", "B2"], source="explicit"
    )
    exp = prepare_cross_section_exposure_panel(uni, size_panel=size, industry_panel=industry)
    out = neutralize_factor_frame(
        factors,
        exposure_panel=exp,
        factor_columns=["momentum"],
        categorical_exposures=["industry_code"],
        numeric_exposures=(),
    )
    by = out.set_index("asset_id")
    assert math.isnan(by.loc["A1", "momentum_neutral"])
    # A2 alone finite in industry A after dropping A1 -> industry sample size 1, residual 0 via demean of singleton group with intercept-only? 
    # With IND_A having only A2 usable, categorical baseline may collapse; residual should still be finite or nan but never inf.
    assert math.isfinite(by.loc["A2", "momentum_neutral"]) or math.isnan(by.loc["A2", "momentum_neutral"])
    assert not np.isinf(by["momentum_neutral"].to_numpy(dtype=float)).any()


def test_indicators_module_has_no_backtest_import() -> None:
    import qrp_atlas.indicators.cross_section.neutralize as mod

    source = Path(mod.__file__).read_text(encoding="utf-8")
    assert "from qrp_atlas.backtest" not in source
    assert "import qrp_atlas.backtest" not in source
    assert "db_path" not in source
