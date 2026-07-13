"""Tests for cross-sectional operators and conventions (task 04-A)."""

from __future__ import annotations

import math
from datetime import date, datetime

import pandas as pd
import pytest

from qrp_atlas.indicators import (
    CrossSectionFrameError,
    apply_cross_section_operators,
    cross_section_percentile_rank,
    cross_section_rank,
    cross_section_winsorize,
    cross_section_zscore,
    ensure_cross_section_frame,
    normalize_trade_date,
    process_cross_section,
)


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).sample(frac=1.0, random_state=7).reset_index(drop=True)


def _day(value: str) -> pd.Timestamp:
    return normalize_trade_date(value)


def test_multi_date_isolation_and_same_day_multi_asset() -> None:
    df = _frame(
        [
            {"trade_date": "2024-01-02", "asset_id": "B", "momentum": 0.2},
            {"trade_date": "2024-01-02", "asset_id": "A", "momentum": 0.1},
            {"trade_date": "2024-01-03", "asset_id": "A", "momentum": 0.9},
            {"trade_date": "2024-01-03", "asset_id": "B", "momentum": 0.3},
        ]
    )
    ranked = cross_section_rank(df, "momentum", ascending=True)
    day1 = ranked[ranked["trade_date"] == _day("2024-01-02")].set_index("asset_id")
    day2 = ranked[ranked["trade_date"] == _day("2024-01-03")].set_index("asset_id")
    assert day1.loc["A", "momentum_rank"] == 1.0
    assert day1.loc["B", "momentum_rank"] == 2.0
    # date 2 ranking is independent; high value is not compared with day 1
    assert day2.loc["B", "momentum_rank"] == 1.0
    assert day2.loc["A", "momentum_rank"] == 2.0


def test_rank_direction_and_ties() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "x": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "C", "x": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "D", "x": 3.0},
        ]
    )
    asc = cross_section_rank(df, "x", ascending=True, method="average")
    desc = cross_section_rank(df, "x", ascending=False, method="average")
    by = asc.set_index("asset_id")
    assert by.loc["A", "x_rank"] == 1.0
    assert by.loc["B", "x_rank"] == 2.5
    assert by.loc["C", "x_rank"] == 2.5
    assert by.loc["D", "x_rank"] == 4.0
    by_desc = desc.set_index("asset_id")
    assert by_desc.loc["D", "x_rank"] == 1.0
    assert by_desc.loc["A", "x_rank"] == 4.0


def test_percentile_rank_bounds() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "x": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "C", "x": 3.0},
        ]
    )
    out = cross_section_percentile_rank(df, "x")
    values = out.set_index("asset_id")["x_pct_rank"]
    assert values["A"] == pytest.approx(1 / 3)
    assert values["B"] == pytest.approx(2 / 3)
    assert values["C"] == pytest.approx(1.0)


def test_winsorize_bounds() -> None:
    values = list(range(1, 101))
    df = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 100,
            "asset_id": [f"A{i:03d}" for i in range(100)],
            "x": values,
        }
    )
    out = cross_section_winsorize(df, "x", limits=(0.05, 0.95))
    clipped = out["x_winsorized"]
    lo = float(pd.Series(values).quantile(0.05))
    hi = float(pd.Series(values).quantile(0.95))
    assert clipped.min() == pytest.approx(lo)
    assert clipped.max() == pytest.approx(hi)
    assert clipped.iloc[0] == pytest.approx(lo)
    assert clipped.iloc[-1] == pytest.approx(hi)


def test_zscore_mean_std_and_zero_variance() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "x": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "C", "x": 3.0},
            {"trade_date": "2024-01-03", "asset_id": "A", "x": 5.0},
            {"trade_date": "2024-01-03", "asset_id": "B", "x": 5.0},
            {"trade_date": "2024-01-03", "asset_id": "C", "x": 5.0},
        ]
    )
    out = cross_section_zscore(df, "x", ddof=0, min_count=2)
    day1 = out[out["trade_date"] == _day("2024-01-02")]["x_zscore"]
    assert day1.mean() == pytest.approx(0.0, abs=1e-12)
    assert day1.std(ddof=0) == pytest.approx(1.0, abs=1e-12)
    day2 = out[out["trade_date"] == _day("2024-01-03")]["x_zscore"]
    assert day2.isna().all()


def test_nan_and_inf_are_excluded() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "x": math.nan},
            {"trade_date": "2024-01-02", "asset_id": "C", "x": math.inf},
            {"trade_date": "2024-01-02", "asset_id": "D", "x": -math.inf},
            {"trade_date": "2024-01-02", "asset_id": "E", "x": 3.0},
        ]
    )
    ranked = cross_section_rank(df, "x")
    by = ranked.set_index("asset_id")
    assert by.loc["A", "x_rank"] == 1.0
    assert by.loc["E", "x_rank"] == 2.0
    assert math.isnan(by.loc["B", "x_rank"])
    assert math.isnan(by.loc["C", "x_rank"])
    assert math.isnan(by.loc["D", "x_rank"])

    z = cross_section_zscore(df, "x", ddof=0)
    zb = z.set_index("asset_id")
    assert math.isnan(zb.loc["B", "x_zscore"])
    assert math.isnan(zb.loc["C", "x_zscore"])
    assert not math.isnan(zb.loc["A", "x_zscore"])


def test_insufficient_sample_and_single_asset() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.5},
        ]
    )
    ranked = cross_section_rank(df, "x")
    assert ranked.loc[0, "x_rank"] == 1.0
    z = cross_section_zscore(df, "x", min_count=2)
    assert math.isnan(z.loc[0, "x_zscore"])
    w = cross_section_winsorize(df, "x")
    assert w.loc[0, "x_winsorized"] == 1.5


def test_multi_feature_and_operator_bundle() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "m": 1.0, "v": 10.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "m": 2.0, "v": 20.0},
            {"trade_date": "2024-01-02", "asset_id": "C", "m": 3.0, "v": 30.0},
        ]
    )
    out = apply_cross_section_operators(df, ["m", "v"], operators=("rank", "zscore"))
    assert {"m_rank", "v_rank", "m_zscore", "v_zscore"}.issubset(out.columns)
    assert out.loc[out["asset_id"] == "A", "m_rank"].iloc[0] == 1.0
    assert out.loc[out["asset_id"] == "C", "v_rank"].iloc[0] == 3.0


def test_empty_input_and_no_mutation_and_stable_sort() -> None:
    empty = cross_section_rank(pd.DataFrame(columns=["trade_date", "asset_id", "x"]), "x")
    assert empty.empty
    assert list(empty.columns[:2]) == ["trade_date", "asset_id"]

    df = _frame(
        [
            {"trade_date": "2024-01-03", "asset_id": "B", "x": 2.0},
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": "2024-01-02", "asset_id": "B", "x": 3.0},
            {"trade_date": "2024-01-03", "asset_id": "A", "x": 4.0},
        ]
    )
    original = df.copy(deep=True)
    first = cross_section_zscore(df, "x")
    second = cross_section_zscore(df.sample(frac=1.0, random_state=3), "x")
    pd.testing.assert_frame_equal(df, original)
    pd.testing.assert_frame_equal(first, second)
    keys = first[["trade_date", "asset_id"]].values.tolist()
    assert keys == sorted(keys)


def test_process_cross_section_entry_on_features() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "B", "momentum": 0.2},
            {"trade_date": "2024-01-02", "asset_id": "A", "momentum": 0.1},
        ]
    )
    out = process_cross_section(
        df,
        feature_columns="momentum",
        operators=("rank",),
        ascending=False,
    )
    assert list(out["asset_id"]) == ["A", "B"]
    assert out.loc[out["asset_id"] == "B", "momentum_rank"].iloc[0] == 1.0


def test_ensure_frame_validation() -> None:
    with pytest.raises(CrossSectionFrameError, match="missing required columns"):
        ensure_cross_section_frame(pd.DataFrame({"asset_id": ["A"]}))
    with pytest.raises(CrossSectionFrameError, match="feature column"):
        ensure_cross_section_frame(
            pd.DataFrame({"trade_date": ["2024-01-02"], "asset_id": ["A"]}),
            feature_columns=["missing"],
        )


def test_invalid_winsor_limits() -> None:
    df = pd.DataFrame(
        [{"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0}]
    )
    with pytest.raises(CrossSectionFrameError):
        cross_section_winsorize(df, "x", limits=(0.8, 0.2))


def test_mixed_date_types_form_one_cross_section() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": date(2024, 1, 2), "asset_id": "B", "x": 2.0},
            {"trade_date": datetime(2024, 1, 2, 15, 30), "asset_id": "C", "x": 3.0},
            {"trade_date": pd.Timestamp("2024-01-02 09:00:00"), "asset_id": "D", "x": 4.0},
        ]
    )
    original = df.copy(deep=True)
    out = cross_section_rank(df, "x")
    pd.testing.assert_frame_equal(df, original)
    assert out["trade_date"].nunique() == 1
    assert (out["trade_date"] == _day("2024-01-02")).all()
    assert set(out["x_rank"]) == {1.0, 2.0, 3.0, 4.0}


def test_duplicate_feature_keys_are_rejected() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": date(2024, 1, 2), "asset_id": "A", "x": 2.0},
        ]
    )
    with pytest.raises(CrossSectionFrameError, match="duplicate cross-section primary key"):
        cross_section_rank(df, "x")


def test_null_trade_date_and_blank_asset_id_are_rejected() -> None:
    with pytest.raises(CrossSectionFrameError, match="trade_date"):
        ensure_cross_section_frame(
            pd.DataFrame(
                [{"trade_date": None, "asset_id": "A", "x": 1.0}]
            )
        )
    with pytest.raises(CrossSectionFrameError, match="trade_date"):
        ensure_cross_section_frame(
            pd.DataFrame(
                [{"trade_date": "", "asset_id": "A", "x": 1.0}]
            )
        )
    with pytest.raises(CrossSectionFrameError, match="asset_id"):
        ensure_cross_section_frame(
            pd.DataFrame(
                [{"trade_date": "2024-01-02", "asset_id": "", "x": 1.0}]
            )
        )
    with pytest.raises(CrossSectionFrameError, match="asset_id"):
        ensure_cross_section_frame(
            pd.DataFrame(
                [{"trade_date": "2024-01-02", "asset_id": None, "x": 1.0}]
            )
        )
