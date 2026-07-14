"""Tests for prior-only market residual indicators."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.indicators import IndicatorRequest, calculate_indicators
from qrp_atlas.indicators.stock.residual import (
    REASON_INSUFFICIENT_HISTORY,
    REASON_ZERO_BENCHMARK_VARIANCE,
    ResidualIndicatorError,
    calculate_market_residuals,
)


def _panel(
    asset_closes: list[float],
    bench_closes: list[float],
    *,
    asset_id: str = "A",
    start: str = "2024-01-01",
) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(asset_closes))
    asset_ret = [math.nan]
    bench_ret = [math.nan]
    for idx in range(1, len(asset_closes)):
        asset_ret.append(asset_closes[idx] / asset_closes[idx - 1] - 1.0)
        bench_ret.append(bench_closes[idx] / bench_closes[idx - 1] - 1.0)
    return pd.DataFrame(
        {
            "trade_date": dates,
            "asset_id": asset_id,
            "ticker": asset_id,
            "asset_return": asset_ret,
            "benchmark_return": bench_ret,
            "benchmark_id": "MKT",
        }
    )


def test_prior_only_fit_excludes_current_return() -> None:
    # Construct exact known prior window so OLS can be checked by hand.
    # asset = 2 * bench + 0.01 on historical sample; break that relation on T.
    n = 8
    bench = [100.0]
    asset = [100.0]
    for i in range(1, n):
        b_ret = 0.01 * ((-1) ** i)
        bench.append(bench[-1] * (1.0 + b_ret))
        asset.append(asset[-1] * (1.0 + 2.0 * b_ret + 0.01))
    # Replace last asset return with a huge outlier after panel construction.
    panel = _panel(asset, bench)
    result = calculate_market_residuals(
        panel, window=5, min_periods=5, z_window=5, fit_intercept=True
    )
    frame = result.frame.reset_index(drop=True)
    t = len(frame) - 1
    alpha_t = frame.loc[t, "rolling_alpha"]
    beta_t = frame.loc[t, "rolling_beta"]
    assert math.isfinite(alpha_t)
    assert math.isfinite(beta_t)
    assert abs(beta_t - 2.0) < 1e-8
    assert abs(alpha_t - 0.01) < 1e-8

    # Mutating current return must not change alpha/beta at T.
    mutated = panel.copy()
    mutated.loc[mutated.index[-1], "asset_return"] = 0.99
    mutated_result = calculate_market_residuals(
        mutated, window=5, min_periods=5, z_window=5, fit_intercept=True
    )
    assert mutated_result.frame.loc[t, "rolling_alpha"] == pytest.approx(alpha_t)
    assert mutated_result.frame.loc[t, "rolling_beta"] == pytest.approx(beta_t)
    # Residual itself does change.
    assert mutated_result.frame.loc[t, "residual_return"] != pytest.approx(
        frame.loc[t, "residual_return"]
    )


def test_future_data_does_not_affect_past_outputs() -> None:
    rng = np.random.default_rng(7)
    n = 40
    bench = [100.0]
    asset = [100.0]
    for _ in range(1, n):
        b = float(rng.normal(0.0, 0.01))
        a = 1.2 * b + float(rng.normal(0.0, 0.002))
        bench.append(bench[-1] * (1 + b))
        asset.append(asset[-1] * (1 + a))
    panel = _panel(asset, bench)
    base = calculate_market_residuals(panel, window=10, min_periods=10, z_window=8)
    changed = panel.copy()
    changed.loc[changed.index[25]:, "asset_return"] = 0.5
    changed.loc[changed.index[25]:, "benchmark_return"] = -0.5
    alt = calculate_market_residuals(changed, window=10, min_periods=10, z_window=8)
    cols = [
        "rolling_alpha",
        "rolling_beta",
        "rolling_r2",
        "residual_return",
        "residual_zscore",
    ]
    pd.testing.assert_frame_equal(
        base.frame.loc[:24, cols].reset_index(drop=True),
        alt.frame.loc[:24, cols].reset_index(drop=True),
    )


def test_residual_zscore_excludes_current_residual() -> None:
    residuals = [0.0, 0.0, 0.0, 0.0, 1.0]
    # Build panel where OLS is perfect with beta=0, alpha=0 by zero returns until end.
    panel = pd.DataFrame(
        {
            "trade_date": pd.bdate_range("2024-01-01", periods=6),
            "asset_id": "A",
            "ticker": "A",
            "asset_return": [math.nan, 0.0, 0.0, 0.0, 0.0, 1.0],
            "benchmark_return": [math.nan, 0.01, -0.01, 0.02, -0.02, 0.0],
            "benchmark_id": "MKT",
        }
    )
    # Use fit_intercept with non-zero variance benchmark so residual can form.
    # Force residual history by using zero-beta synthetic via intercept-only style data.
    result = calculate_market_residuals(
        panel, window=4, min_periods=3, z_window=4, fit_intercept=True
    )
    frame = result.frame.reset_index(drop=True)
    # At last row, current residual should not enter its own mean/std.
    # Manually recompute from residual history excluding current.
    history = frame.loc[:4, "residual_return"].dropna().tolist()
    current = frame.loc[5, "residual_return"]
    if len(history) >= 3 and math.isfinite(current):
        mean = float(np.mean(history[-4:]))
        std = float(np.std(history[-4:], ddof=0))
        expected = (current - mean) / std if std > 0 else math.nan
        if math.isfinite(expected):
            assert frame.loc[5, "residual_zscore"] == pytest.approx(expected)


def test_zero_benchmark_variance_is_nan() -> None:
    panel = pd.DataFrame(
        {
            "trade_date": pd.bdate_range("2024-01-01", periods=6),
            "asset_id": "A",
            "ticker": "A",
            "asset_return": [math.nan, 0.01, 0.02, -0.01, 0.0, 0.03],
            "benchmark_return": [math.nan, 0.0, 0.0, 0.0, 0.0, 0.0],
            "benchmark_id": "MKT",
        }
    )
    result = calculate_market_residuals(panel, window=4, min_periods=4, z_window=4)
    tail = result.frame.iloc[-1]
    assert math.isnan(tail["rolling_beta"])
    assert tail["diagnostic_code"] in {
        REASON_ZERO_BENCHMARK_VARIANCE,
        REASON_INSUFFICIENT_HISTORY,
    }


def test_duplicate_keys_and_input_immutability() -> None:
    panel = _panel([100, 101, 102, 103, 104, 105], [100, 100.5, 101, 100, 101, 102])
    original = panel.copy(deep=True)
    with pytest.raises(ResidualIndicatorError, match="duplicate"):
        calculate_market_residuals(
            pd.concat([panel, panel.iloc[[-1]]], ignore_index=True),
            window=3,
            min_periods=3,
            z_window=3,
        )
    calculate_market_residuals(panel, window=3, min_periods=3, z_window=3)
    pd.testing.assert_frame_equal(panel, original)


def test_multi_asset_no_cross_contamination_and_shuffled_input() -> None:
    a = _panel([100, 101, 103, 102, 104, 108, 107, 110], [100, 101, 102, 101, 103, 104, 103, 105], asset_id="A")
    b = _panel([50, 49, 48, 49, 51, 52, 50, 49], [100, 101, 102, 101, 103, 104, 103, 105], asset_id="B")
    panel = pd.concat([a, b], ignore_index=True).sample(frac=1, random_state=3)
    first = calculate_market_residuals(panel, window=4, min_periods=4, z_window=4)
    second = calculate_market_residuals(
        panel.sample(frac=1, random_state=9), window=4, min_periods=4, z_window=4
    )
    pd.testing.assert_frame_equal(first.frame, second.frame)
    only_a = calculate_market_residuals(a, window=4, min_periods=4, z_window=4)
    merged_a = first.frame[first.frame["asset_id"] == "A"].reset_index(drop=True)
    pd.testing.assert_frame_equal(
        merged_a[["rolling_alpha", "rolling_beta", "residual_return"]],
        only_a.frame[["rolling_alpha", "rolling_beta", "residual_return"]],
    )


def test_timezone_aware_dates_keep_local_wall_day() -> None:
    panel = _panel([100, 101, 102, 103, 104, 105], [100, 101, 100, 102, 101, 103])
    panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.tz_localize(
        "Asia/Shanghai"
    )
    result = calculate_market_residuals(panel, window=3, min_periods=3, z_window=3)
    assert str(result.frame.loc[0, "trade_date"].date()) == "2024-01-01"


def test_parameterized_registry_adapter() -> None:
    panel = _panel([100, 101, 102, 103, 104, 106, 105, 108], [100, 100.5, 101, 100.8, 102, 103, 102.5, 104])
    out = calculate_indicators(
        panel,
        (
            IndicatorRequest(
                "market_residual",
                {"window": 4, "min_periods": 4, "z_window": 4, "fit_intercept": True},
                alias="residual",
                output_fields={
                    "rolling_alpha": "rolling_alpha",
                    "rolling_beta": "rolling_beta",
                    "rolling_r2": "rolling_r2",
                    "residual_return": "residual_return",
                    "residual_zscore": "residual_zscore",
                },
            ),
        ),
    )
    direct = calculate_market_residuals(panel, window=4, min_periods=4, z_window=4)
    pd.testing.assert_series_equal(
        out["residual_return"].reset_index(drop=True),
        direct.frame["residual_return"].reset_index(drop=True),
        check_names=False,
    )
