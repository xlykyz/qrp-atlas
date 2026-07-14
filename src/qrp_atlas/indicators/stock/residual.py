"""Prior-only market residual indicators for relative-value research.

Compute rolling OLS of asset returns on a single market benchmark using only
history strictly before the evaluation date, then form sample-out residual
returns and residual z-scores. Indicators never access databases.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TICKER, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_asset_id,
    normalize_trade_date,
)

ROLLING_ALPHA = "rolling_alpha"
ROLLING_BETA = "rolling_beta"
ROLLING_R2 = "rolling_r2"
RESIDUAL_RETURN = "residual_return"
RESIDUAL_ZSCORE = "residual_zscore"
ASSET_RETURN = "asset_return"
BENCHMARK_RETURN = "benchmark_return"
BENCHMARK_ID = "benchmark_id"
DIAGNOSTIC_CODE = "diagnostic_code"

RESIDUAL_OUTPUT_COLUMNS: tuple[str, ...] = (
    ROLLING_ALPHA,
    ROLLING_BETA,
    ROLLING_R2,
    RESIDUAL_RETURN,
    RESIDUAL_ZSCORE,
)

CALCULATION_VERSION = "1.0.0"

REASON_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
REASON_ZERO_BENCHMARK_VARIANCE = "ZERO_BENCHMARK_VARIANCE"
REASON_RANK_DEFICIENT = "RANK_DEFICIENT"
REASON_MISSING_CURRENT_RETURN = "MISSING_CURRENT_RETURN"
REASON_MISSING_BENCHMARK = "MISSING_BENCHMARK"
REASON_NON_FINITE_INPUT = "NON_FINITE_INPUT"
REASON_OK = "OK"


class ResidualIndicatorError(ValueError):
    """Raised when residual indicator inputs or parameters are invalid."""


@dataclass(frozen=True)
class ResidualIndicatorResult:
    """Residual panel plus stable diagnostics and calculation metadata."""

    frame: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame": self.frame.to_dict(orient="list"),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


def _require_positive_int(name: str, value: Any, *, minimum: int = 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ResidualIndicatorError(f"{name} must be an integer >= {minimum}")
    return int(value)


def _is_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _empty_frame(include_benchmark_id: bool = True) -> pd.DataFrame:
    columns = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        ASSET_RETURN,
        BENCHMARK_RETURN,
        *RESIDUAL_OUTPUT_COLUMNS,
        DIAGNOSTIC_CODE,
    ]
    if include_benchmark_id:
        columns.insert(3, BENCHMARK_ID)
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    for column in columns:
        if column == TRADE_DATE:
            continue
        if column in {ASSET_ID, TICKER, BENCHMARK_ID, DIAGNOSTIC_CODE}:
            frame[column] = pd.Series(dtype=object)
        else:
            frame[column] = pd.Series(dtype="float64")
    return frame


def _resolve_identity_columns(df: pd.DataFrame) -> tuple[str, str]:
    if ASSET_ID in df.columns:
        asset_col = ASSET_ID
    elif TICKER in df.columns:
        asset_col = TICKER
    else:
        raise ResidualIndicatorError(
            "residual input must include asset_id or ticker"
        )
    if TRADE_DATE not in df.columns:
        raise ResidualIndicatorError("residual input must include trade_date")
    return asset_col, TRADE_DATE


def _normalize_panel(
    df: pd.DataFrame,
    *,
    required_value_columns: Sequence[str],
    allow_missing_benchmark_id: bool = True,
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ResidualIndicatorError("residual input must be a pandas DataFrame")
    asset_col, date_col = _resolve_identity_columns(df)
    missing = [column for column in required_value_columns if column not in df.columns]
    if missing:
        raise ResidualIndicatorError(
            f"residual input missing required columns: {missing}"
        )

    if df.empty:
        out = _empty_frame(include_benchmark_id=BENCHMARK_ID in df.columns)
        return out

    work = df.copy()
    try:
        work[date_col] = [normalize_trade_date(value) for value in work[date_col].tolist()]
        work[asset_col] = [normalize_asset_id(value) for value in work[asset_col].tolist()]
    except CrossSectionFrameError as exc:
        raise ResidualIndicatorError(str(exc)) from exc

    if work.duplicated(subset=[date_col, asset_col], keep=False).any():
        raise ResidualIndicatorError(
            "residual input has duplicate (trade_date, asset_id) rows"
        )

    for column in required_value_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    if BENCHMARK_ID in work.columns:
        work[BENCHMARK_ID] = work[BENCHMARK_ID].map(
            lambda value: None if pd.isna(value) else str(value)
        )
    elif not allow_missing_benchmark_id:
        raise ResidualIndicatorError("residual input missing benchmark_id")

    if asset_col != ASSET_ID:
        work[ASSET_ID] = work[asset_col].astype(str)
    if TICKER not in work.columns:
        work[TICKER] = work[ASSET_ID].astype(str)
    else:
        work[TICKER] = work[TICKER].map(lambda value: str(value))

    work = work.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    return work


def _fit_ols(
    y: np.ndarray,
    x: np.ndarray,
    *,
    fit_intercept: bool,
) -> tuple[float, float, float, str | None]:
    """Fit prior-only OLS; returns alpha, beta, r2, failure_reason."""

    if y.size == 0:
        return math.nan, math.nan, math.nan, REASON_INSUFFICIENT_HISTORY
    if not np.all(np.isfinite(y)) or not np.all(np.isfinite(x)):
        return math.nan, math.nan, math.nan, REASON_NON_FINITE_INPUT

    if fit_intercept:
        design = np.column_stack([np.ones(len(x), dtype=float), x.astype(float)])
        expected_rank = 2
    else:
        design = x.astype(float).reshape(-1, 1)
        expected_rank = 1

    rank = int(np.linalg.matrix_rank(design, tol=1e-12))
    if rank < expected_rank:
        # Zero variance benchmark collapses the design matrix when intercept is fit.
        if float(np.var(x)) <= 1e-18:
            return math.nan, math.nan, math.nan, REASON_ZERO_BENCHMARK_VARIANCE
        return math.nan, math.nan, math.nan, REASON_RANK_DEFICIENT

    try:
        coeffs, _, rank_out, _ = np.linalg.lstsq(design, y.astype(float), rcond=None)
    except np.linalg.LinAlgError:
        return math.nan, math.nan, math.nan, REASON_RANK_DEFICIENT
    if int(rank_out) < expected_rank:
        return math.nan, math.nan, math.nan, REASON_RANK_DEFICIENT

    if fit_intercept:
        alpha = float(coeffs[0])
        beta = float(coeffs[1])
        fitted = alpha + beta * x
    else:
        alpha = 0.0
        beta = float(coeffs[0])
        fitted = beta * x

    resid = y - fitted
    ss_res = float(np.sum(resid * resid))
    y_mean = float(np.mean(y))
    ss_tot = float(np.sum((y - y_mean) ** 2))
    if ss_tot <= 1e-18:
        r2 = math.nan if ss_res > 1e-18 else 1.0
    else:
        r2 = 1.0 - ss_res / ss_tot
    if not math.isfinite(alpha) or not math.isfinite(beta) or not math.isfinite(r2):
        return math.nan, math.nan, math.nan, REASON_RANK_DEFICIENT
    return alpha, beta, float(r2), None


def _prior_residual_zscore(
    residual_history: Sequence[float],
    current_residual: float,
    *,
    z_window: int,
    min_periods: int,
) -> float:
    if not math.isfinite(current_residual):
        return math.nan
    history = [
        float(value)
        for value in residual_history[-z_window:]
        if math.isfinite(float(value))
    ]
    if len(history) < min_periods:
        return math.nan
    arr = np.asarray(history, dtype=float)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=0))
    if std <= 1e-18:
        return math.nan
    return (current_residual - mean) / std


def calculate_market_residuals(
    panel: pd.DataFrame,
    *,
    window: int = 60,
    min_periods: int | None = None,
    z_window: int = 60,
    fit_intercept: bool = True,
    asset_return_col: str = ASSET_RETURN,
    benchmark_return_col: str = BENCHMARK_RETURN,
) -> ResidualIndicatorResult:
    """Compute prior-only rolling residual metrics for one or more assets.

    For evaluation date ``T``:

    ```text
    fit on returns in [T-window, ..., T-1]
    residual_return[T] = asset_return[T] - (alpha[T] + beta[T] * benchmark_return[T])
    residual_zscore[T] uses residual history in [T-z_window, ..., T-1] only
    ```

    The current date is never included in the OLS sample or z-score baseline.
    """

    window = _require_positive_int("window", window, minimum=2)
    z_window = _require_positive_int("z_window", z_window, minimum=2)
    if min_periods is None:
        min_periods = window
    min_periods = _require_positive_int("min_periods", min_periods, minimum=2)
    if min_periods > window:
        raise ResidualIndicatorError("min_periods cannot exceed window")
    if not isinstance(fit_intercept, bool):
        raise ResidualIndicatorError("fit_intercept must be a boolean")

    original = panel
    work = _normalize_panel(
        panel,
        required_value_columns=(asset_return_col, benchmark_return_col),
    )
    # Caller immutability: only copies are modified above.
    if original is work:  # pragma: no cover - defensive
        work = work.copy()

    if work.empty:
        empty = _empty_frame(include_benchmark_id=BENCHMARK_ID in panel.columns)
        return ResidualIndicatorResult(
            frame=empty,
            diagnostics=(),
            metadata={
                "calculation_version": CALCULATION_VERSION,
                "window": window,
                "min_periods": min_periods,
                "z_window": z_window,
                "fit_intercept": fit_intercept,
            },
        )

    diagnostics: list[str] = []
    rows: list[dict[str, Any]] = []

    for asset_id, group in work.groupby(ASSET_ID, sort=False):
        asset_returns = group[asset_return_col].to_numpy(dtype=float)
        bench_returns = group[benchmark_return_col].to_numpy(dtype=float)
        dates = group[TRADE_DATE].tolist()
        tickers = group[TICKER].tolist()
        benchmark_ids = (
            group[BENCHMARK_ID].tolist()
            if BENCHMARK_ID in group.columns
            else [None] * len(group)
        )
        residual_history: list[float] = []

        for idx in range(len(group)):
            start = max(0, idx - window)
            hist_slice = slice(start, idx)  # excludes current idx
            y = asset_returns[hist_slice]
            x = bench_returns[hist_slice]
            finite_mask = np.isfinite(y) & np.isfinite(x)
            y_fit = y[finite_mask]
            x_fit = x[finite_mask]

            alpha = math.nan
            beta = math.nan
            r2 = math.nan
            residual = math.nan
            zscore = math.nan
            reason = REASON_OK

            if y_fit.size < min_periods:
                reason = REASON_INSUFFICIENT_HISTORY
            else:
                alpha, beta, r2, fit_reason = _fit_ols(
                    y_fit, x_fit, fit_intercept=fit_intercept
                )
                if fit_reason is not None:
                    reason = fit_reason
                else:
                    asset_t = asset_returns[idx]
                    bench_t = bench_returns[idx]
                    if not math.isfinite(asset_t) or not math.isfinite(bench_t):
                        if not math.isfinite(bench_t) and math.isfinite(asset_t):
                            reason = REASON_MISSING_BENCHMARK
                        else:
                            reason = REASON_MISSING_CURRENT_RETURN
                    else:
                        residual = float(asset_t - (alpha + beta * bench_t))
                        zscore = _prior_residual_zscore(
                            residual_history,
                            residual,
                            z_window=z_window,
                            min_periods=min(min_periods, z_window),
                        )
                        if not math.isfinite(zscore) and math.isfinite(residual):
                            # Keep residual even when z-score warmup is incomplete.
                            reason = REASON_OK

            residual_history.append(residual if math.isfinite(residual) else math.nan)

            if reason != REASON_OK:
                diagnostics.append(
                    f"{asset_id}|{pd.Timestamp(dates[idx]).strftime('%Y-%m-%d')}|{reason}"
                )

            rows.append(
                {
                    TRADE_DATE: dates[idx],
                    ASSET_ID: str(asset_id),
                    TICKER: str(tickers[idx]),
                    BENCHMARK_ID: benchmark_ids[idx],
                    ASSET_RETURN: (
                        float(asset_returns[idx])
                        if math.isfinite(float(asset_returns[idx]))
                        else math.nan
                    ),
                    BENCHMARK_RETURN: (
                        float(bench_returns[idx])
                        if math.isfinite(float(bench_returns[idx]))
                        else math.nan
                    ),
                    ROLLING_ALPHA: alpha,
                    ROLLING_BETA: beta,
                    ROLLING_R2: r2,
                    RESIDUAL_RETURN: residual,
                    RESIDUAL_ZSCORE: zscore,
                    DIAGNOSTIC_CODE: reason,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = _empty_frame()
    else:
        frame = frame.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(
            drop=True
        )

    metadata = {
        "calculation_version": CALCULATION_VERSION,
        "window": window,
        "min_periods": min_periods,
        "z_window": z_window,
        "fit_intercept": fit_intercept,
        "output_fields": list(RESIDUAL_OUTPUT_COLUMNS),
        "available_as_of": "after_close",
        "earliest_execution": "next_trade_date",
    }
    return ResidualIndicatorResult(
        frame=frame,
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


def market_residual_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Parameterized-indicator adapter over :func:`calculate_market_residuals`."""

    result = calculate_market_residuals(
        df,
        window=int(parameters["window"]),
        min_periods=int(parameters["min_periods"]),
        z_window=int(parameters["z_window"]),
        fit_intercept=bool(parameters["fit_intercept"]),
    )
    frame = result.frame
    if df.empty or frame.empty:
        empty = pd.Series(dtype="float64", index=df.index)
        return {name: empty.copy() for name in RESIDUAL_OUTPUT_COLUMNS}

    asset_col = ASSET_ID if ASSET_ID in df.columns else TICKER
    left_assets = df[asset_col].astype(str).tolist()
    left_dates = [
        pd.Timestamp(value).normalize()
        for value in pd.to_datetime(df[TRADE_DATE], errors="coerce")
    ]
    lookup = {
        (str(asset), pd.Timestamp(date).normalize()): idx
        for idx, asset, date in zip(
            range(len(frame)),
            frame[ASSET_ID].astype(str).tolist(),
            frame[TRADE_DATE].tolist(),
            strict=True,
        )
    }
    positions = [
        lookup.get((asset, date))
        for asset, date in zip(left_assets, left_dates, strict=True)
    ]
    outputs: dict[str, pd.Series] = {}
    for name in RESIDUAL_OUTPUT_COLUMNS:
        values: list[float] = []
        series = frame[name].tolist()
        for pos in positions:
            if pos is None:
                values.append(math.nan)
            else:
                raw = series[pos]
                values.append(
                    float(raw)
                    if pd.notna(raw) and math.isfinite(float(raw))
                    else math.nan
                )
        outputs[name] = pd.Series(values, index=df.index, dtype="float64")
    return outputs


__all__ = [
    "ASSET_RETURN",
    "BENCHMARK_ID",
    "BENCHMARK_RETURN",
    "CALCULATION_VERSION",
    "DIAGNOSTIC_CODE",
    "REASON_INSUFFICIENT_HISTORY",
    "REASON_MISSING_BENCHMARK",
    "REASON_MISSING_CURRENT_RETURN",
    "REASON_NON_FINITE_INPUT",
    "REASON_OK",
    "REASON_RANK_DEFICIENT",
    "REASON_ZERO_BENCHMARK_VARIANCE",
    "RESIDUAL_OUTPUT_COLUMNS",
    "RESIDUAL_RETURN",
    "RESIDUAL_ZSCORE",
    "ROLLING_ALPHA",
    "ROLLING_BETA",
    "ROLLING_R2",
    "ResidualIndicatorError",
    "ResidualIndicatorResult",
    "calculate_market_residuals",
    "market_residual_calculator",
]
