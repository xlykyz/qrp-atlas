"""Information coefficient analytics for cross-sectional factors."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_feature_columns,
    normalize_trade_date,
    sort_cross_section_frame,
)

from .forward_returns import forward_return_column

IC_METHODS = ("pearson", "spearman")
DAILY_IC_COLUMNS = (
    TRADE_DATE,
    "factor",
    "horizon",
    "method",
    "ic",
    "n_obs",
)
SUMMARY_IC_COLUMNS = (
    "factor",
    "horizon",
    "method",
    "n_dates",
    "mean_ic",
    "std_ic",
    "ic_ir",
    "t_stat",
    "positive_rate",
)


class InformationCoefficientError(ValueError):
    """Raised when IC analytics cannot proceed."""


@dataclass(frozen=True)
class ICSummaryResult:
    """Daily IC series plus cross-date summary statistics."""

    daily_ic: pd.DataFrame
    summary: pd.DataFrame


def compute_information_coefficient(
    factor_frame: pd.DataFrame,
    forward_returns: pd.DataFrame,
    *,
    factor_columns: str | Sequence[str],
    horizons: Sequence[int] = (1, 5, 20),
    methods: Sequence[str] = IC_METHODS,
    min_obs: int = 3,
) -> pd.DataFrame:
    """Compute daily Pearson and Spearman IC by date, factor and horizon."""
    factors = normalize_feature_columns(factor_columns)
    if not factors:
        raise InformationCoefficientError("factor_columns must be non-empty")
    if not isinstance(min_obs, int) or isinstance(min_obs, bool) or min_obs < 2:
        raise InformationCoefficientError("min_obs must be an integer >= 2")
    method_list = _normalize_methods(methods)
    horizon_list = _normalize_horizons(horizons)

    try:
        factors_df = ensure_cross_section_frame(
            factor_frame,
            feature_columns=factors,
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise InformationCoefficientError(str(exc)) from exc

    return_cols = [forward_return_column(h) for h in horizon_list]
    try:
        returns_df = ensure_cross_section_frame(
            forward_returns,
            feature_columns=return_cols,
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise InformationCoefficientError(str(exc)) from exc

    if factors_df.empty or returns_df.empty:
        return _empty_daily_ic()

    merged = factors_df.merge(
        returns_df[[TRADE_DATE, ASSET_ID, *return_cols]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
        sort=False,
    )
    rows: list[dict[str, Any]] = []
    for trade_date, day in merged.groupby(TRADE_DATE, sort=True):
        for factor in factors:
            factor_values = pd.to_numeric(day[factor], errors="coerce")
            for horizon in horizon_list:
                ret_col = forward_return_column(horizon)
                ret_values = pd.to_numeric(day[ret_col], errors="coerce")
                mask = factor_values.map(_is_finite) & ret_values.map(_is_finite)
                x = factor_values[mask].astype(float)
                y = ret_values[mask].astype(float)
                n_obs = int(len(x))
                for method in method_list:
                    ic_value = math.nan
                    if n_obs >= min_obs:
                        ic_value = _corr(x, y, method=method)
                    rows.append(
                        {
                            TRADE_DATE: normalize_trade_date(trade_date),
                            "factor": factor,
                            "horizon": int(horizon),
                            "method": method,
                            "ic": ic_value,
                            "n_obs": n_obs,
                        }
                    )
    if not rows:
        return _empty_daily_ic()
    out = pd.DataFrame(rows, columns=list(DAILY_IC_COLUMNS))
    return out.sort_values(
        [TRADE_DATE, "factor", "horizon", "method"],
        kind="mergesort",
    ).reset_index(drop=True)


def summarize_information_coefficient(
    daily_ic: pd.DataFrame,
    *,
    min_dates: int = 1,
) -> pd.DataFrame:
    """Summarize daily IC series into mean, std, IR, t-stat and positive rate."""
    if daily_ic is None or not isinstance(daily_ic, pd.DataFrame):
        raise InformationCoefficientError("daily_ic must be a pandas DataFrame")
    if daily_ic.empty:
        return _empty_summary_ic()
    required = list(DAILY_IC_COLUMNS)
    missing = [column for column in required if column not in daily_ic.columns]
    if missing:
        raise InformationCoefficientError(
            f"daily_ic missing required columns: {missing}"
        )
    if not isinstance(min_dates, int) or isinstance(min_dates, bool) or min_dates < 1:
        raise InformationCoefficientError("min_dates must be a positive integer")

    rows: list[dict[str, Any]] = []
    grouped = daily_ic.groupby(["factor", "horizon", "method"], sort=True)
    for (factor, horizon, method), group in grouped:
        values = pd.to_numeric(group["ic"], errors="coerce")
        finite = values[values.map(_is_finite)].astype(float)
        n_dates = int(len(finite))
        if n_dates < min_dates or n_dates == 0:
            mean_ic = math.nan
            std_ic = math.nan
            ic_ir = math.nan
            t_stat = math.nan
            positive_rate = math.nan
        else:
            mean_ic = float(finite.mean())
            std_ic = float(finite.std(ddof=1)) if n_dates >= 2 else math.nan
            if n_dates >= 2 and std_ic > 0 and math.isfinite(std_ic):
                ic_ir = mean_ic / std_ic
                t_stat = mean_ic / (std_ic / math.sqrt(n_dates))
            else:
                ic_ir = math.nan
                t_stat = math.nan
            positive_rate = float((finite > 0).mean())
        rows.append(
            {
                "factor": factor,
                "horizon": int(horizon),
                "method": method,
                "n_dates": n_dates,
                "mean_ic": mean_ic,
                "std_ic": std_ic,
                "ic_ir": ic_ir,
                "t_stat": t_stat,
                "positive_rate": positive_rate,
            }
        )
    out = pd.DataFrame(rows, columns=list(SUMMARY_IC_COLUMNS))
    return out.sort_values(
        ["factor", "horizon", "method"], kind="mergesort"
    ).reset_index(drop=True)


def _corr(x: pd.Series, y: pd.Series, *, method: str) -> float:
    if method == "pearson":
        if float(x.std(ddof=0)) == 0.0 or float(y.std(ddof=0)) == 0.0:
            return math.nan
        value = float(x.corr(y, method="pearson"))
        return value if math.isfinite(value) else math.nan
    if method == "spearman":
        # Rank ties with average ranks; constant ranks => NaN.
        rx = x.rank(method="average")
        ry = y.rank(method="average")
        if float(rx.std(ddof=0)) == 0.0 or float(ry.std(ddof=0)) == 0.0:
            return math.nan
        value = float(rx.corr(ry, method="pearson"))
        return value if math.isfinite(value) else math.nan
    raise InformationCoefficientError(f"unsupported IC method: {method!r}")


def _normalize_methods(methods: Sequence[str]) -> list[str]:
    if not methods:
        raise InformationCoefficientError("methods must be non-empty")
    ordered: list[str] = []
    seen: set[str] = set()
    for method in methods:
        name = str(method).strip().lower()
        if name not in IC_METHODS:
            raise InformationCoefficientError(
                f"unsupported IC method: {method!r}; expected {list(IC_METHODS)}"
            )
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _normalize_horizons(horizons: Sequence[int]) -> list[int]:
    if not horizons:
        raise InformationCoefficientError("horizons must be non-empty")
    ordered: list[int] = []
    seen: set[int] = set()
    for value in horizons:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise InformationCoefficientError("horizon must be a positive integer")
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _is_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _empty_daily_ic() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(DAILY_IC_COLUMNS))
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame["factor"] = pd.Series(dtype=object)
    frame["horizon"] = pd.Series(dtype=int)
    frame["method"] = pd.Series(dtype=object)
    frame["ic"] = pd.Series(dtype=float)
    frame["n_obs"] = pd.Series(dtype=int)
    return frame


def _empty_summary_ic() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(SUMMARY_IC_COLUMNS))
    frame["factor"] = pd.Series(dtype=object)
    frame["horizon"] = pd.Series(dtype=int)
    frame["method"] = pd.Series(dtype=object)
    frame["n_dates"] = pd.Series(dtype=int)
    for column in ("mean_ic", "std_ic", "ic_ir", "t_stat", "positive_rate"):
        frame[column] = pd.Series(dtype=float)
    return frame
