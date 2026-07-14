"""Prepare market residual research panels for indicators and strategies.

This module lives on the backtest data-preparation boundary. It converts asset
and benchmark prices into aligned simple returns without querying DuckDB inside
indicator or strategy layers.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_asset_id,
    normalize_trade_date,
)
from qrp_atlas.indicators.stock.residual import (
    ASSET_RETURN,
    BENCHMARK_ID,
    BENCHMARK_RETURN,
    RESIDUAL_OUTPUT_COLUMNS,
    ResidualIndicatorError,
    calculate_market_residuals,
)


class ResidualDataError(ValueError):
    """Raised when residual market data cannot be prepared."""


@dataclass(frozen=True)
class ResidualPanelPreparation:
    """Aligned residual-ready panel and preparation diagnostics."""

    panel: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "panel": self.panel.to_dict(orient="list"),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


def _empty_panel() -> pd.DataFrame:
    columns = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
        "open",
        "high",
        "low",
    ]
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    for column in columns:
        if column == TRADE_DATE:
            continue
        if column in {ASSET_ID, TICKER, BENCHMARK_ID}:
            frame[column] = pd.Series(dtype=object)
        else:
            frame[column] = pd.Series(dtype="float64")
    return frame


def _require_price_frame(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ResidualDataError(f"{label} must be a pandas DataFrame")
    work = df.copy()
    if ASSET_ID not in work.columns and TICKER in work.columns:
        work[ASSET_ID] = work[TICKER]
    if TICKER not in work.columns and ASSET_ID in work.columns:
        work[TICKER] = work[ASSET_ID]
    required = [TRADE_DATE, ASSET_ID, CLOSE]
    missing = [column for column in required if column not in work.columns]
    if missing:
        raise ResidualDataError(f"{label} missing required columns: {missing}")
    if work.empty:
        return work

    try:
        work[TRADE_DATE] = [normalize_trade_date(value) for value in work[TRADE_DATE].tolist()]
        work[ASSET_ID] = [normalize_asset_id(value) for value in work[ASSET_ID].tolist()]
    except CrossSectionFrameError as exc:
        raise ResidualDataError(str(exc)) from exc

    work[TICKER] = work[TICKER].map(lambda value: str(value))
    work[CLOSE] = pd.to_numeric(work[CLOSE], errors="coerce")
    for column in ("open", "high", "low"):
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")

    if work.duplicated(subset=[ASSET_ID, TRADE_DATE], keep=False).any():
        raise ResidualDataError(
            f"{label} has duplicate (asset_id, trade_date) rows"
        )
    return work.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)


def _simple_returns(closes: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(closes, errors="coerce").astype("float64")
    prev = numeric.shift(1)
    returns = numeric / prev - 1.0
    valid = prev.gt(0) & numeric.map(lambda value: bool(pd.notna(value) and math.isfinite(float(value))))
    return returns.where(valid).astype("float64")


def prepare_market_residual_panel(
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    *,
    benchmark_id: str | None = None,
    window: int = 60,
    min_periods: int | None = None,
    z_window: int = 60,
    fit_intercept: bool = True,
    compute_residuals: bool = True,
) -> ResidualPanelPreparation:
    """Build an exact-date residual panel from asset and benchmark prices.

    Rules:
    - simple close-to-close returns by asset and by benchmark independently;
    - exact trade_date join only (no forward/backward fill of benchmark);
    - duplicate dates are rejected;
    - missing same-day benchmark return yields NaN residual inputs and diagnostics.
    """

    assets = _require_price_frame(asset_prices, label="asset_prices")
    benchmark = _require_price_frame(benchmark_prices, label="benchmark_prices")

    if assets.empty:
        return ResidualPanelPreparation(
            panel=_empty_panel(),
            diagnostics=(),
            metadata={
                "benchmark_id": benchmark_id,
                "asset_rows": 0,
                "benchmark_rows": int(len(benchmark)),
                "compute_residuals": compute_residuals,
            },
        )

    if benchmark.empty:
        raise ResidualDataError("benchmark_prices is empty")

    benchmark_ids = sorted(set(benchmark[ASSET_ID].astype(str)))
    if benchmark_id is None:
        if len(benchmark_ids) != 1:
            raise ResidualDataError(
                "benchmark_id is required when benchmark_prices contains multiple assets"
            )
        resolved_benchmark_id = benchmark_ids[0]
    else:
        resolved_benchmark_id = str(benchmark_id).strip()
        if not resolved_benchmark_id:
            raise ResidualDataError("benchmark_id must be a non-empty string")
        if resolved_benchmark_id not in benchmark_ids:
            # Allow a caller-provided identity label that still maps through one series.
            if len(benchmark_ids) != 1:
                raise ResidualDataError(
                    f"benchmark_id {resolved_benchmark_id!r} not found in benchmark_prices"
                )
            # Keep the single series, but label with caller identity.
            pass

    if len(benchmark_ids) == 1:
        bench = benchmark[benchmark[ASSET_ID] == benchmark_ids[0]].copy()
    else:
        bench = benchmark[benchmark[ASSET_ID] == resolved_benchmark_id].copy()
    if bench.empty:
        raise ResidualDataError("no benchmark rows available after identity resolution")
    if bench.duplicated(subset=[TRADE_DATE], keep=False).any():
        raise ResidualDataError("benchmark_prices has duplicate trade_date rows")

    bench = bench.sort_values(TRADE_DATE, kind="mergesort").reset_index(drop=True)
    bench[BENCHMARK_RETURN] = _simple_returns(bench[CLOSE])
    bench_map = {
        pd.Timestamp(date): (
            float(value) if pd.notna(value) and math.isfinite(float(value)) else math.nan
        )
        for date, value in zip(bench[TRADE_DATE].tolist(), bench[BENCHMARK_RETURN].tolist(), strict=True)
    }
    available_dates = set(bench_map)

    diagnostics: list[str] = []
    pieces: list[pd.DataFrame] = []
    for asset, group in assets.groupby(ASSET_ID, sort=False):
        piece = group.copy()
        piece[ASSET_RETURN] = _simple_returns(piece[CLOSE])
        piece[BENCHMARK_ID] = resolved_benchmark_id
        piece[BENCHMARK_RETURN] = [
            bench_map.get(pd.Timestamp(date), math.nan) for date in piece[TRADE_DATE].tolist()
        ]
        missing_mask = ~piece[TRADE_DATE].isin(available_dates)
        for date in piece.loc[missing_mask, TRADE_DATE].tolist():
            diagnostics.append(
                f"{asset}|{pd.Timestamp(date).strftime('%Y-%m-%d')}|MISSING_BENCHMARK"
            )
        # Same-day exact alignment only; leave NaN when benchmark return absent.
        pieces.append(piece)

    panel = pd.concat(pieces, ignore_index=True) if pieces else _empty_panel()
    panel = panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(drop=True)

    keep_cols = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
    ]
    for column in ("open", "high", "low"):
        if column in panel.columns:
            keep_cols.append(column)
    panel = panel.loc[:, keep_cols].copy()

    metadata: dict[str, Any] = {
        "benchmark_id": resolved_benchmark_id,
        "asset_count": int(panel[ASSET_ID].nunique()) if not panel.empty else 0,
        "row_count": int(len(panel)),
        "benchmark_date_count": int(len(bench)),
        "missing_benchmark_count": int(sum(1 for item in diagnostics if item.endswith("MISSING_BENCHMARK"))),
        "date_range": {
            "start": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].min()).strftime("%Y-%m-%d")
            ),
            "end": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].max()).strftime("%Y-%m-%d")
            ),
        },
        "window": window,
        "min_periods": min_periods if min_periods is not None else window,
        "z_window": z_window,
        "fit_intercept": fit_intercept,
        "compute_residuals": compute_residuals,
    }

    if compute_residuals:
        try:
            residual_result = calculate_market_residuals(
                panel,
                window=window,
                min_periods=min_periods,
                z_window=z_window,
                fit_intercept=fit_intercept,
            )
        except ResidualIndicatorError as exc:
            raise ResidualDataError(str(exc)) from exc
        residual_frame = residual_result.frame
        merge_cols = [
            TRADE_DATE,
            ASSET_ID,
            *RESIDUAL_OUTPUT_COLUMNS,
            "diagnostic_code",
        ]
        panel = panel.merge(
            residual_frame[merge_cols],
            on=[TRADE_DATE, ASSET_ID],
            how="left",
            sort=False,
        )
        diagnostics.extend(residual_result.diagnostics)
        metadata["residual_calculation"] = dict(residual_result.metadata)
        metadata["usable_residual_count"] = int(
            residual_frame["residual_return"].notna().sum()
        ) if not residual_frame.empty else 0

    return ResidualPanelPreparation(
        panel=panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(drop=True),
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


__all__ = [
    "ResidualDataError",
    "ResidualPanelPreparation",
    "prepare_market_residual_panel",
]
