"""Standard product analytics for portfolio result packages.

Pure helpers used by BacktestRunWriter and result APIs. No strategy knowledge.
All outputs must be JSON-safe (no NaN/Inf).
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def json_safe(value: Any) -> Any:
    """Recursively convert NaN/Inf to None for JSON dumps."""

    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    try:
        # numpy scalars
        if hasattr(value, "item"):
            return json_safe(value.item())
    except Exception:  # noqa: BLE001
        pass
    return value


def daily_returns_from_equity(equity_curve: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Compute simple daily returns from normalized equity curve points."""

    rows: list[dict[str, Any]] = []
    prev: float | None = None
    for point in equity_curve:
        date = str(point.get("date") or "")
        equity = _finite(point.get("equity"))
        if equity is None:
            rows.append({"date": date, "daily_return": None, "equity": None})
            prev = None
            continue
        if prev is None or prev == 0:
            daily = 0.0 if prev is not None else 0.0
        else:
            daily = equity / prev - 1.0
        rows.append({"date": date, "daily_return": _finite(daily), "equity": equity})
        prev = equity
    return rows


def sharpe_ratio(
    daily_returns: Sequence[float | None],
    *,
    risk_free_daily: float = 0.0,
    periods_per_year: int = 252,
) -> float | None:
    series = [_finite(v) for v in daily_returns]
    series = [v for v in series if v is not None]
    if len(series) < 2:
        return None
    excess = [v - risk_free_daily for v in series]
    mean = sum(excess) / len(excess)
    var = sum((v - mean) ** 2 for v in excess) / (len(excess) - 1)
    if var <= 0:
        return None
    std = math.sqrt(var)
    if std == 0:
        return None
    return mean / std * math.sqrt(periods_per_year)


def sortino_ratio(
    daily_returns: Sequence[float | None],
    *,
    risk_free_daily: float = 0.0,
    periods_per_year: int = 252,
) -> float | None:
    series = [_finite(v) for v in daily_returns]
    series = [v for v in series if v is not None]
    if len(series) < 2:
        return None
    excess = [v - risk_free_daily for v in series]
    mean = sum(excess) / len(excess)
    downside = [min(0.0, v) for v in excess]
    downside_var = sum(v * v for v in downside) / (len(downside) - 1)
    if downside_var <= 0:
        return None
    downside_std = math.sqrt(downside_var)
    if downside_std == 0:
        return None
    return mean / downside_std * math.sqrt(periods_per_year)


def calmar_ratio(
    annual_return_pct: float | None,
    max_drawdown_pct: float | None,
) -> float | None:
    ann = _finite(annual_return_pct)
    dd = _finite(max_drawdown_pct)
    if ann is None or dd is None:
        return None
    # max_drawdown_pct is typically negative or positive magnitude depending on source.
    magnitude = abs(dd)
    if magnitude == 0:
        return None
    return ann / magnitude


def rolling_performance(
    equity_curve: Sequence[Mapping[str, Any]],
    *,
    windows: Sequence[int] = (20, 60),
    periods_per_year: int = 252,
) -> list[dict[str, Any]]:
    """Rolling return/vol/sharpe/drawdown on equity curve.

    Uses simple equity ratios; no forward/backward fill across missing days.
    """

    points = [
        (str(p.get("date") or ""), _finite(p.get("equity")))
        for p in equity_curve
    ]
    points = [(d, e) for d, e in points if d and e is not None and e > 0]
    if not points:
        return []

    equities = [e for _, e in points]
    dates = [d for d, _ in points]
    daily = [0.0]
    for i in range(1, len(equities)):
        prev = equities[i - 1]
        daily.append(equities[i] / prev - 1.0 if prev else 0.0)

    out: list[dict[str, Any]] = []
    for i, date in enumerate(dates):
        row: dict[str, Any] = {"date": date}
        peak = equities[0]
        max_dd = 0.0
        for j in range(0, i + 1):
            peak = max(peak, equities[j])
            if peak > 0:
                max_dd = min(max_dd, equities[j] / peak - 1.0)
        row["drawdown"] = _finite(max_dd)
        for window in windows:
            key = f"w{int(window)}"
            if i + 1 < window:
                row[f"return_{key}"] = None
                row[f"volatility_{key}"] = None
                row[f"sharpe_{key}"] = None
                row[f"drawdown_{key}"] = None
                continue
            start = i + 1 - window
            window_eq = equities[start : i + 1]
            window_ret = daily[start + 1 : i + 1] if i > start else []
            ret = window_eq[-1] / window_eq[0] - 1.0 if window_eq[0] else None
            if window_ret:
                mean = sum(window_ret) / len(window_ret)
                var = sum((v - mean) ** 2 for v in window_ret) / max(len(window_ret) - 1, 1)
                vol = math.sqrt(var) * math.sqrt(periods_per_year) if var > 0 else 0.0
                sharpe = (
                    (mean / math.sqrt(var) * math.sqrt(periods_per_year))
                    if var > 0
                    else None
                )
            else:
                vol = None
                sharpe = None
            peak = window_eq[0]
            dd = 0.0
            for eq in window_eq:
                peak = max(peak, eq)
                if peak > 0:
                    dd = min(dd, eq / peak - 1.0)
            row[f"return_{key}"] = _finite(ret)
            row[f"volatility_{key}"] = _finite(vol)
            row[f"sharpe_{key}"] = _finite(sharpe)
            row[f"drawdown_{key}"] = _finite(dd)
        out.append(row)
    return out


def align_benchmark_series(
    portfolio_dates: Sequence[str],
    benchmark: pd.DataFrame,
    *,
    date_col: str = "trade_date",
    value_col: str = "close",
) -> tuple[list[dict[str, Any]], list[str]]:
    """Align benchmark to portfolio dates without fill across gaps.

    Returns (aligned points, diagnostics). Missing benchmark dates become None
    values with an explicit diagnostic; no silent replacement.
    """

    diagnostics: list[str] = []
    if benchmark is None or benchmark.empty:
        diagnostics.append("benchmark_missing")
        return (
            [{"date": d, "benchmark_return": None, "excess_return": None} for d in portfolio_dates],
            diagnostics,
        )

    work = benchmark.copy()
    work[date_col] = pd.to_datetime(work[date_col]).dt.strftime("%Y-%m-%d")
    work = work.sort_values(date_col)
    # index levels normalized to first available date in intersection
    values = {
        str(r[date_col]): _finite(r[value_col])
        for r in work[[date_col, value_col]].to_dict(orient="records")
    }
    aligned: list[dict[str, Any]] = []
    base: float | None = None
    prev_port_ret: float | None = None
    # portfolio returns computed outside; here only benchmark return series level
    for date in portfolio_dates:
        level = values.get(date)
        if level is None:
            diagnostics.append(f"benchmark_gap:{date}")
            aligned.append(
                {
                    "date": date,
                    "benchmark_level": None,
                    "benchmark_return": None,
                }
            )
            continue
        if base is None:
            base = level
            b_ret = 0.0
        else:
            b_ret = level / base - 1.0
        aligned.append(
            {
                "date": date,
                "benchmark_level": level,
                "benchmark_return": _finite(b_ret),
            }
        )
    if any(item["benchmark_level"] is None for item in aligned):
        diagnostics.append("benchmark_has_gaps_no_fill")
    return aligned, diagnostics


def enrich_trades_mae_mfe(
    trades: list[dict[str, Any]],
    prices: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    """Fill MAE/MFE from daily OHLC over [entry_date, exit_date].

    Requires columns: asset_id/ticker, trade_date, low, high, close/open.
    If prices are unavailable, leaves MAE/MFE as None.
    """

    if not trades:
        return trades
    if prices is None or prices.empty:
        return trades

    frame = prices.copy()
    if "asset_id" not in frame.columns and "ticker" in frame.columns:
        frame["asset_id"] = frame["ticker"]
    if "trade_date" not in frame.columns:
        return trades
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    frame["asset_id"] = frame["asset_id"].astype(str)
    grouped = {
        asset: part.sort_values("trade_date")
        for asset, part in frame.groupby("asset_id", sort=False)
    }

    out: list[dict[str, Any]] = []
    for trade in trades:
        item = dict(trade)
        if item.get("status") != "closed":
            out.append(item)
            continue
        asset = str(item.get("asset_id") or "")
        entry = str(item.get("entry_date") or "")
        exit_d = str(item.get("exit_date") or "")
        entry_price = _finite(item.get("entry_price"))
        part = grouped.get(asset)
        if part is None or entry_price is None or not entry or not exit_d:
            out.append(item)
            continue
        window = part[(part["trade_date"] >= entry) & (part["trade_date"] <= exit_d)]
        if window.empty:
            out.append(item)
            continue
        low = window["low"] if "low" in window.columns else window.get("close")
        high = window["high"] if "high" in window.columns else window.get("close")
        if low is None or high is None:
            out.append(item)
            continue
        min_low = _finite(low.min())
        max_high = _finite(high.max())
        if min_low is not None:
            item["mae_pct"] = _finite((min_low / entry_price - 1.0) * 100.0)
        if max_high is not None:
            item["mfe_pct"] = _finite((max_high / entry_price - 1.0) * 100.0)
        out.append(item)
    return out


def cost_breakdown(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "commission": _finite(summary.get("commission")),
        "stamp_tax": _finite(summary.get("stamp_tax")),
        "slippage_cost": _finite(summary.get("slippage_cost")),
        "total_cost": _finite(summary.get("total_cost")),
        "turnover": _finite(summary.get("turnover")),
        "final_equity": _finite(summary.get("final_equity")),
        "total_return_pct": _finite(summary.get("total_return_pct")),
    }


__all__ = [
    "json_safe",
    "daily_returns_from_equity",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "rolling_performance",
    "align_benchmark_series",
    "enrich_trades_mae_mfe",
    "cost_breakdown",
]
