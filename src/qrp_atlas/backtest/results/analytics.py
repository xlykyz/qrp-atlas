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
    """Compute simple daily returns from equity curve points.

    Date rows are never dropped. Transition into full loss is -1.0. After equity
    has already reached 0, subsequent returns are None (undefined from a zero base)
    unless equity remains exactly 0 (return 0.0).
    """

    rows: list[dict[str, Any]] = []
    prev: float | None = None
    for point in equity_curve:
        date = str(point.get("date") or "")
        equity = _finite(point.get("equity"))
        if equity is None:
            rows.append({"date": date, "daily_return": None, "equity": None})
            prev = None
            continue
        if prev is None:
            daily: float | None = 0.0
        elif prev == 0.0:
            daily = 0.0 if equity == 0.0 else None
        else:
            daily = equity / prev - 1.0
        rows.append({"date": date, "daily_return": _finite(daily) if daily is not None else None, "equity": equity})
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
    """Calmar = annual_return_pct / abs(max_drawdown_pct).

    For full-loss runs: annual_return_pct=-100 and max_drawdown_pct=-100 => -1.0.
    """

    ann = _finite(annual_return_pct)
    dd = _finite(max_drawdown_pct)
    if ann is None or dd is None:
        return None
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

    Keeps every date, including equity==0 (full loss). Does not drop zero-equity
    rows or stitch across the wipeout. After equity reaches 0, window return /
    vol / sharpe that would require dividing by zero stay None; drawdown is -1.0.
    """

    points = [(str(p.get("date") or ""), _finite(p.get("equity"))) for p in equity_curve]
    points = [(d, e) for d, e in points if d]
    if not points:
        return []

    dates = [d for d, _ in points]
    equities = [e for _, e in points]

    daily: list[float | None] = []
    for i, equity in enumerate(equities):
        if i == 0:
            daily.append(0.0 if equity is not None else None)
            continue
        prev = equities[i - 1]
        if equity is None or prev is None:
            daily.append(None)
        elif prev == 0.0:
            daily.append(0.0 if equity == 0.0 else None)
        else:
            daily.append(equity / prev - 1.0)

    out: list[dict[str, Any]] = []
    running_peak: float | None = None
    for i, date in enumerate(dates):
        row: dict[str, Any] = {"date": date, "equity": equities[i]}
        equity = equities[i]
        if equity is None:
            row["drawdown"] = None
        else:
            if running_peak is None:
                running_peak = equity
            else:
                running_peak = max(running_peak, equity)
            if running_peak > 0:
                row["drawdown"] = _finite(equity / running_peak - 1.0)
            elif equity == 0.0:
                row["drawdown"] = -1.0
            else:
                row["drawdown"] = None
        for window in windows:
            key = f"w{int(window)}"
            if i + 1 < window:
                row[f"return_{key}"] = None
                row[f"volatility_{key}"] = None
                row[f"sharpe_{key}"] = None
                row[f"drawdown_{key}"] = None
                continue
            window_eq = equities[i - window + 1 : i + 1]
            window_rets = daily[i - window + 1 : i + 1]
            start_eq = window_eq[0]
            end_eq = window_eq[-1]
            if start_eq is None or end_eq is None or start_eq <= 0:
                row[f"return_{key}"] = None
            else:
                row[f"return_{key}"] = _finite(end_eq / start_eq - 1.0)
            finite_rets = [r for r in window_rets if r is not None]
            if len(finite_rets) >= 2:
                mean = sum(finite_rets) / len(finite_rets)
                var = sum((r - mean) ** 2 for r in finite_rets) / (len(finite_rets) - 1)
                vol = math.sqrt(var) if var > 0 else 0.0
                row[f"volatility_{key}"] = _finite(vol * math.sqrt(periods_per_year))
                row[f"sharpe_{key}"] = (
                    None if vol == 0 else _finite((mean / vol) * math.sqrt(periods_per_year))
                )
            else:
                row[f"volatility_{key}"] = None
                row[f"sharpe_{key}"] = None
            # window drawdown from first equity in window as peak seed
            peak = None
            max_dd = None
            for e in window_eq:
                if e is None:
                    continue
                peak = e if peak is None else max(peak, e)
                if peak and peak > 0:
                    dd = e / peak - 1.0
                    max_dd = dd if max_dd is None else min(max_dd, dd)
                elif e == 0.0:
                    max_dd = -1.0 if max_dd is None else min(max_dd, -1.0)
            row[f"drawdown_{key}"] = _finite(max_dd)
        out.append(row)
    return out



def align_benchmark_series(
    portfolio_dates: Sequence[str],
    benchmark: pd.DataFrame,
    *,
    portfolio_returns: Sequence[float | None] | None = None,
    date_col: str = "trade_date",
    value_col: str = "close",
) -> tuple[list[dict[str, Any]], list[str]]:
    """Align benchmark to portfolio dates without fill across gaps.

    No forward/backward fill. Gap dates break benchmark cumulative chains.

    Per-date fields:
    - daily_active_return = (1+p)/(1+b)-1
    - excess_return: alias of daily_active_return
    - excess_percentage_point / relative_return only meaningful with continuous coverage
    """

    diagnostics: list[str] = []
    n = len(portfolio_dates)
    port_rets: list[float | None] = list(portfolio_returns) if portfolio_returns is not None else [None] * n
    if len(port_rets) < n:
        port_rets = port_rets + [None] * (n - len(port_rets))

    def _empty(date: str, port_ret: float | None, port_cum: float | None) -> dict[str, Any]:
        return {
            "date": date,
            "benchmark_level": None,
            "benchmark_return": None,
            "benchmark_cumulative_return": None,
            "portfolio_return": port_ret,
            "portfolio_cumulative_return": port_cum,
            "daily_active_return": None,
            "excess_return": None,
            "excess_percentage_point": None,
            "relative_return": None,
        }

    # portfolio cumulative chain (independent of benchmark)
    port_cums: list[float | None] = []
    growth = 1.0
    for i, pr in enumerate(port_rets):
        if i == 0:
            growth = 1.0 + (pr if pr is not None else 0.0)
            port_cums.append(growth - 1.0)
            continue
        if pr is None:
            port_cums.append(None)
        else:
            growth *= 1.0 + pr
            port_cums.append(growth - 1.0)

    if benchmark is None or getattr(benchmark, "empty", True):
        diagnostics.append("benchmark_missing")
        return (
            [_empty(str(d), _finite(port_rets[i]), _finite(port_cums[i])) for i, d in enumerate(portfolio_dates)],
            diagnostics,
        )

    work = benchmark.copy()
    def _as_date_str(v: Any) -> str:
        try:
            ts = pd.Timestamp(v)
            if pd.isna(ts):
                return str(v)
            return ts.strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            return str(v)
    work[date_col] = work[date_col].map(_as_date_str)
    work = work.sort_values(date_col)
    values = {
        str(r[date_col]): _finite(r[value_col])
        for r in work[[date_col, value_col]].to_dict(orient="records")
    }

    aligned: list[dict[str, Any]] = []
    prev_level: float | None = None
    segment_base: float | None = None
    continuous_from_start = True

    for i, date in enumerate(portfolio_dates):
        date = str(date)
        port_ret = _finite(port_rets[i])
        port_cum = _finite(port_cums[i])
        level = values.get(date)
        if level is None:
            diagnostics.append(f"benchmark_gap:{date}")
            continuous_from_start = False
            prev_level = None
            segment_base = None
            aligned.append(_empty(date, port_ret, port_cum))
            continue

        if prev_level is None:
            # Only the first portfolio date may define benchmark return as zero.
            # A level that reappears after any missing portfolio date has no known
            # previous level, so its return and active return remain undefined.
            b_ret = 0.0 if i == 0 else None
            segment_base = level
        else:
            b_ret = level / prev_level - 1.0
        b_cum = (level / segment_base - 1.0) if segment_base else None

        if b_ret is not None and port_ret is not None and (1.0 + b_ret) != 0.0:
            daily_active = (1.0 + port_ret) / (1.0 + b_ret) - 1.0
        else:
            daily_active = None

        excess_pp = None
        relative = None
        if continuous_from_start and port_cum is not None and b_cum is not None:
            excess_pp = port_cum - b_cum
            if (1.0 + b_cum) != 0.0:
                relative = (1.0 + port_cum) / (1.0 + b_cum) - 1.0

        aligned.append(
            {
                "date": date,
                "benchmark_level": level,
                "benchmark_return": _finite(b_ret),
                "benchmark_cumulative_return": _finite(b_cum),
                "portfolio_return": port_ret,
                "portfolio_cumulative_return": port_cum,
                "daily_active_return": _finite(daily_active),
                "excess_return": _finite(daily_active),
                "excess_percentage_point": _finite(excess_pp),
                "relative_return": _finite(relative),
            }
        )
        prev_level = level

    full_range = bool(aligned) and all(p.get("benchmark_level") is not None for p in aligned)
    if not full_range:
        diagnostics.append("benchmark_has_gaps_no_fill")
        diagnostics.append("full_range_excess_unavailable_due_to_benchmark_gaps")
        # ensure full-range fields null when gaps exist (already null after gap break)
        for p in aligned:
            if not full_range:
                # keep per-date active return; null full-range only at summary
                pass
    if all(p.get("benchmark_level") is None for p in aligned):
        diagnostics.append("benchmark_unavailable_for_range")
    return aligned, diagnostics


def benchmark_summary(aligned: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize aligned benchmark/excess series.

    Full-range cumulative excess requires continuous benchmark coverage.
    With gaps: full-range excess totals are None (never sum across gaps).
    """

    b_rets = [_finite(p.get("benchmark_return")) for p in aligned]
    active_rets = [_finite(p.get("daily_active_return", p.get("excess_return"))) for p in aligned]
    finite_b = [v for v in b_rets if v is not None]
    finite_a = [v for v in active_rets if v is not None]
    full_range = bool(aligned) and all(p.get("benchmark_level") is not None for p in aligned)
    last = aligned[-1] if aligned else {}
    total_b = _finite(last.get("benchmark_cumulative_return")) if full_range else None
    total_p = _finite(last.get("portfolio_cumulative_return")) if full_range else None
    excess_pp = _finite((total_p - total_b) if (full_range and total_p is not None and total_b is not None) else None)
    relative = None
    if full_range and total_p is not None and total_b is not None and (1.0 + total_b) != 0.0:
        relative = (1.0 + total_p) / (1.0 + total_b) - 1.0
    return {
        "benchmark_total_return": total_b,
        "benchmark_total_return_pct": _finite(None if total_b is None else total_b * 100.0),
        "portfolio_total_return": total_p,
        "portfolio_total_return_pct": _finite(None if total_p is None else total_p * 100.0),
        "excess_percentage_point": excess_pp,
        "excess_percentage_point_pct": _finite(None if excess_pp is None else excess_pp * 100.0),
        "relative_return": _finite(relative),
        "relative_return_pct": _finite(None if relative is None else relative * 100.0),
        # Compatible keys: geometric relative excess only when full-range continuous.
        "excess_total_return": _finite(relative),
        "excess_total_return_pct": _finite(None if relative is None else relative * 100.0),
        "full_range_excess_available": full_range,
        "benchmark_observation_count": len(finite_b),
        "daily_active_observation_count": len(finite_a),
        "benchmark_sharpe": sharpe_ratio(b_rets),
        "daily_active_sharpe": sharpe_ratio(active_rets),
        "excess_sharpe": sharpe_ratio(active_rets),
    }


def enrich_trades_mae_mfe(
    trades: list[dict[str, Any]],
    prices: pd.DataFrame | None,
    *,
    as_of_date: str | None = None,
) -> list[dict[str, Any]]:
    """Fill MAE/MFE from daily OHLC over the holding window.

    - closed trades: [entry_date, exit_date]
    - open trades: [entry_date, as_of_date] when as_of_date provided
    - invalid / non-positive high-low ignored
    - multi-fill trades already collapsed by writer trade aggregation
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
        asset = str(item.get("asset_id") or "")
        entry = str(item.get("entry_date") or "")
        exit_d = str(item.get("exit_date") or "") or None
        status = str(item.get("status") or "")
        entry_price = _finite(item.get("entry_price"))
        part = grouped.get(asset)
        if part is None or entry_price is None or entry_price <= 0 or not entry:
            out.append(item)
            continue
        end = exit_d if status == "closed" and exit_d else (as_of_date or exit_d)
        if not end:
            out.append(item)
            continue
        window = part[(part["trade_date"] >= entry) & (part["trade_date"] <= end)]
        if window.empty:
            out.append(item)
            continue
        lows = []
        highs = []
        for _, row in window.iterrows():
            low = _finite(row["low"] if "low" in window.columns else row.get("close"))
            high = _finite(row["high"] if "high" in window.columns else row.get("close"))
            if low is not None and low > 0:
                lows.append(low)
            if high is not None and high > 0:
                highs.append(high)
        if lows:
            item["mae_pct"] = _finite((min(lows) / entry_price - 1.0) * 100.0)
        if highs:
            item["mfe_pct"] = _finite((max(highs) / entry_price - 1.0) * 100.0)
        out.append(item)
    return out


def snapshot_hash(payload: Mapping[str, Any]) -> str:
    """Deterministic content hash for reproducibility snapshots."""

    import hashlib
    import json

    canonical = json.dumps(json_safe(dict(payload)), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
    "benchmark_summary",
    "enrich_trades_mae_mfe",
    "snapshot_hash",
    "cost_breakdown",
]
