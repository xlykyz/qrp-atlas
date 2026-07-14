"""Signal-date to execution-date mapping for product backtests."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

REASON_NO_EXECUTION_DATE_IN_RANGE = "NO_EXECUTION_DATE_IN_RANGE"


def market_trade_dates(price_df: pd.DataFrame) -> list[pd.Timestamp]:
    """Return sorted unique market session dates from a price frame."""

    if price_df is None or price_df.empty:
        return []
    dates = pd.to_datetime(price_df["trade_date"]).drop_duplicates().sort_values()
    return [pd.Timestamp(value).normalize() for value in dates.tolist()]


def next_trade_date(
    trade_dates: Sequence[pd.Timestamp],
    signal_date: str | pd.Timestamp,
    *,
    end_date: str | pd.Timestamp | None = None,
) -> pd.Timestamp | None:
    """Pick the first market date strictly after signal_date.

    When end_date is provided, only dates within the formal request window are valid.
    """

    signal_ts = pd.Timestamp(signal_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize() if end_date is not None else None
    for trade_date in trade_dates:
        if trade_date <= signal_ts:
            continue
        if end_ts is not None and trade_date > end_ts:
            return None
        return trade_date
    return None


def map_signal_date_to_execution_date(
    signal_date: str | pd.Timestamp,
    *,
    entry_timing: str,
    trade_dates: Sequence[pd.Timestamp],
    end_date: str | pd.Timestamp | None = None,
) -> tuple[pd.Timestamp | None, str | None]:
    """Map a strategy signal date to portfolio execution date.

    Returns (execution_date, skip_reason).
    """

    timing = str(entry_timing or "next_open").strip()
    signal_ts = pd.Timestamp(signal_date).normalize()
    if timing == "same_close":
        if end_date is not None and signal_ts > pd.Timestamp(end_date).normalize():
            return None, REASON_NO_EXECUTION_DATE_IN_RANGE
        if signal_ts not in set(trade_dates):
            return None, REASON_NO_EXECUTION_DATE_IN_RANGE
        return signal_ts, None

    if timing not in {"next_open", "next_close"}:
        raise ValueError(f"unsupported entry_timing: {timing}")

    execution = next_trade_date(trade_dates, signal_ts, end_date=end_date)
    if execution is None:
        return None, REASON_NO_EXECUTION_DATE_IN_RANGE
    return execution, None


def shift_target_weights_to_execution_dates(
    target_weights: pd.DataFrame,
    *,
    entry_timing: str,
    trade_dates: Sequence[pd.Timestamp],
    end_date: str | pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Shift complete target snapshots from signal dates to execution dates.

    Output keeps both:
    - trade_date: execution date consumed by PortfolioBacktestEngine
    - signal_date: original strategy decision date for result auditability
    """

    if target_weights is None or target_weights.empty:
        empty = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        return empty, []

    frame = target_weights.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame["target_weight"] = pd.to_numeric(frame["target_weight"], errors="coerce").fillna(0.0)
    if "priority" not in frame.columns:
        frame["priority"] = 0.0

    skipped: list[dict[str, str]] = []
    rows: list[dict[str, object]] = []

    for signal_date, group in frame.groupby("trade_date", sort=True):
        signal_iso = pd.Timestamp(signal_date).strftime("%Y-%m-%d")
        execution_date, reason = map_signal_date_to_execution_date(
            signal_date,
            entry_timing=entry_timing,
            trade_dates=trade_dates,
            end_date=end_date,
        )
        if execution_date is None:
            skipped.append(
                {
                    "asset_id": None,
                    "signal_date": signal_iso,
                    "reason": reason or REASON_NO_EXECUTION_DATE_IN_RANGE,
                    "detail": (
                        f"entry_timing={entry_timing}; no executable market date "
                        f"within requested end_date"
                    ),
                }
            )
            continue
        for item in group.itertuples(index=False):
            rows.append(
                {
                    "trade_date": execution_date,
                    "asset_id": str(item.asset_id),
                    "target_weight": float(item.target_weight),
                    "priority": float(getattr(item, "priority", 0.0) or 0.0),
                    "signal_date": signal_iso,
                    "_signal_ts": pd.Timestamp(signal_date),
                }
            )

    if not rows:
        empty = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        return empty, skipped

    shifted = pd.DataFrame(rows)
    # If multiple signal dates map to one execution date, keep the latest signal.
    shifted = shifted.sort_values(
        ["trade_date", "asset_id", "_signal_ts"], kind="mergesort"
    )
    shifted = shifted.drop_duplicates(subset=["trade_date", "asset_id"], keep="last")
    shifted["trade_date"] = shifted["trade_date"].dt.strftime("%Y-%m-%d")
    shifted = shifted.drop(columns=["_signal_ts"]).reset_index(drop=True)
    return shifted, skipped
