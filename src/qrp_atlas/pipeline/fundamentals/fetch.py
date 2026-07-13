"""Fetch financial statements / indicators from Tushare VIP APIs."""

from __future__ import annotations

import time
from typing import Callable, Iterable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro

# VIP period endpoints return market-wide rows for one report period.
API_BY_TABLE = {
    "income_statement": "income_vip",
    "balance_sheet": "balancesheet_vip",
    "cashflow_statement": "cashflow_vip",
    "financial_indicator": "fina_indicator_vip",
}

# Non-VIP per-ticker fallback / targeted pull.
API_BY_TICKER = {
    "income_statement": "income",
    "balance_sheet": "balancesheet",
    "cashflow_statement": "cashflow",
    "financial_indicator": "fina_indicator",
}


def _call_with_retry(func: Callable, *, retries: int = 5, base_sleep: float = 1.2, **kwargs) -> pd.DataFrame:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            df = func(**kwargs)
            if df is None:
                return pd.DataFrame()
            return df
        except Exception as exc:  # network / gateway blips
            last_err = exc
            time.sleep(base_sleep * (i + 1))
    if last_err is not None:
        raise last_err
    return pd.DataFrame()


def _normalize_period(period: str | None) -> str | None:
    if period is None:
        return None
    text = str(period).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"period must be YYYYMMDD, got {period!r}")
    return text


def fetch_financial_by_period(
    table: str,
    period: str,
    *,
    client=None,
    tickers: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Fetch one report period via VIP interface.

    Args:
        table: contracts table name
        period: YYYYMMDD report period
        client: optional tushare pro client / fake client
        tickers: optional filter after fetch
    """
    api_name = API_BY_TABLE[table]
    period = _normalize_period(period)
    pro = client or get_tushare_pro()
    method = getattr(pro, api_name)
    df = _call_with_retry(method, period=period)
    if df is None or df.empty:
        return pd.DataFrame()
    if tickers:
        ticker_set = set(tickers)
        df = df[df["ts_code"].isin(ticker_set)].copy()
    return df.reset_index(drop=True)


def fetch_financial_by_tickers(
    table: str,
    tickers: Iterable[str],
    *,
    periods: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Fetch by ticker using non-VIP APIs (useful for targeted backfill)."""
    api_name = API_BY_TICKER[table]
    pro = client or get_tushare_pro()
    method = getattr(pro, api_name)
    period_set = {_normalize_period(p) for p in periods} if periods else None
    frames: list[pd.DataFrame] = []
    for ts_code in tickers:
        kwargs = {"ts_code": ts_code}
        if start_date:
            kwargs["start_date"] = _normalize_period(start_date)
        if end_date:
            kwargs["end_date"] = _normalize_period(end_date)
        df = _call_with_retry(method, **kwargs)
        if df is None or df.empty:
            continue
        if period_set is not None and "end_date" in df.columns:
            df = df[df["end_date"].astype(str).isin(period_set)]
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_financial(
    table: str,
    *,
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    mode: str = "period",
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Unified fetch entry.

    mode:
      - period: VIP by report period (default, preferred for bulk)
      - ticker: non-VIP by ticker list
    """
    if mode == "period":
        if not periods:
            raise ValueError("periods is required when mode='period'")
        frames = [
            fetch_financial_by_period(table, p, client=client, tickers=tickers)
            for p in periods
        ]
        frames = [f for f in frames if f is not None and not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if mode == "ticker":
        if not tickers:
            raise ValueError("tickers is required when mode='ticker'")
        return fetch_financial_by_tickers(
            table,
            tickers,
            periods=periods,
            start_date=start_date,
            end_date=end_date,
            client=client,
        )
    raise ValueError(f"unsupported mode: {mode}")
