"""Fetch Shenwan industry membership history from Tushare index_member_all."""

from __future__ import annotations

import time
from typing import Callable, Iterable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro


def _call_with_retry(func: Callable, *, retries: int = 5, base_sleep: float = 1.2, **kwargs) -> pd.DataFrame:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            df = func(**kwargs)
            if df is None:
                return pd.DataFrame()
            return df
        except Exception as exc:
            last_err = exc
            time.sleep(base_sleep * (i + 1))
    if last_err is not None:
        raise last_err
    return pd.DataFrame()


def fetch_industry_membership(
    *,
    tickers: Sequence[str] | None = None,
    l1_code: str | None = None,
    l2_code: str | None = None,
    l3_code: str | None = None,
    is_new: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Fetch membership rows.

    Prefer ticker list for small verification. Industry code filters support later backfill.
    """
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_member_all")
    frames: list[pd.DataFrame] = []

    if tickers:
        for ts_code in tickers:
            kwargs = {"ts_code": ts_code}
            if is_new is not None:
                kwargs["is_new"] = is_new
            df = _call_with_retry(method, **kwargs)
            if df is not None and not df.empty:
                frames.append(df)
    else:
        kwargs = {}
        if l1_code:
            kwargs["l1_code"] = l1_code
        if l2_code:
            kwargs["l2_code"] = l2_code
        if l3_code:
            kwargs["l3_code"] = l3_code
        if is_new is not None:
            kwargs["is_new"] = is_new
        if not kwargs:
            raise ValueError("provide tickers or an industry code filter; refuse full-universe pull")
        df = _call_with_retry(method, **kwargs)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
