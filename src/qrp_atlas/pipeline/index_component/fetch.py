"""Fetch index component weights from Tushare index_weight."""

from __future__ import annotations

import time
from typing import Callable, Sequence

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


def _ymd(value: str) -> str:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"date must be YYYYMMDD, got {value!r}")
    return text


def fetch_index_weight(
    index_code: str,
    *,
    start_date: str,
    end_date: str,
    client=None,
) -> pd.DataFrame:
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_weight")
    df = _call_with_retry(
        method,
        index_code=index_code,
        start_date=_ymd(start_date),
        end_date=_ymd(end_date),
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df.reset_index(drop=True)


def fetch_index_weights(
    index_codes: Sequence[str],
    *,
    start_date: str,
    end_date: str,
    client=None,
) -> pd.DataFrame:
    frames = [
        fetch_index_weight(code, start_date=start_date, end_date=end_date, client=client)
        for code in index_codes
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
