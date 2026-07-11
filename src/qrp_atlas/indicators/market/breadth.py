"""市场宽度指标。

基于全市场单日快照计算涨跌家数、涨跌停家数等宽度指标，用于描述当日
市场整体强弱。输入 DataFrame 应包含单日全市场行情，字段遵循 contracts
约定。
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.contracts import (
    IS_LIMIT_DOWN,
    IS_LIMIT_UP,
    PCT_CHANGE,
    TICKER,
    derive_limit_flags,
)

_REQUIRED_COLUMNS = (TICKER, PCT_CHANGE)


def _ensure_limit_flags(df: pd.DataFrame) -> pd.DataFrame:
    """返回保证含 is_limit_up / is_limit_down 列的 DataFrame。

    若输入已含涨跌停标记则原样使用；否则用 contracts 的 derive_limit_flags
    基于 close/pre_close 派生。派生失败（缺昨收）时填 False。
    """
    if IS_LIMIT_UP in df.columns and IS_LIMIT_DOWN in df.columns:
        return df
    return derive_limit_flags(df)


def calculate_market_breadth(df: pd.DataFrame) -> dict:
    """计算单日市场宽度。

    Args:
        df: 单日全市场行情快照，需含 ticker、pct_change；涨跌停标记
            (is_limit_up/is_limit_down) 可选，缺失时按 contracts 规则派生。

    Returns:
        包含以下键的字典：
            - total_count: 股票总数
            - up_count: 上涨家数
            - down_count: 下跌家数
            - flat_count: 平盘家数
            - limit_up_count: 涨停家数
            - limit_down_count: 跌停家数
            - up_ratio: 上涨占比 (0-1)
            - down_ratio: 下跌占比 (0-1)
            - limit_up_down_diff: 涨停减跌停的家数差
    """
    if df is None or df.empty:
        return _empty_breadth()

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"calculate_market_breadth 缺少必要列: {missing}")

    df = _ensure_limit_flags(df)

    total = len(df)
    pct = df[PCT_CHANGE]
    up_count = int((pct > 0).sum())
    down_count = int((pct < 0).sum())
    flat_count = int((pct == 0).sum())

    limit_up_count = int(df[IS_LIMIT_UP].fillna(False).astype(bool).sum())
    limit_down_count = int(df[IS_LIMIT_DOWN].fillna(False).astype(bool).sum())

    up_ratio = up_count / total if total else 0.0
    down_ratio = down_count / total if total else 0.0

    return {
        "total_count": total,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "up_ratio": round(up_ratio, 4),
        "down_ratio": round(down_ratio, 4),
        "limit_up_down_diff": limit_up_count - limit_down_count,
    }


def _empty_breadth() -> dict:
    return {
        "total_count": 0,
        "up_count": 0,
        "down_count": 0,
        "flat_count": 0,
        "limit_up_count": 0,
        "limit_down_count": 0,
        "up_ratio": 0.0,
        "down_ratio": 0.0,
        "limit_up_down_diff": 0,
    }
