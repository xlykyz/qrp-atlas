"""个股趋势指标。

按 ticker 分组计算移动均线及价格相对均线的位置和持续天数，用于描述
个股短期趋势状态。输入 DataFrame 应包含多日多股行情。
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE

_REQUIRED_COLUMNS = (TICKER, TRADE_DATE, CLOSE)
_MA_WINDOW = 5

MA5 = "ma5"
CLOSE_ABOVE_MA5 = "close_above_ma5"
CLOSE_BELOW_MA5 = "close_below_ma5"
CLOSE_ABOVE_MA5_DAYS = "close_above_ma5_days"
CLOSE_BELOW_MA5_DAYS = "close_below_ma5_days"


def _consecutive_true_days(series: pd.Series) -> pd.Series:
    """计算每个位置上连续 True 的天数（遇 False 或 NaN 重置为 0）。

    例: [True, True, False, True, True, True] -> [1, 2, 0, 1, 2, 3]
    """
    mask = series.fillna(False).astype(bool)
    # 用 cumsum 技巧：累计 False 数作为分组 key，组内 cumcount+1 即连续 True 天数
    groups = (~mask).cumsum()
    return mask.groupby(groups).cumsum().astype(int)


def calculate_stock_trend(df: pd.DataFrame) -> pd.DataFrame:
    """计算个股 ma5 趋势指标。

    Args:
        df: 多日多股行情，需含 ticker、trade_date、close。按 ticker 分组、
            按 trade_date 排序后计算。

    Returns:
        在原列基础上追加以下列的 DataFrame：
            - ma5: 5 日收盘均价（不足 5 日为 NaN）
            - close_above_ma5: 收盘价是否高于 ma5
            - close_below_ma5: 收盘价是否低于 ma5
            - close_above_ma5_days: 连续高于 ma5 的天数
            - close_below_ma5_days: 连续低于 ma5 的天数
    """
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                *list(_REQUIRED_COLUMNS),
                MA5,
                CLOSE_ABOVE_MA5,
                CLOSE_BELOW_MA5,
                CLOSE_ABOVE_MA5_DAYS,
                CLOSE_BELOW_MA5_DAYS,
            ]
        )

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"calculate_stock_trend 缺少必要列: {missing}")

    result = df.copy()
    result = result.sort_values([TICKER, TRADE_DATE]).reset_index(drop=True)

    grouped = result.groupby(TICKER, sort=False, group_keys=False)

    result[MA5] = grouped[CLOSE].transform(
        lambda s: s.rolling(window=_MA_WINDOW, min_periods=_MA_WINDOW).mean()
    )

    result[CLOSE_ABOVE_MA5] = (result[CLOSE] > result[MA5]).fillna(False).astype(bool)
    result[CLOSE_BELOW_MA5] = (result[CLOSE] < result[MA5]).fillna(False).astype(bool)

    result[CLOSE_ABOVE_MA5_DAYS] = result.groupby(TICKER, sort=False)[CLOSE_ABOVE_MA5].transform(
        _consecutive_true_days
    )
    result[CLOSE_BELOW_MA5_DAYS] = result.groupby(TICKER, sort=False)[CLOSE_BELOW_MA5].transform(
        _consecutive_true_days
    )

    return result
