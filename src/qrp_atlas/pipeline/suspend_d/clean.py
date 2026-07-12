"""clean.py - 清洗 suspend_d 数据（对齐 contracts）"""

import pandas as pd

from qrp_atlas.contracts import (
    SUSPEND_D,
    TICKER,
    TRADE_DATE,
    align_to_schema,
    canonicalize,
)


def clean_suspend_d(df: pd.DataFrame) -> pd.DataFrame:
    """清洗 suspend_d 原始数据

    Args:
        df: tushare suspend_d 原始返回（含 ts_code, trade_date, ...）

    Returns:
        清洗后 DataFrame（列名对齐 contracts，类型正确）
    """
    # 重命名：ts_code → ticker
    RENAME_MAP = {"ts_code": TICKER}
    df = df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})

    # trade_date 从 YYYYMMDD 转为 DATE
    if TRADE_DATE in df.columns:
        df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE], format="%Y%m%d")

    # 对齐 schema（删除多余列，补充缺失可选列）
    df = align_to_schema(
        df,
        SUSPEND_D.name,
        fill_missing_optional=True,
        drop_extra=True,
    )

    # 标准化（类型转换）
    df = canonicalize(df, SUSPEND_D.name)

    # 按主键去重
    df = df.drop_duplicates(subset=[TRADE_DATE, TICKER, "suspend_type"], keep="last")

    return df
