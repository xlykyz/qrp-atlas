"""clean.py - 清洗 daily_basic 数据（对齐 contracts）"""

import pandas as pd

from qrp_atlas.contracts import (
    DAILY_BASIC,
    TICKER,
    TRADE_DATE,
    align_to_schema,
    canonicalize,
)


def clean_daily_basic(df: pd.DataFrame) -> pd.DataFrame:
    """清洗 daily_basic 原始数据

    Args:
        df: tushare daily_basic 原始返回（含 ts_code, trade_date, ...）

    Returns:
        清洗后 DataFrame（列名对齐 contracts，类型正确）
    """
    # 重命名：tushare 字段名 → contracts 规范字段名
    RENAME_MAP = {
        "ts_code": TICKER,
        "pe": "pe_ratio",
        "total_share": "total_shares",
    }
    df = df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})

    # trade_date 从 YYYYMMDD 转为 DATE
    if TRADE_DATE in df.columns:
        df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE], format="%Y%m%d")

    # 对齐 schema（删除多余列，补充缺失可选列）
    df = align_to_schema(
        df,
        DAILY_BASIC.name,
        fill_missing_optional=True,
        drop_extra=True,
    )

    # 标准化（类型转换）
    df = canonicalize(df, DAILY_BASIC.name)

    # 按主键去重
    df = df.drop_duplicates(subset=[TRADE_DATE, TICKER], keep="last")

    return df