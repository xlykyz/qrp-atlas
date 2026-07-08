import pandas as pd

from qrp_atlas.contracts import (
    TICKER,
    TRADE_DATE,
    VOLUME,
    AMOUNT,
    align_to_schema,
    apply_mapping,
    canonicalize,
    normalize_ticker,
)


def clean_daily_snapshot(df: pd.DataFrame, source: str = "akshare_realtime") -> pd.DataFrame:
    """
    清洗每日快照数据
    
    只做:
    - 列名标准化(对齐 contracts)
    - 类型转换
    - 按 (trade_date, ticker) 去重
    
    Args:
        df: 原始数据 DataFrame(已包含 trade_date)
        source: 数据源类型, 支持 "akshare_realtime" 或 "tushare_daily"
        
    Returns:
        清洗后的 DataFrame
    """
    df = apply_mapping(df, source)

    if source == "tushare_daily":
        df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE], format="%Y%m%d")

    # tushare 源单位转换：手→股，千元→元
    if source == "tushare_daily":
        if VOLUME in df.columns:
            df[VOLUME] = df[VOLUME] * 100
        if AMOUNT in df.columns:
            df[AMOUNT] = df[AMOUNT] * 1000

    # 统一 ticker 格式：去除前缀/补齐6位+交易所后缀
    if TICKER in df.columns:
        df[TICKER] = df[TICKER].apply(lambda x: normalize_ticker(x) if pd.notna(x) else x)

    df = align_to_schema(
        df,
        "daily_market_snapshot",
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = canonicalize(df, "daily_market_snapshot")
    
    df = df.drop_duplicates(subset=[TRADE_DATE, TICKER], keep="last")
    
    return df
