import pandas as pd

from qrp_atlas.contracts import (
    TICKER,
    TRADE_DATE,
    NAME,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    AMOUNT,
    PCT_CHANGE,
    TURNOVER,
    MARKET_CAP,
    FLOAT_CAP,
    PRE_CLOSE,
    get_table,
    canonicalize,
    TUSHARE_DAILY,
    SINA_REALTIME,
)


AKSHARE_REALTIME_MAPPING = {
    "代码": TICKER,
    "名称": NAME,
    "最新价": CLOSE,
    "涨跌幅": PCT_CHANGE,
    "涨跌额": "chg",
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "振幅": "amplitude",
    "最高": HIGH,
    "最低": LOW,
    "今开": OPEN,
    "昨收": PRE_CLOSE,
    "换手率": TURNOVER,
    "市盈率-动态": "pe_ttm",
    "市净率": "pb",
    "总市值": MARKET_CAP,
    "流通市值": FLOAT_CAP,
    "涨速": "rise_speed",
    "5分钟涨跌": "min5_chg",
    "60日涨跌幅": "day60_pct",
    "年初至今涨跌幅": "ytd_pct",
}

SOURCE_MAPPINGS = {
    "akshare_realtime": AKSHARE_REALTIME_MAPPING,
    "eastmoney_realtime": AKSHARE_REALTIME_MAPPING,
    "sina_realtime": SINA_REALTIME,
    "tushare_daily": TUSHARE_DAILY,
}


def clean_daily_snapshot(df: pd.DataFrame, source: str = "akshare_realtime") -> pd.DataFrame:
    """
    清洗每日快照数据
    
    只做：
    - 列名标准化（对齐 contracts）
    - 类型转换
    - 按 (trade_date, ticker) 去重
    
    Args:
        df: 原始数据 DataFrame（已包含 trade_date）
        source: 数据源类型，支持 "akshare_realtime" 或 "tushare_daily"
        
    Returns:
        清洗后的 DataFrame
    """
    if source not in SOURCE_MAPPINGS:
        raise ValueError(f"Unknown source: {source}. Available: {list(SOURCE_MAPPINGS.keys())}")
    
    mapping = SOURCE_MAPPINGS[source]
    df = df.rename(columns=mapping)
    
    if source == "tushare_daily":
        df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE], format="%Y%m%d")
    
    schema = get_table("daily_market_snapshot")
    required_cols = list(schema.column_names())
    
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    
    df = df[[col for col in required_cols if col in df.columns]]
    
    df = canonicalize(df, "daily_market_snapshot")
    
    df = df.drop_duplicates(subset=[TRADE_DATE, TICKER], keep="last")
    
    return df
