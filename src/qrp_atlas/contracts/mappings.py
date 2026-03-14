"""
mappings.py - 各数据源字段映射

定义东财/AKShare/自有CSV等数据源字段到标准字段的映射。
数据清洗时使用这些映射进行字段转换。

使用示例:
    from qrp_atlas.contracts import apply_mapping, EASTMONEY_DAILY_BAR

    # 方式1: 使用预定义映射
    df = apply_mapping(df, "eastmoney_daily_bar")

    # 方式2: 获取映射字典自行处理
    mapping = get_mapping("eastmoney_daily_bar")
    df = df.rename(columns=mapping)

    # 方式3: 自定义映射
    custom_mapping = build_custom_mapping({"股票代码": "ticker", "交易日期": "trade_date"})
    df = df.rename(columns=custom_mapping)

支持的数据源:
    - eastmoney_daily_bar: 东方财富日线行情
    - eastmoney_snapshot: 东方财富实时快照
    - akshare_daily_bar: AKShare日线行情
    - akshare_realtime: AKShare实时行情
"""

from typing import Dict

from .fields import (
    TICKER, TRADE_DATE, NAME,
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
)


EASTMONEY_DAILY_BAR: Dict[str, str] = {
    "代码": TICKER,
    "日期": TRADE_DATE,
    "股票名称": NAME,
    "开盘": OPEN,
    "最高": HIGH,
    "最低": LOW,
    "收盘": CLOSE,
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "涨跌幅": PCT_CHANGE,
    "换手率": TURNOVER,
}

EASTMONEY_SNAPSHOT: Dict[str, str] = {
    "代码": TICKER,
    "股票名称": NAME,
    "最新价": CLOSE,
    "涨跌幅": PCT_CHANGE,
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "换手率": TURNOVER,
    "总市值": MARKET_CAP,
    "流通市值": FLOAT_CAP,
    "今开": OPEN,
    "最高": HIGH,
    "最低": LOW,
}

AKSHARE_DAILY_BAR: Dict[str, str] = {
    "代码": TICKER,
    "日期": TRADE_DATE,
    "开盘": OPEN,
    "最高": HIGH,
    "最低": LOW,
    "收盘": CLOSE,
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "涨跌幅": PCT_CHANGE,
    "换手率": TURNOVER,
}

AKSHARE_REALTIME: Dict[str, str] = {
    "代码": TICKER,
    "名称": NAME,
    "最新价": CLOSE,
    "涨跌幅": PCT_CHANGE,
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "换手率": TURNOVER,
    "总市值": MARKET_CAP,
    "流通市值": FLOAT_CAP,
}

SINA_REALTIME: Dict[str, str] = {
    "代码": TICKER,
    "名称": NAME,
    "最新价": CLOSE,
    "涨跌额": "change",
    "涨跌幅": PCT_CHANGE,
    "昨收": PRE_CLOSE,
    "今开": OPEN,
    "最高": HIGH,
    "最低": LOW,
    "成交量": VOLUME,
    "成交额": AMOUNT,
}

TUSHARE_DAILY: Dict[str, str] = {
    "ts_code": TICKER,
    "trade_date": TRADE_DATE,
    "open": OPEN,
    "high": HIGH,
    "low": LOW,
    "close": CLOSE,
    "pre_close": PRE_CLOSE,
    "pct_chg": PCT_CHANGE,
    "vol": VOLUME,
    "amount": AMOUNT,
}

SOURCE_MAPPINGS = {
    "eastmoney_daily_bar": EASTMONEY_DAILY_BAR,
    "eastmoney_snapshot": EASTMONEY_SNAPSHOT,
    "akshare_daily_bar": AKSHARE_DAILY_BAR,
    "akshare_realtime": AKSHARE_REALTIME,
    "sina_realtime": SINA_REALTIME,
    "tushare_daily": TUSHARE_DAILY,
}


def get_mapping(source: str) -> Dict[str, str]:
    """根据数据源名称获取字段映射

    Args:
        source: 数据源名称，如 "eastmoney_daily_bar"

    Returns:
        字段映射字典 {源字段名: 标准字段名}

    Raises:
        ValueError: 数据源名称不存在时抛出
    """
    if source not in SOURCE_MAPPINGS:
        raise ValueError(f"Unknown source: {source}. Available: {list(SOURCE_MAPPINGS.keys())}")
    return SOURCE_MAPPINGS[source]


def apply_mapping(df, source: str, drop_extra: bool = False):
    """对 DataFrame 应用字段映射

    将数据源的字段名转换为标准字段名。

    Args:
        df: pandas DataFrame
        source: 数据源名称
        drop_extra: 是否删除未映射的额外列，默认 False

    Returns:
        转换后的 DataFrame

    Example:
        df = apply_mapping(df, "eastmoney_daily_bar")
        df = apply_mapping(df, "eastmoney_daily_bar", drop_extra=True)
    """
    mapping = get_mapping(source)
    reverse_mapping = {v: k for k, v in mapping.items()}
    rename_map = {}
    for col in df.columns:
        if col in mapping:
            rename_map[col] = mapping[col]
        elif col in reverse_mapping:
            pass
    df = df.rename(columns=rename_map)
    if drop_extra:
        standard_cols = set(mapping.values())
        extra_cols = set(df.columns) - standard_cols
        if extra_cols:
            df = df.drop(columns=list(extra_cols))
    return df


def build_custom_mapping(field_pairs: Dict[str, str]) -> Dict[str, str]:
    """构建自定义字段映射

    Args:
        field_pairs: 字段对 {源字段名: 标准字段名}

    Returns:
        字段映射字典

    Example:
        mapping = build_custom_mapping({"股票代码": "ticker", "交易日期": "trade_date"})
    """
    return {src: dst for src, dst in field_pairs.items()}
