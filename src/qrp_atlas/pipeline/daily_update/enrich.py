"""
enrich.py - 增补缺失数据

职责：
1. 补全缺失股票（从上一交易日复制）
2. 计算缺失字段
3. 派生 is_st, is_limit_up, is_limit_down

使用示例:
    from qrp_atlas.pipeline.daily_update.enrich import enrich_daily_snapshot

    df_enriched = enrich_daily_snapshot(df_clean, trade_date, con)

注意事项:
    - 需要数据库连接来获取历史数据
    - 停牌股票的成交量设为 0
"""

from datetime import date
from typing import Optional

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH
from qrp_atlas.contracts import (
    TICKER,
    TRADE_DATE,
    NAME,
    CLOSE,
    VOLUME,
    AMOUNT,
    PCT_CHANGE,
    PRE_CLOSE,
    IS_ST,
    IS_LIMIT_UP,
    IS_LIMIT_DOWN,
)


def _get_previous_trade_date(con: duckdb.DuckDBPyConnection, trade_date: date) -> Optional[date]:
    """
    获取上一交易日

    Args:
        con: DuckDB 连接
        trade_date: 当前交易日

    Returns:
        上一交易日日期，如果不存在返回 None
    """
    result = con.execute(
        f"SELECT MAX({TRADE_DATE}) FROM daily_market_snapshot WHERE {TRADE_DATE} < ?",
        [trade_date]
    ).fetchone()
    return result[0] if result and result[0] else None


def _get_previous_day_data(
    con: duckdb.DuckDBPyConnection, 
    prev_date: date
) -> pd.DataFrame:
    """
    获取上一交易日的数据

    Args:
        con: DuckDB 连接
        prev_date: 上一交易日

    Returns:
        上一交易日的 DataFrame
    """
    df = con.execute(
        f"SELECT * FROM daily_market_snapshot WHERE {TRADE_DATE} = ?",
        [prev_date]
    ).fetchdf()
    return df


def _derive_is_st(df: pd.DataFrame) -> pd.DataFrame:
    """
    派生 is_st 字段

    规则：股票名称包含 "ST" 或 "*ST"

    Args:
        df: DataFrame

    Returns:
        添加 is_st 字段后的 DataFrame
    """
    if NAME not in df.columns:
        return df
    
    df = df.copy()
    df[IS_ST] = df[NAME].astype(str).str.upper().str.contains("ST", na=False)
    return df


def _derive_limit_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    派生 is_limit_up, is_limit_down 字段

    规则：
    - 普通股票：涨跌幅 >= 9.9% 或 <= -9.9%
    - ST 股票：涨跌幅 >= 4.9% 或 <= -4.9%
    - 科创板/创业板：涨跌幅 >= 19.9% 或 <= -19.9%

    Args:
        df: DataFrame

    Returns:
        添加涨跌停标识后的 DataFrame
    """
    df = df.copy()
    
    if PCT_CHANGE not in df.columns:
        df[IS_LIMIT_UP] = False
        df[IS_LIMIT_DOWN] = False
        return df
    
    is_st = df.get(IS_ST, False)
    
    ticker_prefix = df[TICKER].astype(str).str[:3]
    is_kcb = ticker_prefix.isin(["688", "300"])  # 科创板、创业板
    
    limit_up_threshold = pd.Series(9.9, index=df.index)
    limit_down_threshold = pd.Series(-9.9, index=df.index)
    
    limit_up_threshold = limit_up_threshold.mask(is_st, 4.9)
    limit_down_threshold = limit_down_threshold.mask(is_st, -4.9)
    
    limit_up_threshold = limit_up_threshold.mask(is_kcb & ~is_st, 19.9)
    limit_down_threshold = limit_down_threshold.mask(is_kcb & ~is_st, -19.9)
    
    df[IS_LIMIT_UP] = df[PCT_CHANGE] >= limit_up_threshold
    df[IS_LIMIT_DOWN] = df[PCT_CHANGE] <= limit_down_threshold
    
    return df


def _calculate_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算缺失的 pct_change 字段

    公式：(close - pre_close) / pre_close * 100

    Args:
        df: DataFrame

    Returns:
        填充 pct_change 后的 DataFrame
    """
    df = df.copy()
    
    if CLOSE not in df.columns or PRE_CLOSE not in df.columns:
        return df
    
    mask = df[PCT_CHANGE].isna() & df[PRE_CLOSE].notna() & (df[PRE_CLOSE] != 0)
    df.loc[mask, PCT_CHANGE] = (
        (df.loc[mask, CLOSE] - df.loc[mask, PRE_CLOSE]) 
        / df.loc[mask, PRE_CLOSE] * 100
    )
    
    return df


def _fill_missing_stocks(
    df: pd.DataFrame,
    trade_date: date,
    con: duckdb.DuckDBPyConnection
) -> pd.DataFrame:
    """
    补全缺失股票

    从上一交易日复制数据，volume 和 amount 设为 0

    Args:
        df: 当日数据
        trade_date: 交易日期
        con: DuckDB 连接

    Returns:
        补全后的 DataFrame
    """
    prev_date = _get_previous_trade_date(con, trade_date)
    if prev_date is None:
        return df
    
    prev_df = _get_previous_day_data(con, prev_date)
    if prev_df.empty:
        return df
    
    current_tickers = set(df[TICKER].tolist()) if TICKER in df.columns else set()
    prev_tickers = set(prev_df[TICKER].tolist()) if TICKER in prev_df.columns else set()
    missing_tickers = prev_tickers - current_tickers
    
    if not missing_tickers:
        return df
    
    missing_df = prev_df[prev_df[TICKER].isin(missing_tickers)].copy()
    missing_df[TRADE_DATE] = trade_date
    missing_df[VOLUME] = 0
    missing_df[AMOUNT] = 0.0
    
    df = pd.concat([df, missing_df], ignore_index=True)
    
    return df


def enrich_daily_snapshot(
    df: pd.DataFrame,
    trade_date: date,
    con: Optional[duckdb.DuckDBPyConnection] = None
) -> pd.DataFrame:
    """
    增补缺失数据

    执行步骤：
    1. 补全缺失股票（从上一交易日复制）
    2. 计算缺失的 pct_change
    3. 派生 is_st
    4. 派生 is_limit_up, is_limit_down

    Args:
        df: 清洗后的 DataFrame
        trade_date: 交易日期
        con: DuckDB 连接（可选，用于获取历史数据）

    Returns:
        增补后的 DataFrame
    """
    df = df.copy()
    
    if con is not None:
        df = _fill_missing_stocks(df, trade_date, con)
    
    df = _calculate_pct_change(df)
    df = _derive_is_st(df)
    df = _derive_limit_flags(df)
    
    return df


def enrich_with_db_path(
    df: pd.DataFrame,
    trade_date: date,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    使用数据库路径增补数据（便捷函数）

    Args:
        df: 清洗后的 DataFrame
        trade_date: 交易日期
        db_path: 数据库路径，默认使用 config.DB_PATH

    Returns:
        增补后的 DataFrame
    """
    if db_path is None:
        db_path = str(DB_PATH)
    
    con = duckdb.connect(db_path)
    try:
        return enrich_daily_snapshot(df, trade_date, con)
    finally:
        con.close()
