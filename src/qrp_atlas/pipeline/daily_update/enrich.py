"""
enrich.py - 增补缺失数据

职责:
1. 补全缺失股票(从上一交易日复制)
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


def _fill_names_from_stock_info(
    df: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """从本地 stock_info 表补全缺失的股票名称

    对于 name 为空的股票，在 stock_info 中查找并填入。
    查不到时保留空值并打印警告。

    Args:
        df: DataFrame
        con: DuckDB 连接

    Returns:
        补全 name 后的 DataFrame
    """
    if NAME not in df.columns or df[NAME].notna().all():
        return df

    # 检查 stock_info 表是否存在
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    if "stock_info" not in table_names:
        print("[WARN] stock_info 表不存在，跳过名称补全")
        return df

    missing_mask = df[NAME].isna()
    missing_tickers = df.loc[missing_mask, TICKER].unique().tolist()
    if not missing_tickers:
        return df

    # 从 stock_info 批量查
    placeholders = ", ".join(["?"] * len(missing_tickers))
    lookup = con.execute(
        f"SELECT ticker, name FROM stock_info WHERE ticker IN ({placeholders})",
        missing_tickers,
    ).fetchall()
    name_map = {row[0]: row[1] for row in lookup if row[1] is not None}

    filled = 0
    still_missing = []
    for idx in df.index[missing_mask]:
        ticker = df.at[idx, TICKER]
        if ticker in name_map:
            df.at[idx, NAME] = name_map[ticker]
            filled += 1
        else:
            still_missing.append(ticker)

    if still_missing:
        print(f"[WARN] stock_info 中找不到以下 {len(still_missing)} 只股票的 name:")
        print(f"       {still_missing[:10]}{'...' if len(still_missing) > 10 else ''}")

    if filled > 0:
        print(f"[ENRICH] 从 stock_info 补全 {filled} 只股票名称")

    return df


def _fill_pre_close_from_db(
    df: pd.DataFrame,
    trade_date: date,
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """从数据库补全缺失的 pre_close

    对于 pre_close 为空的股票，查该 ticker 在 trade_date 之前
    最近一个交易日的 close 作为 pre_close。

    只有数据库完全没有该股票历史数据时才会留空（新股首日）。
    """
    if PRE_CLOSE not in df.columns or df[PRE_CLOSE].notna().all():
        return df

    missing_mask = df[PRE_CLOSE].isna()
    missing_tickers = df.loc[missing_mask, TICKER].unique().tolist()
    if not missing_tickers:
        return df

    filled = 0
    still_missing = []
    for idx in df.index[missing_mask]:
        ticker = df.at[idx, TICKER]
        row = con.execute(
            "SELECT close FROM daily_market_snapshot "
            "WHERE ticker = ? AND trade_date < ? "
            "AND close IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT 1",
            [ticker, trade_date],
        ).fetchone()
        if row and row[0] is not None:
            df.at[idx, PRE_CLOSE] = row[0]
            filled += 1
        else:
            still_missing.append(ticker)

    if still_missing:
        print(f"[WARN] 以下 {len(still_missing)} 只股票无历史数据，pre_close 留空:")
        print(f"       {still_missing[:10]}{'...' if len(still_missing) > 10 else ''}")

    if filled > 0:
        print(f"[ENRICH] 从数据库历史 close 补全 {filled} 只股票 pre_close")

    return df


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

    规则: 股票名称包含 "ST" 或 "*ST"

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

    规则:
    - 根据昨收精确计算涨跌停价，而非固定百分比阈值
    - 普通股票: ±10% 涨跌停
    - ST 股票: ±5% 涨跌停
    - 科创板/创业板: ±20% 涨跌停
    - 涨停判定: close >= round(pre_close * (1 + limit_pct/100), 2)
    - 跌停判定: close <= round(pre_close * (1 - limit_pct/100), 2)

    Args:
        df: DataFrame

    Returns:
        添加涨跌停标识后的 DataFrame
    """
    df = df.copy()

    if CLOSE not in df.columns or PRE_CLOSE not in df.columns:
        df[IS_LIMIT_UP] = False
        df[IS_LIMIT_DOWN] = False
        return df

    is_st = df.get(IS_ST, False)

    ticker_prefix = df[TICKER].astype(str).str[:3]
    ticker_suffix = df[TICKER].astype(str).str[-3:]
    # 创业板: 300/301/302  科创板: 688/689  北交所: .BJ
    is_kcb = ticker_prefix.isin(["688", "689", "300", "301", "302"])
    is_bj = ticker_suffix == ".BJ"

    # 确定每只股票的涨跌幅限制比例
    limit_pct = pd.Series(10.0, index=df.index)             # 主板非ST: 10%
    limit_pct = limit_pct.mask(is_kcb, 20.0)                 # 科创/创业板: 20%（含ST）
    limit_pct = limit_pct.mask(is_bj, 30.0)                  # 北交所: 30%（含ST）
    limit_pct = limit_pct.mask(is_st & ~is_kcb & ~is_bj, 5.0) # 仅主板ST降为5%

    pre_close = df[PRE_CLOSE]
    close = df[CLOSE].round(2)  # 确保价格保留两位小数

    # 精确计算涨跌停价 → round(昨收 × (1 ± 比例), 2) 四舍五入到分
    limit_up_price = (pre_close * (1 + limit_pct / 100)).round(2)
    limit_down_price = (pre_close * (1 - limit_pct / 100)).round(2)

    df[IS_LIMIT_UP] = close >= limit_up_price
    df[IS_LIMIT_DOWN] = close <= limit_down_price

    # 昨收缺失或为 0（新股首日）→ 不判定涨跌停
    mask_no_pre = pre_close.isna() | (pre_close == 0)
    df.loc[mask_no_pre, IS_LIMIT_UP] = False
    df.loc[mask_no_pre, IS_LIMIT_DOWN] = False

    return df


def _calculate_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算缺失的 pct_change 字段

    公式: (close - pre_close) / pre_close * 100

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

    执行步骤:
    1. 补全缺失股票(从上一交易日复制)
    2. 计算缺失的 pct_change
    3. 派生 is_st
    4. 派生 is_limit_up, is_limit_down

    Args:
        df: 清洗后的 DataFrame
        trade_date: 交易日期
        con: DuckDB 连接(可选, 用于获取历史数据)

    Returns:
        增补后的 DataFrame
    """
    df = df.copy()
    
    if con is not None:
        df = _fill_missing_stocks(df, trade_date, con)

    df = _fill_names_from_stock_info(df, con)

    df = _fill_pre_close_from_db(df, trade_date, con)

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
    使用数据库路径增补数据(便捷函数)

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
