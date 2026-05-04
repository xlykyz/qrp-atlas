"""DuckDB 存储层 - 数据持久化"""
from pathlib import Path
from typing import Optional, List
import duckdb
import pandas as pd

from ..config import DB_PATH, ensure_dirs
from ..contracts import (
    get_table,
    init_database,
    DAILY_MARKET_SNAPSHOT,
    MARKET_PHASE,
    TRADE_EXECUTION,
    quick_validate,
)


def get_connection(read_only: bool = False):
    """获取 DuckDB 连接

    Args:
        read_only: 是否只读模式

    Returns:
        DuckDB 连接对象
    """
    ensure_dirs()
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    return con


def init_db():
    """初始化数据库，创建所有表"""
    con = get_connection()
    init_database(con)
    con.close()


def save_daily_market_snapshot(df: pd.DataFrame, replace: bool = False) -> None:
    """保存每日行情快照

    Args:
        df: 包含行情数据的 DataFrame
        replace: 是否替换现有数据(按主键)
    """
    df = quick_validate(df, "daily_market_snapshot")
    con = get_connection()
    try:
        if replace:
            con.register("tmp_df", df)
            con.execute("""
                DELETE FROM daily_market_snapshot
                WHERE (trade_date, ticker) IN (SELECT trade_date, ticker FROM tmp_df)
            """)
        con.register("tmp_df", df)
        con.execute("INSERT INTO daily_market_snapshot SELECT * FROM tmp_df")
    finally:
        con.close()


def get_daily_market_snapshot(
    trade_date: Optional[str] = None,
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """获取每日行情快照

    Args:
        trade_date: 单个交易日期
        ticker: 单个股票代码
        start_date: 起始日期(范围查询)
        end_date: 结束日期(范围查询)

    Returns:
        行情数据 DataFrame
    """
    con = get_connection(read_only=True)
    try:
        query = "SELECT * FROM daily_market_snapshot WHERE 1=1"
        params = []

        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)

        query += " ORDER BY trade_date, ticker"
        return con.execute(query, params).fetchdf()
    finally:
        con.close()


def save_market_phase(df: pd.DataFrame, replace: bool = False) -> None:
    """保存市场阶段数据

    Args:
        df: 包含市场阶段数据的 DataFrame
        replace: 是否替换现有数据
    """
    df = quick_validate(df, "market_phase")
    con = get_connection()
    try:
        if replace:
            con.register("tmp_df", df)
            con.execute("""
                DELETE FROM market_phase
                WHERE trade_date IN (SELECT trade_date FROM tmp_df)
            """)
        con.register("tmp_df", df)
        con.execute("INSERT INTO market_phase SELECT * FROM tmp_df")
    finally:
        con.close()


def get_market_phase(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """获取市场阶段数据

    Args:
        trade_date: 单个交易日期
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        市场阶段 DataFrame
    """
    con = get_connection(read_only=True)
    try:
        query = "SELECT * FROM market_phase WHERE 1=1"
        params = []

        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)

        query += " ORDER BY trade_date"
        return con.execute(query, params).fetchdf()
    finally:
        con.close()


def save_trade_execution(df: pd.DataFrame, replace: bool = False) -> None:
    """保存交易执行记录

    Args:
        df: 包含交易执行记录的 DataFrame
        replace: 是否替换现有数据
    """
    df = quick_validate(df, "trade_execution")
    con = get_connection()
    try:
        if replace:
            con.register("tmp_df", df)
            con.execute("""
                DELETE FROM trade_execution
                WHERE trade_id IN (SELECT trade_id FROM tmp_df)
            """)
        con.register("tmp_df", df)
        con.execute("INSERT INTO trade_execution SELECT * FROM tmp_df")
    finally:
        con.close()


def get_trade_execution(trade_id: Optional[str] = None) -> pd.DataFrame:
    """获取交易执行记录

    Args:
        trade_id: 交易ID(可选)

    Returns:
        交易执行记录 DataFrame
    """
    con = get_connection(read_only=True)
    try:
        if trade_id:
            return con.execute(
                "SELECT * FROM trade_execution WHERE trade_id = ?",
                [trade_id]
            ).fetchdf()
        return con.execute("SELECT * FROM trade_execution ORDER BY entry_date").fetchdf()
    finally:
        con.close()


def list_tables() -> List[str]:
    """列出所有表"""
    con = get_connection(read_only=True)
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        return [t[0] for t in tables]
    finally:
        con.close()


def get_table_info(table_name: str) -> pd.DataFrame:
    """获取表结构信息"""
    con = get_connection(read_only=True)
    try:
        return con.execute(f"DESCRIBE {table_name}").fetchdf()
    finally:
        con.close()
