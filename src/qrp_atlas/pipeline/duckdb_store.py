"""DuckDB 存储层 - 数据持久化"""
from pathlib import Path
from typing import Optional, List
import duckdb
import pandas as pd

from ..config import DB_PATH, ensure_dirs
from ..contracts import (
    CREATED_AT,
    get_table,
    init_database,
    DAILY_MARKET_SNAPSHOT,
    MARKET_PHASE,
    TRADE_EXECUTION,
    ZT_POOL,
    DT_POOL,
    TRADE_DATE,
    align_to_schema,
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
    df = align_to_schema(
        df,
        DAILY_MARKET_SNAPSHOT.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, DAILY_MARKET_SNAPSHOT.name, allow_extra=False)
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
    df = align_to_schema(
        df,
        MARKET_PHASE.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, MARKET_PHASE.name, allow_extra=False)
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
    df = align_to_schema(
        df,
        TRADE_EXECUTION.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, TRADE_EXECUTION.name, allow_extra=False)
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


def save_zt_pool(df: pd.DataFrame, replace: bool = False) -> None:
    """保存涨停股池数据

    Args:
        df: 涨停股池 DataFrame
        replace: 是否替换当日数据
    """
    df = align_to_schema(df, ZT_POOL.name, fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, ZT_POOL.name, allow_extra=False)

    con = get_connection()
    try:
        if replace:
            dates = df[TRADE_DATE].unique().tolist()
            for d in dates:
                con.execute(f"DELETE FROM {ZT_POOL.name} WHERE {TRADE_DATE} = ?", [d])
        insert_cols = [col for col in ZT_POOL.column_names() if col != CREATED_AT]
        cols = ", ".join(insert_cols)
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {ZT_POOL.name} ({cols}) SELECT {cols} FROM tmp_df")
    finally:
        con.close()


def save_dt_pool(df: pd.DataFrame, replace: bool = False) -> None:
    """保存跌停股池数据

    Args:
        df: 跌停股池 DataFrame
        replace: 是否替换当日数据
    """
    df = align_to_schema(df, DT_POOL.name, fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, DT_POOL.name, allow_extra=False)

    con = get_connection()
    try:
        if replace:
            dates = df[TRADE_DATE].unique().tolist()
            for d in dates:
                con.execute(f"DELETE FROM {DT_POOL.name} WHERE {TRADE_DATE} = ?", [d])
        insert_cols = [col for col in DT_POOL.column_names() if col != CREATED_AT]
        cols = ", ".join(insert_cols)
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {DT_POOL.name} ({cols}) SELECT {cols} FROM tmp_df")
    finally:
        con.close()


def save_daily_basic(df: pd.DataFrame, replace: bool = False) -> None:
    """保存每日基本面指标数据

    Args:
        df: 包含 daily_basic 数据的 DataFrame
        replace: 是否替换当日数据
    """
    from qrp_atlas.contracts import DAILY_BASIC, TRADE_DATE

    df = align_to_schema(df, DAILY_BASIC.name, fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, DAILY_BASIC.name, allow_extra=False)

    con = get_connection()
    try:
        if replace:
            dates = df[TRADE_DATE].unique().tolist()
            for d in dates:
                con.execute(f"DELETE FROM {DAILY_BASIC.name} WHERE {TRADE_DATE} = ?", [d])
        insert_cols = [col for col in DAILY_BASIC.column_names() if col != CREATED_AT]
        cols = ", ".join(insert_cols)
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {DAILY_BASIC.name} ({cols}) SELECT {cols} FROM tmp_df")
    finally:
        con.close()


def save_suspend_d(df: pd.DataFrame, replace: bool = False) -> None:
    """保存每日停复牌数据

    Args:
        df: 包含 suspend_d 数据的 DataFrame
        replace: 是否替换已有日期的数据
    """
    from qrp_atlas.contracts import SUSPEND_D, TRADE_DATE

    df = align_to_schema(df, SUSPEND_D.name, fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, SUSPEND_D.name, allow_extra=False)

    con = get_connection()
    try:
        if replace:
            dates = df[TRADE_DATE].unique().tolist()
            for d in dates:
                con.execute(f"DELETE FROM {SUSPEND_D.name} WHERE {TRADE_DATE} = ?", [d])
        insert_cols = [col for col in SUSPEND_D.column_names() if col != CREATED_AT]
        cols = ", ".join(insert_cols)
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {SUSPEND_D.name} ({cols}) SELECT {cols} FROM tmp_df")
    finally:
        con.close()


def get_table_info(table_name: str) -> pd.DataFrame:
    """获取表结构信息"""
    con = get_connection(read_only=True)
    try:
        return con.execute(f"DESCRIBE {table_name}").fetchdf()
    finally:
        con.close()
