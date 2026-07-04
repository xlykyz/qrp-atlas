"""行情查询路由"""

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from qrp_atlas.api.db import get_db
from qrp_atlas.contracts.conventions import get_board

router = APIRouter(prefix="/api/daily", tags=["行情数据"])


class DailyRow(BaseModel):
    trade_date: str
    ticker: str
    name: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pct_change: Optional[float] = None
    pre_close: Optional[float] = None
    volume: Optional[int] = None
    amount: Optional[float] = None
    turnover: Optional[float] = None
    market_cap: Optional[float] = None
    float_cap: Optional[float] = None
    is_st: Optional[bool] = None
    is_limit_up: Optional[bool] = None
    is_limit_down: Optional[bool] = None
    pct_5d: Optional[float] = None
    pct_10d: Optional[float] = None
    pct_20d: Optional[float] = None
    board: Optional[str] = None
    created_at: Optional[str] = None


@router.get("")
def query_daily(
    date: Optional[str] = Query(None, description="交易日期 YYYYMMDD"),
    ticker: Optional[str] = Query(None, description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="起始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日期 YYYYMMDD"),
    limit: int = Query(10000, ge=1, le=100000, description="最大返回行数"),
    offset: int = Query(0, ge=0, description="跳过行数"),
):
    """查询每日行情数据"""
    con = get_db()
    try:
        where_clauses = []
        params = []

        if date:
            where_clauses.append("trade_date = ?")
            params.append(date)
        if ticker:
            where_clauses.append("ticker = ?")
            params.append(ticker)
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)

        # ── 单日查询：用窗口函数计算 5/10/20 日累计涨跌幅 ──
        if date and not start_date and not end_date:
            # 兼容 YYYYMMDD 和 YYYY-MM-DD 两种输入格式
            date_fmt = date if "-" in date else f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            prev_dates = con.execute(
                "SELECT trade_date FROM trading_calendar WHERE trade_date <= ? AND is_open = 1 ORDER BY trade_date DESC LIMIT 21",
                [date_fmt],
            ).fetchall()
            prev_dates = [r[0] for r in prev_dates]
            start_date_for_window = prev_dates[-1] if prev_dates else date_fmt

            window_query = """
                WITH base AS (
                    SELECT *,
                        LAG(close, 5) OVER (PARTITION BY ticker ORDER BY trade_date) AS close_5d_ago,
                        LAG(close, 10) OVER (PARTITION BY ticker ORDER BY trade_date) AS close_10d_ago,
                        LAG(close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) AS close_20d_ago
                    FROM daily_market_snapshot
                    WHERE trade_date BETWEEN ? AND ?
                )
                SELECT
                    trade_date, ticker, name, open, high, low, close, pct_change,
                    pre_close, volume, amount, turnover, market_cap, float_cap,
                    is_st, is_limit_up, is_limit_down, created_at,
                    CASE WHEN close_5d_ago IS NOT NULL AND close_5d_ago != 0
                         THEN ROUND((close - close_5d_ago) / close_5d_ago * 100, 2) END AS pct_5d,
                    CASE WHEN close_10d_ago IS NOT NULL AND close_10d_ago != 0
                         THEN ROUND((close - close_10d_ago) / close_10d_ago * 100, 2) END AS pct_10d,
                    CASE WHEN close_20d_ago IS NOT NULL AND close_20d_ago != 0
                         THEN ROUND((close - close_20d_ago) / close_20d_ago * 100, 2) END AS pct_20d
                FROM base
                WHERE trade_date = ?
                ORDER BY trade_date, ticker
                LIMIT ? OFFSET ?
            """
            rows = con.execute(
                window_query, [start_date_for_window, date, date, limit, offset]
            ).fetchall()
            columns = [desc[0] for desc in con.description]
            result = [dict(zip(columns, row)) for row in rows]
        else:
            # ── 范围查询（无单一日期的基准）或其它组合：走原始逻辑 ──
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = (
                f"SELECT * FROM daily_market_snapshot WHERE {where_sql} "
                f"ORDER BY trade_date, ticker LIMIT ? OFFSET ?"
            )
            rows = con.execute(query, params + [limit, offset]).fetchall()
            columns = [desc[0] for desc in con.description]
            result = [dict(zip(columns, row)) for row in rows]

        # 日期/DuckDB date 对象转字符串
        for row in result:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        # 添加 board 分类（统一调用 conventions.get_board）
        for row in result:
            row["board"] = get_board(row.get("ticker", ""))

        # 过滤退市股
        result = [row for row in result if not (row.get("name") and "退市" in str(row.get("name", "")))]

        return result
    finally:
        con.close()


@router.get("/dates")
def query_trade_dates(
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="截止日期 YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=10000, description="最大返回天数"),
):
    """查询交易日列表（从 trading_calendar 表）"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        # is_open=1 表示交易日
        where_sql += " AND is_open = 1"

        rows = con.execute(
            f"SELECT trade_date FROM trading_calendar WHERE {where_sql} ORDER BY trade_date DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()
