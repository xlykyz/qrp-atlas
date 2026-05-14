"""行情查询路由"""

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from qrp_atlas.api.db import get_db

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
    created_at: Optional[str] = None


@router.get("")
def query_daily(
    date: Optional[str] = Query(None, description="交易日期 YYYYMMDD"),
    ticker: Optional[str] = Query(None, description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="起始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日期 YYYYMMDD"),
    limit: int = Query(10000, description="最大返回行数"),
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

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        query = f"SELECT * FROM daily_market_snapshot WHERE {where_sql} ORDER BY trade_date, ticker LIMIT ?"

        rows = con.execute(query, params + [limit]).fetchall()
        columns = [desc[0] for desc in con.description]
        result = [dict(zip(columns, row)) for row in rows]

        # 日期/DuckDB date 对象转字符串
        for row in result:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        # 添加 board 分类（对齐 pipeline enrich.py + conventions.py 规则）
        for row in result:
            ticker = row.get("ticker", "")
            ticker = str(ticker)
            code = ticker.split(".")[0] if "." in ticker else ticker
            exchange = ticker.split(".")[1] if "." in ticker and len(ticker.split(".")) > 1 else ""
            
            if code.startswith("68"):
                row["board"] = "科创板"
            elif code.startswith("60"):
                row["board"] = "上证主板"
            elif code.startswith("30"):
                row["board"] = "创业板"
            elif code.startswith("00"):
                row["board"] = "深证主板"
            elif exchange == "BJ" or code.startswith(("43", "83", "87", "88", "92")):
                row["board"] = "北交所"
            else:
                row["board"] = "其他"

        return result
    finally:
        con.close()


@router.get("/dates")
def query_trade_dates(
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="截止日期 YYYY-MM-DD"),
    limit: int = Query(100, description="最大返回天数"),
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
