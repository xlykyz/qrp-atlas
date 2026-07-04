"""涨跌停股池路由"""

from typing import Optional

from fastapi import APIRouter, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict
from qrp_atlas.contracts.conventions import format_ticker

router = APIRouter(prefix="/api", tags=["涨跌停股池"])


def _normalize_ticker_code(ticker: str) -> str:
    """把用户传入的 ticker 规整为 6 位纯数字代码。

    zt_pool/dt_pool 表中 ticker 字段以纯数字形式存储（如 603137），
    但 daily / stock_info 等表使用 000001.SZ 这种带交易所后缀的形式。
    这里统一转为 6 位数字以便命中。
    """
    raw = str(ticker).strip().upper()
    code = raw.split(".", 1)[0] if "." in raw else raw
    return format_ticker(code.replace("SH", "").replace("SZ", "").replace("BJ", ""))


@router.get("/zt-pool")
def query_zt_pool(
    date: Optional[str] = Query(None, description="交易日期 YYYY-MM-DD 或 YYYYMMDD"),
    start_date: Optional[str] = Query(None, description="起始日期"),
    end_date: Optional[str] = Query(None, description="截止日期"),
    ticker: Optional[str] = Query(None, description="股票代码，如 000001.SZ"),
    min_boards: int = Query(1, ge=1, description="连板数下限"),
    industry: Optional[str] = Query(None, description="所属行业（模糊匹配）"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """查询涨停股池"""
    return _query_pool(
        table="zt_pool",
        consecutive_field="consecutive_boards",
        date=date,
        start_date=start_date,
        end_date=end_date,
        ticker=ticker,
        min_consecutive=min_boards,
        industry=industry,
        limit=limit,
        offset=offset,
    )


@router.get("/dt-pool")
def query_dt_pool(
    date: Optional[str] = Query(None, description="交易日期 YYYY-MM-DD 或 YYYYMMDD"),
    start_date: Optional[str] = Query(None, description="起始日期"),
    end_date: Optional[str] = Query(None, description="截止日期"),
    ticker: Optional[str] = Query(None, description="股票代码，如 000001.SZ"),
    min_days: int = Query(1, ge=1, description="跌停天数下限"),
    industry: Optional[str] = Query(None, description="所属行业（模糊匹配）"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """查询跌停股池"""
    return _query_pool(
        table="dt_pool",
        consecutive_field="consecutive_days",
        date=date,
        start_date=start_date,
        end_date=end_date,
        ticker=ticker,
        min_consecutive=min_days,
        industry=industry,
        limit=limit,
        offset=offset,
    )


def _query_pool(
    table: str,
    consecutive_field: str,
    date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    ticker: Optional[str],
    min_consecutive: int,
    industry: Optional[str],
    limit: int,
    offset: int,
):
    con = get_db()
    try:
        where_clauses = [f"{consecutive_field} >= ?"]
        params = [min_consecutive]
        if date:
            where_clauses.append("trade_date = ?")
            params.append(date)
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)
        if ticker:
            where_clauses.append("ticker = ?")
            params.append(_normalize_ticker_code(ticker))
        if industry:
            where_clauses.append("industry_name LIKE ?")
            params.append(f"%{industry}%")

        where_sql = " AND ".join(where_clauses)
        rows = con.execute(
            f'SELECT * FROM "{table}" WHERE {where_sql} '
            f"ORDER BY trade_date DESC, {consecutive_field} DESC, ticker "
            f"LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()
