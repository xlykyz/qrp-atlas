"""股票主数据路由"""

from typing import Optional

from fastapi import APIRouter, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api/stock", tags=["股票主数据"])


@router.get("/list")
def list_stocks(
    keyword: Optional[str] = Query(
        None, description="对 ticker 或 name 模糊匹配"
    ),
    exchange: Optional[str] = Query(None, description="交易所：SH / SZ / BJ"),
    is_active: Optional[bool] = Query(None, description="是否在市"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """查询股票列表，按 ticker 排序，支持关键字/交易所/是否在市过滤与分页。"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if keyword:
            where_clauses.append("(ticker LIKE ? OR name LIKE ?)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if exchange:
            where_clauses.append("exchange = ?")
            params.append(exchange.upper())
        if is_active is not None:
            where_clauses.append("is_active = ?")
            params.append(is_active)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT ticker, name, exchange, market, list_date, delist_date, is_active "
            f"FROM stock_info WHERE {where_sql} "
            f"ORDER BY ticker LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


@router.get("/{ticker}")
def get_stock(ticker: str):
    """查询单只股票信息。ticker 接受 000001 / 000001.SZ 形式。"""
    con = get_db()
    try:
        cond_ticker = _normalize(ticker)
        row = con.execute(
            "SELECT ticker, name, exchange, market, list_date, delist_date, "
            "is_active, updated_at FROM stock_info WHERE ticker = ?",
            [cond_ticker],
        ).fetchone()
        if row is None:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"股票不存在: {ticker}")
        columns = [desc[0] for desc in con.description]
        return row_to_dict(row, columns)
    finally:
        con.close()


def _normalize(ticker: str) -> str:
    """规范化 ticker：若已带后缀直接返回；否则按代码段补后缀。"""
    raw = str(ticker).strip().upper()
    if "." in raw:
        return raw
    code = raw.zfill(6)
    if code.startswith(("60", "68")):
        return f"{code}.SH"
    if code.startswith(("43", "83", "87", "88", "92")):
        return f"{code}.BJ"
    return f"{code}.SZ"
