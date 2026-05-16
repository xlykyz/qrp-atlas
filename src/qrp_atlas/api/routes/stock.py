"""股票主数据路由"""

from fastapi import APIRouter

from qrp_atlas.api.db import get_db

router = APIRouter(prefix="/api/stock", tags=["股票主数据"])


@router.get("/list")
def list_stocks():
    """查询所有股票列表，按 ticker 排序"""
    con = get_db()
    try:
        rows = con.execute(
            "SELECT ticker, name, exchange, is_active FROM stock_info ORDER BY ticker"
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        result = [dict(zip(columns, row)) for row in rows]
        return result
    finally:
        con.close()
