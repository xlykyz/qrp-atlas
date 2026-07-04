"""指数日线查询路由"""

from typing import Optional

from fastapi import APIRouter, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api/index-daily", tags=["指数行情"])


def _normalize_index_code(code: str) -> str:
    """把用户输入的指数代码规整为库中存储的格式（如 sh000001）。

    接受: 000001 / 000001.SH / sh000001 / SH000001
    若不含交易所前缀，则默认按上交所处理（沪深指数代码段不重叠时也能命中）。
    """
    raw = str(code).strip().lower()
    if "." in raw:
        prefix, num = raw.split(".", 1)
        return f"{prefix}{num}"
    if raw.startswith(("sh", "sz", "bj")):
        return raw
    # 纯数字：000001/399001 等，按代码段补交易所前缀
    if raw.startswith("399"):
        return f"sz{raw}"
    return f"sh{raw}"


@router.get("")
def query_index_daily(
    code: Optional[str] = Query(
        None, description="指数代码，如 000001 / sh000001 / 000001.SH"
    ),
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD 或 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日期"),
    limit: int = Query(1000, ge=1, le=10000, description="最大返回行数"),
    offset: int = Query(0, ge=0, description="跳过行数"),
):
    """查询指数日线数据"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if code:
            where_clauses.append("index_code = ?")
            params.append(_normalize_index_code(code))
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM index_daily WHERE {where_sql} "
            f"ORDER BY trade_date DESC, index_code LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


@router.get("/codes")
def list_index_codes():
    """列出所有指数代码及名称"""
    con = get_db()
    try:
        rows = con.execute(
            "SELECT DISTINCT index_code, index_name FROM index_daily "
            "ORDER BY index_code"
        ).fetchall()
        return [{"index_code": r[0], "index_name": r[1]} for r in rows]
    finally:
        con.close()
