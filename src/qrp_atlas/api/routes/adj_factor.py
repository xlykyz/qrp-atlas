"""复权因子查询路由"""

from typing import Optional

from fastapi import APIRouter, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api/adj-factor", tags=["复权因子"])


@router.get("")
def query_adj_factor(
    ticker: Optional[str] = Query(
        None, description="股票代码，如 000001.SZ 或 000001"
    ),
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD 或 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日期"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """查询复权因子变更记录。

    `adj_factor_changes` 表中 ticker 字段存储格式，与每日行情一致为
    `000001.SZ`；本路由同时兼容用户传入纯 6 位代码的形式。
    """
    con = get_db()
    try:
        where_clauses = []
        params = []
        if ticker:
            where_clauses.append("ticker = ?")
            params.append(_match_ticker(ticker))
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM adj_factor_changes WHERE {where_sql} "
            f"ORDER BY trade_date DESC, ticker LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


def _match_ticker(ticker: str) -> str:
    """规范化 ticker：含点号视为已带交易所后缀，直接返回；
    否则在 adj_factor_changes 中按 ticker 前缀匹配（最常用形式补 .SZ/.SH 后缀）。
    """
    raw = str(ticker).strip().upper()
    if "." in raw:
        return raw
    # 纯数字代码：补最常见的后缀。若调用方需要精确匹配其他形式，请显式传入后缀。
    if raw.startswith(("60", "68")):
        return f"{raw}.SH"
    return f"{raw}.SZ"
