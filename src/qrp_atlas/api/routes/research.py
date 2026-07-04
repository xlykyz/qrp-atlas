"""研报与机构调研查询路由"""

from typing import Optional

from fastapi import APIRouter, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api", tags=["研报与调研"])


# ── 个股研报 ──────────────────────────────────


@router.get("/reports/stock")
def query_stock_reports(
    ticker: Optional[str] = Query(
        None, description="股票代码，如 000001 / 000001.SZ（库内字段为纯数字 stock_code）"
    ),
    org: Optional[str] = Query(None, description="机构简称模糊匹配，如 中信"),
    rating: Optional[str] = Query(None, description="评级，如 买入 / 增持"),
    start_date: Optional[str] = Query(None, description="publish_date 起始"),
    end_date: Optional[str] = Query(None, description="publish_date 截止"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """查询个股研报。ticker 库内存的是 6 位数字 stock_code。"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if ticker:
            where_clauses.append("stock_code = ?")
            params.append(_to_pure_code(ticker))
        if org:
            where_clauses.append("org_sname LIKE ?")
            params.append(f"%{org}%")
        if rating:
            where_clauses.append("em_rating_name = ?")
            params.append(rating)
        if start_date:
            where_clauses.append("publish_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("publish_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM research_report_stock WHERE {where_sql} "
            f"ORDER BY publish_date DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


# ── 行业研报 ──────────────────────────────────


@router.get("/reports/industry")
def query_industry_reports(
    industry: Optional[str] = Query(
        None, description="行业名模糊匹配（indv_indu_name 或 industry_name）"
    ),
    org: Optional[str] = Query(None, description="机构简称模糊匹配"),
    rating: Optional[str] = Query(None, description="评级"),
    start_date: Optional[str] = Query(None, description="publish_date 起始"),
    end_date: Optional[str] = Query(None, description="publish_date 截止"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """查询行业研报。行业研报的 stock_code 通常为空。"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if industry:
            where_clauses.append(
                "(indv_indu_name LIKE ? OR industry_name LIKE ?)"
            )
            params.extend([f"%{industry}%", f"%{industry}%"])
        if org:
            where_clauses.append("org_sname LIKE ?")
            params.append(f"%{org}%")
        if rating:
            where_clauses.append("em_rating_name = ?")
            params.append(rating)
        if start_date:
            where_clauses.append("publish_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("publish_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM research_report_industry WHERE {where_sql} "
            f"ORDER BY publish_date DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


# ── 机构调研 ──────────────────────────────────


@router.get("/visits")
def query_visits(
    secu_code: Optional[str] = Query(
        None, description="证券代码，如 000001 / 000001.SZ（库内为带后缀形式）"
    ),
    start_date: Optional[str] = Query(None, description="notice_date 起始"),
    end_date: Optional[str] = Query(None, description="notice_date 截止"),
    keyword: Optional[str] = Query(
        None, description="对 sec_name 模糊匹配"
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """查询机构调研记录。secu_code 库内存为带后缀形式（000001.SZ）。"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if secu_code:
            where_clauses.append("secu_code = ?")
            params.append(_to_with_suffix(secu_code))
        if keyword:
            where_clauses.append("sec_name LIKE ?")
            params.append(f"%{keyword}%")
        if start_date:
            where_clauses.append("notice_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("notice_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM cninfo_research_visits WHERE {where_sql} "
            f"ORDER BY notice_date DESC, receive_date DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


# ── 工具 ──────────────────────────────────


def _to_pure_code(ticker: str) -> str:
    """提取 6 位纯数字代码（库内 stock_code 字段）。

    接受 000001 / 000001.SZ / SZ000001 等形式。
    """
    raw = str(ticker).strip().upper()
    code = raw.split(".", 1)[0] if "." in raw else raw
    # 去掉交易所前缀
    for prefix in ("SH", "SZ", "BJ"):
        if code.startswith(prefix):
            code = code[len(prefix):]
            break
    return code.zfill(6)


def _to_with_suffix(ticker: str) -> str:
    """补全交易所后缀（库内 secu_code 字段为 000001.SZ 形式）。"""
    raw = str(ticker).strip().upper()
    if "." in raw:
        return raw
    code = raw
    for prefix in ("SH", "SZ", "BJ"):
        if code.startswith(prefix):
            code = code[len(prefix):]
            break
    code = code.zfill(6)
    if code.startswith(("60", "68")):
        return f"{code}.SH"
    if code.startswith(("43", "83", "87", "88", "92")):
        return f"{code}.BJ"
    return f"{code}.SZ"
