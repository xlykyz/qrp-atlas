"""市场判读笔记路由"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api/phase", tags=["市场判读"])


class PhaseRead(BaseModel):
    trade_date: str
    phase: Optional[str] = None
    M1_core: Optional[bool] = None
    M2_front: Optional[bool] = None
    M3_identifiable: Optional[bool] = None
    V_triggered: Optional[bool] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None


class PhaseWrite(BaseModel):
    trade_date: str
    phase: Optional[str] = None
    M1_core: Optional[bool] = None
    M2_front: Optional[bool] = None
    M3_identifiable: Optional[bool] = None
    V_triggered: Optional[bool] = None
    notes: Optional[str] = None


@router.get("", response_model=list[PhaseRead])
def query_phase(
    date: Optional[str] = Query(None, description="交易日期 YYYY-MM-DD"),
    start_date: Optional[str] = Query(None, description="起始日期"),
    end_date: Optional[str] = Query(None, description="截止日期"),
):
    """查询市场判读记录"""
    con = get_db()
    try:
        where_clauses = []
        params = []
        if date:
            where_clauses.append("trade_date = ?")
            params.append(date)
        if start_date:
            where_clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        rows = con.execute(
            f"SELECT * FROM market_phase WHERE {where_sql} ORDER BY trade_date DESC",
            params,
        ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


@router.post("", response_model=PhaseRead)
def upsert_phase(body: PhaseWrite):
    """写入（或更新）市场判读记录"""
    con = get_db(read_only=False)
    try:
        con.execute(
            """INSERT INTO market_phase (trade_date, phase, M1_core, M2_front, M3_identifiable, V_triggered, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (trade_date) DO UPDATE SET
                   phase = EXCLUDED.phase,
                   M1_core = EXCLUDED.M1_core,
                   M2_front = EXCLUDED.M2_front,
                   M3_identifiable = EXCLUDED.M3_identifiable,
                   V_triggered = EXCLUDED.V_triggered,
                   notes = EXCLUDED.notes""",
            [
                body.trade_date,
                body.phase,
                body.M1_core,
                body.M2_front,
                body.M3_identifiable,
                body.V_triggered,
                body.notes,
            ],
        )
        row = con.execute(
            "SELECT * FROM market_phase WHERE trade_date = ?", [body.trade_date]
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="无法读取刚写入的记录")
        columns = [desc[0] for desc in con.description]
        return row_to_dict(row, columns)
    finally:
        con.close()
