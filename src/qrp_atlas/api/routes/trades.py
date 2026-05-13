"""交易执行记录路由"""

from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from qrp_atlas.api.db import get_db
from qrp_atlas.api.utils import row_to_dict

router = APIRouter(prefix="/api/trades", tags=["交易记录"])


class TradeRead(BaseModel):
    trade_id: str
    ticker: Optional[str] = None
    entry_date: Optional[str] = None
    entry_price: Optional[float] = None
    path_type: Optional[str] = None
    half_sell_trigger: Optional[float] = None
    half_sell_date: Optional[str] = None
    half_sell_price: Optional[float] = None
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    position_pct: Optional[float] = None
    notes: Optional[str] = None


class TradeWrite(BaseModel):
    ticker: Optional[str] = None
    entry_date: Optional[str] = None
    entry_price: Optional[float] = None
    path_type: Optional[str] = None
    half_sell_trigger: Optional[float] = None
    half_sell_date: Optional[str] = None
    half_sell_price: Optional[float] = None
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    position_pct: Optional[float] = None
    notes: Optional[str] = None


class TradePatch(BaseModel):
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    half_sell_date: Optional[str] = None
    half_sell_price: Optional[float] = None
    notes: Optional[str] = None


@router.get("", response_model=list[TradeRead])
def list_trades(trade_id: Optional[str] = None):
    """查询交易记录"""
    con = get_db()
    try:
        if trade_id:
            rows = con.execute(
                "SELECT * FROM trade_execution WHERE trade_id = ?", [trade_id]
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM trade_execution ORDER BY entry_date DESC"
            ).fetchall()
        columns = [desc[0] for desc in con.description]
        return [row_to_dict(r, columns) for r in rows]
    finally:
        con.close()


@router.post("", response_model=TradeRead)
def create_trade(body: TradeWrite):
    """新建交易记录（自动生成 trade_id）"""
    trade_id = uuid4().hex[:12].upper()
    con = get_db(read_only=False)
    try:
        con.execute(
            """INSERT INTO trade_execution
               (trade_id, ticker, entry_date, entry_price, path_type,
                half_sell_trigger, half_sell_date, half_sell_price,
                exit_date, exit_price, position_pct, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                trade_id,
                body.ticker,
                body.entry_date,
                body.entry_price,
                body.path_type,
                body.half_sell_trigger,
                body.half_sell_date,
                body.half_sell_price,
                body.exit_date,
                body.exit_price,
                body.position_pct,
                body.notes,
            ],
        )
        row = con.execute(
            "SELECT * FROM trade_execution WHERE trade_id = ?", [trade_id]
        ).fetchone()
        columns = [desc[0] for desc in con.description]
        return row_to_dict(row, columns)
    finally:
        con.close()


@router.patch("/{trade_id}", response_model=TradeRead)
def patch_trade(trade_id: str, body: TradePatch):
    """更新交易记录（只传要改的字段）"""
    updates = {}
    if body.exit_date is not None:
        updates["exit_date"] = body.exit_date
    if body.exit_price is not None:
        updates["exit_price"] = body.exit_price
    if body.half_sell_date is not None:
        updates["half_sell_date"] = body.half_sell_date
    if body.half_sell_price is not None:
        updates["half_sell_price"] = body.half_sell_price
    if body.notes is not None:
        updates["notes"] = body.notes

    if not updates:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    params = list(updates.values()) + [trade_id]

    con = get_db(read_only=False)
    try:
        con.execute(
            f"UPDATE trade_execution SET {set_clause} WHERE trade_id = ?", params
        )
        if con.execute(
            "SELECT COUNT(*) FROM trade_execution WHERE trade_id = ?", [trade_id]
        ).fetchone()[0] == 0:
            raise HTTPException(status_code=404, detail="交易记录不存在")

        row = con.execute(
            "SELECT * FROM trade_execution WHERE trade_id = ?", [trade_id]
        ).fetchone()
        columns = [desc[0] for desc in con.description]
        return row_to_dict(row, columns)
    finally:
        con.close()
