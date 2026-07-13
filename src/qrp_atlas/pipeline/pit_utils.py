"""point-in-time helpers shared by financial / industry / index pipelines."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping, Sequence

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH
from qrp_atlas.contracts import IS_OPEN, TRADE_DATE

SOURCE_TUSHARE = "tushare"


def to_date(value) -> date | None:
    """Normalize YYYYMMDD / datetime / date / str to date."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date()
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "nat", "null"}:
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return pd.to_datetime(text).date()


def normalize_date_series(series: pd.Series) -> pd.Series:
    return series.map(to_date)


def stable_hash(parts: Sequence[object], *, length: int = 16) -> str:
    """Stable, reproducible content/business hash (not random UUID)."""
    payload = "\u001f".join("" if p is None or (isinstance(p, float) and pd.isna(p)) else str(p) for p in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:length]


def load_open_trade_dates(db_path: str | None = None) -> list[date]:
    """Load ascending open trade dates from local trading_calendar."""
    path = str(db_path or DB_PATH)
    con = duckdb.connect(path, read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT {TRADE_DATE}
            FROM trading_calendar
            WHERE COALESCE({IS_OPEN}, TRUE)
            ORDER BY {TRADE_DATE}
            """
        ).fetchall()
    finally:
        con.close()
    return [to_date(r[0]) for r in rows if to_date(r[0]) is not None]


class NextTradeDateResolver:
    """Map announcement/event dates to the next open trade date (strictly later)."""

    def __init__(self, open_dates: Sequence[date] | None = None, *, db_path: str | None = None):
        if open_dates is None:
            open_dates = load_open_trade_dates(db_path)
        self.open_dates = sorted({d for d in open_dates if d is not None})
        if not self.open_dates:
            raise ValueError("open trade dates are empty; trading_calendar is required")

    def next_trade_date(self, event_date: date | str | None) -> date | None:
        d = to_date(event_date)
        if d is None:
            return None
        # local calendar only stores open days; search first open day strictly after d
        lo, hi = 0, len(self.open_dates)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.open_dates[mid] <= d:
                lo = mid + 1
            else:
                hi = mid
        if lo >= len(self.open_dates):
            # fall back: if calendar ends, use +1 calendar day heuristic only for tests with synthetic calendars
            return d + timedelta(days=1)
        return self.open_dates[lo]

    def map_series(self, series: pd.Series) -> pd.Series:
        return series.map(self.next_trade_date)


def choose_announcement_date(ann_date, f_ann_date=None) -> date | None:
    """Prefer actual announcement date f_ann_date, then ann_date."""
    return to_date(f_ann_date) or to_date(ann_date)


def empty_to_none(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def content_signature(row: Mapping, columns: Iterable[str]) -> str:
    parts = []
    for col in columns:
        val = empty_to_none(row.get(col))
        if isinstance(val, (datetime, date, pd.Timestamp)):
            val = to_date(val)
            val = val.isoformat() if val else ""
        elif isinstance(val, float):
            # stabilize float text
            val = f"{val:.10g}"
        parts.append(val)
    return stable_hash(parts, length=20)


def append_only_insert(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    *,
    id_column: str = "revision_id",
) -> int:
    """Insert rows whose id_column is not already present. Returns inserted count."""
    if df is None or df.empty:
        return 0
    ids = df[id_column].astype(str).tolist()
    existing: set[str] = set()
    # chunk IN lists
    chunk = 500
    for i in range(0, len(ids), chunk):
        part = ids[i : i + chunk]
        placeholders = ", ".join(["?"] * len(part))
        rows = con.execute(
            f"SELECT {id_column} FROM {table_name} WHERE {id_column} IN ({placeholders})",
            part,
        ).fetchall()
        existing.update(str(r[0]) for r in rows)
    new_df = df[~df[id_column].astype(str).isin(existing)].copy()
    if new_df.empty:
        return 0
    con.register("tmp_pit_df", new_df)
    cols = list(new_df.columns)
    col_sql = ", ".join(cols)
    con.execute(f"INSERT INTO {table_name} ({col_sql}) SELECT {col_sql} FROM tmp_pit_df")
    return len(new_df)
