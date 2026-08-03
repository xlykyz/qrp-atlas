"""Tushare index daily bars -> DuckDB incremental loader."""

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.config import get_settings
from qrp_atlas.config.tushare_client import get_tushare_pro

INDICES = [
    ("000001.SH", "上证综指"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000688.SH", "科创50"),
]
CHINA_TZ = ZoneInfo("Asia/Shanghai")

UPSERT_SQL = """
INSERT INTO index_daily (
    trade_date, index_code, index_name, open, high, low, close,
    pre_close, change, pct_change, volume, amount
)
SELECT
    trade_date, index_code, index_name, open, high, low, close,
    pre_close, change, pct_change, volume, amount
FROM df
ON CONFLICT (trade_date, index_code) DO UPDATE SET
    index_name = excluded.index_name,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    pre_close = excluded.pre_close,
    change = excluded.change,
    pct_change = excluded.pct_change,
    volume = excluded.volume,
    amount = excluded.amount
"""


def get_latest_date_in_db(con, index_code: str) -> str | None:
    row = con.execute(
        "SELECT MAX(trade_date) FROM index_daily WHERE index_code = ?",
        [index_code]
    ).fetchone()
    return str(row[0]) if row[0] else None


def fetch_and_update(db_path: Path | None = None) -> int:
    settings = get_settings()
    effective_db_path = Path(db_path or settings.paths.duckdb_path)
    target_date = datetime.now(CHINA_TZ).date()
    client = get_tushare_pro(settings=settings)
    frames: list[pd.DataFrame] = []

    with duckdb.connect(str(effective_db_path), read_only=True) as con:
        latest_dates = {
            index_code: get_latest_date_in_db(con, index_code)
            for index_code, _ in INDICES
        }

    for index_code, index_name in INDICES:
        latest = latest_dates[index_code]
        print(f"  [{index_name}] 库里最新日期: {latest}")
        request = {"ts_code": index_code, "end_date": target_date.strftime("%Y%m%d")}
        if latest:
            request["start_date"] = (
                date.fromisoformat(latest) + timedelta(days=1)
            ).strftime("%Y%m%d")
        raw = client.index_daily(**request)
        if raw is None or raw.empty:
            print("  ✓ 无新增数据")
            continue
        required = {
            "ts_code", "trade_date", "open", "high", "low", "close",
            "pre_close", "change", "pct_chg", "vol", "amount",
        }
        missing = sorted(required - set(raw.columns))
        if missing:
            raise RuntimeError(f"{index_code} index_daily missing fields: {missing}")
        df = raw.rename(
            columns={"ts_code": "index_code", "pct_chg": "pct_change", "vol": "volume"}
        ).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise").dt.date
        df["index_code"] = df["index_code"].astype(str).str.strip().str.upper()
        df = df.loc[(df["trade_date"] <= target_date) & (df["index_code"] == index_code)].copy()
        if latest:
            df = df.loc[df["trade_date"] > date.fromisoformat(latest)].copy()
        if df.empty:
            print("  ✓ 无新增数据")
            continue
        df["index_name"] = index_name
        df["volume"] = pd.to_numeric(df["volume"], errors="raise").astype("int64")
        for column in ("open", "high", "low", "close", "pre_close", "change", "pct_change", "amount"):
            df[column] = pd.to_numeric(df[column], errors="raise")
        frames.append(
            df[
                [
                    "trade_date", "index_code", "index_name", "open", "high", "low", "close",
                    "pre_close", "change", "pct_change", "volume", "amount",
                ]
            ]
        )
        print(f"  ✓ 新增 {len(df)} 条")

    total_new = sum(len(frame) for frame in frames)
    if not frames:
        return 0
    prepared = pd.concat(frames, ignore_index=True)
    with duckdb.connect(str(effective_db_path)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            con.register("df", prepared)
            con.execute(UPSERT_SQL)
            con.unregister("df")
            con.execute("COMMIT")
        except Exception:
            try:
                con.unregister("df")
            except Exception:
                pass
            con.execute("ROLLBACK")
            raise
    return total_new


def main():
    print(f"指数日更开始...")
    total = fetch_and_update()
    print(f"完成，共新增 {total} 条")


if __name__ == "__main__":
    main()
