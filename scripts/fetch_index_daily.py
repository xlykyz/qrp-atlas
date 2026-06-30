"""
fetch_index_daily.py — 指数日更脚本

每日运行：从 AKshare 拉取指数最新数据并 upsert 到 DuckDB。
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
import akshare as ak

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db" / "quant.db"

INDICES = [
    ("sh000001", "上证综指"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000688", "科创50"),
]

UPSERT_SQL = """
INSERT INTO index_daily (trade_date, index_code, index_name, open, high, low, close, volume)
SELECT trade_date, index_code, index_name, open, high, low, close, volume FROM df
ON CONFLICT (trade_date, index_code) DO UPDATE SET
    index_name = excluded.index_name,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume
"""


def get_latest_date_in_db(con, index_code: str) -> str | None:
    row = con.execute(
        "SELECT MAX(trade_date) FROM index_daily WHERE index_code = ?",
        [index_code]
    ).fetchone()
    return str(row[0]) if row[0] else None


def fetch_and_update():
    total_new = 0
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            for index_code, index_name in INDICES:
                latest = get_latest_date_in_db(con, index_code)
                print(f"  [{index_name}] 库里最新日期: {latest}")

                df = ak.stock_zh_index_daily(symbol=index_code)
                df = df.rename(columns={"date": "trade_date"})
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
                df["index_code"] = index_code
                df["index_name"] = index_name
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

                if latest:
                    df = df[df["trade_date"] > pd.Timestamp(latest).date()]

                if df.empty:
                    print(f"  ✓ 无新增数据")
                    continue

                df = df[["trade_date", "index_code", "index_name", "open", "high", "low", "close", "volume"]]
                con.register("df", df)
                con.execute(UPSERT_SQL)
                con.unregister("df")
                print(f"  ✓ 新增 {len(df)} 条")
                total_new += len(df)

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return total_new


def main():
    print(f"指数日更开始...")
    total = fetch_and_update()
    print(f"完成，共新增 {total} 条")


if __name__ == "__main__":
    main()
