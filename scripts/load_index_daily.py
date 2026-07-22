"""
load_index_daily.py — 指数 CSV → DuckDB 入库

从 data/raw/ 读取指数 CSV 文件，清洗后 upsert 到 index_daily 表。
"""

from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, RAW_DIR

# CSV 文件名 → (index_code, index_name)
INDEX_FILES = {
    "sh000001_上证综指.csv": ("sh000001", "上证综指"),
    "sz399001_深证成指.csv": ("sz399001", "深证成指"),
    "sz399006_创业板指.csv": ("sz399006", "创业板指"),
    "sh000688_科创50.csv":   ("sh000688", "科创50"),
}

EXPECTED_COLUMNS = ["trade_date", "index_code", "index_name", "open", "high", "low", "close", "volume"]

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


def clean_file(file_path: Path, index_code: str, index_name: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)

    df = df.rename(columns={"date": "trade_date"})
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["index_code"] = index_code
    df["index_name"] = index_name

    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    df = df[EXPECTED_COLUMNS]
    df = df.sort_values(["trade_date", "index_code"])

    # drop rows where open/close is NaN (shouldn't happen but guard)
    df = df.dropna(subset=["open", "close"])

    return df


def load_all() -> int:
    total = 0
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            for filename, (index_code, index_name) in INDEX_FILES.items():
                file_path = RAW_DIR / filename
                if not file_path.exists():
                    print(f"  ⚠ 跳过: {filename} 不存在")
                    continue
                df = clean_file(file_path, index_code, index_name)
                con.register("df", df)
                con.execute(UPSERT_SQL)
                con.unregister("df")
                print(f"  ✓ {filename} → {len(df)} 条")
                total += len(df)
            con.execute("COMMIT")
            print(f"  ✓ 共入库 {total} 条")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return total


if __name__ == "__main__":
    load_all()
