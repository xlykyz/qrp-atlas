"""backfill_daily_basic_bulk.py - 批量回补全部历史 daily_basic 数据

两阶段策略：
  1. fetch — 只拉 API 存 raw CSV，纯网络耗时 ~80 分钟
  2. load  — 读取全部 raw CSV，批量 clean + 一次 DuckDB 入库，几秒完成

支持断点续传。
"""

import os
import time
import duckdb
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

from qrp_atlas.config import DB_PATH, RAW_DIR as QRP_RAW_DIR, STATE_DIR, get_tushare_pro
from qrp_atlas.pipeline.daily_basic.clean import clean_daily_basic
from qrp_atlas.contracts import (
    CREATED_AT, DAILY_BASIC, align_to_schema, quick_validate,
)
from qrp_atlas.contracts.schema import init_database

RAW_DIR = str(QRP_RAW_DIR / "daily_basic_raw")
CHECKPOINT_FILE = str(STATE_DIR / ".daily_basic_backfill_checkpoint")


def load_checkpoint() -> date | None:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            raw = f.read().strip()
            return date.fromisoformat(raw) if raw else None
    return None


def save_checkpoint(d: date):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(d.isoformat())


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


def raw_csv_path(trade_date: date) -> str:
    return f"{RAW_DIR}/{trade_date.isoformat()}.csv"


def fetch_all():
    """逐日 fetch，存 raw CSV"""
    con = duckdb.connect(str(DB_PATH))
    all_days = con.execute(
        "SELECT DISTINCT trade_date FROM daily_market_snapshot ORDER BY trade_date"
    ).fetchdf()
    earliest_db = con.execute("SELECT MIN(trade_date) FROM daily_basic").fetchone()[0]
    con.close()

    all_dates = [d.date() if hasattr(d, "date") else d for d in all_days["trade_date"]]
    earliest_db = earliest_db.date() if hasattr(earliest_db, "date") else earliest_db
    start_idx = all_dates.index(earliest_db)
    to_fetch = list(reversed(all_dates[:start_idx]))
    total = len(to_fetch)
    print(f"需要 fetch: {total} 天，范围: {all_dates[0]} ~ {earliest_db - timedelta(days=1)}")

    # 断点续传
    checkpoint = load_checkpoint()
    if checkpoint:
        try:
            cp_idx = next(i for i, d in enumerate(to_fetch) if d <= checkpoint)
            to_fetch = to_fetch[cp_idx:]
            print(f"断点续传: 从 {checkpoint} 继续 ({len(to_fetch)} 天剩余)")
        except StopIteration:
            print("checkpoint 不在列表中，从头开始")
            clear_checkpoint()

    os.makedirs(RAW_DIR, exist_ok=True)
    pro = get_tushare_pro()
    success = 0
    fail = 0
    consecutive_empty = 0
    start_time = time.time()

    for i, d in enumerate(to_fetch):
        raw_path = raw_csv_path(d)
        if os.path.exists(raw_path):
            save_checkpoint(d)
            success += 1
            continue

        date_str = d.strftime("%Y%m%d")
        try:
            df = pro.daily_basic(trade_date=date_str)
            if df is None or df.empty:
                consecutive_empty += 1
                print(f"[EMPTY] {d} (连续 {consecutive_empty} 天)")
                save_checkpoint(d)
                if consecutive_empty >= 5:
                    print(f"[BOUNDARY] 连续 5 天空数据，停止")
                    break
                continue
            consecutive_empty = 0
            df.to_csv(raw_path, index=False, encoding="utf-8")
            success += 1
            save_checkpoint(d)
        except Exception as e:
            print(f"[FAIL] {d}: {e}")
            time.sleep(2)
            try:
                df = pro.daily_basic(trade_date=date_str)
                if df is not None and not df.empty:
                    df.to_csv(raw_path, index=False, encoding="utf-8")
                    success += 1
                    save_checkpoint(d)
                    consecutive_empty = 0
                    continue
            except Exception:
                pass
            fail += 1
            save_checkpoint(d)

        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed * 60
            print(f"[PROGRESS] {i+1}/{total} ({success} ok, {fail} fail, {rate:.0f} 天/分钟, 当前: {d})")

    elapsed = time.time() - start_time
    print(f"\n[FETCH DONE] {success} ok, {fail} fail, 耗时 {elapsed/60:.0f} 分钟")
    print(f"运行以下命令入库: python scripts/backfill_daily_basic_bulk.py load")


def load_all():
    """读取全部 raw CSV，批量 clean + 一次性入库"""
    raw_dir = Path(RAW_DIR)
    raw_files = sorted(raw_dir.glob("*.csv"))
    if not raw_files:
        print("❌ 无 raw CSV 文件")
        return

    print(f"找到 {len(raw_files)} 个 raw CSV，开始批量入库...")

    total_loaded = 0
    con = duckdb.connect(str(DB_PATH))
    init_database(con)

    for batch_start in range(0, len(raw_files), 500):
        batch_files = raw_files[batch_start:batch_start + 500]
        dfs = [pd.read_csv(f) for f in batch_files]
        raw_df = pd.concat(dfs, ignore_index=True)
        clean_df = clean_daily_basic(raw_df)

        if clean_df.empty:
            continue

        clean_df = align_to_schema(clean_df, DAILY_BASIC.name,
                                   fill_missing_optional=True, drop_extra=True)
        clean_df = quick_validate(clean_df, DAILY_BASIC.name, allow_extra=False)

        dates_in_batch = clean_df["trade_date"].unique().tolist()
        con.execute("BEGIN")
        try:
            for td in dates_in_batch:
                con.execute("DELETE FROM daily_basic WHERE trade_date = ?", [td])
            cols = [c for c in DAILY_BASIC.column_names() if c != CREATED_AT]
            col_names = ", ".join(cols)
            con.register("tmp_df", clean_df)
            con.execute(f"INSERT INTO daily_basic ({col_names}) SELECT {col_names} FROM tmp_df")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total_loaded += len(clean_df)
        print(f"[LOAD] batch {batch_start//500 + 1}/{(len(raw_files)-1)//500 + 1}: {len(clean_df)} rows → {total_loaded} total")

    con.close()
    con = duckdb.connect(str(DB_PATH))
    r = con.execute("SELECT MIN(trade_date)::VARCHAR, MAX(trade_date)::VARCHAR, COUNT(DISTINCT trade_date), COUNT(*) FROM daily_basic").fetchone()
    con.close()
    print(f"\n[LOAD DONE] daily_basic: {r[0]} ~ {r[1]}, {r[2]} 天, {r[3]} 行")
    clear_checkpoint()


def main():
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "load":
        load_all()
    else:
        fetch_all()


if __name__ == "__main__":
    main()