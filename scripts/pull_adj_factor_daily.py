"""
pull_adj_factor_daily.py - 按日期增量更新复权因子

用法:
    python scripts/pull_adj_factor_daily.py
"""

import sys
import os
import time
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import duckdb
import pandas as pd
from qrp_atlas.config import get_tushare_pro, DB_PATH


def main():
    t0 = time.time()
    print("=" * 50)
    print(f"按日期增量更新复权因子 @ {date.today()}")

    con = duckdb.connect(str(DB_PATH))
    pro = get_tushare_pro()

    # 获取最新日期
    last_date = con.execute("SELECT MAX(trade_date) FROM adj_factor_changes").fetchone()[0]
    print(f"复权因子最新日期: {last_date}")

    # 之后的所有交易日
    dates = [
        r[0] for r in con.execute(
            "SELECT trade_date FROM trading_calendar "
            "WHERE is_open = true AND trade_date > ? "
            "ORDER BY trade_date",
            [last_date]
        ).fetchall()
    ]
    if not dates:
        print("没有需要更新的日期 ✅")
        con.close()
        return

    print(f"需更新 {len(dates)} 个交易日: {dates[0]} ~ {dates[-1]}")

    # 获取每只股票的最新 adj_factor
    last_known = dict(
        con.execute(
            "SELECT ticker, adj_factor FROM ("
            "  SELECT ticker, adj_factor, ROW_NUMBER() OVER ("
            "    PARTITION BY ticker ORDER BY trade_date DESC"
            "  ) AS rn FROM adj_factor_changes"
            ") WHERE rn = 1"
        ).fetchall()
    )
    print(f"已有 {len(last_known)} 只股票的复权因子")

    total_inserted = 0
    errors = []

    for i, d in enumerate(dates, 1):
        try:
            date_str = d.strftime("%Y%m%d")
            df = pro.adj_factor(trade_date=date_str)

            if df is None or len(df) == 0:
                continue

            # 筛选有变化的股票
            changes = []
            for _, row in df.iterrows():
                tid = row["ts_code"]
                new_val = row["adj_factor"]
                old_val = last_known.get(tid)
                if old_val is None or abs(new_val - old_val) > 1e-9:
                    changes.append((str(tid), d, float(new_val)))
                    last_known[tid] = new_val

            if changes:
                chunk = pd.DataFrame(changes, columns=["ticker", "trade_date", "adj_factor"])
                con.register("_adj_tmp", chunk)
                con.execute("INSERT OR REPLACE INTO adj_factor_changes SELECT * FROM _adj_tmp")
                con.unregister("_adj_tmp")
                total_inserted += len(changes)

            if i % 30 == 0 or i == len(dates):
                elapsed = time.time() - t0
                eta = (len(dates) - i) * (elapsed / i)
                print(f"  [{i}/{len(dates)}] {d} | 新增 {total_inserted} 条 | "
                      f"已用 {elapsed:.0f}s | 预估剩余 {eta:.0f}s")

        except Exception as e:
            errors.append((d, str(e)))
            print(f"  ❌ {d}: {e}")

    elapsed = time.time() - t0
    print(f"\n✅ 完成!  耗时 {elapsed:.0f}s | 新增变化点 {total_inserted} 条")

    if errors:
        print(f"失败: {len(errors)} 天")
        for d, e in errors[:5]:
            print(f"  {d}: {e}")

    cnt = con.execute("SELECT COUNT(*) FROM adj_factor_changes").fetchone()[0]
    max_d = con.execute("SELECT MAX(trade_date) FROM adj_factor_changes").fetchone()[0]
    min_d = con.execute("SELECT MIN(trade_date) FROM adj_factor_changes").fetchone()[0]
    print(f"数据库: {cnt} 条, {min_d} ~ {max_d}")
    con.close()


if __name__ == "__main__":
    main()