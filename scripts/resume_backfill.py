"""
resume_backfill.py - 续跑日线回补，从已有数据最大日期后开始
"""

import sys
import os
import time
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import duckdb
from qrp_atlas.config import DB_PATH
from qrp_atlas.pipeline.daily_update.run import run_for_date

END = date(2026, 2, 13)


def main():
    # 1. 获取已补到哪
    con = duckdb.connect(str(DB_PATH))
    last = con.execute(
        "SELECT MAX(trade_date) FROM daily_market_snapshot "
        "WHERE trade_date BETWEEN '2013-01-01' AND ?",
        [END]
    ).fetchone()[0]
    con.close()

    if last and last >= END:
        print(f"[RESUME] 数据已完整至 {END}，无需回补 ✅")
        return

    start = last if last else date(2013, 1, 4)
    # 从已补到的下一天开始
    con = duckdb.connect(str(DB_PATH))
    dates = [
        r[0] for r in con.execute(
            "SELECT trade_date FROM trading_calendar "
            "WHERE is_open = true AND trade_date > ? AND trade_date <= ? "
            "ORDER BY trade_date",
            [start, END]
        ).fetchall()
    ]
    con.close()
    total = len(dates)
    print(f"[RESUME] 从 {start} 之后开始，共 {total} 个交易日: {dates[0]} ~ {dates[-1]}")

    t_start = time.time()
    errors = []

    for i, d in enumerate(dates, 1):
        try:
            run_for_date(d)
        except Exception as e:
            errors.append((d, str(e)))
            print(f"[RESUME] ❌ {d} 失败: {e}")

        if i % 100 == 0 or i == total:
            elapsed = time.time() - t_start
            avg = elapsed / i
            remaining = (total - i) * avg
            print(
                f"[RESUME] 进度 {i}/{total} ({100*i/total:.0f}%) | "
                f"已用 {elapsed/60:.0f}min | "
                f"预计剩余 {remaining/60:.0f}min"
            )

    t_elapsed = time.time() - t_start
    print(f"\n[RESUME] 🏁 完成！")
    print(f"  总耗时: {t_elapsed/60:.1f}min")
    print(f"  成功: {total - len(errors)}/{total}")
    if errors:
        print(f"  失败: {len(errors)} 天")
        for d, e in errors[:10]:
            print(f"    {d}: {e}")


if __name__ == "__main__":
    main()