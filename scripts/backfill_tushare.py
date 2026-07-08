"""
backfill_tushare.py - 批量回补 2013~2026-02-13 日线原始数据

跳过已有 raw 文件的日期, 仅补缺失的。不影响 DB 已有数据。

用法:
    python scripts/backfill_tushare.py 2>&1 | tee logs/backfill.log
"""

import sys
import time
from datetime import date

import duckdb
from qrp_atlas.config import DB_PATH
from qrp_atlas.pipeline.daily_update.run import run_for_date

START = date(2013, 1, 4)
END = date(2026, 2, 13)


def main():
    # 1. 获取交易日历
    con = duckdb.connect(str(DB_PATH))
    all_dates = [
        r[0] for r in con.execute(
            "SELECT trade_date FROM trading_calendar "
            "WHERE is_open = true AND trade_date BETWEEN ? AND ? "
            "ORDER BY trade_date",
            [START, END]
        ).fetchall()
    ]
    con.close()

    # 2. 过滤掉已有 raw 文件的日期
    from qrp_atlas.config import DAILY_SNAPSHOT_RAW_DIR
    dates = []
    for d in all_dates:
        raw_path = DAILY_SNAPSHOT_RAW_DIR / str(d.year) / f"{d.isoformat()}_Astock_tushare.csv"
        if not raw_path.exists():
            dates.append(d)

    total_all = len(all_dates)
    total = len(dates)
    skipped = total_all - total
    print(f"[BACKFILL] 交易日历共 {total_all} 天，已有 raw 跳过 {skipped} 天，需补 {total} 天")

    # 3. 逐日回补
    t_start = time.time()
    errors = []

    for i, d in enumerate(dates, 1):
        try:
            run_for_date(d)
        except Exception as e:
            errors.append((d, str(e)))
            print(f"[BACKFILL] ❌ {d} 失败: {e}")

        if i % 100 == 0 or i == total:
            elapsed = time.time() - t_start
            avg = elapsed / i
            remaining = (total - i) * avg
            print(
                f"[BACKFILL] 进度 {i}/{total} ({100*i/total:.0f}%) | "
                f"已用 {elapsed/60:.0f}min | "
                f"预计剩余 {remaining/60:.0f}min"
            )

    # 4. 汇总
    t_elapsed = time.time() - t_start
    print(f"\n[BACKFILL] 🏁 完成！")
    print(f"  总耗时: {t_elapsed/60:.1f}min")
    print(f"  成功: {total - len(errors)}/{total}")
    if errors:
        print(f"  失败: {len(errors)} 天")
        for d, e in errors[:10]:
            print(f"    {d}: {e}")
        if len(errors) > 10:
            print(f"    ... 共 {len(errors)} 个失败")


if __name__ == "__main__":
    main()