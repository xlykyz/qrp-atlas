"""verify_backfill.py - 全量验证日线回补完整性

输出格式: 每行 "日期：行数（股票数）"
缺失日期显示 "❌ 日期"
"""

import duckdb

from qrp_atlas.config import DB_PATH
START = "2014-05-14"
END = "2026-02-13"

con = duckdb.connect(str(DB_PATH))

dates = con.execute(
    "SELECT trade_date::DATE FROM trading_calendar "
    "WHERE is_open = true AND trade_date BETWEEN ? AND ? "
    "ORDER BY trade_date",
    [START, END]
).fetchall()

ok, missing = 0, 0
for (d,) in dates:
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT ticker) "
        "FROM daily_market_snapshot "
        "WHERE trade_date::DATE = ?",
        [d]
    ).fetchone()
    if row[0] > 0:
        print(f"{d}：{row[0]}（{row[1]}）")
        ok += 1
    else:
        print(f"❌ {d}")
        missing += 1

print(f"\n共计 {len(dates)} 个交易日，正常 {ok}，缺失 {missing}")
con.close()