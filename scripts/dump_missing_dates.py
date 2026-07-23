"""Query trading calendar and save pre-2013 trading dates"""
import duckdb
from qrp_atlas.config import DATA_DIR, DB_PATH

OUTPUT = DATA_DIR / "historical_dates_to_fill.txt"

con = duckdb.connect(str(DB_PATH))

# Get all trading days before 2013-01-01 that are open
rows = con.execute("""
    SELECT trade_date 
    FROM trading_calendar 
    WHERE trade_date < DATE '2013-01-01' 
      AND is_open = true
    ORDER BY trade_date
""").fetchall()

dates = [r[0].strftime("%Y-%m-%d") for r in rows]

# Also check: already have data for some of these dates?
existing = set()
for d in dates:
    cnt = con.execute(
        "SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date = ?", [d]
    ).fetchone()[0]
    if cnt > 0:
        existing.add(d)

con.close()

# Write all dates to file
with open(OUTPUT, "w") as f:
    for d in dates:
        f.write(d + "\n")

print(f"交易日历范围: {dates[0]} ~ {dates[-1]}")
print(f"2013年前交易日总数: {len(dates)}")
print(f"其中已有数据的日期: {len(existing)}")
print(f"需要补的日期: {len(dates) - len(existing)}")
print(f"\n文件已写入: {OUTPUT}")
print(f"文件大小: {OUTPUT.stat().st_size} 字节")

# Show sample
print(f"\n前10个: {dates[:10]}")
print(f"后10个: {dates[-10:]}")
