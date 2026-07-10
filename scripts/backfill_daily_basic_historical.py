"""backfill_daily_basic_historical.py - 向前回补全部历史 daily_basic 数据

从已有最早交易日向前逐日回补，直到第一个交易日。
支持 checkpoint 断点续传。
"""

import os
import duckdb
from datetime import date, timedelta

from qrp_atlas.pipeline.daily_basic.run import run_for_date


CHECKPOINT_FILE = "data/db/.daily_basic_backfill_checkpoint"


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


def main():
    con = duckdb.connect("data/db/quant.db")
    all_trading_days = con.execute(
        "SELECT DISTINCT trade_date FROM daily_market_snapshot ORDER BY trade_date"
    ).fetchdf()
    earliest_market = con.execute("SELECT MIN(trade_date) FROM daily_market_snapshot").fetchone()[0]
    earliest_db = con.execute("SELECT MIN(trade_date) FROM daily_basic").fetchone()[0]
    con.close()

    all_dates = [d.date() if hasattr(d, "date") else d for d in all_trading_days["trade_date"]]
    earliest_market = earliest_market.date() if hasattr(earliest_market, "date") else earliest_market

    print(f"daily_market_snapshot 范围: {earliest_market} ~ {all_dates[-1]}")
    print(f"daily_basic 已有最早: {earliest_db}")
    print(f"总交易日: {len(all_dates)}")

    # 从 earliest_db 的前一个交易日开始，一直向前到 earliest_market
    # 找到 earliest_db 在 all_dates 中的位置
    start_idx = all_dates.index(earliest_db)
    to_backfill = list(reversed(all_dates[:start_idx]))
    total = len(to_backfill)
    print(f"需要回补: {total} 天")

    # 检查 checkpoint
    checkpoint = load_checkpoint()
    if checkpoint:
        # 找到 checkpoint 的位置
        try:
            cp_idx = next(i for i, d in enumerate(to_backfill) if d <= checkpoint)
            to_backfill = to_backfill[cp_idx:]
            print(f"断点续传: 从 {checkpoint} 继续 ({len(to_backfill)} 天剩余)")
        except StopIteration:
            print(f"checkpoint {checkpoint} 不在待补列表中，从头开始")
            clear_checkpoint()

    success = 0
    fail = 0
    consecutive_empty = 0

    for i, d in enumerate(to_backfill):
        try:
            run_for_date(d, init_db=(i == 0))
            success += 1
            save_checkpoint(d)
            consecutive_empty = 0
        except ValueError as e:
            # 可能是真实边界或瞬断，直接调用 API 二次确认
            msg = str(e).lower()
            if "empty" in msg or "no data" in msg:
                # 二次确认：直接调用 API 验证
                from qrp_atlas.config import get_tushare_pro
                try:
                    pro = get_tushare_pro()
                    verify = pro.daily_basic(trade_date=d.strftime("%Y%m%d"))
                    if verify is not None and not verify.empty:
                        print(f"[RETRY] {d}: 瞬断误判，重试 fetch...")
                        run_for_date(d)
                        success += 1
                        save_checkpoint(d)
                        consecutive_empty = 0
                        continue
                except Exception:
                    pass
                # 真·空数据
                consecutive_empty += 1
                print(f"[EMPTY] {d}: 无数据 (连续 {consecutive_empty} 天)")
                save_checkpoint(d)
                if consecutive_empty >= 5:
                    print(f"[BOUNDARY] 连续 5 天空数据，达到时间边界，停止")
                    break
                continue
            else:
                # 其他 ValueError，重试
                print(f"[FAIL] {d}: {e}")
                try:
                    run_for_date(d)
                    success += 1
                    save_checkpoint(d)
                    consecutive_empty = 0
                except Exception as e2:
                    print(f"[FAIL] {d} 重试仍失败: {e2}")
                    fail += 1
                    save_checkpoint(d)
        except Exception as e:
            # 网络/SSL/超时 等异常
            print(f"[FAIL] {d}: {e}")
            try:
                run_for_date(d)
                success += 1
                save_checkpoint(d)
                consecutive_empty = 0
            except Exception as e2:
                print(f"[FAIL] {d} 重试仍失败: {e2}")
                fail += 1
                save_checkpoint(d)

        if (i + 1) % 50 == 0:
            print(f"[PROGRESS] {i+1}/{total} ({success} ok, {fail} fail, 当前: {d})")

    print(f"\n[DONE] 本次回补完成")
    print(f"  OK: {success}")
    print(f"  FAIL: {fail}")

    if fail > 0 or consecutive_empty > 0:
        print(f"  未完成，可再次运行脚本继续")
    else:
        clear_checkpoint()
        print(f"  ✅ 全部完成，checkpoint 已清除")

    con = duckdb.connect("data/db/quant.db")
    r = con.execute("SELECT MIN(trade_date)::VARCHAR, MAX(trade_date)::VARCHAR, COUNT(DISTINCT trade_date) FROM daily_basic").fetchone()
    con.close()
    print(f"  daily_basic 当前: {r[0]} ~ {r[1]}, {r[2]} 天")


if __name__ == "__main__":
    main()