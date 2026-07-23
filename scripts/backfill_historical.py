"""backfill_historical.py - 回填 1990~2012 历史日线数据

用法:
    python scripts/backfill_historical.py              # 全量回填
    python scripts/backfill_historical.py --resume     # 断点续传
    python scripts/backfill_historical.py --dry-run    # 仅打印日期，不执行

流程:
    逐天调用 pipeline daily_update.run_for_date()
    进度记录到 data/.backfill_progress.json
    失败自动重试3次，单天失败不中断
"""
import sys
import os
import json
import time
import traceback
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qrp_atlas.config import DATA_DIR, LOG_DIR, STATE_DIR

# --- 配置 ---
DATES_FILE = DATA_DIR / "historical_dates_to_fill.txt"
PROGRESS_FILE = STATE_DIR / ".backfill_progress.json"
LOG_FILE = LOG_DIR / "backfill_history.log"

API_INTERVAL = 0.3   # API 调用间隔（秒）
MAX_RETRIES = 3
RETRY_DELAY = 5


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_progress() -> set:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return set(json.load(f))
    return set()


def save_progress(done: set, current: str, total: int):
    done.add(current)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(done), f)
    pct = len(done) / total * 100
    log(f"[{len(done)}/{total}] {pct:.1f}% ✅ {current}")


def run_single_date(trade_date_str: str) -> bool:
    """执行单天回填，成功返回 True"""
    from qrp_atlas.pipeline.daily_update.run import run_for_date
    from datetime import date as dt_date

    trade_date = dt_date.fromisoformat(trade_date_str)
    run_for_date(trade_date)
    return True


def main():
    dry_run = "--dry-run" in sys.argv
    resume = "--resume" in sys.argv

    # 读日期列表
    with open(DATES_FILE) as f:
        all_dates = [line.strip() for line in f if line.strip()]

    total = len(all_dates)
    log(f"📅 共 {total} 个交易日待回填")

    if dry_run:
        log(f"🧪 DRY RUN - 首日: {all_dates[0]}, 末日: {all_dates[-1]}")
        return

    # 加载已完成
    done = load_progress() if resume else set()
    if done:
        remaining = [d for d in all_dates if d not in done]
        log(f"⏸ 已有 {len(done)} 天完成，剩余 {len(remaining)} 天")
    else:
        remaining = all_dates

    t0 = time.time()
    errors = []

    for i, date_str in enumerate(remaining):
        attempt = 0
        success = False
        while attempt < MAX_RETRIES and not success:
            try:
                run_single_date(date_str)
                success = True
            except Exception as e:
                attempt += 1
                if attempt < MAX_RETRIES:
                    log(f"⚠️ {date_str} 第{attempt}次失败: {e}，{RETRY_DELAY}s后重试")
                    # 打印详细错误到日志方便排查
                    with open(LOG_FILE, "a") as f:
                        traceback.print_exc(file=f)
                    time.sleep(RETRY_DELAY)
                else:
                    log(f"❌ {date_str} 失败{MAX_RETRIES}次，跳过")
                    errors.append(date_str)

        if success:
            save_progress(done, date_str, total)

        # 进度估算
        elapsed = time.time() - t0
        days_done = len(done)
        rate = days_done / elapsed if elapsed > 0 else 0
        remaining_days = total - days_done
        eta = remaining_days / rate if rate > 0 else 0

        if (days_done % 100 == 0) or (days_done == total):
            log(f"⏱ 已过{elapsed/60:.0f}分 | 速率{rate:.2f}天/秒 | 预计剩余{eta/60:.0f}分")

        # API 礼貌间隔
        time.sleep(API_INTERVAL)

    # 完成
    elapsed = time.time() - t0
    log(f"\n{'='*50}")
    log(f"✅ 回填完成！")
    log(f"   总耗时: {elapsed:.0f}s ({elapsed/60:.1f}min)")
    log(f"   成功: {len(done)} 天")
    log(f"   失败: {len(errors)} 天")
    if errors:
        log(f"   失败日期: {errors[:10]}{'...' if len(errors)>10 else ''}")

    # 清理进度文件
    if PROGRESS_FILE.exists() and len(errors) == 0:
        PROGRESS_FILE.unlink()
        log("   📋 进度文件已清理")


if __name__ == "__main__":
    main()
