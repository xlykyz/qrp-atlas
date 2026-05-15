"""run.py - 日更数据管道

使用方式:
    python src/qrp_atlas/pipeline/daily_update/run.py                          # 跑最新交易日（正常 fetch）
    python src/qrp_atlas/pipeline/daily_update/run.py --date 2026-02-24       # 跑指定日期
    python src/qrp_atlas/pipeline/daily_update/run.py --skip-fetch            # 从已有 raw 重跑全部 tushare 数据
    python src/qrp_atlas/pipeline/daily_update/run.py --skip-fetch sina       # 从已有 raw 重跑全部 sina 数据
    python src/qrp_atlas/pipeline/daily_update/run.py --date 2026-05-13 --skip-fetch  # 只重跑指定日期的 raw 数据
"""

import argparse
from datetime import date

import duckdb
import pandas as pd

from qrp_atlas.config import (
    DAILY_MARKET_SNAPSHOT_CANONICAL_DIR,
    DAILY_SNAPSHOT_RAW_DIR,
    DB_PATH,
    ensure_dirs,
)

from .fetch import fetch_current_snapshot, save_raw_snapshot, get_latest_trade_date
from .clean import clean_daily_snapshot
from .enrich import enrich_daily_snapshot
from .load_duckdb import load_daily_market_snapshot


FETCH_TO_CLEAN_SOURCE = {
    "tushare": "tushare_daily",
    "sina": "sina_realtime",
}


def run_for_date(trade_date: date) -> None:
    """对指定交易日执行完整 pipeline（fetch → clean → enrich → load）

    Args:
        trade_date: 交易日期（历史或当天均可）
    """
    ensure_dirs()

    trade_date_str = trade_date.isoformat()
    print(f"[DAILY_UPDATE] trade_date={trade_date_str}")

    df_raw, fetch_source = fetch_current_snapshot(trade_date)

    if fetch_source != "tushare":
        df_raw["trade_date"] = trade_date_str

    raw_path = save_raw_snapshot(df_raw, fetch_source, trade_date)
    print(f"[DAILY_UPDATE] raw_saved={raw_path}")

    clean_source = FETCH_TO_CLEAN_SOURCE.get(fetch_source, "akshare_realtime")
    df_clean = clean_daily_snapshot(df_raw, source=clean_source)

    canonical_dir = DAILY_MARKET_SNAPSHOT_CANONICAL_DIR
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = canonical_dir / f"{trade_date_str}.csv"
    df_clean.to_csv(canonical_path, index=False, encoding="utf-8")
    print(f"[DAILY_UPDATE] canonical_saved={canonical_path}")

    con = duckdb.connect(str(DB_PATH))
    try:
        df_enriched = enrich_daily_snapshot(df_clean, trade_date, con)
    finally:
        con.close()
    print(f"[DAILY_UPDATE] enriched_rows={len(df_enriched)}")

    rows_loaded = load_daily_market_snapshot(df_enriched, trade_date_str)
    print(f"[DAILY_UPDATE] rows_loaded={rows_loaded}")


def reprocess_from_raw(
    trade_date: date,
    source: str,
    *,
    enrich_con: duckdb.DuckDBPyConnection | None = None,
) -> tuple[bool, pd.DataFrame | None]:
    """从已有 raw CSV 执行 re-clean → enrich → load，跳过 fetch

    Args:
        trade_date: 交易日期
        source: 数据源标识，如 "tushare" / "sina"
        enrich_con: 共享 DuckDB 连接（批量模式用），不传则内部新建

    Returns:
        (是否找到文件, 清洗后的 DataFrame 或 None)
        返回 DataFrame 以便批量模式调用方控制 enrich + load
    """
    date_str = trade_date.isoformat()
    year = trade_date.year
    raw_dir = DAILY_SNAPSHOT_RAW_DIR / str(year)

    raw_file = raw_dir / f"{date_str}_Astock_{source}.csv"
    if not raw_file.exists():
        return False, None

    df_raw = pd.read_csv(raw_file)
    if source != "tushare":
        df_raw["trade_date"] = date_str

    clean_source = FETCH_TO_CLEAN_SOURCE.get(source, f"{source}_realtime")
    df_clean = clean_daily_snapshot(df_raw, source=clean_source)

    canonical_dir = DAILY_MARKET_SNAPSHOT_CANONICAL_DIR
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = canonical_dir / f"{date_str}.csv"
    df_clean.to_csv(canonical_path, index=False, encoding="utf-8")

    if enrich_con is not None:
        return True, df_clean

    # 单日期模式：自行连接、enrich、load
    con = duckdb.connect(str(DB_PATH))
    try:
        df_enriched = enrich_daily_snapshot(df_clean, trade_date, con)
    finally:
        con.close()

    rows_loaded = load_daily_market_snapshot(df_enriched, date_str)
    print(f"[REPROCESS] {date_str}: enriched={len(df_enriched)}, loaded={rows_loaded}")

    return True, None


def run(source: str | None = None) -> None:
    """默认行为：跑最新交易日"""
    if source:
        trade_date = get_latest_trade_date()
        found, _ = reprocess_from_raw(trade_date, source)
        if not found:
            print(f"[RUN] ❌ 最新交易日 {trade_date} 未找到 {source} 的 raw 文件")
    else:
        run_for_date(get_latest_trade_date())


def main() -> None:
    parser = argparse.ArgumentParser(description="QRP Atlas 日更数据管道")
    parser.add_argument(
        "--date", type=str, default=None,
        help="指定交易日 (YYYY-MM-DD)，不传则跑最新交易日"
    )
    parser.add_argument(
        "--skip-fetch", nargs="?", const="tushare", default=None,
        help="跳过 fetch，从已有 raw CSV 重新处理 clean → enrich → load。"
             "可选指定源 (tushare/sina)，默认 tushare"
    )
    args = parser.parse_args()

    if args.date:
        try:
            trade_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}，请使用 YYYY-MM-DD")
            return

        if args.skip_fetch:
            found, df = reprocess_from_raw(trade_date, args.skip_fetch)
            if not found:
                print(f"❌ {trade_date} 未找到 {args.skip_fetch} 的 raw 文件")
            elif df is not None:
                # 单日期模式 reprocess_from_raw 内部已处理 enrich + load
                pass
        else:
            run_for_date(trade_date)
    else:
        if args.skip_fetch:
            # 不指定日期 → 扫描全部 raw 文件
            source = args.skip_fetch
            pattern = f"*_Astock_{source}.csv"
            raw_files = sorted(DAILY_SNAPSHOT_RAW_DIR.rglob(pattern))
            if not raw_files:
                print(f"❌ 未找到任何 {source} 的 raw 文件")
                return
            print(f"[RUN] 找到 {len(raw_files)} 个 {source} raw 文件，开始重跑...")

            # 逐个处理：read → clean → enrich(共享连接) → load
            con = duckdb.connect(str(DB_PATH))
            try:
                for i, raw_file in enumerate(raw_files):
                    date_str = raw_file.stem.split("_Astock_")[0]
                    trade_date = date.fromisoformat(date_str)

                    # 读取 raw CSV
                    df_raw = pd.read_csv(raw_file)
                    if source != "tushare":
                        df_raw["trade_date"] = date_str

                    # clean
                    clean_source = FETCH_TO_CLEAN_SOURCE.get(source, f"{source}_realtime")
                    df_clean = clean_daily_snapshot(df_raw, source=clean_source)

                    # 保存 canonical
                    canonical_dir = DAILY_MARKET_SNAPSHOT_CANONICAL_DIR
                    canonical_dir.mkdir(parents=True, exist_ok=True)
                    canonical_path = canonical_dir / f"{date_str}.csv"
                    df_clean.to_csv(canonical_path, index=False, encoding="utf-8")

                    # enrich（共享连接）
                    df_enriched = enrich_daily_snapshot(df_clean, trade_date, con)

                    # load
                    rows_loaded = load_daily_market_snapshot(df_enriched, date_str)

                    if (i + 1) % 500 == 0 or i == len(raw_files) - 1:
                        print(f"[RUN] 进度 {i + 1}/{len(raw_files)}: {date_str} → {rows_loaded} rows")
            finally:
                con.close()

            print(f"[RUN] ✅ 完成，重跑 {len(raw_files)} 个日期")
        else:
            run()


if __name__ == "__main__":
    main()
