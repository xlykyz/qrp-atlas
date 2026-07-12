"""
run.py - 全景网互动问答数据管道入口

接口活跃深度有限，默认策略：
- 全量抓取当前接口可见回复
- 按 pid 增量入库（INSERT OR IGNORE）

用法:
    python -m qrp_atlas.pipeline.irm_qa.run
    python -m qrp_atlas.pipeline.irm_qa.run --company-code 001205
    python -m qrp_atlas.pipeline.irm_qa.run --keywords 回购
    python -m qrp_atlas.pipeline.irm_qa.run --replace
"""

from __future__ import annotations

import argparse
import sys

import duckdb

from qrp_atlas.config.paths import DB_PATH
from qrp_atlas.contracts.schema import init_database
from qrp_atlas.pipeline.irm_qa.clean import clean_interaction_qa
from qrp_atlas.pipeline.irm_qa.fetch import fetch_interaction_qa
from qrp_atlas.pipeline.irm_qa.load import upsert_interaction_qa


def run(
    *,
    company_code: str = "",
    keywords: str = "",
    replace: bool = False,
    max_pages: int | None = None,
) -> int:
    """执行互动问答数据管道。

    默认全量抓取 + 按 pid 增量入库；传 replace=True 时覆盖同 pid。
    """
    mode = "replace" if replace else "incremental"
    print(
        f"[irm_qa] Starting full fetch "
        f"company={company_code or '*'} keywords={keywords or '-'} ({mode})"
    )

    fetch_kwargs: dict = {
        "company_code": company_code,
        "keywords": keywords,
    }
    if max_pages is not None:
        fetch_kwargs["max_pages"] = max_pages

    print("[irm_qa] Fetching all available pages...")
    raw_records = fetch_interaction_qa(**fetch_kwargs)
    print(f"[irm_qa] Fetched {len(raw_records)} raw records")

    print("[irm_qa] Cleaning data...")
    cleaned = clean_interaction_qa(raw_records, keywords=keywords)
    print(f"[irm_qa] Cleaned to {len(cleaned)} unique records")

    if not cleaned:
        print("[irm_qa] No records to load")
        print("[irm_qa] Pipeline completed successfully")
        return 0

    incremental = not replace
    print(f"[irm_qa] Loading to database ({mode})...")
    con = duckdb.connect(str(DB_PATH))
    try:
        init_database(con)
        count = upsert_interaction_qa(con, cleaned, incremental=incremental)
        action = "Ignored existing / inserted" if incremental else "Replaced"
        print(f"[irm_qa] {action} {count} records")

        row_count = con.execute(
            "SELECT COUNT(*) FROM irm_interaction_qa"
        ).fetchone()[0]
        print(f"[irm_qa] Verified: {row_count} rows in DB")
    finally:
        con.close()

    print("[irm_qa] Pipeline completed successfully")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="全景网互动问答数据管道")
    parser.add_argument(
        "--company-code",
        default="",
        help="6 位证券代码；不传则抓取接口当前可见全量最新回复",
    )
    parser.add_argument(
        "--keywords",
        default="",
        help="关键词过滤（如 回购、股东人数）",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="覆盖同 pid（默认 INSERT OR IGNORE 增量更新）",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="最大翻页数（默认 50）",
    )
    args = parser.parse_args()

    exit_code = run(
        company_code=args.company_code.strip(),
        keywords=args.keywords.strip(),
        replace=args.replace,
        max_pages=args.max_pages,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
