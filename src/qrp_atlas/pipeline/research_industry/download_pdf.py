"""download_pdf.py - 行业研报 PDF 下载模块

独立脚本，从数据库的 attach_url 字段下载 PDF 文件。

Usage:
    python -m qrp_atlas.pipeline.research_industry.download_pdf --mode today
    python -m qrp_atlas.pipeline.research_industry.download_pdf --mode all
"""

import argparse
import logging
import random
import re
import time
import urllib.request
from pathlib import Path

import duckdb

from qrp_atlas.config.paths import DB_PATH, RESEARCH_PDFS_DIR

from .config import INTERVAL_MAX, INTERVAL_MIN

logger = logging.getLogger(__name__)

# 非法文件名字符
ILLEGAL_CHARS = re.compile(r'[\\/:*?"<>|]')

# PDF 文件最大路径长度限制
MAX_PATH_LENGTH = 200


def sanitize_filename(name: str) -> str:
    """替换文件名中的非法字符为下划线"""
    return ILLEGAL_CHARS.sub("_", name)


def build_pdf_path(
    publish_date,
    title: str,
    industry_name: str,
    base_dir: Path,
) -> Path:
    """
    构建 PDF 本地存储路径。

    路径格式: data/pdfs/research_industry/{YYYY}/{MM}/{DD}/{filename}.pdf
    文件名格式: {title}_{industryName}.pdf

    Args:
        publish_date: 发布日期
        title: 研报标题
        industry_name: 行业名称
        base_dir: PDF 存储根目录

    Returns:
        Path 对象
    """
    yyyy = publish_date.year
    mm = publish_date.month
    dd = publish_date.day

    # 清理文件名中的非法字符
    safe_title = sanitize_filename(title)
    safe_industry_name = sanitize_filename(industry_name) if industry_name else ""

    # 初始文件名: {title}_{industry_name}.pdf
    if safe_industry_name:
        filename_base = f"{safe_title}_{safe_industry_name}"
    else:
        filename_base = safe_title

    # 构建完整路径并检查长度
    date_dir = base_dir / str(yyyy) / f"{mm:02d}" / f"{dd:02d}"
    full_path = date_dir / f"{filename_base}.pdf"

    # 如果路径超过最大长度限制，截断 title 部分
    if len(str(full_path)) > MAX_PATH_LENGTH:
        # 预留给 industry_name、日期路径等的长度
        reserved = len(str(date_dir / f"_{safe_industry_name}.pdf")) if safe_industry_name else len(str(date_dir / ".pdf"))
        max_title_len = MAX_PATH_LENGTH - reserved - 1  # 减1避免边界
        if max_title_len < 10:
            max_title_len = 10  # 至少保留10个字符
        safe_title = safe_title[:max_title_len]
        if safe_industry_name:
            filename_base = f"{safe_title}_{safe_industry_name}"
        else:
            filename_base = safe_title
        full_path = date_dir / f"{filename_base}.pdf"

    return full_path


def sleep_interval() -> float:
    """随机休眠时间（秒）"""
    return random.uniform(INTERVAL_MIN, INTERVAL_MAX)


def download_pdf(url: str, dest_path: Path, timeout: int = 30) -> bool:
    """
    下载单个 PDF 文件。

    Args:
        url: PDF 下载地址
        dest_path: 本地保存路径
        timeout: 下载超时（秒）

    Returns:
        True 表示下载成功，False 表示失败
    """
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://data.eastmoney.com/",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                logger.warning("HTTP %s for %s", resp.status, url)
                return False
            content = resp.read()
            if len(content) < 1000:
                logger.warning("File too small (%d bytes) for %s", len(content), url)
                return False
            dest_path.write_bytes(content)
        return True
    except Exception as exc:
        logger.warning("Download failed for %s: %s", url, exc)
        return False


def query_reports(mode: str, pdf_base_dir: Path) -> list[tuple]:
    """
    从数据库查询行业研报记录。

    Args:
        mode: 'today' 或 'all'
        pdf_base_dir: PDF 存储根目录，用于判断文件是否已存在

    Returns:
        记录列表，每条记录为 (info_code, title, industry_name, indv_indu_name, attach_url, publish_date)
    """
    con = duckdb.connect(str(DB_PATH))

    query = """
        SELECT info_code, title, industry_name, indv_indu_name, attach_url, publish_date
        FROM research_report_industry
        WHERE attach_url IS NOT NULL AND attach_url != ''
    """

    if mode == "today":
        query += " AND publish_date >= CURRENT_DATE - INTERVAL '2 days'"

    rows = con.execute(query).fetchall()
    con.close()

    # 过滤掉 PDF 已存在的记录
    pending = []
    for row in rows:
        info_code, title, industry_name, indv_indu_name, attach_url, publish_date = row
        effective_industry_name = industry_name if industry_name else (indv_indu_name if indv_indu_name else "")
        pdf_path = build_pdf_path(
            publish_date=publish_date,
            title=title,
            industry_name=effective_industry_name,
            base_dir=pdf_base_dir,
        )
        if pdf_path.exists():
            continue
        pending.append(row)

    return pending


def main() -> None:
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description="下载行业研报 PDF 文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["today", "all"],
        default="today",
        help="today: 只下载今天的记录; all: 下载所有记录",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 数据目录
    pdf_base_dir = RESEARCH_PDFS_DIR / "research_industry"

    rows = query_reports(args.mode, pdf_base_dir)
    total = len(rows)

    if total == 0:
        if args.mode == "all":
            print(f"[download_pdf] ✅ PDF 已全量下载，任务完成")
        else:
            print(f"[download_pdf] 今日无待下载研报")
        return

    downloaded = 0
    failed = 0

    for idx, (info_code, title, industry_name, indv_indu_name, attach_url, publish_date) in enumerate(rows, 1):
        # 优先使用 industry_name，否则使用 indv_indu_name
        effective_industry_name = industry_name if industry_name else (indv_indu_name if indv_indu_name else "")

        pdf_path = build_pdf_path(
            publish_date=publish_date,
            title=title,
            industry_name=effective_industry_name,
            base_dir=pdf_base_dir,
        )

        # 显示进度（截断标题避免过长）
        display_title = title[:40] + "..." if len(title) > 40 else title
        print(f"[download_pdf] ({idx}/{total}) Downloading: {display_title}...")

        success = download_pdf(attach_url, pdf_path)
        if success:
            downloaded += 1
            print(f"[download_pdf]   Saved: {pdf_path}")
        else:
            failed += 1
            print(f"[download_pdf]   Failed: {attach_url}")

        # 最后一个不需要等待
        if idx < total:
            time.sleep(sleep_interval())

    print(f"[download_pdf] Summary: {total} pending, {downloaded} downloaded, {failed} failed")


if __name__ == "__main__":
    main()
