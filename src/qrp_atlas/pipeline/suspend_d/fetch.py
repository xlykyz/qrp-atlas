"""fetch.py - 从 tushare suspend_d 接口获取每日停复牌信息

接口: pro.suspend_d(start_date='YYYYMMDD', end_date='YYYYMMDD')
注意: tushare 单次请求有 limit=5000 上限，超过的自动截断。
      fetch 10天分片 + 自动降级，确保数据完整。
"""

from datetime import date, timedelta

import pandas as pd

from qrp_atlas.config import get_tushare_pro


def _month_ranges(year: int):
    """生成指定年份的月度区间列表"""
    starts = [
        date(year, 1, 1), date(year, 2, 1), date(year, 3, 1),
        date(year, 4, 1), date(year, 5, 1), date(year, 6, 1),
        date(year, 7, 1), date(year, 8, 1), date(year, 9, 1),
        date(year, 10, 1), date(year, 11, 1), date(year, 12, 1),
    ]
    ends = [
        date(year, 1, 31), date(year, 2, 28), date(year, 3, 31),
        date(year, 4, 30), date(year, 5, 31), date(year, 6, 30),
        date(year, 7, 31), date(year, 8, 31), date(year, 9, 30),
        date(year, 10, 31), date(year, 11, 30), date(year, 12, 31),
    ]
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        ends[1] = date(year, 2, 29)
    return list(zip(starts, ends))


def _chunk_range(start: date, end: date, max_days: int = 10):
    """将日期区间按 max_days 天切分成小段"""
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _fetch_single(start_date: date, end_date: date, _retries: int = 3) -> pd.DataFrame:
    """单次 fetch，带自动重试"""
    import time
    for attempt in range(1, _retries + 1):
        try:
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")
            pro = get_tushare_pro()
            df = pro.suspend_d(start_date=start_str, end_date=end_str)
            if df is None or df.empty:
                raise ValueError(f"suspend_d returned empty data for {start_str}~{end_str}")
            return df
        except (Exception,) as e:
            err_str = str(e).lower()
            if "ssl" in err_str or "eof" in err_str or "timeout" in err_str or "max retries" in err_str:
                if attempt < _retries:
                    wait = 2 ** attempt
                    print(f"    🔁 重试 {attempt}/{_retries} (等待 {wait}s)...")
                    time.sleep(wait)
                    continue
            raise
    raise RuntimeError(f"Exhausted {_retries} retries for {start_date}~{end_date}")


def _fetch_with_reduce(start_date: date, end_date: date) -> pd.DataFrame:
    """fetch 并自动检查是否被截断（刚好 5000 行则降级到 10天分片）"""
    df = _fetch_single(start_date, end_date)

    # 刚好 5000 行 → 可能被截断，降级到 10 天分片重新 fetch
    if len(df) >= 5000:
        chunks = _chunk_range(start_date, end_date)
        # 如果已经是单月内分片就不降了，但 10 天分片几乎不会超 5000
        print(f"    ⚠️  {start_date}~{end_date}: {len(df)} rows (可能截断，降级 10天分片)")
        result = []
        for cs, ce in chunks:
            try:
                ck = _fetch_single(cs, ce)
                result.append(ck)
            except ValueError:
                continue
        if result:
            return pd.concat(result, ignore_index=True)
        return df  # 降级失败，原样返回

    return df


def fetch_suspend_d(start_date: date, end_date: date) -> pd.DataFrame:
    """获取指定日期区间全市场停复牌数据（自动分片防截断）

    Args:
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        DataFrame(ts_code, trade_date, suspend_timing, suspend_type)
    """
    # 超过 90 天的区间自动按月分片
    if (end_date - start_date).days > 90:
        chunks = []
        for m_start, m_end in _month_ranges(start_date.year):
            if m_end < start_date or m_start > end_date:
                continue
            cs = max(m_start, start_date)
            ce = min(m_end, end_date)
            try:
                ck = _fetch_with_reduce(cs, ce)
                chunks.append(ck)
            except ValueError:
                continue
        if not chunks:
            raise ValueError(f"suspend_d returned empty data for {start_date}~{end_date}")
        return pd.concat(chunks, ignore_index=True)

    return _fetch_with_reduce(start_date, end_date)


def fetch_suspend_d_year(year: int) -> pd.DataFrame:
    """获取指定整年的停复牌数据（按月分片 + 自动降级）

    Args:
        year: 年份，如 2025

    Returns:
        DataFrame(ts_code, trade_date, suspend_timing, suspend_type)
    """
    chunks = []
    for i, (m_start, m_end) in enumerate(_month_ranges(year), 1):
        try:
            chunk = _fetch_with_reduce(m_start, m_end)
            chunks.append(chunk)
            print(f"  [M{i:02d}] {m_start}~{m_end}: {len(chunk)} rows")
        except ValueError:
            print(f"  [M{i:02d}] {m_start}~{m_end}: empty")
            continue

    if not chunks:
        raise ValueError(f"suspend_d returned empty data for {year}")

    return pd.concat(chunks, ignore_index=True)