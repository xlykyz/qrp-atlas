"""Batch plan generation for PIT historical backfill."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Iterable, Sequence

import pandas as pd

from qrp_atlas.pipeline.fundamentals.run import ALL_TABLES

FINANCIAL_START = date(2010, 3, 31)
FINANCIAL_END = date(2026, 6, 30)
INDEX_START = date(2010, 1, 1)
INDEX_END = date(2026, 7, 14)

DEFAULT_INDEX_CODES = (
    "000300.SH",
    "000905.SH",
    "000852.SH",
    "000688.SH",
)

CLASSIFICATION_SYSTEM = "SW2021"
QUARTER_MONTH_DAY = ((3, 31), (6, 30), (9, 30), (12, 31))


@dataclass(frozen=True)
class Batch:
    """One unit of work for the orchestrator."""

    batch_id: str
    dataset: str  # fundamentals | industry | index
    key: str  # table / l1_code / index_code
    period: str | None = None  # report period YYYYMMDD
    start_date: str | None = None  # YYYYMMDD
    end_date: str | None = None  # YYYYMMDD
    meta: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d.get("meta") is None:
            d["meta"] = {}
        return d


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def iter_quarter_ends(start: date = FINANCIAL_START, end: date = FINANCIAL_END) -> list[date]:
    if end < start:
        raise ValueError(f"end {end} before start {start}")
    out: list[date] = []
    for year in range(start.year, end.year + 1):
        for month, day in QUARTER_MONTH_DAY:
            d = date(year, month, day)
            if start <= d <= end:
                out.append(d)
    return out


def iter_month_ranges(start: date = INDEX_START, end: date = INDEX_END) -> list[tuple[date, date]]:
    if end < start:
        raise ValueError(f"end {end} before start {start}")
    ranges: list[tuple[date, date]] = []
    y, m = start.year, start.month
    while True:
        month_start = date(y, m, 1)
        last_day = monthrange(y, m)[1]
        month_end = date(y, m, last_day)
        range_start = max(month_start, start)
        range_end = min(month_end, end)
        if range_start <= range_end:
            ranges.append((range_start, range_end))
        if y == end.year and m == end.month:
            break
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return ranges


def financial_batches(
    *,
    tables: Sequence[str] = ALL_TABLES,
    start: date = FINANCIAL_START,
    end: date = FINANCIAL_END,
    periods: Sequence[str] | None = None,
) -> list[Batch]:
    if periods is None:
        period_list = [_ymd(d) for d in iter_quarter_ends(start, end)]
    else:
        period_list = [str(p).replace("-", "") for p in periods]
    batches: list[Batch] = []
    for table in tables:
        for period in period_list:
            batches.append(
                Batch(
                    batch_id=f"fundamentals:{table}:{period}",
                    dataset="fundamentals",
                    key=table,
                    period=period,
                )
            )
    return batches


def industry_batches(l1_codes: Sequence[str]) -> list[Batch]:
    batches: list[Batch] = []
    for code in l1_codes:
        code = str(code).strip()
        if not code:
            continue
        batches.append(
            Batch(
                batch_id=f"industry:l1:{code}",
                dataset="industry",
                key=code,
                meta={"classification_system": CLASSIFICATION_SYSTEM, "level": "L1"},
            )
        )
    return batches


def index_batches(
    *,
    index_codes: Sequence[str] = DEFAULT_INDEX_CODES,
    start: date = INDEX_START,
    end: date = INDEX_END,
    ranges: Sequence[tuple[date, date]] | None = None,
) -> list[Batch]:
    month_ranges = list(ranges) if ranges is not None else iter_month_ranges(start, end)
    batches: list[Batch] = []
    for code in index_codes:
        for rs, re in month_ranges:
            start_s, end_s = _ymd(rs), _ymd(re)
            batches.append(
                Batch(
                    batch_id=f"index:{code}:{start_s}:{end_s}",
                    dataset="index",
                    key=code,
                    start_date=start_s,
                    end_date=end_s,
                )
            )
    return batches


def discover_sw2021_l1_codes(client=None) -> list[str]:
    """Fetch Shenwan 2021 level-1 industry codes via Tushare index_classify.

    Strict SW2021 only. Never silently fall back to legacy SW.
    Only signature compatibility: some older clients reject the `src` kwarg.
    """
    from qrp_atlas.config import get_tushare_pro

    pro = client or get_tushare_pro()
    method = getattr(pro, "index_classify")
    used_src_kw = True
    try:
        df = method(level="L1", src="SW2021")
    except TypeError:
        # Client signature does not accept src=...; call without it, but still
        # require SW2021 evidence in the payload (never treat legacy SW as SW2021).
        used_src_kw = False
        try:
            df = method(level="L1")
        except Exception as exc:
            raise RuntimeError(f"index_classify(L1) failed without src kw: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"index_classify(SW2021/L1) failed: {exc}") from exc

    if df is None or getattr(df, "empty", True):
        raise RuntimeError("index_classify(SW2021/L1) returned empty result")

    if "src" in df.columns:
        srcs = {str(x).strip().upper() for x in df["src"].dropna().tolist()}
        if not srcs:
            raise RuntimeError("index_classify result missing src values; cannot verify SW2021")
        if not any(s in {"SW2021", "SW21"} or "2021" in s for s in srcs):
            raise RuntimeError(f"index_classify result src not SW2021: {sorted(srcs)}")
        # keep only SW2021 rows if mixed
        mask = df["src"].astype(str).str.upper().str.contains("2021")
        df = df.loc[mask]
        if df.empty:
            raise RuntimeError("index_classify produced no SW2021 rows after filtering")
    elif not used_src_kw:
        raise RuntimeError(
            "index_classify client rejected src= and response has no src column; "
            "refusing to treat unverified payload as SW2021"
        )

    code_col = "index_code" if "index_code" in df.columns else None
    if code_col is None:
        for c in ("code", "l1_code", "ts_code"):
            if c in df.columns:
                code_col = c
                break
    if code_col is None:
        raise RuntimeError(f"index_classify missing code column: {list(df.columns)}")
    codes = sorted({str(x).strip() for x in df[code_col].tolist() if str(x).strip()})
    if not codes:
        raise RuntimeError("index_classify(SW2021/L1) produced empty L1 code list")
    return codes


def precheck_batches(*, l1_code: str | None = None) -> list[Batch]:
    """Minimal smoke plan: 1 financial + 1 industry + 1 index month."""
    fin = financial_batches(tables=["income_statement"], periods=["20231231"])
    ind_code = l1_code or "801010.SI"
    ind = industry_batches([ind_code])
    # one recent month for CSI 300
    idx = index_batches(
        index_codes=["000300.SH"],
        ranges=[(date(2024, 1, 1), date(2024, 1, 31))],
    )
    return fin + ind + idx


def summarize_plan(batches: Sequence[Batch]) -> dict[str, Any]:
    by_dataset: dict[str, int] = {}
    for b in batches:
        by_dataset[b.dataset] = by_dataset.get(b.dataset, 0) + 1
    return {
        "total_batches": len(batches),
        "by_dataset": by_dataset,
        "planned_requests": len(batches),
    }


def batches_from_dicts(items: Iterable[dict[str, Any]]) -> list[Batch]:
    out: list[Batch] = []
    for item in items:
        out.append(
            Batch(
                batch_id=item["batch_id"],
                dataset=item["dataset"],
                key=item["key"],
                period=item.get("period"),
                start_date=item.get("start_date"),
                end_date=item.get("end_date"),
                meta=item.get("meta") or {},
            )
        )
    return out


def parse_ymd(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("-", "")
    return datetime.strptime(text, "%Y%m%d").date()
