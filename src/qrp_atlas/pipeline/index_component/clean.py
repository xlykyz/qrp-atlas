"""Normalize index_weight snapshots and derive adjacent effective intervals."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    AVAILABLE_TRADE_DATE,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    INDEX_CODE,
    INGESTED_AT,
    REVISION_ID,
    SNAPSHOT_DATE,
    SOURCE,
    SOURCE_RECORD_ID,
    WEIGHT,
    align_to_schema,
    apply_mapping,
    canonicalize,
    quick_validate,
)
from qrp_atlas.pipeline.pit_utils import (
    SOURCE_TUSHARE,
    NextTradeDateResolver,
    content_signature,
    normalize_date_series,
    stable_hash,
    to_date,
)


def clean_index_component(
    df: pd.DataFrame,
    *,
    trade_date_resolver: NextTradeDateResolver | None = None,
    open_dates: Sequence | None = None,
    ingested_at: datetime | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = apply_mapping(df.copy(), "tushare_index_weight")
    out[SNAPSHOT_DATE] = normalize_date_series(out[SNAPSHOT_DATE])
    out = out[out[INDEX_CODE].notna() & out[ASSET_ID].notna() & out[SNAPSHOT_DATE].notna()].copy()
    if out.empty:
        return out

    # derive effective interval from adjacent snapshots per index
    snapshots = sorted({to_date(d) for d in out[SNAPSHOT_DATE].tolist() if to_date(d) is not None})
    next_map: dict = {}
    for i, snap in enumerate(snapshots):
        next_map[snap] = snapshots[i + 1] if i + 1 < len(snapshots) else None

    out[EFFECTIVE_FROM] = out[SNAPSHOT_DATE].map(to_date)
    out[EFFECTIVE_TO] = out[EFFECTIVE_FROM].map(lambda d: next_map.get(d))

    resolver = trade_date_resolver or NextTradeDateResolver(open_dates)
    # weight snapshots are known on snapshot date; still use next trade day for safety when non-trade day
    out[AVAILABLE_TRADE_DATE] = out[SNAPSHOT_DATE].map(
        lambda d: resolver.next_trade_date(d) if to_date(d) not in set(resolver.open_dates) else to_date(d)
    )
    # if snapshot itself is open trade day, available same day after close is still conservative next day?
    # Task rule for announcements: after announcement next day. For monthly index weights, snapshot_date is
    # the published trade date of the weight file; use next open day strictly after snapshot for consistency
    # with no-intraday-time conservative rule.
    out[AVAILABLE_TRADE_DATE] = out[SNAPSHOT_DATE].map(resolver.next_trade_date)

    out[SOURCE] = SOURCE_TUSHARE
    now = ingested_at or datetime.now(timezone.utc).replace(tzinfo=None)
    out[INGESTED_AT] = now

    source_ids = []
    revision_ids = []
    for _, row in out.iterrows():
        snap = to_date(row[SNAPSHOT_DATE])
        biz = [
            "index_component_history",
            str(row[INDEX_CODE]),
            str(row[ASSET_ID]),
            snap.isoformat() if snap else "",
        ]
        source_ids.append(stable_hash(biz, length=20))
        payload = {
            "weight": row.get(WEIGHT),
            "effective_from": to_date(row[EFFECTIVE_FROM]).isoformat() if to_date(row[EFFECTIVE_FROM]) else "",
            "effective_to": to_date(row[EFFECTIVE_TO]).isoformat() if to_date(row[EFFECTIVE_TO]) else "",
            **{f"b{i}": v for i, v in enumerate(biz)},
        }
        revision_ids.append(content_signature(payload, list(payload.keys())))
    out[SOURCE_RECORD_ID] = source_ids
    out[REVISION_ID] = revision_ids
    out = out.drop_duplicates(subset=[REVISION_ID], keep="last")

    out = align_to_schema(out, "index_component_history", fill_missing_optional=True, drop_extra=True)
    out = canonicalize(out, "index_component_history")
    out = quick_validate(out, "index_component_history", allow_extra=False)
    return out.reset_index(drop=True)
