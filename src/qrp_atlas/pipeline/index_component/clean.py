"""Normalize index_weight monthly weight snapshots.

Snapshot model (task 03-B):
- retain snapshot_date + weight
- effective_from = snapshot_date for unified time field compatibility
- effective_to is left empty; do NOT construct adjacent intervals from a single batch
- later task 03-C selects the latest available snapshot by as_of_date
"""

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

    # Snapshot model: effective_from mirrors snapshot_date; effective_to stays open/empty.
    # Intervals are intentionally not derived from the current fetch batch.
    out[EFFECTIVE_FROM] = out[SNAPSHOT_DATE].map(to_date)
    out[EFFECTIVE_TO] = None

    resolver = trade_date_resolver or NextTradeDateResolver(open_dates)
    # No trusted intraday timestamp: available on the first open day strictly after snapshot_date.
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
            "effective_to": "",
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
