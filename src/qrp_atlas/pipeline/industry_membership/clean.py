"""Normalize index_member_all into industry_membership_history (3 levels)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    AVAILABLE_TRADE_DATE,
    CLASSIFICATION_SYSTEM,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    INDUSTRY_CODE,
    INDUSTRY_LEVEL,
    INDUSTRY_NAME,
    INGESTED_AT,
    REVISION_ID,
    SOURCE,
    SOURCE_RECORD_ID,
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
from qrp_atlas.orchestration.execution_control import ExecutionControl

LEVEL_FIELDS = {
    1: ("l1_code", "l1_name"),
    2: ("l2_code", "l2_name"),
    3: ("l3_code", "l3_name"),
}


def clean_industry_membership(
    df: pd.DataFrame,
    *,
    trade_date_resolver: NextTradeDateResolver | None = None,
    open_dates: Sequence | None = None,
    ingested_at: datetime | None = None,
    classification_system: str = "sw2021",
    execution_control: ExecutionControl | None = None,
) -> pd.DataFrame:
    if execution_control is not None:
        execution_control.check()
    if df is None or df.empty:
        return pd.DataFrame()

    raw = apply_mapping(df.copy(), "tushare_index_member_all")
    # mapping renames ts_code->asset_id and in/out dates; keep l*_code/name
    for col in ["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name", EFFECTIVE_FROM, EFFECTIVE_TO]:
        if col not in raw.columns:
            # maybe mapping didn't rename if already standard
            pass

    # if apply_mapping didn't keep l* fields (they map to themselves), ensure present
    for col in ["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"]:
        if col not in raw.columns and col in df.columns:
            raw[col] = df[col].values

    if ASSET_ID not in raw.columns and "ts_code" in raw.columns:
        raw[ASSET_ID] = raw["ts_code"]

    raw[EFFECTIVE_FROM] = normalize_date_series(raw[EFFECTIVE_FROM]) if EFFECTIVE_FROM in raw.columns else None
    if EFFECTIVE_TO in raw.columns:
        raw[EFFECTIVE_TO] = normalize_date_series(raw[EFFECTIVE_TO])
    else:
        raw[EFFECTIVE_TO] = None

    raw = raw[raw[ASSET_ID].notna() & raw[EFFECTIVE_FROM].notna()].copy()
    if raw.empty:
        return raw

    resolver = trade_date_resolver or NextTradeDateResolver(open_dates)
    now = ingested_at or datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[dict] = []
    for _, r in raw.iterrows():
        if execution_control is not None:
            execution_control.check()
        for level, (code_col, name_col) in LEVEL_FIELDS.items():
            if execution_control is not None:
                execution_control.check()
            code = r.get(code_col)
            name = r.get(name_col)
            if code is None or (isinstance(code, float) and pd.isna(code)) or str(code).strip() == "":
                continue
            eff_from = to_date(r[EFFECTIVE_FROM])
            eff_to = to_date(r[EFFECTIVE_TO]) if r.get(EFFECTIVE_TO) is not None else None
            available = resolver.next_trade_date(eff_from)
            biz = [
                "industry_membership_history",
                classification_system,
                str(r[ASSET_ID]),
                str(level),
                str(code),
                eff_from.isoformat() if eff_from else "",
                eff_to.isoformat() if eff_to else "",
            ]
            payload = {
                "industry_name": name,
                "effective_from": eff_from.isoformat() if eff_from else "",
                "effective_to": eff_to.isoformat() if eff_to else "",
                **{f"b{i}": v for i, v in enumerate(biz)},
            }
            rows.append(
                {
                    ASSET_ID: str(r[ASSET_ID]),
                    CLASSIFICATION_SYSTEM: classification_system,
                    INDUSTRY_LEVEL: int(level),
                    INDUSTRY_CODE: str(code),
                    INDUSTRY_NAME: None if name is None or (isinstance(name, float) and pd.isna(name)) else str(name),
                    EFFECTIVE_FROM: eff_from,
                    EFFECTIVE_TO: eff_to,
                    AVAILABLE_TRADE_DATE: available,
                    SOURCE: SOURCE_TUSHARE,
                    SOURCE_RECORD_ID: stable_hash(biz, length=20),
                    REVISION_ID: content_signature(payload, list(payload.keys())),
                    INGESTED_AT: now,
                }
            )

    if not rows:
        return pd.DataFrame()
    if execution_control is not None:
        execution_control.check()
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=[REVISION_ID], keep="last")
    out = align_to_schema(out, "industry_membership_history", fill_missing_optional=True, drop_extra=True)
    out = canonicalize(out, "industry_membership_history")
    out = quick_validate(out, "industry_membership_history", allow_extra=False)
    return out.reset_index(drop=True)
