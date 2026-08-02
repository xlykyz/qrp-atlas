"""Clean / normalize Tushare financial tables into contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

import pandas as pd

from qrp_atlas.contracts import (
    ANNOUNCEMENT_DATE,
    AVAILABLE_TRADE_DATE,
    F_ANN_DATE,
    INGESTED_AT,
    PUBLISHED_AT,
    REPORT_PERIOD,
    REPORT_TYPE,
    REVISION_ID,
    SOURCE,
    SOURCE_RECORD_ID,
    TICKER,
    UPDATE_FLAG,
    align_to_schema,
    apply_mapping,
    canonicalize,
    quick_validate,
)
from qrp_atlas.pipeline.pit_utils import (
    SOURCE_TUSHARE,
    NextTradeDateResolver,
    choose_announcement_date,
    content_signature,
    normalize_date_series,
    stable_hash,
    to_date,
)
from qrp_atlas.orchestration.execution_control import ExecutionControl

MAPPING_BY_TABLE = {
    "income_statement": "tushare_income",
    "balance_sheet": "tushare_balancesheet",
    "cashflow_statement": "tushare_cashflow",
    "financial_indicator": "tushare_fina_indicator",
}

# Columns used for content hash (version identity beyond business keys).
CONTENT_COLS = {
    "income_statement": [
        "basic_eps", "diluted_eps", "total_revenue", "revenue", "operate_profit",
        "total_profit", "n_income", "n_income_attr_p", "ebit", "ebitda",
        "report_type", "update_flag", "comp_type", "end_type",
        "announcement_date", "f_ann_date",
    ],
    "balance_sheet": [
        "total_assets", "total_liab", "total_cur_assets", "total_nca",
        "total_cur_liab", "total_ncl", "total_hldr_eqy_exc_min_int",
        "total_hldr_eqy_inc_min_int", "money_cap", "accounts_receiv", "inventories",
        "report_type", "update_flag", "comp_type", "end_type",
        "announcement_date", "f_ann_date",
    ],
    "cashflow_statement": [
        "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act",
        "n_incr_cash_cash_equ", "c_cash_equ_end_period", "free_cashflow",
        "report_type", "update_flag", "comp_type", "end_type",
        "announcement_date", "f_ann_date",
    ],
    "financial_indicator": [
        "eps", "bps", "cfps", "roe", "roa", "grossprofit_margin",
        "netprofit_margin", "debt_to_assets", "current_ratio", "quick_ratio",
        "update_flag", "announcement_date",
    ],
}


def _as_str_flag(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "0"
    return str(value).strip()


def clean_financial(
    df: pd.DataFrame,
    table: str,
    *,
    trade_date_resolver: NextTradeDateResolver | None = None,
    open_dates: Sequence | None = None,
    ingested_at: datetime | None = None,
    execution_control: ExecutionControl | None = None,
) -> pd.DataFrame:
    """Map raw Tushare financial rows to contracts schema."""
    if execution_control is not None:
        execution_control.check()
    if df is None or df.empty:
        return pd.DataFrame(columns=[])

    source_name = MAPPING_BY_TABLE[table]
    out = apply_mapping(df.copy(), source_name)

    # date normalization
    for col in [REPORT_PERIOD, ANNOUNCEMENT_DATE, F_ANN_DATE]:
        if col in out.columns:
            out[col] = normalize_date_series(out[col])

    if ANNOUNCEMENT_DATE not in out.columns:
        out[ANNOUNCEMENT_DATE] = None
    if F_ANN_DATE not in out.columns:
        out[F_ANN_DATE] = None

    # announcement date: prefer f_ann_date
    out[ANNOUNCEMENT_DATE] = [
        choose_announcement_date(a, f) for a, f in zip(out[ANNOUNCEMENT_DATE], out.get(F_ANN_DATE, [None] * len(out)))
    ]

    # drop rows missing required business dates
    out = out[out[TICKER].notna() & out[REPORT_PERIOD].notna() & out[ANNOUNCEMENT_DATE].notna()].copy()
    if out.empty:
        return out

    if REPORT_TYPE not in out.columns:
        out[REPORT_TYPE] = "1"
    out[REPORT_TYPE] = out[REPORT_TYPE].map(lambda x: "1" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip())

    if UPDATE_FLAG not in out.columns:
        out[UPDATE_FLAG] = "0"
    out[UPDATE_FLAG] = out[UPDATE_FLAG].map(_as_str_flag)

    resolver = trade_date_resolver or NextTradeDateResolver(open_dates)
    out[AVAILABLE_TRADE_DATE] = out[ANNOUNCEMENT_DATE].map(resolver.next_trade_date)
    # published_at remains NULL when no trusted intraday timestamp
    out[PUBLISHED_AT] = None

    out[SOURCE] = SOURCE_TUSHARE
    now = ingested_at or datetime.now(timezone.utc).replace(tzinfo=None)
    out[INGESTED_AT] = now

    content_cols = CONTENT_COLS[table]
    source_ids = []
    revision_ids = []
    for _, row in out.iterrows():
        if execution_control is not None:
            execution_control.check()
        # business identity without content
        if table == "financial_indicator":
            biz = [
                table,
                row[TICKER],
                to_date(row[REPORT_PERIOD]).isoformat(),
                to_date(row[ANNOUNCEMENT_DATE]).isoformat(),
                row[UPDATE_FLAG],
            ]
        else:
            biz = [
                table,
                row[TICKER],
                to_date(row[REPORT_PERIOD]).isoformat(),
                str(row[REPORT_TYPE]),
                to_date(row[ANNOUNCEMENT_DATE]).isoformat(),
                row[UPDATE_FLAG],
            ]
        source_ids.append(stable_hash(biz, length=20))
        payload = {c: row[c] if c in row.index else None for c in content_cols}
        # include business keys in content signature too
        for k, v in zip(["_biz" + str(i) for i in range(len(biz))], biz):
            payload[k] = v
        revision_ids.append(content_signature(payload, list(payload.keys())))

    out[SOURCE_RECORD_ID] = source_ids
    out[REVISION_ID] = revision_ids

    # stable dedupe within batch: same revision keeps last
    out = out.drop_duplicates(subset=[REVISION_ID], keep="last")

    out = align_to_schema(out, table, fill_missing_optional=True, drop_extra=True)
    out = canonicalize(out, table)
    out = quick_validate(out, table, allow_extra=False)
    if execution_control is not None:
        execution_control.check()
    return out.reset_index(drop=True)
