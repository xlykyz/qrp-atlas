"""Point-in-time historical query services for financial, industry and index data.

These helpers sit on the backtest data-preparation boundary: they only read local
DuckDB tables, apply availability constraints, and reuse
``select_latest_available_records`` for version resolution.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import _build_where, _normalize_date, _resolve_con
from qrp_atlas.backtest.point_in_time import select_latest_available_records
from qrp_atlas.contracts import (
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INCOME_STATEMENT,
    INDEX_COMPONENT_HISTORY,
    INDUSTRY_MEMBERSHIP_HISTORY,
)

FINANCIAL_TABLES: dict[str, tuple[str, ...]] = {
    INCOME_STATEMENT.name: ("ticker", "report_period", "report_type", "comp_type", "end_type"),
    BALANCE_SHEET.name: ("ticker", "report_period", "report_type", "comp_type", "end_type"),
    CASHFLOW_STATEMENT.name: ("ticker", "report_period", "report_type", "comp_type", "end_type"),
    FINANCIAL_INDICATOR.name: ("ticker", "report_period"),
}

FINANCIAL_AUDIT_COLUMNS = (
    "announcement_date",
    "f_ann_date",
    "published_at",
    "available_trade_date",
    "report_type",
    "update_flag",
    "source",
    "source_record_id",
    "revision_id",
    "ingested_at",
)


class IndustryMembershipConflictError(ValueError):
    """Raised when overlapping industry memberships exist for the same entity."""


def _as_list(values: str | Sequence[str] | None) -> list[str] | None:
    if values is None:
        return None
    if isinstance(values, str):
        return [values]
    out = [str(v) for v in values]
    return out


def _require_as_of_date(as_of_date: Any) -> str:
    normalized = _normalize_date(as_of_date)
    if normalized is None:
        raise ValueError("as_of_date is required")
    # validate parseability
    try:
        pd.Timestamp(normalized)
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"invalid as_of_date: {as_of_date!r}") from exc
    return str(normalized)


def _read_table(
    *,
    table_name: str,
    db_path: Any = None,
    con: Any = None,
    where_sql: str = "",
    params: list | None = None,
    order_by: str | None = None,
) -> pd.DataFrame:
    own_con, should_close = _resolve_con(con, db_path)
    try:
        sql = f"SELECT * FROM {table_name}{where_sql}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        return own_con.execute(sql, params or []).fetchdf()
    finally:
        if should_close:
            own_con.close()


def query_financial_as_of(
    *,
    as_of_date: Any,
    table: str,
    tickers: str | Sequence[str] | None = None,
    report_period_start: Any = None,
    report_period_end: Any = None,
    db_path: Any = None,
    con: Any = None,
) -> pd.DataFrame:
    """Query point-in-time financial statements / indicators.

    Args:
        as_of_date: Historical trading date used for availability filtering.
        table: Financial table name from the whitelist.
        tickers: Optional ticker or ticker list.
        report_period_start / report_period_end: Optional report-period bounds.
        db_path / con: Optional DuckDB source. Defaults to project read-only DB.

    Returns:
        DataFrame with at most one latest available version per business entity.
        Empty DataFrame when no eligible rows exist.
    """
    if table not in FINANCIAL_TABLES:
        raise ValueError(
            f"unsupported financial table: {table!r}; "
            f"allowed={sorted(FINANCIAL_TABLES)}"
        )
    as_of = _require_as_of_date(as_of_date)
    entity_keys = list(FINANCIAL_TABLES[table])

    clauses: list[str] = ["available_trade_date <= ?"]
    params: list[Any] = [as_of]
    ticker_list = _as_list(tickers)
    if ticker_list:
        placeholders = ", ".join("?" * len(ticker_list))
        clauses.append(f"ticker IN ({placeholders})")
        params.extend(ticker_list)
    if report_period_start is not None:
        clauses.append("report_period >= ?")
        params.append(_normalize_date(report_period_start))
    if report_period_end is not None:
        clauses.append("report_period <= ?")
        params.append(_normalize_date(report_period_end))
    where_sql = " WHERE " + " AND ".join(clauses)

    raw = _read_table(
        table_name=table,
        db_path=db_path,
        con=con,
        where_sql=where_sql,
        params=params,
        order_by="ticker, report_period, available_trade_date, revision_id",
    )
    if raw.empty:
        return raw.reset_index(drop=True)

    selected = select_latest_available_records(
        raw,
        as_of_date=as_of,
        entity_keys=entity_keys,
        available_date_col="available_trade_date",
        published_at_col="published_at" if "published_at" in raw.columns else None,
        ingested_at_col="ingested_at" if "ingested_at" in raw.columns else None,
        revision_col="revision_id" if "revision_id" in raw.columns else None,
    )
    sort_cols = [c for c in entity_keys if c in selected.columns]
    if sort_cols:
        selected = selected.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return selected


def query_industry_as_of(
    *,
    as_of_date: Any,
    asset_ids: str | Sequence[str] | None = None,
    classification_system: str | None = None,
    industry_level: int | Sequence[int] | None = None,
    include_full_path: bool = False,
    mask_future_effective_to: bool = True,
    db_path: Any = None,
    con: Any = None,
) -> pd.DataFrame:
    """Query historical Shenwan industry membership as of a trading date.

    Interval semantics are half-open:

    ``effective_from <= as_of_date`` and
    ``(effective_to IS NULL OR as_of_date < effective_to)``.

    Availability also requires ``available_trade_date <= as_of_date``.

    When ``mask_future_effective_to`` is True (default research output), any
    ``effective_to`` strictly after ``as_of_date`` is set to NULL so future exits
    do not leak into research inputs.
    """
    as_of = _require_as_of_date(as_of_date)
    as_of_ts = pd.Timestamp(as_of)

    clauses = [
        "available_trade_date <= ?",
        "effective_from <= ?",
        "(effective_to IS NULL OR effective_to > ?)",
    ]
    params: list[Any] = [as_of, as_of, as_of]

    asset_list = _as_list(asset_ids)
    if asset_list:
        placeholders = ", ".join("?" * len(asset_list))
        clauses.append(f"asset_id IN ({placeholders})")
        params.extend(asset_list)
    if classification_system is not None:
        clauses.append("classification_system = ?")
        params.append(str(classification_system))
    if industry_level is not None:
        if isinstance(industry_level, int):
            levels = [industry_level]
        else:
            levels = [int(x) for x in industry_level]
        placeholders = ", ".join("?" * len(levels))
        clauses.append(f"industry_level IN ({placeholders})")
        params.extend(levels)

    where_sql = " WHERE " + " AND ".join(clauses)
    raw = _read_table(
        table_name=INDUSTRY_MEMBERSHIP_HISTORY.name,
        db_path=db_path,
        con=con,
        where_sql=where_sql,
        params=params,
        order_by="asset_id, classification_system, industry_level, available_trade_date, revision_id",
    )
    if raw.empty:
        return raw.reset_index(drop=True)

    # Version identity includes industry_code so same-code revisions are resolved,
    # while different codes at the same level remain visible for conflict detection.
    version_keys = ["asset_id", "classification_system", "industry_level", "industry_code"]
    selected = select_latest_available_records(
        raw,
        as_of_date=as_of,
        entity_keys=version_keys,
        available_date_col="available_trade_date",
        published_at_col=None,
        ingested_at_col="ingested_at" if "ingested_at" in raw.columns else None,
        revision_col="revision_id" if "revision_id" in raw.columns else None,
    )
    if selected.empty:
        return selected

    membership_keys = ["asset_id", "classification_system", "industry_level"]
    conflict_counts = (
        selected.groupby(membership_keys, dropna=False).size().reset_index(name="n")
    )
    conflicts = conflict_counts[conflict_counts["n"] > 1]
    if not conflicts.empty:
        raise IndustryMembershipConflictError(
            "overlapping industry memberships detected for "
            f"{conflicts[membership_keys].to_dict('records')}"
        )

    out = selected.copy()
    if mask_future_effective_to and "effective_to" in out.columns:
        eff_to = pd.to_datetime(out["effective_to"], errors="coerce")
        out.loc[eff_to.notna() & (eff_to > as_of_ts), "effective_to"] = pd.NaT

    out = out.sort_values(
        ["asset_id", "classification_system", "industry_level", "industry_code"],
        kind="mergesort",
    ).reset_index(drop=True)

    if include_full_path:
        path = out.pivot_table(
            index=["asset_id", "classification_system"],
            columns="industry_level",
            values=["industry_code", "industry_name"],
            aggfunc="first",
        )
        # Flatten multiindex columns.
        flat = pd.DataFrame(index=path.index)
        for level in (1, 2, 3):
            code_key = ("industry_code", level)
            name_key = ("industry_name", level)
            flat[f"l{level}_code"] = path[code_key] if code_key in path.columns else pd.NA
            flat[f"l{level}_name"] = path[name_key] if name_key in path.columns else pd.NA
        flat = flat.reset_index().sort_values(
            ["asset_id", "classification_system"], kind="mergesort"
        ).reset_index(drop=True)
        return flat

    return out


def query_index_components_as_of(
    *,
    as_of_date: Any,
    index_code: str,
    db_path: Any = None,
    con: Any = None,
) -> pd.DataFrame:
    """Query index component weights using the snapshot model.

    Selection rule:
    - restrict to rows with ``snapshot_date <= as_of_date`` and
      ``available_trade_date <= as_of_date``;
    - choose the latest eligible ``snapshot_date`` for ``index_code``;
    - within that snapshot, resolve per-asset revisions with
      ``select_latest_available_records``.
    """
    if not index_code or not str(index_code).strip():
        raise ValueError("index_code is required")
    as_of = _require_as_of_date(as_of_date)
    index_code = str(index_code).strip()

    where_sql = (
        " WHERE index_code = ?"
        " AND snapshot_date <= ?"
        " AND available_trade_date <= ?"
    )
    params: list[Any] = [index_code, as_of, as_of]
    raw = _read_table(
        table_name=INDEX_COMPONENT_HISTORY.name,
        db_path=db_path,
        con=con,
        where_sql=where_sql,
        params=params,
        order_by="snapshot_date, asset_id, available_trade_date, revision_id",
    )
    if raw.empty:
        return raw.reset_index(drop=True)

    snap_series = pd.to_datetime(raw["snapshot_date"], errors="coerce")
    latest_snap = snap_series.max()
    if pd.isna(latest_snap):
        return raw.iloc[0:0].copy().reset_index(drop=True)

    snapshot_rows = raw.loc[snap_series == latest_snap].copy()
    selected = select_latest_available_records(
        snapshot_rows,
        as_of_date=as_of,
        entity_keys=["index_code", "snapshot_date", "asset_id"],
        available_date_col="available_trade_date",
        published_at_col=None,
        ingested_at_col="ingested_at" if "ingested_at" in snapshot_rows.columns else None,
        revision_col="revision_id" if "revision_id" in snapshot_rows.columns else None,
    )
    if selected.empty:
        return selected

    selected = selected.sort_values(
        ["index_code", "snapshot_date", "asset_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    return selected


def summarize_index_components(components: pd.DataFrame) -> dict[str, Any]:
    """Return stable audit stats for an index component snapshot result."""
    if components is None or components.empty:
        return {
            "component_count": 0,
            "weight_sum": 0.0,
            "snapshot_date": None,
            "index_code": None,
        }
    snap = components["snapshot_date"].iloc[0] if "snapshot_date" in components.columns else None
    code = components["index_code"].iloc[0] if "index_code" in components.columns else None
    weight_sum = float(pd.to_numeric(components.get("weight"), errors="coerce").fillna(0).sum())
    return {
        "component_count": int(len(components)),
        "weight_sum": weight_sum,
        "snapshot_date": snap,
        "index_code": code,
    }
