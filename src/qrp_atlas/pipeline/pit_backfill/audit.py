"""Post-backfill quality audit queries for PIT tables (revision-aware)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.pipeline.fundamentals.run import ALL_TABLES
from qrp_atlas.pipeline.pit_backfill.batches import (
    DEFAULT_INDEX_CODES,
    FINANCIAL_END,
    FINANCIAL_START,
    iter_quarter_ends,
)


def _con(db_path: str | Path):
    return duckdb.connect(str(db_path), read_only=True)


def _to_date(v) -> date | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, pd.Timestamp):
        return v.date()
    text = str(v).strip()
    if not text or text.lower() in {"none", "nan", "nat", "null"}:
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def audit_fundamentals(db_path: str | Path) -> dict[str, Any]:
    expected_periods = {d.isoformat() for d in iter_quarter_ends(FINANCIAL_START, FINANCIAL_END)}
    con = _con(db_path)
    try:
        by_table: dict[str, Any] = {}
        for table in ALL_TABLES:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()[0]
            if not exists:
                by_table[table] = {"exists": False}
                continue
            total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            tickers = con.execute(f"SELECT COUNT(DISTINCT ticker) FROM {table}").fetchone()[0]
            pr = con.execute(f"SELECT MIN(report_period), MAX(report_period) FROM {table}").fetchone()
            # physical revision duplicates
            rev_dup = con.execute(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT revision_id, COUNT(*) c FROM {table}
                  GROUP BY revision_id HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
            multi_rev = con.execute(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT source_record_id, COUNT(DISTINCT revision_id) c
                  FROM {table}
                  GROUP BY source_record_id HAVING COUNT(DISTINCT revision_id) > 1
                )
                """
            ).fetchone()[0]
            periods = {
                str(_to_date(r[0]) or r[0])
                for r in con.execute(f"SELECT DISTINCT report_period FROM {table}").fetchall()
            }
            # normalize iso dates if present
            periods_norm = set()
            for p in periods:
                d = _to_date(p)
                periods_norm.add(d.isoformat() if d else str(p))
            missing = sorted(expected_periods - periods_norm)
            by_table[table] = {
                "exists": True,
                "rows": int(total),
                "tickers": int(tickers),
                "report_period_min": str(pr[0]) if pr[0] is not None else None,
                "report_period_max": str(pr[1]) if pr[1] is not None else None,
                "physical_revision_id_duplicates": int(rev_dup),
                "source_record_id_multi_revision": int(multi_rev),
                "distinct_report_periods": len(periods_norm),
                "missing_expected_report_periods": missing,
                "missing_expected_report_period_count": len(missing),
            }
        return {
            "tables": by_table,
            "table_row_sum": sum(v.get("rows", 0) for v in by_table.values() if v.get("exists")),
            "expected_report_period_count": len(expected_periods),
        }
    finally:
        con.close()


def _latest_industry_versions(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve latest revision per business identity for industry membership."""
    if df is None or df.empty:
        return df
    work = df.copy()
    # business identity for a membership interval version chain
    keys = ["asset_id", "classification_system", "industry_level", "industry_code", "effective_from"]
    for k in keys:
        if k not in work.columns:
            raise RuntimeError(f"industry audit missing column {k}")
    # prefer newest ingested_at then revision_id
    if "ingested_at" in work.columns:
        work = work.sort_values(["ingested_at", "revision_id"], kind="mergesort")
    else:
        work = work.sort_values(["revision_id"], kind="mergesort")
    return work.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def audit_industry(db_path: str | Path) -> dict[str, Any]:
    con = _con(db_path)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'industry_membership_history'"
        ).fetchone()[0]
        if not exists:
            return {"exists": False}
        raw = con.execute("SELECT * FROM industry_membership_history").fetchdf()
        physical_rev_dup = int(
            raw.groupby("revision_id").size().gt(1).sum() if not raw.empty else 0
        )
        latest = _latest_industry_versions(raw)
        total = len(raw)
        l1 = int(latest.loc[latest["industry_level"] == 1, "industry_code"].nunique()) if not latest.empty else 0
        assets = int(latest["asset_id"].nunique()) if not latest.empty else 0
        by_level = (
            latest.groupby("industry_level").size().astype(int).to_dict() if not latest.empty else {}
        )
        by_level = {int(k): int(v) for k, v in by_level.items()}
        active = int(((latest["effective_to"].isna()) & (latest["industry_level"] == 3)).sum()) if not latest.empty else 0
        historical_exit = int(latest["effective_to"].notna().sum()) if not latest.empty else 0

        # half-open interval overlaps on resolved versions: same asset/level/code
        overlap_groups = 0
        same_level_conflicts = 0
        if not latest.empty:
            latest = latest.copy()
            latest["_from"] = latest["effective_from"].map(_to_date)
            latest["_to"] = latest["effective_to"].map(_to_date)
            # treat null end as open
            for (asset, level, code), g in latest.groupby(["asset_id", "industry_level", "industry_code"]):
                rows = g.sort_values("_from").to_dict("records")
                for i in range(len(rows)):
                    a0 = _to_date(rows[i]["_from"])
                    a1 = _to_date(rows[i]["_to"])
                    if a0 is None:
                        continue
                    for j in range(i + 1, len(rows)):
                        b0 = _to_date(rows[j]["_from"])
                        b1 = _to_date(rows[j]["_to"])
                        if b0 is None:
                            continue
                        # half-open [from, to)
                        a_end = a1 or date(9999, 12, 31)
                        b_end = b1 or date(9999, 12, 31)
                        if a0 < b_end and b0 < a_end:
                            overlap_groups += 1
                            break
            # same asset + level + overlapping time but different industry_code
            for (asset, level), g in latest.groupby(["asset_id", "industry_level"]):
                rows = g.sort_values("_from").to_dict("records")
                for i in range(len(rows)):
                    for j in range(i + 1, len(rows)):
                        if rows[i]["industry_code"] == rows[j]["industry_code"]:
                            continue
                        a0 = _to_date(rows[i]["_from"])
                        a1 = _to_date(rows[i]["_to"])
                        b0 = _to_date(rows[j]["_from"])
                        b1 = _to_date(rows[j]["_to"])
                        if a0 is None or b0 is None:
                            continue
                        a_end = a1 or date(9999, 12, 31)
                        b_end = b1 or date(9999, 12, 31)
                        if a0 < b_end and b0 < a_end:
                            same_level_conflicts += 1

        # true level-3 path missing: L3 row without L1/L2 code on same source chain is hard;
        # proxy: any level row with empty industry_code/name/asset
        path_missing = 0
        if not latest.empty:
            path_missing = int(
                (
                    latest["industry_code"].isna()
                    | (latest["industry_code"].astype(str).str.strip() == "")
                    | latest["asset_id"].isna()
                    | (latest["asset_id"].astype(str).str.strip() == "")
                ).sum()
            )
            # L3 without corresponding L1/L2 for same asset on overlapping half-open interval
            l3 = latest[latest["industry_level"] == 3]
            l1df = latest[latest["industry_level"] == 1].copy()
            l2df = latest[latest["industry_level"] == 2].copy()
            if not l1df.empty:
                l1df["_from"] = l1df["effective_from"].map(_to_date)
                l1df["_to"] = l1df["effective_to"].map(_to_date)
            if not l2df.empty:
                l2df["_from"] = l2df["effective_from"].map(_to_date)
                l2df["_to"] = l2df["effective_to"].map(_to_date)
            missing_path_assets = 0
            for _, r in l3.iterrows():
                asset = r["asset_id"]
                t0 = _to_date(r["effective_from"])
                if t0 is None:
                    continue
                t1 = _to_date(r["effective_to"]) or date(9999, 12, 31)
                def covers(df):
                    if df is None or df.empty:
                        return False
                    sub = df[df["asset_id"] == asset]
                    for _, x in sub.iterrows():
                        a0 = _to_date(x["_from"] if "_from" in x.index else x.get("effective_from"))
                        if a0 is None:
                            continue
                        a1 = _to_date(x["_to"] if "_to" in x.index else x.get("effective_to")) or date(9999, 12, 31)
                        # half-open overlap of membership windows
                        if a0 < t1 and t0 < a1:
                            return True
                    return False
                if not covers(l1df) or not covers(l2df):
                    missing_path_assets += 1
            path_missing = max(path_missing, missing_path_assets)

        return {
            "exists": True,
            "physical_rows": int(total),
            "resolved_rows": int(len(latest)),
            "physical_revision_id_duplicates": physical_rev_dup,
            "l1_industry_count": l1,
            "asset_count": assets,
            "rows_by_level_resolved": by_level,
            "current_active_l3_rows_resolved": active,
            "historical_exit_rows_resolved": historical_exit,
            "resolved_same_code_overlap_groups": int(overlap_groups),
            "resolved_same_level_different_industry_conflicts": int(same_level_conflicts),
            "path_missing_rows_or_assets": int(path_missing),
        }
    finally:
        con.close()


def _latest_index_versions(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    work = df.copy()
    keys = ["index_code", "snapshot_date", "asset_id"]
    for k in keys:
        if k not in work.columns:
            raise RuntimeError(f"index audit missing column {k}")
    if "ingested_at" in work.columns:
        work = work.sort_values(["ingested_at", "revision_id"], kind="mergesort")
    else:
        work = work.sort_values(["revision_id"], kind="mergesort")
    return work.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def audit_index(db_path: str | Path, index_codes: list[str] | None = None) -> dict[str, Any]:
    codes = list(index_codes or DEFAULT_INDEX_CODES)
    con = _con(db_path)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'index_component_history'"
        ).fetchone()[0]
        if not exists:
            return {"exists": False}
        raw = con.execute("SELECT * FROM index_component_history").fetchdf()
        physical_rev_dup = int(raw.groupby("revision_id").size().gt(1).sum() if not raw.empty else 0)
        latest = _latest_index_versions(raw)
        by_index: dict[str, Any] = {}
        for code in codes:
            sub = latest[latest["index_code"] == code] if not latest.empty else latest
            if sub is None or sub.empty:
                by_index[code] = {"snapshot_count": 0}
                continue
            snaps = sub.groupby("snapshot_date")
            component_counts = []
            weight_sums = []
            dup_snaps = 0
            for snap, g in snaps:
                # after resolve, asset_id should be unique per snapshot
                n = len(g)
                n_assets = g["asset_id"].nunique()
                if n != n_assets:
                    dup_snaps += 1
                component_counts.append(int(n_assets))
                if "weight" in g.columns:
                    weight_sums.append(float(g["weight"].sum()))
            by_index[code] = {
                "snapshot_count": int(sub["snapshot_date"].nunique()),
                "snapshot_min": str(sub["snapshot_date"].min()),
                "snapshot_max": str(sub["snapshot_date"].max()),
                "components_per_snapshot_min": min(component_counts) if component_counts else 0,
                "components_per_snapshot_max": max(component_counts) if component_counts else 0,
                "components_per_snapshot_avg": (
                    round(sum(component_counts) / len(component_counts), 2) if component_counts else 0
                ),
                "weight_sum_min": min(weight_sums) if weight_sums else None,
                "weight_sum_max": max(weight_sums) if weight_sums else None,
                "snapshots_with_duplicate_assets_after_resolve": dup_snaps,
                "physical_rows_for_index": int((raw["index_code"] == code).sum()) if not raw.empty else 0,
                "resolved_rows_for_index": int(len(sub)),
            }
        return {
            "exists": True,
            "physical_revision_id_duplicates": physical_rev_dup,
            "physical_rows": int(len(raw)),
            "resolved_rows": int(len(latest)),
            "by_index": by_index,
        }
    finally:
        con.close()


def run_full_audit(db_path: str | Path) -> dict[str, Any]:
    return {
        "fundamentals": audit_fundamentals(db_path),
        "industry": audit_industry(db_path),
        "index": audit_index(db_path),
        "db_path": str(Path(db_path).resolve()),
        "db_size_bytes": Path(db_path).stat().st_size if Path(db_path).exists() else 0,
    }
