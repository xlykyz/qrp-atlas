"""Post-backfill quality audit queries for PIT tables."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from qrp_atlas.pipeline.fundamentals.run import ALL_TABLES
from qrp_atlas.pipeline.pit_backfill.batches import DEFAULT_INDEX_CODES


def _con(db_path: str | Path):
    return duckdb.connect(str(db_path), read_only=True)


def audit_fundamentals(db_path: str | Path) -> dict[str, Any]:
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
            pr = con.execute(
                f"SELECT MIN(report_period), MAX(report_period) FROM {table}"
            ).fetchone()
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
                str(r[0])
                for r in con.execute(
                    f"SELECT DISTINCT report_period FROM {table} ORDER BY 1"
                ).fetchall()
            }
            by_table[table] = {
                "exists": True,
                "rows": int(total),
                "tickers": int(tickers),
                "report_period_min": str(pr[0]) if pr[0] is not None else None,
                "report_period_max": str(pr[1]) if pr[1] is not None else None,
                "revision_id_duplicates": int(rev_dup),
                "source_record_id_multi_revision": int(multi_rev),
                "distinct_report_periods": len(periods),
            }
        return {"tables": by_table, "table_row_sum": sum(
            v.get("rows", 0) for v in by_table.values() if v.get("exists")
        )}
    finally:
        con.close()


def audit_industry(db_path: str | Path) -> dict[str, Any]:
    con = _con(db_path)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'industry_membership_history'"
        ).fetchone()[0]
        if not exists:
            return {"exists": False}
        total = con.execute("SELECT COUNT(*) FROM industry_membership_history").fetchone()[0]
        l1 = con.execute(
            """
            SELECT COUNT(DISTINCT industry_code)
            FROM industry_membership_history
            WHERE industry_level = 1
            """
        ).fetchone()[0]
        assets = con.execute(
            "SELECT COUNT(DISTINCT asset_id) FROM industry_membership_history"
        ).fetchone()[0]
        by_level = {
            int(r[0]): int(r[1])
            for r in con.execute(
                """
                SELECT industry_level, COUNT(*)
                FROM industry_membership_history
                GROUP BY 1 ORDER BY 1
                """
            ).fetchall()
        }
        active = con.execute(
            """
            SELECT COUNT(*) FROM industry_membership_history
            WHERE effective_to IS NULL AND industry_level = 3
            """
        ).fetchone()[0]
        historical_exit = con.execute(
            """
            SELECT COUNT(*) FROM industry_membership_history
            WHERE effective_to IS NOT NULL
            """
        ).fetchone()[0]
        # Overlapping intervals for same asset/level/code
        overlaps = con.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT a.asset_id, a.industry_level, a.industry_code
              FROM industry_membership_history a
              JOIN industry_membership_history b
                ON a.asset_id = b.asset_id
               AND a.industry_level = b.industry_level
               AND a.industry_code = b.industry_code
               AND a.revision_id < b.revision_id
               AND a.effective_from <= COALESCE(b.effective_to, DATE '9999-12-31')
               AND b.effective_from <= COALESCE(a.effective_to, DATE '9999-12-31')
              GROUP BY 1,2,3
            )
            """
        ).fetchone()[0]
        missing_path = con.execute(
            """
            SELECT COUNT(*) FROM industry_membership_history
            WHERE industry_code IS NULL OR industry_code = ''
               OR asset_id IS NULL OR asset_id = ''
            """
        ).fetchone()[0]
        return {
            "exists": True,
            "rows": int(total),
            "l1_industry_count": int(l1),
            "asset_count": int(assets),
            "rows_by_level": by_level,
            "current_active_l3_rows": int(active),
            "historical_exit_rows": int(historical_exit),
            "overlap_groups": int(overlaps),
            "path_missing_rows": int(missing_path),
        }
    finally:
        con.close()


def audit_index(db_path: str | Path, index_codes: list[str] | None = None) -> dict[str, Any]:
    codes = list(index_codes or DEFAULT_INDEX_CODES)
    con = _con(db_path)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'index_component_history'"
        ).fetchone()[0]
        if not exists:
            return {"exists": False}
        by_index: dict[str, Any] = {}
        for code in codes:
            snaps = con.execute(
                """
                SELECT COUNT(DISTINCT snapshot_date), MIN(snapshot_date), MAX(snapshot_date)
                FROM index_component_history WHERE index_code = ?
                """,
                [code],
            ).fetchone()
            comps = con.execute(
                """
                SELECT snapshot_date, COUNT(*) AS n, SUM(weight) AS w,
                       COUNT(*) - COUNT(DISTINCT asset_id) AS dup_assets
                FROM index_component_history
                WHERE index_code = ?
                GROUP BY snapshot_date
                ORDER BY snapshot_date
                """,
                [code],
            ).fetchall()
            component_counts = [int(r[1]) for r in comps]
            weight_sums = [float(r[2]) if r[2] is not None else None for r in comps]
            dup_snaps = sum(1 for r in comps if int(r[3]) > 0)
            by_index[code] = {
                "snapshot_count": int(snaps[0] or 0),
                "snapshot_min": str(snaps[1]) if snaps[1] is not None else None,
                "snapshot_max": str(snaps[2]) if snaps[2] is not None else None,
                "components_per_snapshot_min": min(component_counts) if component_counts else 0,
                "components_per_snapshot_max": max(component_counts) if component_counts else 0,
                "components_per_snapshot_avg": (
                    round(sum(component_counts) / len(component_counts), 2) if component_counts else 0
                ),
                "weight_sum_min": min((w for w in weight_sums if w is not None), default=None),
                "weight_sum_max": max((w for w in weight_sums if w is not None), default=None),
                "snapshots_with_duplicate_assets": dup_snaps,
            }
        return {"exists": True, "by_index": by_index}
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
