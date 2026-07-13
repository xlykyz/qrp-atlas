"""Load financial tables into DuckDB with append-only revision semantics."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import align_to_schema, init_database, quick_validate
from qrp_atlas.pipeline.pit_utils import append_only_insert


def load_financial(
    df: pd.DataFrame,
    table: str,
    *,
    db_path: str | Path | None = None,
    init: bool = True,
) -> int:
    """Append new revision rows only. Returns inserted row count."""
    ensure_dirs()
    path = Path(db_path or DB_PATH)
    if df is None or df.empty:
        # still ensure table exists when requested
        if init:
            con = duckdb.connect(str(path))
            try:
                init_database(con)
            finally:
                con.close()
        return 0

    df = align_to_schema(df, table, fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, table, allow_extra=False)

    con = duckdb.connect(str(path))
    try:
        if init:
            init_database(con)
        con.execute("BEGIN")
        inserted = append_only_insert(con, table, df, id_column="revision_id")
        con.execute("COMMIT")
        return inserted
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
