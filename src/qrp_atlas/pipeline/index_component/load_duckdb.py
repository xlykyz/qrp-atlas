from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import align_to_schema, init_database, quick_validate
from qrp_atlas.pipeline.pit_utils import append_only_insert


def load_index_component(
    df: pd.DataFrame,
    *,
    db_path: str | Path | None = None,
    init: bool = True,
) -> int:
    ensure_dirs()
    path = Path(db_path or DB_PATH)
    if df is None or df.empty:
        if init:
            con = duckdb.connect(str(path))
            try:
                init_database(con)
            finally:
                con.close()
        return 0
    df = align_to_schema(df, "index_component_history", fill_missing_optional=True, drop_extra=True)
    df = quick_validate(df, "index_component_history", allow_extra=False)
    con = duckdb.connect(str(path))
    try:
        if init:
            init_database(con)
        con.execute("BEGIN")
        n = append_only_insert(con, "index_component_history", df, id_column="revision_id")
        con.execute("COMMIT")
        return n
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
