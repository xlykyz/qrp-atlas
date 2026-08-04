from __future__ import annotations

from pathlib import Path

import duckdb

from scripts.migrate_tushare_snapshot_tables import migrate


def test_migration_creates_only_missing_tushare_snapshot_tables_and_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE existing_table (value INTEGER)")
    finally:
        connection.close()

    result = migrate(db_path, do_backup=False)

    assert result["action"] == "migrated"
    assert result["created_tables"] == ["limit_step", "ths_daily", "stk_high_shock"]
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {"existing_table", "limit_step", "ths_daily", "stk_high_shock"} <= tables
        assert connection.execute("SELECT COUNT(*) FROM limit_step").fetchone()[0] == 0
    finally:
        connection.close()

    repeated = migrate(db_path, do_backup=False)
    assert repeated["action"] == "noop"
    assert repeated["created_tables"] == []
