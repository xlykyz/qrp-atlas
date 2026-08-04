from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import scripts.migrate_tushare_snapshot_tables as migration


def _database_snapshot(db_path: Path) -> dict[str, tuple[tuple[object, ...], int]]:
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        return _connection_snapshot(connection)
    finally:
        connection.close()


def _connection_snapshot(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, tuple[tuple[object, ...], int]]:
    result: dict[str, tuple[tuple[object, ...], int]] = {}
    for row in connection.execute("SHOW TABLES").fetchall():
        table_name = str(row[0])
        description = tuple(connection.execute(f'DESCRIBE "{table_name}"').fetchall())
        count = int(connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0])
        result[table_name] = (description, count)
    return result


def _backup_paths(db_path: Path) -> list[Path]:
    return sorted(db_path.parent.glob(f"{db_path.stem}.backup_tushare_snapshots_*"))


def test_default_native_backup_captures_uncheckpointed_table_and_matches_baseline(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quant.db"
    writer = duckdb.connect(str(db_path))
    try:
        writer.execute("PRAGMA disable_checkpoint_on_shutdown")
        writer.execute("CREATE TABLE uncheckpointed_source (value INTEGER, label VARCHAR)")
        writer.execute("INSERT INTO uncheckpointed_source VALUES (99, 'wal-data')")
    finally:
        writer.close()
    assert Path(f"{db_path}.wal").is_file()
    before = _database_snapshot(db_path)

    result = migration.migrate(db_path)

    backup_path = Path(str(result["backup"]))
    assert result["action"] == "migrated"
    assert backup_path.is_file()
    assert set(result["baseline_tables"]) == set(before)
    assert _database_snapshot(backup_path) == before
    backup_connection = duckdb.connect(str(backup_path), read_only=True)
    try:
        assert backup_connection.execute(
            "SELECT value, label FROM uncheckpointed_source"
        ).fetchall() == [(99, "wal-data")]
    finally:
        backup_connection.close()


def test_backup_validation_failure_removes_backup_and_does_not_migrate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE existing_table (value INTEGER)")
        connection.execute("INSERT INTO existing_table VALUES (1)")
    finally:
        connection.close()

    def fail_validation(*_args, **_kwargs) -> None:
        raise migration.BackupValidationError("simulated backup mismatch")

    monkeypatch.setattr(migration, "validate_backup", fail_validation)

    with pytest.raises(migration.BackupValidationError, match="simulated backup mismatch"):
        migration.migrate(db_path)

    assert _backup_paths(db_path) == []
    assert set(_database_snapshot(db_path)) == {"existing_table"}


def test_migration_transaction_failure_rolls_back_all_new_tables_and_keeps_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE existing_table (value INTEGER)")
    finally:
        connection.close()

    class BrokenTable:
        name = "broken_tushare_table"

        @staticmethod
        def duckdb_create_sql() -> str:
            return "CREATE TABLE broken_tushare_table ("

    monkeypatch.setattr(
        migration,
        "TABLES",
        (migration.LIMIT_STEP, BrokenTable()),
    )

    with pytest.raises(duckdb.Error):
        migration.migrate(db_path)

    assert set(_database_snapshot(db_path)) == {"existing_table"}
    backups = _backup_paths(db_path)
    assert len(backups) == 1
    assert set(_database_snapshot(backups[0])) == {"existing_table"}


def test_migration_creates_only_missing_tables_and_second_default_run_is_noop(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE existing_table (value INTEGER)")
    finally:
        connection.close()

    first = migration.migrate(db_path)
    assert first["action"] == "migrated"
    assert first["created_tables"] == ["limit_step", "ths_daily", "stk_high_shock"]
    assert set(_database_snapshot(db_path)) == {
        "existing_table",
        "limit_step",
        "ths_daily",
        "stk_high_shock",
    }

    second = migration.migrate(db_path)
    assert second["action"] == "noop"
    assert second["created_tables"] == []
    assert Path(str(second["backup"])).is_file()


def test_no_backup_is_explicitly_supported_for_isolated_test_database(tmp_path: Path) -> None:
    db_path = tmp_path / "quant.db"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE existing_table (value INTEGER)")
    finally:
        connection.close()

    result = migration.migrate(db_path, do_backup=False)

    assert result["action"] == "migrated"
    assert result["backup"] is None
    assert _backup_paths(db_path) == []
