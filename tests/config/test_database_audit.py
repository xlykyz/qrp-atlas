from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.operations import (
    CheckLevel,
    audit_databases,
    database_cleanup_candidates,
    initialize_runtime,
)
from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database, init_irm_database
from qrp_atlas.orchestration.store import JobRuntimeStore


def load_settings(tmp_path: Path, **values: str) -> AppSettings:
    environ = {
        "QRP_HOME": str(tmp_path / "home"),
        "QRP_DATA_DIR": str(tmp_path / "data"),
    }
    environ.update(values)
    return AppSettings.load(environ=environ, project_root=tmp_path / "repo")


def load_migration_module():
    path = Path(__file__).parents[2] / "scripts" / "migrate_canonical_schema.py"
    spec = importlib.util.spec_from_file_location("test_schema_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def database_snapshot(path: Path) -> dict[str, tuple[tuple[object, ...], int]]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        result: dict[str, tuple[tuple[object, ...], int]] = {}
        for row in connection.execute("SHOW TABLES").fetchall():
            table_name = str(row[0])
            description = tuple(connection.execute(f'DESCRIBE "{table_name}"').fetchall())
            count = int(connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0])
            result[table_name] = (description, count)
        return result
    finally:
        connection.close()


def migration_backups(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f"{path.stem}.schema_migration_backup_*"))


def test_database_audit_reports_configured_paths_and_cleanup_candidates(tmp_path):
    settings = load_settings(
        tmp_path,
        QRP_EPISODE_DB_PATH=str(tmp_path / "episode.duckdb"),
        QRP_POOL_DB_PATH=str(tmp_path / "pools.duckdb"),
    )
    initialize_runtime(settings)

    connection = duckdb.connect(str(settings.paths.duckdb_path))
    init_database(connection)
    connection.close()
    connection = duckdb.connect(str(settings.paths.irm_qa_duckdb_path))
    init_irm_database(connection)
    connection.close()
    JobRuntimeStore(settings.paths.job_runtime_db_path).initialize()

    settings.paths.episode_db_path.parent.mkdir(parents=True, exist_ok=True)
    episode = duckdb.connect(str(settings.paths.episode_db_path))
    episode.execute("CREATE TABLE system_b_episode (id INTEGER)")
    episode.close()
    pool = duckdb.connect(str(settings.paths.pool_db_path))
    pool.execute("CREATE TABLE system_b_pool_membership_daily (id INTEGER)")
    pool.execute("CREATE TABLE system_b_pool_run (id INTEGER)")
    pool.close()

    stale = settings.paths.data_dir / "quant.db"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.touch()
    backup = settings.paths.db_dir / "quant.backup_old.db"
    backup.touch()

    results = audit_databases(settings)
    by_id = {item.database_id: item for item in results}
    assert by_id["canonical"].level is CheckLevel.OK
    assert by_id["irm_qa"].level is CheckLevel.OK
    assert by_id["system_b_episode"].level is CheckLevel.FAILURE
    assert "system_b_episode_observation" in by_id["system_b_episode"].missing_tables
    assert by_id["system_b_pools"].level is CheckLevel.OK
    assert by_id["job_runtime"].level is CheckLevel.OK
    assert stale in database_cleanup_candidates(settings)
    assert backup in database_cleanup_candidates(settings)


def test_schema_migration_dry_run_and_apply_are_fail_closed(tmp_path):
    module = load_migration_module()
    target = tmp_path / "quant.db"
    connection = duckdb.connect(str(target))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.close()
    settings = load_settings(tmp_path, QRP_DUCKDB_PATH=str(target))
    backup = tmp_path / "quant.before.db"

    dry_run = module.migrate(settings, apply=False, backup_path=backup)
    assert dry_run["status"] == "PENDING"
    assert not backup.exists()
    assert "etf_daily" in dry_run["missing_tables"]
    readonly = load_settings(
        tmp_path,
        QRP_DUCKDB_PATH=str(target),
        QRP_READ_ONLY="true",
    )
    assert module.migrate(readonly, apply=False)["status"] == "PENDING"

    result = module.migrate(settings, apply=True, backup_path=backup)
    assert result["status"] == "MIGRATED"
    assert result["backup"] == str(backup.resolve())
    assert backup.exists()
    tables = set(module._table_names(target))
    assert {"etf_daily", "etf_adj_factor", "marker"} <= tables

    second = module.migrate(settings, apply=True, backup_path=tmp_path / "unused.db")
    assert second["status"] == "NOOP"
    assert second["backup"] is None


def test_schema_migration_default_backup_captures_uncheckpointed_data_and_structure(
    tmp_path: Path,
) -> None:
    module = load_migration_module()
    target = tmp_path / "quant.db"
    writer = duckdb.connect(str(target))
    try:
        writer.execute("PRAGMA disable_checkpoint_on_shutdown")
        writer.execute("CREATE TABLE uncheckpointed_source (value INTEGER, label VARCHAR)")
        writer.execute("INSERT INTO uncheckpointed_source VALUES (99, 'wal-data')")
    finally:
        writer.close()
    assert Path(f"{target}.wal").is_file()
    before = database_snapshot(target)

    settings = load_settings(tmp_path, QRP_DUCKDB_PATH=str(target))
    result = module.migrate(settings, apply=True)

    backup = Path(str(result["backup"]))
    assert result["status"] == "MIGRATED"
    assert backup.is_file()
    assert database_snapshot(backup) == before
    connection = duckdb.connect(str(backup), read_only=True)
    try:
        assert connection.execute(
            "SELECT value, label FROM uncheckpointed_source"
        ).fetchall() == [(99, "wal-data")]
    finally:
        connection.close()


def test_schema_migration_backup_validation_failure_does_not_migrate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_migration_module()
    target = tmp_path / "quant.db"
    connection = duckdb.connect(str(target))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.execute("INSERT INTO marker VALUES (1)")
    connection.close()

    def fail_validation(*_args, **_kwargs) -> None:
        raise module.MigrationError("simulated backup mismatch")

    monkeypatch.setattr(module, "validate_backup", fail_validation)
    settings = load_settings(tmp_path, QRP_DUCKDB_PATH=str(target))
    with pytest.raises(module.MigrationError, match="simulated backup mismatch"):
        module.migrate(settings, apply=True)

    assert migration_backups(target) == []
    assert set(database_snapshot(target)) == {"marker"}


def test_schema_migration_transaction_failure_rolls_back_and_keeps_verified_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_migration_module()
    target = tmp_path / "quant.db"
    connection = duckdb.connect(str(target))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.close()

    def fail_init(connection) -> None:
        connection.execute("CREATE TABLE partial_new (value INTEGER)")
        raise RuntimeError("simulated schema transaction failure")

    monkeypatch.setattr(module, "init_database", fail_init)
    settings = load_settings(tmp_path, QRP_DUCKDB_PATH=str(target))
    with pytest.raises(RuntimeError, match="simulated schema transaction failure"):
        module.migrate(settings, apply=True)

    assert set(database_snapshot(target)) == {"marker"}
    backups = migration_backups(target)
    assert len(backups) == 1
    assert set(database_snapshot(backups[0])) == {"marker"}


def test_schema_migration_read_only_apply_is_rejected(tmp_path: Path) -> None:
    module = load_migration_module()
    target = tmp_path / "quant.db"
    connection = duckdb.connect(str(target))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.close()
    settings = load_settings(
        tmp_path,
        QRP_DUCKDB_PATH=str(target),
        QRP_READ_ONLY="true",
    )

    with pytest.raises(module.MigrationError, match="QRP_READ_ONLY"):
        module.migrate(settings, apply=True)
