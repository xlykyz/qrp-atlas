from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb

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
