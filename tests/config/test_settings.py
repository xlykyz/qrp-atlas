from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from qrp_atlas.config.auth import AuthSettings
from qrp_atlas.config.operations import InitStatus, doctor, has_failures, initialize_runtime
from qrp_atlas.config.settings import (
    AppSettings,
    ConfigError,
    PathSyntax,
    apply_proxy_environment,
    classify_path_syntax,
    redact_secrets,
)


def load(tmp_path: Path, **values: str) -> AppSettings:
    base = {
        "QRP_HOME": str(tmp_path / "home"),
        "QRP_DATA_DIR": str(tmp_path / "data"),
    }
    base.update(values)
    return AppSettings.load(environ=base, project_root=tmp_path / "repo")


def test_default_configuration_preserves_repository_layout(tmp_path):
    root = tmp_path / "repo"
    settings = AppSettings.load(environ={}, project_root=root)

    assert settings.paths.home == root.resolve()
    assert settings.paths.data_dir == (root / "data").resolve()
    assert settings.paths.duckdb_path == (root / "data" / "db" / "quant.db").resolve()
    assert settings.paths.irm_qa_duckdb_path == (root / "data" / "db" / "irm_qa.duckdb").resolve()
    assert settings.paths.job_runtime_db_path == (
        root / "data" / "runtime" / "job" / "job_runtime.sqlite3"
    ).resolve()
    assert settings.paths.backtest_runs_dir == (root / "data" / "backtest_runs").resolve()
    assert settings.database.read_only is False
    assert settings.authentication.mode.value == "local"
    assert settings.external_services.tushare_token is None


def test_environment_overrides_and_relative_paths_resolve_from_project_root(tmp_path):
    root = tmp_path / "repo"
    settings = AppSettings.load(
        environ={
            "QRP_HOME": "runtime-home",
            "QRP_DATA_DIR": "storage",
            "QRP_DUCKDB_PATH": "database/custom.duckdb",
            "QRP_IRM_QA_DUCKDB_PATH": "database/irm_qa_custom.duckdb",
            "QRP_API_PORT": "9100",
            "QRP_READ_ONLY": "yes",
        },
        project_root=root,
    )

    assert settings.paths.home == (root / "runtime-home").resolve()
    assert settings.paths.data_dir == (root / "storage").resolve()
    assert settings.paths.duckdb_path == (root / "database" / "custom.duckdb").resolve()
    assert settings.paths.irm_qa_duckdb_path == (root / "database" / "irm_qa_custom.duckdb").resolve()
    assert settings.paths.job_runtime_db_path == (
        root / "storage" / "runtime" / "job" / "job_runtime.sqlite3"
    ).resolve()
    assert settings.api.port == 9100
    assert settings.runtime.read_only is True


def test_job_runtime_database_path_can_be_configured_independently(tmp_path):
    settings = AppSettings.load(
        environ={
            "QRP_JOB_RUNTIME_DIR": str(tmp_path / "runtime"),
            "QRP_JOB_RUNTIME_DB_PATH": str(tmp_path / "state" / "jobs.sqlite3"),
        },
        project_root=tmp_path,
    )
    assert settings.paths.job_runtime_db_path == (tmp_path / "state" / "jobs.sqlite3").resolve()


def test_precedence_explicit_over_environment_over_dotenv_over_default(tmp_path):
    env_file = tmp_path / "settings.env"
    env_file.write_text("QRP_API_PORT=7001\nQRP_LOG_LEVEL=WARNING\n", encoding="utf-8")


    settings = AppSettings.load(
        overrides={"QRP_API_PORT": "7003"},
        environ={"QRP_API_PORT": "7002", "QRP_ENV_FILE": str(env_file)},
        project_root=tmp_path,
    )

    assert settings.api.port == 7003
    assert settings.logging.level == "WARNING"
    assert settings.sources["QRP_API_PORT"] == "explicit"
    assert settings.sources["QRP_LOG_LEVEL"].startswith("dotenv:")


def test_legacy_path_alias_remains_compatible(tmp_path):
    expected = tmp_path / "legacy-runs"
    settings = AppSettings.load(
        environ={"QRP_ATLAS_BACKTEST_RUNS_DIR": str(expected)},
        project_root=tmp_path,
    )
    assert settings.paths.backtest_runs_dir == expected.resolve()
    assert settings.sources["QRP_BACKTEST_RUNS_DIR"].endswith(
        "QRP_ATLAS_BACKTEST_RUNS_DIR"
    )


def test_windows_and_posix_path_syntax_is_platform_independent():
    assert classify_path_syntax(r"C:\qrp\data") is PathSyntax.WINDOWS_ABSOLUTE
    assert classify_path_syntax(r"\\server\share\data") is PathSyntax.WINDOWS_ABSOLUTE
    assert classify_path_syntax("/srv/qrp/data") is PathSyntax.POSIX_ABSOLUTE
    assert classify_path_syntax("data/quant.db") is PathSyntax.RELATIVE


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("QRP_READ_ONLY", "sometimes"),
        ("QRP_API_PORT", "eight-thousand"),
        ("QRP_API_PORT", "70000"),
        ("QRP_TUSHARE_API_URL", "not-a-url"),
        ("QRP_HTTP_PROXY", "ftp://proxy.example"),
        ("QRP_DUCKDB_PATH", "bad\x00path"),
    ],
)
def test_invalid_typed_values_fail_without_silent_fallback(tmp_path, name, value):
    with pytest.raises(ConfigError, match=name):
        AppSettings.load(environ={name: value}, project_root=tmp_path)


def test_foreign_absolute_path_is_rejected(tmp_path):
    value = "/srv/qrp/data" if os.name == "nt" else r"C:\qrp\data"
    with pytest.raises(ConfigError, match="absolute path"):
        AppSettings.load(environ={"QRP_DATA_DIR": value}, project_root=tmp_path)


def test_database_auth_requires_postgres_dsn(tmp_path):
    with pytest.raises(ConfigError, match="QRP_AUTH_DATABASE_URL"):
        AppSettings.load(
            environ={"QRP_AUTH_MODE": "database"},
            project_root=tmp_path,
        )


def test_optional_secret_can_be_missing_and_safe_output_is_redacted(tmp_path):
    secret = "secret-token-value"
    dsn = "postgresql://user:password@db.example:5432/qrp"
    settings = AppSettings.load(
        environ={
            "QRP_AUTH_MODE": "database",
            "QRP_AUTH_DATABASE_URL": dsn,
            "TUSHARE_TOKEN": secret,
        },
        project_root=tmp_path,
    )
    safe = settings.safe_json()

    assert secret not in safe
    assert dsn not in safe
    assert "configured" in safe
    assert secret not in repr(settings)
    assert dsn not in repr(settings)
    compatibility = AuthSettings(postgres_dsn=dsn)
    assert dsn not in repr(compatibility)
    assert redact_secrets(f"failure token={secret} dsn={dsn}", settings).count(secret) == 0


def test_init_is_idempotent_and_does_not_create_database(tmp_path):
    settings = load(tmp_path)
    first = initialize_runtime(settings)
    second = initialize_runtime(settings)

    assert any(item.status is InitStatus.CREATED for item in first)
    assert not has_failures(first)
    assert not has_failures(second)
    assert not settings.paths.duckdb_path.exists()
    assert all(
        item.status in {InitStatus.EXISTS, InitStatus.SKIPPED}
        for item in second
    )


def test_read_only_init_does_not_pollute_missing_paths(tmp_path):
    settings = load(tmp_path, QRP_READ_ONLY="true")
    results = initialize_runtime(settings)

    assert has_failures(results)
    assert not settings.paths.data_dir.exists()
    assert not settings.paths.home.exists()


def test_doctor_reports_success_and_blocking_failure(tmp_path):
    import duckdb

    settings = load(tmp_path)
    initialize_runtime(settings)
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    connection.execute("CREATE TABLE healthcheck(value INTEGER)")
    connection.close()

    assert not has_failures(doctor(settings))

    readonly_missing = load(
        tmp_path / "missing",
        QRP_READ_ONLY="true",
    )
    assert has_failures(doctor(readonly_missing))


def test_compatibility_auth_settings_uses_unified_parser(monkeypatch):
    monkeypatch.setenv("QRP_AUTH_MODE", "local")
    monkeypatch.setenv("QRP_AUTH_SESSION_TTL_SECONDS", "3600")
    settings = AuthSettings.from_env()
    assert settings.mode.value == "local"
    assert settings.session_ttl_seconds == 3600



def test_env_example_matches_public_configuration_names():
    repository_root = Path(__file__).resolve().parents[2]
    names: set[str] = set()
    pattern = re.compile(r"^#?\s*([A-Z][A-Z0-9_]*)=")
    for line in (repository_root / ".env.example").read_text(encoding="utf-8").splitlines():
        match = pattern.match(line)
        if match:
            names.add(match.group(1))

    from qrp_atlas.config.settings import SUPPORTED_ENV_VARS

    assert names == SUPPORTED_ENV_VARS


def test_proxy_environment_is_applied_from_unified_settings(tmp_path, monkeypatch):
    settings = AppSettings.load(
        environ={
            "QRP_HTTP_PROXY": "http://proxy.example.com:8080",
            "QRP_HTTPS_PROXY": "https://proxy.example.com:8443",
            "QRP_NO_PROXY": "localhost,127.0.0.1",
        },
        project_root=tmp_path,
    )
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)

    apply_proxy_environment(settings)

    assert os.environ["HTTP_PROXY"] == "http://proxy.example.com:8080"
    assert os.environ["http_proxy"] == "http://proxy.example.com:8080"
    assert os.environ["HTTPS_PROXY"] == "https://proxy.example.com:8443"
    assert os.environ["NO_PROXY"] == "localhost,127.0.0.1"
