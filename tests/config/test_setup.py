from __future__ import annotations

import io
import os
from pathlib import Path

import duckdb
import pytest
from dotenv import dotenv_values

from qrp_atlas.config.operations import CheckLevel, CheckResult
from qrp_atlas.config.settings import AppSettings, ConfigError
from qrp_atlas.config.setup import (
    DatabaseMode,
    SetupCancelled,
    SetupError,
    SetupIO,
    SetupOptions,
    atomic_write_env,
    profile_values,
    render_env_file,
    run_setup,
    validate_candidate,
)
from qrp_atlas.database import BASE_TABLES, create_empty_database, validate_existing_database


def non_interactive_options(tmp_path: Path, **changes) -> SetupOptions:
    values = {
        "profile": "local",
        "env_file": tmp_path / "runtime.env",
        "non_interactive": True,
        "assume_yes": True,
        "database_mode": DatabaseMode.CREATE,
        "home": str(tmp_path / "runtime"),
        "data_dir": str(tmp_path / "data"),
    }
    values.update(changes)
    return SetupOptions(**values)


def test_profiles_supply_expected_defaults_and_validate(tmp_path):
    local = profile_values("local", project_root=tmp_path)
    lan = profile_values("lan", project_root=tmp_path)

    assert local["QRP_API_HOST"] == "127.0.0.1"
    assert local["QRP_AUTH_MODE"] == "local"
    assert lan["QRP_API_HOST"] == "0.0.0.0"
    assert lan["QRP_RUNTIME_ENV"] == "development"
    assert AppSettings.load(overrides=local, environ={}, project_root=tmp_path)
    assert AppSettings.load(overrides=lan, environ={}, project_root=tmp_path)

    production = profile_values("production", project_root=tmp_path)
    production.update(
        {
            "QRP_API_CORS_ORIGINS": "https://atlas.example.com",
            "QRP_AUTH_DATABASE_URL": "postgresql://user:password@db.example/atlas",
        }
    )
    assert AppSettings.load(overrides=production, environ={}, project_root=tmp_path)


def test_production_rejects_missing_secret_and_insecure_cors(tmp_path):
    with pytest.raises(SetupError, match="CORS"):
        run_setup(
            non_interactive_options(
                tmp_path,
                profile="production",
                database_mode=DatabaseMode.SKIP,
            ),
            environ={"QRP_AUTH_DATABASE_URL": "postgresql://user:pass@db/atlas"},
            project_root=tmp_path,
        )

    with pytest.raises(SetupError, match="QRP_AUTH_DATABASE_URL"):
        run_setup(
            non_interactive_options(
                tmp_path,
                profile="production",
                database_mode=DatabaseMode.SKIP,
                cors_origins=("https://atlas.example.com",),
            ),
            environ={},
            project_root=tmp_path,
        )


def test_non_interactive_setup_creates_empty_database_and_custom_launch(tmp_path):
    output = io.StringIO()
    result = run_setup(
        non_interactive_options(tmp_path),
        io_adapter=SetupIO(output_stream=output, interactive=False),
        environ={},
        project_root=tmp_path,
    )

    assert result.database_status == "已创建"
    assert result.launch_command == f'qrp-atlas-api --env-file "{tmp_path / "runtime.env"}"'
    if os.name == "posix":
        assert result.env_file.stat().st_mode & 0o777 == 0o600
    assert validate_existing_database(result.settings.paths.duckdb_path) == tuple(sorted(BASE_TABLES))
    connection = duckdb.connect(str(result.settings.paths.duckdb_path), read_only=True)
    try:
        assert all(
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
            for table in BASE_TABLES
        )
    finally:
        connection.close()


def test_database_initializer_never_replaces_existing_file(tmp_path):
    database = tmp_path / "quant.db"
    database.write_text("not a database", encoding="utf-8")
    with pytest.raises(FileExistsError):
        create_empty_database(database)
    assert database.read_text(encoding="utf-8") == "not a database"


def test_reuse_database_is_read_only_and_skip_is_allowed(tmp_path):
    existing = tmp_path / "existing.db"
    connection = duckdb.connect(str(existing))
    connection.execute("CREATE TABLE marker(value INTEGER)")
    connection.close()

    reuse = run_setup(
        non_interactive_options(
            tmp_path,
            env_file=tmp_path / "reuse.env",
            database_mode=DatabaseMode.REUSE,
            duckdb_path=str(existing),
        ),
        io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
        environ={},
        project_root=tmp_path,
    )
    assert reuse.database_status == "已验证"
    assert validate_existing_database(existing) == ("marker",)

    skipped = run_setup(
        non_interactive_options(
            tmp_path / "skip",
            database_mode=DatabaseMode.SKIP,
        ),
        io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
        environ={},
        project_root=tmp_path / "skip",
    )
    assert skipped.database_status == "尚未创建"
    assert not skipped.settings.paths.duckdb_path.exists()


def test_existing_file_requires_explicit_update_and_preserves_unknowns_and_secret(tmp_path):
    env_file = tmp_path / "runtime.env"
    secret = "keep-this-token"
    env_file.write_text(
        "# operator note\nUNKNOWN_FLAG=keep\nTUSHARE_TOKEN='keep-this-token'\n",
        encoding="utf-8",
    )

    with pytest.raises(SetupError, match="--update-existing"):
        run_setup(
            non_interactive_options(
                tmp_path,
                env_file=env_file,
                database_mode=DatabaseMode.SKIP,
            ),
            io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
            environ={},
            project_root=tmp_path,
        )

    result = run_setup(
        non_interactive_options(
            tmp_path,
            env_file=env_file,
            database_mode=DatabaseMode.SKIP,
            update_existing=True,
        ),
        io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
        environ={},
        project_root=tmp_path,
    )
    text = env_file.read_text(encoding="utf-8")
    assert "# operator note" in text
    assert "UNKNOWN_FLAG=keep" in text
    assert dotenv_values(env_file)["TUSHARE_TOKEN"] == secret
    assert result.backup_path is not None
    assert result.backup_path.exists()
    if os.name == "posix":
        assert result.backup_path.stat().st_mode & 0o777 == 0o600


def test_render_round_trips_windows_and_posix_paths_and_tilde(tmp_path):
    content = render_env_file(
        "# keep\nUNKNOWN=value\n",
        {
            "QRP_HOME": r"C:\Users\Ryan\QRP Runtime",
            "QRP_DATA_DIR": "/srv/qrp atlas/data",
            "QRP_DUCKDB_PATH": "~/qrp-data/quant.db",
        },
    )
    env_file = tmp_path / "paths.env"
    env_file.write_text(content, encoding="utf-8")
    values = dotenv_values(env_file)
    assert values["QRP_HOME"] == r"C:\Users\Ryan\QRP Runtime"
    assert values["QRP_DATA_DIR"] == "/srv/qrp atlas/data"
    assert values["QRP_DUCKDB_PATH"] == "~/qrp-data/quant.db"
    assert values["UNKNOWN"] == "value"


def test_atomic_write_failure_keeps_original(tmp_path, monkeypatch):
    env_file = tmp_path / "runtime.env"
    env_file.write_text("ORIGINAL=yes\n", encoding="utf-8")
    original_replace = Path.replace

    def fail_replace(path, target):
        if Path(target) == env_file:
            raise OSError("simulated replace failure")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        atomic_write_env(env_file, "NEW=yes\n")
    assert env_file.read_text(encoding="utf-8") == "ORIGINAL=yes\n"


def test_doctor_failure_restores_original_configuration(tmp_path, monkeypatch):
    env_file = tmp_path / "runtime.env"
    env_file.write_text("# original\nUNKNOWN=yes\n", encoding="utf-8")
    original = env_file.read_bytes()

    monkeypatch.setattr(
        "qrp_atlas.config.setup.doctor",
        lambda settings: [CheckResult(CheckLevel.FAILURE, "forced", "forced failure")],
    )
    with pytest.raises(SetupError, match="doctor"):
        run_setup(
            non_interactive_options(
                tmp_path,
                env_file=env_file,
                database_mode=DatabaseMode.CREATE,
                update_existing=True,
            ),
            io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
            environ={},
            project_root=tmp_path,
        )
    assert env_file.read_bytes() == original
    assert not (tmp_path / "data" / "db" / "quant.db").exists()


def test_secret_values_never_appear_in_output_or_repr(tmp_path):
    token = "top-secret-token"
    dsn = "postgresql://user:password@db.example/atlas"
    proxy = "http://user:password@proxy.example:8080"
    output = io.StringIO()
    result = run_setup(
        non_interactive_options(
            tmp_path,
            profile="production",
            database_mode=DatabaseMode.SKIP,
            cors_origins=("https://atlas.example.com",),
            auth_mode="database",
            home=str(tmp_path.parent / f"{tmp_path.name}-runtime"),
            data_dir=str(tmp_path.parent / f"{tmp_path.name}-data"),
        ),
        io_adapter=SetupIO(output_stream=output, interactive=False),
        environ={
            "QRP_AUTH_DATABASE_URL": dsn,
            "TUSHARE_TOKEN": token,
            "QRP_HTTP_PROXY": proxy,
        },
        project_root=tmp_path,
    )
    combined = output.getvalue() + repr(result) + result.settings.safe_json()
    assert token not in combined
    assert dsn not in combined
    assert proxy not in combined


def test_non_tty_and_missing_non_interactive_inputs_fail_without_writes(tmp_path):
    with pytest.raises(SetupError, match="交互式终端"):
        run_setup(
            SetupOptions(env_file=tmp_path / "runtime.env"),
            io_adapter=SetupIO(input_stream=io.StringIO(), output_stream=io.StringIO()),
            project_root=tmp_path,
        )
    with pytest.raises(SetupError, match="--profile"):
        run_setup(
            SetupOptions(
                env_file=tmp_path / "runtime.env",
                non_interactive=True,
                assume_yes=True,
            ),
            io_adapter=SetupIO(output_stream=io.StringIO(), interactive=False),
            project_root=tmp_path,
        )
    assert not (tmp_path / "runtime.env").exists()


def test_interactive_cancel_and_eof_leave_no_files(tmp_path, monkeypatch):
    monkeypatch.setattr("qrp_atlas.config.setup._port_is_available", lambda host, port: True)
    cancel_input = io.StringIO("\n\nlocal\n")
    with pytest.raises(SetupCancelled):
        run_setup(
            SetupOptions(),
            io_adapter=SetupIO(
                input_stream=cancel_input,
                output_stream=io.StringIO(),
                interactive=True,
            ),
            project_root=tmp_path,
        )
    assert not (tmp_path / ".env").exists()

    with pytest.raises(SetupCancelled, match="输入已结束"):
        run_setup(
            SetupOptions(),
            io_adapter=SetupIO(
                input_stream=io.StringIO(""),
                output_stream=io.StringIO(),
                interactive=True,
            ),
            project_root=tmp_path,
        )


def test_interactive_defaults_invalid_port_correction_and_summary_confirmation(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("qrp_atlas.config.setup._port_is_available", lambda host, port: True)
    answers = "\n".join(
        [
            "",
            "local",
            str(tmp_path / "runtime"),
            str(tmp_path / "data"),
            "",
            "3",
            "",
            "invalid",
            "8123",
            "",
            "",
            "alice",
            "Alice",
            "n",
            "n",
            "save",
        ]
    ) + "\n"
    output = io.StringIO()
    result = run_setup(
        SetupOptions(),
        io_adapter=SetupIO(
            input_stream=io.StringIO(answers),
            output_stream=output,
            interactive=True,
        ),
        environ={},
        project_root=tmp_path,
    )
    assert result.settings.api.port == 8123
    assert result.settings.authentication.local_username == "alice"
    assert "配置摘要（秘密已脱敏）" in output.getvalue()
    assert "端口必须是" in output.getvalue()


def test_validate_candidate_uses_app_settings_strict_validation(tmp_path):
    values = profile_values("local", project_root=tmp_path)
    values["QRP_API_PORT"] = "not-an-integer"
    with pytest.raises(ConfigError, match="QRP_API_PORT"):
        validate_candidate(values, project_root=tmp_path)


def test_summary_can_return_to_previous_configuration(tmp_path, monkeypatch):
    calls = 0

    def fake_interactive(adapter, *, profile, values, project_root):
        nonlocal calls
        calls += 1
        values = dict(values)
        values["QRP_HOME"] = str(tmp_path / "runtime")
        values["QRP_DATA_DIR"] = str(tmp_path / "data")
        values["QRP_DUCKDB_PATH"] = str(tmp_path / "data" / "db" / "quant.db")
        values["QRP_API_PORT"] = str(8100 + calls)
        return values, DatabaseMode.SKIP

    monkeypatch.setattr("qrp_atlas.config.setup._interactive_values", fake_interactive)
    monkeypatch.setattr("qrp_atlas.config.setup._port_is_available", lambda host, port: True)
    result = run_setup(
        SetupOptions(),
        io_adapter=SetupIO(
            input_stream=io.StringIO("\nlocal\nback\nsave\n"),
            output_stream=io.StringIO(),
            interactive=True,
        ),
        environ={},
        project_root=tmp_path,
    )
    assert calls == 2
    assert result.settings.api.port == 8102


def test_interactive_secrets_use_hidden_reader_and_never_print(tmp_path, monkeypatch):
    monkeypatch.setattr("qrp_atlas.config.setup._port_is_available", lambda host, port: True)
    runtime = tmp_path.parent / f"{tmp_path.name}-runtime"
    data = tmp_path.parent / f"{tmp_path.name}-data"
    dsn = "postgresql://user:password@db.example/atlas"
    token = "hidden-tushare-token"
    http_proxy = "http://user:password@proxy.example:8080"
    https_proxy = "https://user:password@proxy.example:8443"
    secrets = iter((dsn, token, http_proxy, https_proxy))
    secret_prompts: list[str] = []

    def read_secret(prompt: str) -> str:
        secret_prompts.append(prompt)
        return next(secrets)

    answers = "\n".join(
        [
            "",
            "production",
            str(runtime),
            str(data),
            "",
            "3",
            "",
            "8126",
            "https://atlas.example.com",
            "y",
            "y",
            "",
            "save",
        ]
    ) + "\n"
    output = io.StringIO()
    result = run_setup(
        SetupOptions(),
        io_adapter=SetupIO(
            input_stream=io.StringIO(answers),
            output_stream=output,
            secret_reader=read_secret,
            interactive=True,
        ),
        environ={},
        project_root=tmp_path,
    )
    combined = output.getvalue() + repr(result)
    assert len(secret_prompts) == 4
    for secret in (dsn, token, http_proxy, https_proxy):
        assert secret not in combined
