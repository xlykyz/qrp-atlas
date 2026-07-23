from __future__ import annotations

import subprocess
import sys

from qrp_atlas.api import cli


def test_api_cli_loads_explicit_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        f"QRP_HOME='{tmp_path / 'runtime'}'\n"
        f"QRP_DATA_DIR='{tmp_path / 'data'}'\n"
        "QRP_API_HOST='127.0.0.1'\n"
        "QRP_API_PORT='8124'\n",
        encoding="utf-8",
    )
    captured = {}

    def fake_run(app, **kwargs):
        captured.update({"app": app, **kwargs})

    monkeypatch.setattr(cli.uvicorn, "run", fake_run)
    monkeypatch.delenv("QRP_ENV_FILE", raising=False)
    assert cli.main(["--env-file", str(env_file)]) == 0
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8124


def test_api_cli_sets_env_file_before_config_package_import(tmp_path):
    database = tmp_path / "custom.db"
    env_file = tmp_path / "runtime.env"
    env_file.write_text(f"QRP_DUCKDB_PATH='{database}'\n", encoding="utf-8")
    code = """
import uvicorn
def fake_run(*args, **kwargs):
    from qrp_atlas.config.paths import DB_PATH
    print(DB_PATH)
uvicorn.run = fake_run
from qrp_atlas.api.cli import main
raise SystemExit(main(['--env-file', sys.argv[1]]))
"""
    completed = subprocess.run(
        [sys.executable, "-c", "import sys\n" + code, str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == str(database)
