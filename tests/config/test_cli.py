from __future__ import annotations

import json
from pathlib import Path

import duckdb

from qrp_atlas.config.cli import main


def common_args(tmp_path: Path) -> list[str]:
    return [
        "--set",
        f"QRP_HOME={tmp_path / 'home'}",
        "--set",
        f"QRP_DATA_DIR={tmp_path / 'data'}",
    ]


def test_show_redacts_secrets(tmp_path, capsys):
    secret = "never-print-this-token"
    args = common_args(tmp_path) + [
        "--set",
        f"TUSHARE_TOKEN={secret}",
        "show",
    ]

    assert main(args) == 0
    output = capsys.readouterr().out
    assert secret not in output
    assert json.loads(output)["external_services"]["tushare_token"] == "configured"


def test_doctor_success_and_failure_exit_codes(tmp_path, capsys):
    args = common_args(tmp_path)
    assert main(args + ["init"]) == 0
    capsys.readouterr()

    database = tmp_path / "data" / "db" / "quant.db"
    connection = duckdb.connect(str(database))
    connection.execute("SELECT 1")
    connection.close()

    assert main(args + ["doctor"]) == 0
    assert "[FAIL]" not in capsys.readouterr().out

    failing = common_args(tmp_path / "missing") + [
        "--set",
        "QRP_READ_ONLY=true",
        "doctor",
    ]
    assert main(failing) == 1
    assert "[FAIL]" in capsys.readouterr().out


def test_configuration_error_exit_code(tmp_path, capsys):
    code = main(common_args(tmp_path) + ["--set", "QRP_API_PORT=invalid", "show"])
    assert code == 2
    assert "QRP_API_PORT" in capsys.readouterr().err
