from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.api import db as api_db
from qrp_atlas.api.server import app
from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import IRM_INTERACTION_QA

from .asgi_client import ASGITestClient


def _insert_irm_row(connection, pid: str, reply_date: date) -> None:
    connection.execute(
        f"""
        INSERT INTO {IRM_INTERACTION_QA.name} (
            pid, ticker, company_code, company_shortname,
            question_content, reply_content, question_time, reply_time,
            reply_date, nickname, keywords, source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            pid,
            "000001.SZ",
            "000001",
            "测试公司",
            "问题",
            "回复",
            datetime(2026, 8, 4, 9, 0),
            datetime(2026, 8, 4, 10, 0),
            reply_date,
            "测试用户",
            None,
            "test",
            datetime(2026, 8, 4, 10, 1),
        ],
    )


def _configure_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    dedicated_db: bool,
) -> ASGITestClient:
    main_path = tmp_path / "quant.duckdb"
    main = duckdb.connect(str(main_path))
    main.execute(IRM_INTERACTION_QA.duckdb_create_sql())
    _insert_irm_row(main, "legacy", date(2026, 8, 1))
    main.close()

    overrides = {
        "QRP_DUCKDB_PATH": str(main_path),
        "QRP_READ_ONLY": "true",
    }
    if dedicated_db:
        irm_path = tmp_path / "irm_qa.duckdb"
        irm = duckdb.connect(str(irm_path))
        irm.execute(IRM_INTERACTION_QA.duckdb_create_sql())
        _insert_irm_row(irm, "current", date(2026, 8, 4))
        irm.close()
        overrides["QRP_IRM_QA_DUCKDB_PATH"] = str(irm_path)

    settings = AppSettings.load(
        overrides=overrides,
        environ={},
        project_root=tmp_path,
    )
    monkeypatch.setattr(api_db, "get_settings", lambda: settings)
    return ASGITestClient(app)


def test_irm_table_api_reads_dedicated_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _configure_api(monkeypatch, tmp_path, dedicated_db=True)

    response = client.get(
        "/api/tables/irm_interaction_qa",
        params={"limit": 10, "order_by": "reply_date"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["rows"][0]["pid"] == "current"
    assert payload["rows"][0]["reply_date"] == "2026-08-04"

    schema = client.get("/api/tables/irm_interaction_qa/schema")
    assert schema.status_code == 200
    assert {column["name"] for column in schema.json()} >= {"pid", "reply_date"}

    stats = client.get("/api/stats")
    assert stats.status_code == 200
    assert stats.json()["tables"]["irm_interaction_qa"] == {
        "rows": 1,
        "earliest_date": "2026-08-04",
        "latest_date": "2026-08-04",
    }


def test_irm_table_api_fails_closed_without_dedicated_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _configure_api(monkeypatch, tmp_path, dedicated_db=False)

    response = client.get("/api/tables/irm_interaction_qa")

    assert response.status_code == 503
    assert response.json()["detail"] == "IRM_QA_DB_NOT_AVAILABLE"
