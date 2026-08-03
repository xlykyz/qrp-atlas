from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.api.routes import system_b
from qrp_atlas.api.server import app
from qrp_atlas.pipeline.system_b.service import initialize_history

from .asgi_client import ASGITestClient
from tests.conftest import make_fake_get_db
from tests.pipeline.system_b.test_production import _seed_market


@pytest.fixture
def system_b_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[ASGITestClient, list]:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source,
        output_database=output,
        staging_root=tmp_path / "stage",
        end_date=dates[-1],
    )
    monkeypatch.setattr(system_b, "get_db", make_fake_get_db(output))
    return ASGITestClient(app), dates


def test_system_b_latest_history_transitions_summary_and_run(system_b_client) -> None:
    client, dates = system_b_client
    latest = client.get("/api/v1/system-b/states/latest", params={"limit": 10})
    assert latest.status_code == 200
    assert {row["asset_id"] for row in latest.json()} == {"A", "B", "NEW"}

    history = client.get("/api/v1/system-b/assets/A/history", params={"limit": 5})
    assert history.status_code == 200
    assert len(history.json()) == 5

    states = client.get(
        "/api/v1/system-b/states", params={"trade_date": dates[-1].isoformat(), "limit": 10}
    )
    assert states.status_code == 200
    assert len(states.json()) == 3

    transitions = client.get(
        "/api/v1/system-b/transitions", params={"trade_date": dates[-1].isoformat()}
    )
    assert transitions.status_code == 200
    assert all(
        row["previous_trend_state"] is not None and row["trend_state"] is not None
        for row in transitions.json()
    )

    summary = client.get(
        "/api/v1/system-b/summary", params={"trade_date": dates[-1].isoformat()}
    )
    assert summary.status_code == 200
    payload = summary.json()
    assert payload["base_count"] == 1
    assert payload["active_count"] == 1
    assert payload["null_state_count"] == 1
    assert payload["new_listing_warmup_count"] == 1
    assert payload["candidate_count"] == 0
    assert payload["production_run_id"]
    assert payload["calculation_completed_at"].endswith("Z")
    assert payload["trade_date"] == dates[-1].isoformat()

    run = client.get("/api/v1/system-b/production-runs/latest")
    assert run.status_code == 200
    assert run.json()["status"] == "SUCCEEDED"
    assert run.json()["created_at"].endswith("Z")
    assert run.json()["completed_at"].endswith("Z")

    assert latest.json()[0]["created_at"].endswith("Z")
    assert states.json()[0]["trade_date"] == dates[-1].isoformat()


def test_system_b_summary_without_observation_is_not_ready(system_b_client) -> None:
    client, dates = system_b_client
    missing_date = dates[-1] + timedelta(days=1)

    response = client.get(
        "/api/v1/system-b/summary",
        params={"trade_date": missing_date.isoformat()},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "SYSTEM_B_SUMMARY_NOT_READY"


@pytest.fixture
def failed_system_b_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source,
        output_database=output,
        staging_root=tmp_path / "stage",
        end_date=dates[-1],
    )
    connection = duckdb.connect(str(output))
    connection.execute("UPDATE system_b_production_run SET status = 'FAILED'")
    connection.close()
    monkeypatch.setattr(system_b, "get_db", make_fake_get_db(output))
    return ASGITestClient(app), dates


def test_system_b_summary_with_failed_run_is_not_ready(failed_system_b_client) -> None:
    client, dates = failed_system_b_client
    response = client.get(
        "/api/v1/system-b/summary",
        params={"trade_date": dates[-1].isoformat()},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "SYSTEM_B_SUMMARY_NOT_READY"
