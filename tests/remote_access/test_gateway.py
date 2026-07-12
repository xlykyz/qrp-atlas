"""Security-focused tests for the standalone temporary read-only gateway."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tests.api.asgi_client import ASGITestClient


REMOTE_ACCESS_DIR = Path(__file__).resolve().parents[2] / "tools" / "remote_access"
sys.path.insert(0, str(REMOTE_ACCESS_DIR))

from app import create_app  # noqa: E402
from config import GatewaySettings  # noqa: E402


@pytest.fixture
def client(sample_db_path):
    app = create_app(GatewaySettings(database_path=sample_db_path, token="t" * 48))
    return ASGITestClient(app)


def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {'t' * 48}"}


def test_health_is_non_sensitive_and_available(client):
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database_connected"] is True
    assert "database_path" not in body


def test_token_required_and_invalid_tokens_rejected(client):
    assert client.get("/v1/tables").status_code == 401
    assert client.get("/v1/tables", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get("/v1/tables", headers=auth_headers()).status_code == 200


def test_query_allows_parameterized_small_read(client):
    response = client.request(
        "POST",
        "/v1/tables/daily_market_snapshot/query",
        headers=auth_headers(),
        json={
            "fields": ["trade_date", "ticker", "close"],
            "filters": [{"field": "ticker", "operator": "eq", "value": "000001.SZ"}],
            "order_by": "trade_date",
            "order_direction": "desc",
            "limit": 2,
        },
    )

    assert response.status_code == 200
    assert response.json()["returned_rows"] == 2
    assert response.json()["data"][0]["trade_date"] == "2024-01-03"


@pytest.mark.parametrize(
    ("url", "payload"),
    [
        ("/v1/tables/trade_execution/query", {"limit": 1}),
        ("/v1/tables/daily_market_snapshot/query", {"fields": ["not_a_field"]}),
        ("/v1/tables/daily_market_snapshot/query", {"sql": "SELECT * FROM daily_market_snapshot"}),
        ("/v1/tables/daily_market_snapshot/query", {"limit": 201}),
    ],
)
def test_unsafe_or_unavailable_requests_are_rejected(client, url, payload):
    response = client.request("POST", url, headers=auth_headers(), json=payload)

    assert response.status_code in {400, 404, 422}
