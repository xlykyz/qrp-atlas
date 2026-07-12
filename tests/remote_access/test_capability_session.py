"""Security-focused tests for the capability-session (ChatGPT-compatible) mode."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.api.asgi_client import ASGITestClient

REMOTE_ACCESS_DIR = Path(__file__).resolve().parents[2] / "tools" / "remote_access"
sys.path.insert(0, str(REMOTE_ACCESS_DIR))

from app import create_app  # noqa: E402
from config import GatewaySettings  # noqa: E402
from session import CapabilitySession  # noqa: E402

VALID_SESSION_ID = "valid-test-session-id-0123456789ab"
WRONG_SESSION_ID = "wrong-test-session-id-9876543210cd"
EXPIRED_SESSION_ID = "expired-test-session-id-0123456789ef"
REVOKED_SESSION_ID = "revoked-test-session-id-0123456789gh"
OLD_SESSION_ID = "old-test-session-id-0123456789ij"
NEW_SESSION_ID = "new-test-session-id-0123456789kl"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def future_ts():
    return (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()


@pytest.fixture
def past_ts():
    return (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


@pytest.fixture
def valid_session(future_ts) -> CapabilitySession:
    return CapabilitySession(
        session_id=VALID_SESSION_ID,
        created_at=datetime.now(timezone.utc).isoformat(),
        expires_at=future_ts,
        revoked=False,
    )


@pytest.fixture
def expired_session(past_ts) -> CapabilitySession:
    return CapabilitySession(
        session_id=EXPIRED_SESSION_ID,
        created_at=past_ts,
        expires_at=past_ts,
        revoked=False,
    )


@pytest.fixture
def revoked_session(future_ts) -> CapabilitySession:
    return CapabilitySession(
        session_id=REVOKED_SESSION_ID,
        created_at=datetime.now(timezone.utc).isoformat(),
        expires_at=future_ts,
        revoked=True,
    )


@pytest.fixture
def new_session(future_ts) -> CapabilitySession:
    return CapabilitySession(
        session_id=NEW_SESSION_ID,
        created_at=datetime.now(timezone.utc).isoformat(),
        expires_at=future_ts,
        revoked=False,
    )


@pytest.fixture
def client(sample_db_path):
    """No capability session active — /share/ should return 404."""
    app = create_app(GatewaySettings(database_path=sample_db_path, token="t" * 48))
    return ASGITestClient(app)


@pytest.fixture
def client_with_session(sample_db_path, valid_session):
    """Valid capability session active."""
    app = create_app(
        GatewaySettings(database_path=sample_db_path, token="t" * 48),
        capability_session=valid_session,
    )
    return ASGITestClient(app)


@pytest.fixture
def client_expired(sample_db_path, expired_session):
    """Capability session has expired."""
    app = create_app(
        GatewaySettings(database_path=sample_db_path, token="t" * 48),
        capability_session=expired_session,
    )
    return ASGITestClient(app)


@pytest.fixture
def client_revoked(sample_db_path, revoked_session):
    """Capability session has been revoked."""
    app = create_app(
        GatewaySettings(database_path=sample_db_path, token="t" * 48),
        capability_session=revoked_session,
    )
    return ASGITestClient(app)


@pytest.fixture
def client_regenerated(sample_db_path, new_session):
    """New session after regeneration (old ID should be rejected)."""
    app = create_app(
        GatewaySettings(database_path=sample_db_path, token="t" * 48),
        capability_session=new_session,
    )
    return ASGITestClient(app)


# ---------------------------------------------------------------------------
# Tests: session lifecycle
# ---------------------------------------------------------------------------

class TestSessionLifecycle:
    """1–6: Session lifecycle: inactive, valid, wrong ID, expired, revoked, regenerated."""

    def test_no_session_returns_404(self, client):
        """1. No active capability session → all /share/ requests return 404."""
        response = client.get(f"/share/{VALID_SESSION_ID}/meta")
        assert response.status_code == 404

    def test_valid_session_meta_200(self, client_with_session):
        """2. Valid session ID can access meta."""
        response = client_with_session.get(f"/share/{VALID_SESSION_ID}/meta")
        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "URL-based capability session (ChatGPT compatible)"
        assert "session_expires_at" in body
        assert body["session_revoked"] is False

    def test_wrong_session_id_rejected(self, client_with_session):
        """3. Wrong session ID returns 401."""
        response = client_with_session.get(f"/share/{WRONG_SESSION_ID}/meta")
        assert response.status_code == 401

    def test_expired_session_410(self, client_expired):
        """4. Expired session returns 410 Gone."""
        response = client_expired.get(f"/share/{EXPIRED_SESSION_ID}/meta")
        assert response.status_code == 410

    def test_revoked_session_410(self, client_revoked):
        """5. Revoked session returns 410 Gone."""
        response = client_revoked.get(f"/share/{REVOKED_SESSION_ID}/meta")
        assert response.status_code == 410

    def test_old_session_invalid_after_regeneration(self, client_regenerated):
        """6. Old session ID returns 401 after regeneration."""
        response = client_regenerated.get(f"/share/{VALID_SESSION_ID}/meta")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# Tests: endpoint functionality
# ---------------------------------------------------------------------------

class TestEndpointFunctionality:
    """7: GET query returns data via capability session."""

    def test_get_tables_list(self, client_with_session):
        response = client_with_session.get(f"/share/{VALID_SESSION_ID}/tables")
        assert response.status_code == 200
        tables = response.json()["tables"]
        table_names = [t["name"] for t in tables]
        assert "daily_market_snapshot" in table_names
        assert "trade_execution" not in table_names

    def test_get_table_schema(self, client_with_session):
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/schema"
        )
        assert response.status_code == 200
        assert response.json()["table"] == "daily_market_snapshot"

    def test_get_table_overview(self, client_with_session):
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/overview"
        )
        assert response.status_code == 200
        assert "total_rows" in response.json()

    def test_get_query_returns_data(self, client_with_session):
        """7. GET query returns data from an allowlisted table."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date,ticker,close", "limit": 2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["returned_rows"] == 2
        assert len(body["data"]) == 2
        assert "trade_date" in body["data"][0]
        assert "ticker" in body["data"][0]

    def test_get_query_with_filters(self, client_with_session):
        """GET query with URL-encoded JSON filters."""
        import json
        filters = json.dumps([{"field": "ticker", "operator": "eq", "value": "000001.SZ"}])
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date,ticker,close", "filters": filters, "limit": 2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["returned_rows"] > 0

    def test_get_query_with_date_range(self, client_with_session):
        """GET query with date_from/date_to."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={
                "fields": "trade_date,ticker,close",
                "date_from": "2024-01-01",
                "date_to": "2024-01-10",
                "limit": 5,
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert 0 <= body["returned_rows"] <= 5

    def test_get_query_date_range_validation(self, client_with_session):
        """date_from after date_to returns 400."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={
                "date_from": "2024-06-01",
                "date_to": "2024-01-01",
                "limit": 2,
            },
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Tests: security boundaries
# ---------------------------------------------------------------------------

class TestSecurityBoundaries:
    """8–11: Non-allowlisted tables, fields, limits, and SQL injection."""

    def test_unavailable_table_rejected(self, client_with_session):
        """8. Non-allowlisted table returns 404."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/trade_execution/query",
            params={"limit": 1},
        )
        assert response.status_code == 404

    def test_invalid_field_rejected(self, client_with_session):
        """9. Invalid field name returns 400."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "not_a_field", "limit": 1},
        )
        assert response.status_code == 400

    def test_limit_too_high_rejected(self, client_with_session):
        """10. limit > 50 (capability max) returns 422."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"limit": 100},
        )
        # FastAPI's Query(le=CAPABILITY_MAX_ROWS) returns 422 for out-of-range
        assert response.status_code == 422

    def test_offset_too_high_rejected(self, client_with_session):
        """Negative offset returns 422."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"offset": -1, "limit": 1},
        )
        assert response.status_code == 422

    def test_sql_injection_in_fields(self, client_with_session):
        """11. SQL injection attempt in fields returns 400 (field not in allowlist)."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date; DROP TABLE daily_market_snapshot", "limit": 1},
        )
        assert response.status_code == 400

    def test_sql_injection_in_order_by(self, client_with_session):
        """SQL injection in order_by returns 400 (not an allowlisted field)."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={
                "fields": "trade_date,ticker",
                "order_by": "trade_date; SELECT 1",
                "limit": 1,
            },
        )
        assert response.status_code == 400

    def test_invalid_filter_operator(self, client_with_session):
        """Invalid filter operator returns 400."""
        import json
        filters = json.dumps([{"field": "ticker", "operator": "LIKE", "value": "%test%"}])
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date,ticker", "filters": filters, "limit": 1},
        )
        assert response.status_code == 400

    def test_invalid_filter_json(self, client_with_session):
        """Malformed filter JSON returns 400."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date,ticker", "filters": "not-json", "limit": 1},
        )
        assert response.status_code == 400

    def test_extra_query_params_ignored(self, client_with_session):
        """Unknown query params (like sql=) are ignored, not executed."""
        response = client_with_session.get(
            f"/share/{VALID_SESSION_ID}/tables/daily_market_snapshot/query",
            params={"fields": "trade_date", "limit": 1, "sql": "SELECT * FROM trade_execution"},
        )
        # Extra params are ignored by FastAPI, so this should succeed
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Tests: Bearer token API remains unchanged
# ---------------------------------------------------------------------------

class TestBearerApiUnchanged:
    """12. Original Bearer token API behavior is unchanged."""

    def test_bearer_tables_still_work(self, client_with_session):
        """Bearer API returns tables even with a capability session present."""
        response = client_with_session.get(
            "/v1/tables",
            headers={"Authorization": "Bearer " + "t" * 48},
        )
        assert response.status_code == 200

    def test_bearer_requires_auth(self, client_with_session):
        """Bearer API still rejects unauthenticated requests."""
        response = client_with_session.get("/v1/tables")
        assert response.status_code == 401

    def test_bearer_post_query(self, client_with_session):
        """Bearer POST query still works."""
        response = client_with_session.request(
            "POST",
            "/v1/tables/daily_market_snapshot/query",
            headers={"Authorization": "Bearer " + "t" * 48},
            json={"fields": ["trade_date", "ticker"], "limit": 2},
        )
        assert response.status_code == 200
        assert response.json()["returned_rows"] == 2