from fastapi import FastAPI
import pytest

from qrp_atlas.api.routes.auth import router
from qrp_atlas.auth.dependencies import get_auth_service, reset_auth_service_cache
from qrp_atlas.auth.exceptions import AuthBackendUnavailableError
from qrp_atlas.auth.service import AuthService
from tests.api.asgi_client import ASGITestClient


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("QRP_AUTH_MODE", "local")
    monkeypatch.setenv(
        "QRP_LOCAL_USER_ID", "f445c8c9-96d8-4ce7-9f8a-9e884dd038d8"
    )
    monkeypatch.setenv("QRP_LOCAL_USERNAME", "ryan")
    monkeypatch.setenv("QRP_LOCAL_DISPLAY_NAME", "Ryan")
    reset_auth_service_cache()
    app = FastAPI()
    app.include_router(router)
    yield ASGITestClient(app)
    reset_auth_service_cache()


def test_me_returns_local_user_without_login(client):
    response = client.get("/api/auth/me")

    assert response.status_code == 200
    assert response.json() == {
        "user_id": "f445c8c9-96d8-4ce7-9f8a-9e884dd038d8",
        "username": "ryan",
        "display_name": "Ryan",
    }


def test_login_is_explicitly_disabled_in_local_mode(client):
    response = client.request(
        "POST",
        "/api/auth/login",
        json={"username": "ryan", "password": "anything"},
    )

    assert response.status_code == 409


def test_local_logout_is_explicitly_disabled(client):
    response = client.request(
        "POST",
        "/api/auth/logout",
        headers={"Authorization": "Bearer ignored"},
    )

    assert response.status_code == 409


def test_database_mode_maps_backend_unavailable(monkeypatch):
    class UnavailableProvider:
        def resolve_user(self, bearer_token=None):
            raise AuthBackendUnavailableError("down")

        def login(self, credentials):
            raise AuthBackendUnavailableError("down")

        def logout(self, bearer_token):
            raise AuthBackendUnavailableError("down")

    monkeypatch.setenv("QRP_AUTH_MODE", "database")
    monkeypatch.setenv(
        "QRP_AUTH_DATABASE_URL",
        "postgresql://qrp_auth:unused@127.0.0.1:5432/qrp_auth",
    )
    reset_auth_service_cache()

    app = FastAPI()
    app.include_router(router)
    service = AuthService(UnavailableProvider())
    app.dependency_overrides[get_auth_service] = lambda: service
    client = ASGITestClient(app)

    me = client.get("/api/auth/me", headers={"Authorization": "Bearer x"})
    assert me.status_code == 503
    assert me.json()["detail"] == "authentication backend unavailable"
    assert "user_id" not in me.json()

    login = client.request(
        "POST",
        "/api/auth/login",
        json={"username": "ryan", "password": "secret"},
    )
    assert login.status_code == 503

    logout = client.request(
        "POST",
        "/api/auth/logout",
        headers={"Authorization": "Bearer x"},
    )
    assert logout.status_code == 503
    reset_auth_service_cache()
