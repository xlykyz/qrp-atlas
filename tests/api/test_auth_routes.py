from fastapi import FastAPI
import pytest

from qrp_atlas.api.routes.auth import router
from qrp_atlas.auth.dependencies import reset_auth_service_cache
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
