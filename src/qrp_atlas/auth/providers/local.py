"""Local single-user provider.

This provider is a first-class runtime mode. It never reads PostgreSQL and never
requires a login state.
"""

from __future__ import annotations

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.exceptions import LoginNotSupportedError
from qrp_atlas.auth.schemas import IssuedSession, LoginCredentials


class LocalAuthProvider:
    def __init__(self, user: UserContext) -> None:
        self._user = user

    def resolve_user(self, bearer_token: str | None = None) -> UserContext:
        del bearer_token
        return self._user

    def login(self, credentials: LoginCredentials) -> IssuedSession:
        del credentials
        raise LoginNotSupportedError("login is disabled in local auth mode")

    def logout(self, bearer_token: str) -> None:
        del bearer_token
        raise LoginNotSupportedError("logout is disabled in local auth mode")
