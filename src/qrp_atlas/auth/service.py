"""Authentication application service."""

from __future__ import annotations

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.providers.base import AuthProvider
from qrp_atlas.auth.schemas import IssuedSession, LoginCredentials


class AuthService:
    def __init__(self, provider: AuthProvider) -> None:
        self._provider = provider

    def current_user(self, bearer_token: str | None = None) -> UserContext:
        return self._provider.resolve_user(bearer_token)

    def login(self, credentials: LoginCredentials) -> IssuedSession:
        return self._provider.login(credentials)

    def logout(self, bearer_token: str) -> None:
        self._provider.logout(bearer_token)
