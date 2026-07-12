"""Authentication-provider contracts."""

from __future__ import annotations

from typing import Protocol

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.schemas import IssuedSession, LoginCredentials


class AuthProvider(Protocol):
    def resolve_user(self, bearer_token: str | None) -> UserContext: ...

    def login(self, credentials: LoginCredentials) -> IssuedSession: ...

    def logout(self, bearer_token: str) -> None: ...
