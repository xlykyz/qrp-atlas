"""PostgreSQL-backed password and opaque-session provider."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.exceptions import InvalidCredentialsError, InvalidSessionError
from qrp_atlas.auth.passwords import PasswordHasher
from qrp_atlas.auth.repository import AuthRepository
from qrp_atlas.auth.schemas import IssuedSession, LoginCredentials
from qrp_atlas.users.schemas import User
from qrp_atlas.users.service import (
    UserDisabledError,
    UserNotFoundError,
    UserService,
)


class DatabaseAuthProvider:
    def __init__(
        self,
        users: UserService,
        repository: AuthRepository,
        password_hasher: PasswordHasher,
        session_ttl_seconds: int,
    ) -> None:
        self._users = users
        self._repository = repository
        self._password_hasher = password_hasher
        self._session_ttl = timedelta(seconds=session_ttl_seconds)

    def resolve_user(self, bearer_token: str | None) -> UserContext:
        if not bearer_token:
            raise InvalidSessionError("missing bearer token")

        session = self._repository.get_session_by_token_hash(_hash_token(bearer_token))
        now = datetime.now(timezone.utc)
        if (
            session is None
            or session.revoked_at is not None
            or session.expires_at <= now
        ):
            raise InvalidSessionError("invalid or expired session")

        try:
            user = self._users.get_active(session.user_id)
        except (UserNotFoundError, UserDisabledError) as exc:
            raise InvalidSessionError("invalid session user") from exc
        return _to_context(user)

    def login(self, credentials: LoginCredentials) -> IssuedSession:
        try:
            user = self._users.get_active_by_username(credentials.username)
        except (UserNotFoundError, UserDisabledError) as exc:
            raise InvalidCredentialsError("invalid username or password") from exc

        password_hash = self._repository.get_password_hash(user.user_id)
        if password_hash is None or not self._password_hasher.verify(
            password_hash, credentials.password
        ):
            raise InvalidCredentialsError("invalid username or password")

        raw_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + self._session_ttl
        self._repository.create_session(
            user_id=user.user_id,
            token_hash=_hash_token(raw_token),
            expires_at=expires_at,
        )
        return IssuedSession(
            access_token=raw_token,
            expires_at=expires_at,
            user=_to_context(user),
        )

    def logout(self, bearer_token: str) -> None:
        if not bearer_token:
            raise InvalidSessionError("missing bearer token")
        self._repository.revoke_session(
            token_hash=_hash_token(bearer_token),
            revoked_at=datetime.now(timezone.utc),
        )


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _to_context(user: User) -> UserContext:
    return UserContext(
        user_id=user.user_id,
        username=user.username,
        display_name=user.display_name,
    )
