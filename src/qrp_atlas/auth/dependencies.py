"""FastAPI dependencies and auth-provider composition root."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from functools import lru_cache
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request, status

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.exceptions import (
    AuthBackendUnavailableError,
    AuthenticationError,
)
from qrp_atlas.auth.passwords import PasswordHasher
from qrp_atlas.auth.providers.database import DatabaseAuthProvider
from qrp_atlas.auth.providers.local import LocalAuthProvider
from qrp_atlas.auth.repository import PostgresAuthRepository
from qrp_atlas.auth.service import AuthService
from qrp_atlas.config.auth import AuthMode, AuthSettings
from qrp_atlas.users.repository import PostgresUserRepository
from qrp_atlas.users.service import UserService


@lru_cache(maxsize=1)
def get_auth_service() -> AuthService:
    settings = AuthSettings.from_env()
    if settings.mode is AuthMode.LOCAL:
        return AuthService(
            LocalAuthProvider(
                UserContext(
                    user_id=settings.local_user_id,
                    username=settings.local_username,
                    display_name=settings.local_display_name,
                )
            )
        )

    connection_factory = make_connection_factory(settings.postgres_dsn)
    users = UserService(PostgresUserRepository(connection_factory))
    provider = DatabaseAuthProvider(
        users=users,
        repository=PostgresAuthRepository(connection_factory),
        password_hasher=PasswordHasher(),
        session_ttl_seconds=settings.session_ttl_seconds,
    )
    return AuthService(provider)


def reset_auth_service_cache() -> None:
    """Clear composition cache for tests or explicit configuration reloads."""

    get_auth_service.cache_clear()


def get_current_user(
    request: Request,
    auth_service: AuthService = Depends(get_auth_service),
) -> UserContext:
    token = _extract_bearer_token(request)
    try:
        return auth_service.current_user(token)
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except AuthBackendUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication backend unavailable",
        ) from exc


CurrentUser = Annotated[UserContext, Depends(get_current_user)]


def _extract_bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("Authorization")
    if not authorization:
        return None
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def make_connection_factory(
    dsn: str | None,
) -> Callable[[], AbstractContextManager[Any]]:
    if not dsn:
        raise ValueError("PostgreSQL DSN is required for database auth mode")

    @contextmanager
    def connect() -> Iterator[Any]:
        import psycopg
        from psycopg.rows import dict_row

        try:
            with psycopg.connect(dsn, row_factory=dict_row) as connection:
                yield connection
        except psycopg.Error as exc:
            raise AuthBackendUnavailableError(
                "authentication backend unavailable"
            ) from exc

    return connect
