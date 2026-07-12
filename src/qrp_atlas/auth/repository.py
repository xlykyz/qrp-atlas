"""Authentication persistence boundary and PostgreSQL adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from qrp_atlas.auth.schemas import AuthSession
from qrp_atlas.users.repository import ConnectionFactory


class AuthRepository(Protocol):
    def get_password_hash(self, user_id: UUID) -> str | None: ...

    def set_password_hash(self, user_id: UUID, password_hash: str) -> None: ...

    def add_identity(
        self, user_id: UUID, provider: str, provider_subject: str
    ) -> None: ...

    def create_session(
        self, user_id: UUID, token_hash: str, expires_at: datetime
    ) -> AuthSession: ...

    def get_session_by_token_hash(self, token_hash: str) -> AuthSession | None: ...

    def revoke_session(self, token_hash: str, revoked_at: datetime) -> None: ...


class PostgresAuthRepository:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def get_password_hash(self, user_id: UUID) -> str | None:
        with self._connection_factory() as connection:
            row = connection.execute(
                "SELECT password_hash FROM auth_credentials WHERE user_id = %s",
                (user_id,),
            ).fetchone()
        return row["password_hash"] if row is not None else None

    def set_password_hash(self, user_id: UUID, password_hash: str) -> None:
        with self._connection_factory() as connection:
            connection.execute(
                """
                INSERT INTO auth_credentials (user_id, password_hash, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, password_hash),
            )

    def add_identity(
        self, user_id: UUID, provider: str, provider_subject: str
    ) -> None:
        with self._connection_factory() as connection:
            connection.execute(
                """
                INSERT INTO auth_identities (
                    identity_id, user_id, provider, provider_subject, created_at
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (provider, provider_subject) DO NOTHING
                """,
                (uuid4(), user_id, provider, provider_subject),
            )

    def create_session(
        self, user_id: UUID, token_hash: str, expires_at: datetime
    ) -> AuthSession:
        session_id = uuid4()
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                INSERT INTO auth_sessions (
                    session_id, user_id, token_hash, created_at, expires_at
                )
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
                RETURNING session_id, user_id, token_hash, created_at,
                          expires_at, revoked_at
                """,
                (session_id, user_id, token_hash, expires_at),
            ).fetchone()
        if row is None:
            raise RuntimeError("PostgreSQL did not return the created session")
        return _row_to_session(row)

    def get_session_by_token_hash(self, token_hash: str) -> AuthSession | None:
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                SELECT session_id, user_id, token_hash, created_at,
                       expires_at, revoked_at
                FROM auth_sessions
                WHERE token_hash = %s
                """,
                (token_hash,),
            ).fetchone()
        return _row_to_session(row) if row is not None else None

    def revoke_session(self, token_hash: str, revoked_at: datetime) -> None:
        with self._connection_factory() as connection:
            connection.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = %s
                WHERE token_hash = %s AND revoked_at IS NULL
                """,
                (revoked_at, token_hash),
            )


def _row_to_session(row: Any) -> AuthSession:
    return AuthSession(
        session_id=row["session_id"],
        user_id=row["user_id"],
        token_hash=row["token_hash"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
    )
