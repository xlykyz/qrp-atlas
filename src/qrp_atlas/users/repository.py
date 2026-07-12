"""User persistence boundary and PostgreSQL adapter."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from qrp_atlas.users.schemas import User, UserCreate, UserStatus

ConnectionFactory = Callable[[], AbstractContextManager[Any]]


class UserRepository(Protocol):
    def get_by_id(self, user_id: UUID) -> User | None: ...

    def get_by_username(self, username: str) -> User | None: ...

    def create(self, data: UserCreate) -> User: ...


class PostgresUserRepository:
    """PostgreSQL implementation.

    The connection factory is injected so importing or using local auth does not
    import psycopg or open a database connection.
    """

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def get_by_id(self, user_id: UUID) -> User | None:
        return self._fetch_one(
            """
            SELECT user_id, username, display_name, status, created_at, updated_at
            FROM users
            WHERE user_id = %s
            """,
            (user_id,),
        )

    def get_by_username(self, username: str) -> User | None:
        return self._fetch_one(
            """
            SELECT user_id, username, display_name, status, created_at, updated_at
            FROM users
            WHERE username = %s
            """,
            (username.strip().lower(),),
        )

    def create(self, data: UserCreate) -> User:
        user_id = uuid4()
        now = datetime.now(timezone.utc)
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                INSERT INTO users (
                    user_id, username, display_name, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id, username, display_name, status, created_at, updated_at
                """,
                (
                    user_id,
                    data.username,
                    data.display_name,
                    data.status.value,
                    now,
                    now,
                ),
            ).fetchone()
        if row is None:
            raise RuntimeError("PostgreSQL did not return the created user")
        return _row_to_user(row)

    def _fetch_one(self, sql: str, params: tuple[Any, ...]) -> User | None:
        with self._connection_factory() as connection:
            row = connection.execute(sql, params).fetchone()
        return _row_to_user(row) if row is not None else None


def _row_to_user(row: Any) -> User:
    return User(
        user_id=row["user_id"],
        username=row["username"],
        display_name=row["display_name"],
        status=UserStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
