"""Authentication and user-control-plane settings.

Local mode is the default and never opens a PostgreSQL connection. Database mode
must be enabled explicitly and requires a PostgreSQL DSN.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class AuthMode(StrEnum):
    LOCAL = "local"
    DATABASE = "database"


DEFAULT_LOCAL_USER_ID = UUID("f445c8c9-96d8-4ce7-9f8a-9e884dd038d8")


@dataclass(frozen=True, slots=True)
class AuthSettings:
    mode: AuthMode = AuthMode.LOCAL
    local_user_id: UUID = DEFAULT_LOCAL_USER_ID
    local_username: str = "ryan"
    local_display_name: str = "Ryan"
    postgres_dsn: str | None = None
    session_ttl_seconds: int = 60 * 60 * 24 * 7

    @classmethod
    def from_env(cls) -> "AuthSettings":
        raw_mode = os.getenv("QRP_AUTH_MODE", AuthMode.LOCAL.value).strip().lower()
        try:
            mode = AuthMode(raw_mode)
        except ValueError as exc:
            allowed = ", ".join(item.value for item in AuthMode)
            raise ValueError(f"QRP_AUTH_MODE must be one of: {allowed}") from exc

        raw_user_id = os.getenv("QRP_LOCAL_USER_ID", str(DEFAULT_LOCAL_USER_ID)).strip()
        try:
            local_user_id = UUID(raw_user_id)
        except ValueError as exc:
            raise ValueError("QRP_LOCAL_USER_ID must be a valid UUID") from exc

        ttl = int(os.getenv("QRP_AUTH_SESSION_TTL_SECONDS", str(60 * 60 * 24 * 7)))
        if ttl <= 0:
            raise ValueError("QRP_AUTH_SESSION_TTL_SECONDS must be positive")

        postgres_dsn = os.getenv("QRP_AUTH_DATABASE_URL") or None
        if mode is AuthMode.DATABASE and not postgres_dsn:
            raise ValueError(
                "QRP_AUTH_DATABASE_URL is required when QRP_AUTH_MODE=database"
            )

        return cls(
            mode=mode,
            local_user_id=local_user_id,
            local_username=os.getenv("QRP_LOCAL_USERNAME", "ryan").strip() or "ryan",
            local_display_name=(
                os.getenv("QRP_LOCAL_DISPLAY_NAME", "Ryan").strip() or "Ryan"
            ),
            postgres_dsn=postgres_dsn,
            session_ttl_seconds=ttl,
        )
