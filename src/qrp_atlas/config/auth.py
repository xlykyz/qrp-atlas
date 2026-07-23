"""Compatibility authentication settings facade."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from qrp_atlas.config.settings import (
    AppSettings,
    AuthMode,
    DEFAULT_LOCAL_USER_ID,
)


@dataclass(frozen=True, slots=True)
class AuthSettings:
    mode: AuthMode = AuthMode.LOCAL
    local_user_id: UUID = DEFAULT_LOCAL_USER_ID
    local_username: str = "ryan"
    local_display_name: str = "Ryan"
    postgres_dsn: str | None = field(default=None, repr=False)
    session_ttl_seconds: int = 60 * 60 * 24 * 7

    @classmethod
    def from_env(cls) -> "AuthSettings":
        settings = AppSettings.load().authentication
        return cls(
            mode=settings.mode,
            local_user_id=settings.local_user_id,
            local_username=settings.local_username,
            local_display_name=settings.local_display_name,
            postgres_dsn=settings.postgres_dsn,
            session_ttl_seconds=settings.session_ttl_seconds,
        )
