"""Authentication request and result schemas."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from qrp_atlas.auth.context import UserContext


class LoginCredentials(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=1024)


class IssuedSession(BaseModel):
    model_config = ConfigDict(frozen=True)

    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
    user: UserContext


class AuthSession(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: UUID
    user_id: UUID
    token_hash: str
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
