"""Stable user context consumed by QRP business services."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict


class UserContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_id: UUID
    username: str
    display_name: str
