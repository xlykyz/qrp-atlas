"""Domain schemas for QRP users."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class UserStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class User(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_id: UUID
    username: str
    display_name: str
    status: UserStatus
    created_at: datetime
    updated_at: datetime


class UserCreate(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=128)
    status: UserStatus = UserStatus.ACTIVE

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("username cannot be blank")
        if not normalized.replace("_", "").replace("-", "").isalnum():
            raise ValueError("username may contain only letters, numbers, '_' and '-'")
        return normalized

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("display_name cannot be blank")
        return normalized
