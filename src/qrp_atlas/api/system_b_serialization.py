"""Serialization helpers for the System B monitoring API."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any


def serialize_system_b_value(value: Any) -> Any:
    """Serialize dates and timestamps without losing UTC semantics."""
    if isinstance(value, datetime):
        aware = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        return aware.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value
