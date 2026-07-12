"""Capability Session management for ChatGPT-compatible temporary read-only access."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import DEFAULT_RUNTIME_DIR

SESSION_FILE = "capability_session.json"
DEFAULT_SESSION_DURATION_MINUTES = 30
CAPABILITY_MAX_ROWS = 50


@dataclass
class CapabilitySession:
    """A short-lived, revocable, single-use session for URL-based access."""

    session_id: str
    created_at: str
    expires_at: str
    revoked: bool = False
    max_rows: int = CAPABILITY_MAX_ROWS
    duration_minutes: int = DEFAULT_SESSION_DURATION_MINUTES

    @classmethod
    def generate(cls, duration_minutes: int = DEFAULT_SESSION_DURATION_MINUTES) -> CapabilitySession:
        now = datetime.now(timezone.utc)
        return cls(
            session_id=secrets.token_urlsafe(32),
            created_at=now.isoformat(),
            expires_at=(now + timedelta(minutes=duration_minutes)).isoformat(),
            duration_minutes=duration_minutes,
        )

    def is_valid(self) -> bool:
        if self.revoked:
            return False
        expires = datetime.fromisoformat(self.expires_at)
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < expires

    def to_dict(self) -> dict:
        return asdict(self)


def load_session(runtime_dir: Path | None = None) -> CapabilitySession | None:
    """Load capability session from the runtime directory, or None if absent/invalid."""
    runtime_dir = runtime_dir or DEFAULT_RUNTIME_DIR
    session_path = runtime_dir / SESSION_FILE
    if not session_path.exists():
        return None
    try:
        data = json.loads(session_path.read_text(encoding="utf-8"))
        return CapabilitySession(**data)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def save_session(session: CapabilitySession, runtime_dir: Path | None = None) -> None:
    """Persist a capability session to the runtime directory."""
    runtime_dir = runtime_dir or DEFAULT_RUNTIME_DIR
    runtime_dir.mkdir(parents=True, exist_ok=True)
    session_path = runtime_dir / SESSION_FILE
    session_path.write_text(
        json.dumps(session.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    session_path.chmod(0o600)


def clear_session(runtime_dir: Path | None = None) -> None:
    """Remove the capability session file."""
    runtime_dir = runtime_dir or DEFAULT_RUNTIME_DIR
    session_path = runtime_dir / SESSION_FILE
    if session_path.exists():
        session_path.unlink()