"""Bearer-token authentication for the temporary gateway."""

from __future__ import annotations

import secrets
from collections.abc import Callable

from fastapi import Header, HTTPException, status


def require_bearer_token(expected_token: str) -> Callable:
    """Create a dependency that compares a Bearer token in constant time."""

    def _require_bearer_token(authorization: str | None = Header(default=None)) -> None:
        scheme, _, supplied_token = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not supplied_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization: Bearer <token> is required.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not secrets.compare_digest(supplied_token, expected_token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid access token.",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return _require_bearer_token
