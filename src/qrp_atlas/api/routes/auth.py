"""Authentication and current-user endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.dependencies import CurrentUser, get_auth_service
from qrp_atlas.auth.exceptions import (
    AuthBackendUnavailableError,
    InvalidCredentialsError,
    InvalidSessionError,
    LoginNotSupportedError,
)
from qrp_atlas.auth.schemas import IssuedSession, LoginCredentials
from qrp_atlas.auth.service import AuthService

router = APIRouter(prefix="/api/auth", tags=["用户认证"])


@router.get("/me", response_model=UserContext)
def get_me(current_user: CurrentUser) -> UserContext:
    return current_user


@router.post("/login", response_model=IssuedSession)
def login(
    credentials: LoginCredentials,
    auth_service: AuthService = Depends(get_auth_service),
) -> IssuedSession:
    try:
        return auth_service.login(credentials)
    except LoginNotSupportedError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except AuthBackendUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication backend unavailable",
        ) from exc


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    request: Request,
    auth_service: AuthService = Depends(get_auth_service),
) -> None:
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing token")
    try:
        auth_service.logout(token.strip())
    except LoginNotSupportedError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvalidSessionError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    except AuthBackendUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication backend unavailable",
        ) from exc
