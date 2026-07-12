"""Authentication boundary for local and database-backed runtime modes."""

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.dependencies import CurrentUser, get_current_user
from qrp_atlas.auth.exceptions import (
    AuthenticationError,
    InvalidCredentialsError,
    InvalidSessionError,
    LoginNotSupportedError,
)
from qrp_atlas.auth.service import AuthService

__all__ = [
    "AuthenticationError",
    "AuthService",
    "CurrentUser",
    "InvalidCredentialsError",
    "InvalidSessionError",
    "LoginNotSupportedError",
    "UserContext",
    "get_current_user",
]
