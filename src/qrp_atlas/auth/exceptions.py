"""Authentication exceptions independent of HTTP transport."""


class AuthenticationError(PermissionError):
    pass


class InvalidCredentialsError(AuthenticationError):
    pass


class InvalidSessionError(AuthenticationError):
    pass


class LoginNotSupportedError(AuthenticationError):
    pass


class AuthBackendUnavailableError(RuntimeError):
    """Raised when the database auth control plane is unavailable."""

    pass
