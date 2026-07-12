"""Authentication exceptions independent of HTTP transport."""


class AuthenticationError(PermissionError):
    pass


class InvalidCredentialsError(AuthenticationError):
    pass


class InvalidSessionError(AuthenticationError):
    pass


class LoginNotSupportedError(AuthenticationError):
    pass
