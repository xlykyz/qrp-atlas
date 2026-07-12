from uuid import UUID

import pytest

from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.exceptions import LoginNotSupportedError
from qrp_atlas.auth.providers.local import LocalAuthProvider
from qrp_atlas.auth.schemas import LoginCredentials


def test_local_provider_returns_stable_user_without_credentials():
    expected = UserContext(
        user_id=UUID("f445c8c9-96d8-4ce7-9f8a-9e884dd038d8"),
        username="ryan",
        display_name="Ryan",
    )
    provider = LocalAuthProvider(expected)

    assert provider.resolve_user(None) == expected
    assert provider.resolve_user("ignored") == expected


def test_local_provider_explicitly_disables_login():
    provider = LocalAuthProvider(
        UserContext(
            user_id=UUID("f445c8c9-96d8-4ce7-9f8a-9e884dd038d8"),
            username="ryan",
            display_name="Ryan",
        )
    )

    with pytest.raises(LoginNotSupportedError):
        provider.login(LoginCredentials(username="ryan", password="secret"))
