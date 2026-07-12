from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from qrp_atlas.auth.exceptions import InvalidCredentialsError, InvalidSessionError
from qrp_atlas.auth.passwords import PasswordHasher
from qrp_atlas.auth.providers.database import DatabaseAuthProvider
from qrp_atlas.auth.schemas import AuthSession, LoginCredentials
from qrp_atlas.users.schemas import User, UserStatus
from qrp_atlas.users.service import UserService


class FakeUserRepository:
    def __init__(self, user: User):
        self.user = user

    def get_by_id(self, user_id):
        return self.user if self.user.user_id == user_id else None

    def get_by_username(self, username):
        return self.user if self.user.username == username else None

    def create(self, data):
        raise NotImplementedError


class FakeAuthRepository:
    def __init__(self, user_id, password_hash):
        self.user_id = user_id
        self.password_hash = password_hash
        self.sessions = {}

    def get_password_hash(self, user_id):
        return self.password_hash if user_id == self.user_id else None

    def set_password_hash(self, user_id, password_hash):
        self.password_hash = password_hash

    def add_identity(self, user_id, provider, provider_subject):
        return None

    def create_session(self, user_id, token_hash, expires_at):
        session = AuthSession(
            session_id=uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
        )
        self.sessions[token_hash] = session
        return session

    def get_session_by_token_hash(self, token_hash):
        return self.sessions.get(token_hash)

    def revoke_session(self, token_hash, revoked_at):
        session = self.sessions[token_hash]
        self.sessions[token_hash] = session.model_copy(update={"revoked_at": revoked_at})


def make_provider():
    user = User(
        user_id=UUID("f445c8c9-96d8-4ce7-9f8a-9e884dd038d8"),
        username="ryan",
        display_name="Ryan",
        status=UserStatus.ACTIVE,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    hasher = PasswordHasher()
    auth_repo = FakeAuthRepository(user.user_id, hasher.hash("correct-password"))
    provider = DatabaseAuthProvider(
        users=UserService(FakeUserRepository(user)),
        repository=auth_repo,
        password_hasher=hasher,
        session_ttl_seconds=3600,
    )
    return provider, auth_repo


def test_database_provider_issues_and_resolves_opaque_session():
    provider, _ = make_provider()
    issued = provider.login(
        LoginCredentials(username="ryan", password="correct-password")
    )

    assert issued.access_token
    assert provider.resolve_user(issued.access_token) == issued.user


def test_database_provider_rejects_wrong_password():
    provider, _ = make_provider()

    with pytest.raises(InvalidCredentialsError):
        provider.login(LoginCredentials(username="ryan", password="wrong"))


def test_database_provider_rejects_revoked_session():
    provider, _ = make_provider()
    issued = provider.login(
        LoginCredentials(username="ryan", password="correct-password")
    )
    provider.logout(issued.access_token)

    with pytest.raises(InvalidSessionError):
        provider.resolve_user(issued.access_token)
