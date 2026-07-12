"""User-domain services."""

from __future__ import annotations

from uuid import UUID

from qrp_atlas.users.repository import UserRepository
from qrp_atlas.users.schemas import User, UserCreate, UserStatus


class UserNotFoundError(LookupError):
    pass


class UserDisabledError(PermissionError):
    pass


class UserAlreadyExistsError(ValueError):
    pass


class UserService:
    def __init__(self, repository: UserRepository) -> None:
        self._repository = repository

    def get(self, user_id: UUID) -> User:
        user = self._repository.get_by_id(user_id)
        if user is None:
            raise UserNotFoundError(f"user not found: {user_id}")
        return user

    def get_active(self, user_id: UUID) -> User:
        return self._require_active(self.get(user_id))

    def get_active_by_username(self, username: str) -> User:
        user = self._repository.get_by_username(username.strip().lower())
        if user is None:
            raise UserNotFoundError("user not found")
        return self._require_active(user)

    def create(self, data: UserCreate) -> User:
        if self._repository.get_by_username(data.username) is not None:
            raise UserAlreadyExistsError(f"username already exists: {data.username}")
        return self._repository.create(data)

    @staticmethod
    def _require_active(user: User) -> User:
        if user.status is not UserStatus.ACTIVE:
            raise UserDisabledError(f"user is not active: {user.user_id}")
        return user
