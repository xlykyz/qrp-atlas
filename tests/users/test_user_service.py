from datetime import datetime, timezone
from uuid import uuid4

import pytest

from qrp_atlas.users.schemas import User, UserStatus
from qrp_atlas.users.service import UserDisabledError, UserService


class FakeRepository:
    def __init__(self, user):
        self.user = user

    def get_by_id(self, user_id):
        return self.user if self.user.user_id == user_id else None

    def get_by_username(self, username):
        return self.user if self.user.username == username else None

    def create(self, data):
        raise NotImplementedError


def test_disabled_user_cannot_be_resolved_as_active():
    user = User(
        user_id=uuid4(),
        username="ryan",
        display_name="Ryan",
        status=UserStatus.DISABLED,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    service = UserService(FakeRepository(user))

    with pytest.raises(UserDisabledError):
        service.get_active(user.user_id)
