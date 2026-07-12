"""Administrative CLI for the PostgreSQL auth control plane."""

from __future__ import annotations

import argparse
import getpass

from qrp_atlas.auth.dependencies import make_connection_factory
from qrp_atlas.auth.passwords import PasswordHasher
from qrp_atlas.auth.repository import PostgresAuthRepository
from qrp_atlas.config.auth import AuthSettings
from qrp_atlas.users.repository import PostgresUserRepository
from qrp_atlas.users.schemas import UserCreate
from qrp_atlas.users.service import UserService


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage QRP auth users")
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create-user", help="create a password user")
    create.add_argument("--username", required=True)
    create.add_argument("--display-name", required=True)
    args = parser.parse_args()

    settings = AuthSettings.from_env()
    factory = make_connection_factory(settings.postgres_dsn)
    users = UserService(PostgresUserRepository(factory))
    auth = PostgresAuthRepository(factory)

    if args.command == "create-user":
        password = getpass.getpass("Password: ")
        confirmation = getpass.getpass("Confirm password: ")
        if password != confirmation:
            raise SystemExit("passwords do not match")
        user = users.create(
            UserCreate(username=args.username, display_name=args.display_name)
        )
        auth.set_password_hash(user.user_id, PasswordHasher().hash(password))
        auth.add_identity(user.user_id, "password", user.username)
        print(f"created user {user.username} ({user.user_id})")


if __name__ == "__main__":
    main()
