"""Configured QRP Atlas API launcher."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

import uvicorn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qrp-atlas-api")
    parser.add_argument(
        "--env-file",
        help="explicit QRP Atlas dotenv file; relative paths resolve from the repository root",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file:
        os.environ["QRP_ENV_FILE"] = args.env_file
    from qrp_atlas.config.settings import ConfigError, get_settings, reset_settings_cache

    reset_settings_cache()
    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    uvicorn.run(
        "qrp_atlas.api.server:app",
        host=settings.api.host,
        port=settings.api.port,
        log_level=settings.logging.level.lower(),
    )
    return 0


if __name__ == "__main__":
    main()
