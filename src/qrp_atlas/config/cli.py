"""Command-line interface for effective configuration, doctor, and init."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from qrp_atlas.config.operations import (
    CheckLevel,
    InitStatus,
    doctor,
    has_failures,
    initialize_runtime,
)
from qrp_atlas.config.settings import (
    AppSettings,
    ConfigError,
    LEGACY_ENV_ALIASES,
    SUPPORTED_ENV_VARS,
)


def _parse_overrides(values: Sequence[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    supported = SUPPORTED_ENV_VARS | LEGACY_ENV_ALIASES
    for item in values:
        name, separator, value = item.partition("=")
        if not separator or not name.strip():
            raise ConfigError("--set values must use NAME=VALUE")
        name = name.strip()
        if name not in supported:
            raise ConfigError(f"unsupported configuration name: {name}")
        overrides[name] = value
    return overrides


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qrp-atlas-config",
        description="Show, validate, and initialize QRP Atlas runtime configuration",
    )
    parser.add_argument(
        "--env-file",
        help="explicit .env path; relative paths resolve from the repository root",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="explicit override (highest priority); may be repeated",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    show = subparsers.add_parser("show", help="show effective redacted configuration")
    show.add_argument("--compact", action="store_true", help="emit compact JSON")
    doctor_parser = subparsers.add_parser(
        "doctor", help="run non-destructive deployment diagnostics"
    )
    doctor_parser.add_argument("--json", action="store_true", help="emit JSON")
    init_parser = subparsers.add_parser(
        "init", help="create configured directories without creating databases"
    )
    init_parser.add_argument("--json", action="store_true", help="emit JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = AppSettings.load(
            overrides=_parse_overrides(args.set),
            env_file=args.env_file,
        )
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    if args.command == "show":
        print(settings.safe_json(indent=None if args.compact else 2))
        return 0

    if args.command == "doctor":
        results = doctor(settings)
        if args.json:
            print(
                json.dumps(
                    [
                        {
                            "level": item.level.value,
                            "name": item.name,
                            "message": item.message,
                        }
                        for item in results
                    ],
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            labels = {
                CheckLevel.OK: "OK",
                CheckLevel.WARNING: "WARN",
                CheckLevel.FAILURE: "FAIL",
            }
            for item in results:
                print(f"[{labels[item.level]}] {item.name}: {item.message}")
        return 1 if has_failures(results) else 0

    results = initialize_runtime(settings)
    if args.json:
        print(
            json.dumps(
                [
                    {
                        "status": item.status.value,
                        "path": str(item.path),
                        "message": item.message,
                    }
                    for item in results
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        labels = {
            InitStatus.CREATED: "CREATED",
            InitStatus.EXISTS: "EXISTS",
            InitStatus.SKIPPED: "SKIP",
            InitStatus.FAILURE: "FAIL",
        }
        for item in results:
            print(f"[{labels[item.status]}] {item.path}: {item.message}")
    return 1 if has_failures(results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
