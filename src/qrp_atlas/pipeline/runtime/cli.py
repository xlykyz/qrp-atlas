"""Operational CLI for the isolated Pipeline runtime database."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

from qrp_atlas.config.settings import AppSettings, ConfigError

from .definitions import DEFAULT_DEFINITIONS_PATH, DefinitionValidationError, definitions_by_id, load_definitions
from .models import PipelineStatus
from .runner import PipelineRunner, PipelineRuntimePaths
from .scheduler import PipelineScheduler
from .store import PipelineRuntimeStore


def _parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone offset")
    return parsed.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qrp-atlas-pipeline")
    parser.add_argument("--env-file", help="explicit QRP environment file")
    parser.add_argument(
        "--runtime-dir",
        help="override runtime directory; intended for controlled deployment or tests",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="initialize only the isolated SQLite runtime database")
    validate = subparsers.add_parser("validate-definitions", help="validate a Git-versioned JSON manifest")
    validate.add_argument("--definitions", type=Path, default=DEFAULT_DEFINITIONS_PATH)
    listing = subparsers.add_parser("list-definitions", help="list definitions without scheduling or executing")
    listing.add_argument("--definitions", type=Path, default=DEFAULT_DEFINITIONS_PATH)
    scan = subparsers.add_parser("scan", help="create due PENDING/BLOCKED run records; never executes commands")
    scan.add_argument("--definitions", type=Path, default=DEFAULT_DEFINITIONS_PATH)
    scan.add_argument("--at", type=_parse_instant, help="ISO-8601 scan instant with timezone")
    run = subparsers.add_parser("run-pending", help="execute one pending record from an explicit manifest")
    run.add_argument("--definitions", type=Path, required=True)
    run.add_argument("--run-id", help="specific pending run id; defaults to the oldest pending record")
    runs = subparsers.add_parser("status", help="show isolated Pipeline runtime records")
    runs.add_argument("--pipeline-id")
    runs.add_argument("--status", choices=[status.value for status in PipelineStatus])
    runs.add_argument("--limit", type=int, default=100)
    cleanup = subparsers.add_parser("cleanup", help="fail stale RUNNING records and reclaim expired leases")
    cleanup.add_argument("--stale-after-seconds", type=int, required=True)
    retry = subparsers.add_parser("retry", help="create a new attempt while retaining failed evidence")
    retry.add_argument("run_id")
    retry.add_argument("--definitions", type=Path, required=True)
    return parser


def _runtime_paths(args: argparse.Namespace) -> PipelineRuntimePaths:
    if args.runtime_dir:
        return PipelineRuntimePaths(Path(args.runtime_dir).resolve(strict=False))
    settings = AppSettings.load(env_file=args.env_file)
    return PipelineRuntimePaths.from_settings(settings)


def _print_run(run) -> None:
    print(
        json.dumps(
            {
                "run_id": run.run_id,
                "pipeline_id": run.pipeline_id,
                "definition_version": run.definition_version,
                "scheduled_at": run.scheduled_at.isoformat(),
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "status": run.status.value,
                "attempt": run.attempt,
                "exit_code": run.exit_code,
                "timed_out": run.timed_out,
                "stdout_path": str(run.stdout_path) if run.stdout_path else None,
                "stderr_path": str(run.stderr_path) if run.stderr_path else None,
                "error_summary": run.error_summary,
                "heartbeat_at": run.heartbeat_at.isoformat() if run.heartbeat_at else None,
                "wall_duration_ms": run.wall_duration_ms,
                "user_cpu_ms": run.user_cpu_ms,
                "system_cpu_ms": run.system_cpu_ms,
                "peak_rss_kb": run.peak_rss_kb,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        paths = _runtime_paths(args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    store = PipelineRuntimeStore(paths.database_path)
    try:
        if args.command == "init":
            store.initialize()
            print(paths.database_path)
            return 0
        if args.command in {"validate-definitions", "list-definitions", "scan", "run-pending", "retry"}:
            definitions = load_definitions(args.definitions)
        else:
            definitions = ()
        if args.command == "validate-definitions":
            print(f"valid definitions: {len(definitions)}")
            return 0
        if args.command == "list-definitions":
            for definition in definitions:
                print(
                    json.dumps(
                        {
                            "pipeline_id": definition.pipeline_id,
                            "name": definition.name,
                            "enabled": definition.enabled,
                            "schedule": definition.schedule,
                            "timezone": definition.timezone,
                            "definition_version": definition.definition_version,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
            return 0
        if args.command == "scan":
            created = PipelineScheduler(store, definitions).scan(now=args.at)
            for run in created:
                _print_run(run)
            return 0
        if args.command == "run-pending":
            pending = store.list_runs(status=PipelineStatus.PENDING, limit=1_000)
            target = next((run for run in pending if run.run_id == args.run_id), None) if args.run_id else None
            if target is None and args.run_id is None:
                target = min(pending, key=lambda run: (run.scheduled_at, run.attempt), default=None)
            if target is None:
                print("no matching pending run", file=sys.stderr)
                return 1
            definition = definitions_by_id(definitions).get(target.pipeline_id)
            if definition is None:
                print(f"definition missing for pipeline {target.pipeline_id}", file=sys.stderr)
                return 2
            result = PipelineRunner(store, paths).run(target.run_id, definition)
            if result is None:
                print("run was not claimable", file=sys.stderr)
                return 1
            _print_run(result)
            return 0 if result.status is PipelineStatus.SUCCESS else 1
        if args.command == "status":
            status = PipelineStatus(args.status) if args.status else None
            for run in store.list_runs(pipeline_id=args.pipeline_id, status=status, limit=args.limit):
                _print_run(run)
            return 0
        if args.command == "cleanup":
            stale_runs, expired_locks = store.recover_stale(stale_after_seconds=args.stale_after_seconds)
            print(json.dumps({"stale_runs": stale_runs, "expired_locks": expired_locks}))
            return 0
        if args.command == "retry":
            previous = store.get_run(args.run_id)
            if previous is None:
                print(f"unknown pipeline run {args.run_id}", file=sys.stderr)
                return 2
            definition = definitions_by_id(definitions).get(previous.pipeline_id)
            if definition is None or definition.definition_version != previous.definition_version:
                print("matching definition/version is required to retry", file=sys.stderr)
                return 2
            _print_run(store.retry_run(args.run_id, max_retries=definition.max_retries))
            return 0
    except (DefinitionValidationError, ValueError, KeyError) as exc:
        print(f"pipeline error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
