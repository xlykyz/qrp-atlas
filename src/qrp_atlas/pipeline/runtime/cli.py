"""Operational CLI for the isolated Pipeline runtime database."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Sequence

from qrp_atlas.config.settings import AppSettings, ConfigError
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry

from .contract_adapter import (
    ContractDeploymentSelection,
    contract_runtime_definition,
    definitions_from_contract_selections,
    load_contract_selections,
)
from .definitions import DEFAULT_DEFINITIONS_PATH, DefinitionValidationError, definitions_by_id, load_definitions
from .models import PipelineStatus
from .runner import PipelineRunner, PipelineRuntimePaths
from .scheduler import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAX_CATCH_UP_MINUTES,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_SCHEDULER_ID,
    PipelineScheduler,
)
from .store import PipelineRuntimeStore, RunClaimFailure


def _parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone offset")
    return parsed.astimezone(UTC)


def _parse_trade_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("trade date must use YYYY-MM-DD") from exc


def _parse_parameter_assignments(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        name, separator, raw = value.partition("=")
        if not separator or not name or name in parsed:
            raise ValueError("INVALID_PARAMETER_ASSIGNMENT")
        parsed[name] = raw
    return parsed


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
    validate_contracts_parser = subparsers.add_parser(
        "validate-contracts",
        help="validate source-registered formal Pipeline contracts without executing them",
    )
    validate_contracts_parser.add_argument("--contract-selections", type=Path)
    list_contracts = subparsers.add_parser(
        "list-contracts",
        help="print machine-readable source contracts without scheduling or executing",
    )
    list_contracts.add_argument("--contract-selections", type=Path)
    listing = subparsers.add_parser("list-definitions", help="list definitions without scheduling or executing")
    listing.add_argument("--definitions", type=Path)
    listing.add_argument("--contract-selections", type=Path)
    scan = subparsers.add_parser("scan", help="create due PENDING/BLOCKED run records; never executes commands")
    scan.add_argument("--definitions", type=Path)
    scan.add_argument("--contract-selections", type=Path)
    scan.add_argument("--at", type=_parse_instant, help="ISO-8601 scan instant with timezone")
    scan.add_argument("--scheduler-id", default=DEFAULT_SCHEDULER_ID)
    scan.add_argument("--max-catch-up-minutes", type=int, default=DEFAULT_MAX_CATCH_UP_MINUTES)
    scan.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    scan.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    scan.add_argument("--stale-after-seconds", type=int, default=DEFAULT_STALE_AFTER_SECONDS)
    run = subparsers.add_parser("run-pending", help="execute one pending record from an explicit definition source")
    run.add_argument("--definitions", type=Path)
    run.add_argument("--contract-selections", type=Path)
    run.add_argument("--run-id", help="specific pending run id; defaults to the oldest pending record")
    run.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    runs = subparsers.add_parser("status", help="show isolated Pipeline runtime records")
    runs.add_argument("--pipeline-id")
    runs.add_argument("--status", choices=[status.value for status in PipelineStatus])
    runs.add_argument("--limit", type=int, default=100)
    runs.add_argument("--include-result", action="store_true")
    cleanup = subparsers.add_parser("cleanup", help="fail stale RUNNING records and reclaim expired leases")
    cleanup.add_argument("--stale-after-seconds", type=int, required=True)
    retry = subparsers.add_parser("retry", help="create a new attempt while retaining failed evidence")
    retry.add_argument("run_id")
    retry.add_argument("--definitions", type=Path)
    retry.add_argument("--contract-selections", type=Path)
    formal_run = subparsers.add_parser(
        "run",
        help="create and execute one formal Pipeline run by pipeline_id through the existing runtime",
    )
    formal_run.add_argument("pipeline_id")
    formal_run.add_argument("--trade-date", type=_parse_trade_date)
    formal_run.add_argument("--set", dest="parameter_assignments", action="append", default=[], metavar="NAME=VALUE")
    formal_run.add_argument("--scheduled-for", type=_parse_instant)
    formal_run.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    formal_run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    execute_contract = subparsers.add_parser("execute-contract", help=argparse.SUPPRESS)
    execute_contract.add_argument("pipeline_id")
    return parser


def _runtime_paths(args: argparse.Namespace) -> PipelineRuntimePaths:
    if args.runtime_dir:
        return PipelineRuntimePaths(Path(args.runtime_dir).resolve(strict=False))
    settings = AppSettings.load(env_file=args.env_file)
    return PipelineRuntimePaths.from_settings(settings)


def _print_run(run, *, result: object | None = None) -> None:
    payload = {
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
    }
    if result is not None:
        payload["result"] = result
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def _print_error(reason: str, *, detail: str | None = None) -> None:
    payload: dict[str, str] = {"status": "ERROR", "reason": reason}
    if detail is not None:
        payload["detail"] = detail
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)


def _load_definitions_for_args(args: argparse.Namespace, *, require_source: bool = False):
    if args.definitions is not None and args.contract_selections is not None:
        raise DefinitionValidationError("choose either --definitions or --contract-selections")
    if args.contract_selections is not None:
        return definitions_from_contract_selections(load_contract_selections(args.contract_selections))
    if args.definitions is not None:
        return load_definitions(args.definitions)
    if require_source:
        raise DefinitionValidationError("--definitions or --contract-selections is required")
    return load_definitions(DEFAULT_DEFINITIONS_PATH)


def _write_result_atomically(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _execute_contract_command(args: argparse.Namespace) -> int:
    run_id = os.environ.get("QRP_PIPELINE_RUN_ID")
    pipeline_id = os.environ.get("QRP_PIPELINE_ID")
    scheduled_for = os.environ.get("QRP_PIPELINE_SCHEDULED_FOR")
    attempt = os.environ.get("QRP_PIPELINE_ATTEMPT")
    result_path = os.environ.get("QRP_PIPELINE_RESULT_PATH")
    if not all((run_id, pipeline_id, scheduled_for, attempt, result_path)):
        _print_error("EXECUTION_CONTEXT_MISSING")
        return 2
    if pipeline_id != args.pipeline_id:
        _print_error("PIPELINE_ID_MISMATCH", detail=args.pipeline_id)
        return 2
    try:
        scheduled = _parse_instant(scheduled_for)
        parsed_attempt = int(attempt)
        if parsed_attempt < 1:
            raise ValueError("attempt must be positive")
        trade_date_raw = os.environ.get("QRP_PIPELINE_TRADE_DATE")
        trade_date = _parse_trade_date(trade_date_raw) if trade_date_raw else None
        raw_parameters = json.loads(os.environ.get("QRP_PIPELINE_PARAMETER_OVERRIDES", "{}"))
        if not isinstance(raw_parameters, dict) or any(not isinstance(key, str) for key in raw_parameters):
            raise ValueError("INVALID_PARAMETER_ASSIGNMENT")
        contract = default_registry().get(args.pipeline_id)
        settings = AppSettings.load(env_file=args.env_file or os.environ.get("QRP_ENV_FILE"))
        result = execute_pipeline_contract(
            contract,
            PipelineInvocation(
                run_id=run_id,
                pipeline_id=args.pipeline_id,
                scheduled_for=scheduled,
                attempt=parsed_attempt,
                settings=settings,
                trade_date_override=trade_date,
                parameter_overrides=raw_parameters,
                audit_context={"runtime": "pipeline_runtime"},
            ),
        )
        payload = result.as_dict()
        _write_result_atomically(Path(result_path), payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if result.status in {ResultStatus.SUCCESS, ResultStatus.NOOP} else 1
    except (ValueError, KeyError, ConfigError, ContractValidationError) as exc:
        _print_error("CONTRACT_EXECUTION_ERROR", detail=str(exc))
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "validate-contracts":
        try:
            registry = default_registry()
            contracts = validate_contracts(registry.all())
            if args.contract_selections is not None:
                definitions_from_contract_selections(load_contract_selections(args.contract_selections), registry=registry)
            print(f"valid contracts: {len(contracts)}")
            return 0
        except (DefinitionValidationError, ContractValidationError, ValueError, KeyError) as exc:
            print(f"pipeline error: {exc}", file=sys.stderr)
            return 2
    if args.command == "list-contracts":
        try:
            registry = default_registry()
            contracts = validate_contracts(registry.all())
            if args.contract_selections is not None:
                definitions_from_contract_selections(load_contract_selections(args.contract_selections), registry=registry)
            for contract in contracts:
                print(json.dumps(contract.describe(), ensure_ascii=False, sort_keys=True))
            return 0
        except (DefinitionValidationError, ContractValidationError, ValueError, KeyError) as exc:
            print(f"pipeline error: {exc}", file=sys.stderr)
            return 2
    if args.command == "execute-contract":
        return _execute_contract_command(args)
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
            definitions = (
                load_definitions(args.definitions)
                if args.command == "validate-definitions"
                else _load_definitions_for_args(
                    args,
                    require_source=args.command in {"run-pending", "retry"},
                )
            )
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
            result = PipelineScheduler(
                store,
                definitions,
                scheduler_id=args.scheduler_id,
                max_catch_up_minutes=args.max_catch_up_minutes,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_seconds=args.lease_seconds,
                stale_after_seconds=args.stale_after_seconds,
            ).scan(now=args.at)
            for run in result:
                _print_run(run)
            print(
                json.dumps(
                    {
                        "status": "CATCH_UP_LIMITED" if result.catch_up_limited else "SCANNED",
                        "scheduler_id": result.scheduler_id,
                        "requested_start_at": (
                            result.requested_start_at.isoformat() if result.requested_start_at else None
                        ),
                        "scan_start_at": result.scan_start_at.isoformat() if result.scan_start_at else None,
                        "scanned_through_at": result.scanned_through_at.isoformat(),
                        "created_runs": len(result),
                        "stale_runs_recovered": result.stale_runs_recovered,
                        "expired_locks_reclaimed": result.expired_locks_reclaimed,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "run-pending":
            store.initialize()
            if args.run_id is not None:
                target = store.get_run(args.run_id)
                if target is None:
                    _print_error("RUN_NOT_FOUND", detail=args.run_id)
                    return 2
                if target.status is not PipelineStatus.PENDING:
                    _print_error("RUN_NOT_PENDING", detail=target.status.value)
                    return 2
            else:
                pending = store.list_runs(status=PipelineStatus.PENDING, limit=1_000)
                target = min(pending, key=lambda run: (run.scheduled_at, run.attempt), default=None)
                if target is None:
                    print(json.dumps({"status": "IDLE", "reason": "NO_PENDING_RUN"}, sort_keys=True))
                    return 0
            definition = definitions_by_id(definitions).get(target.pipeline_id)
            if definition is None:
                _print_error("DEFINITION_MISSING", detail=target.pipeline_id)
                return 2
            if definition.definition_version != target.definition_version:
                _print_error(
                    "DEFINITION_VERSION_MISMATCH",
                    detail=f"run={target.definition_version}, definition={definition.definition_version}",
                )
                return 2
            try:
                result = PipelineRunner(
                    store,
                    paths,
                    heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                    lease_seconds=args.lease_seconds,
                ).run(target.run_id, definition)
            except RunClaimFailure as exc:
                _print_error(exc.code, detail=exc.detail)
                return 1
            _print_run(result)
            return 0 if result.status is PipelineStatus.SUCCESS else 1
        if args.command == "run":
            registry = default_registry()
            contracts = validate_contracts(registry.all())
            contract = registry.get(args.pipeline_id)
            scheduled_for = args.scheduled_for or datetime.now(UTC)
            environment: dict[str, str] = {}
            if args.trade_date is not None:
                environment["QRP_PIPELINE_TRADE_DATE"] = args.trade_date.isoformat()
            environment["QRP_PIPELINE_PARAMETER_OVERRIDES"] = json.dumps(
                _parse_parameter_assignments(args.parameter_assignments),
                sort_keys=True,
            )
            if args.env_file:
                environment["QRP_ENV_FILE"] = args.env_file
            definition = contract_runtime_definition(
                contract,
                ContractDeploymentSelection(
                    pipeline_id=contract.pipeline_id,
                    enabled=True,
                    schedule="* * * * *",
                ),
                environment=environment,
            )
            store.initialize()
            eligibility = PipelineScheduler(
                store,
                (definition,),
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_seconds=args.lease_seconds,
                stale_after_seconds=max(DEFAULT_STALE_AFTER_SECONDS, args.lease_seconds + 1),
            ).eligibility(definition, scheduled_for)
            target, _ = store.create_scheduled_run(
                definition,
                scheduled_at=scheduled_for,
                trigger_type="MANUAL",
                status=eligibility[0],
                error_summary=eligibility[1],
            )
            if target.status is not PipelineStatus.PENDING:
                _print_run(target)
                return 1
            result = PipelineRunner(
                store,
                paths,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_seconds=args.lease_seconds,
            ).run(target.run_id, definition)
            _print_run(result)
            payload = store.get_result(result.run_id)
            if payload is not None:
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0 if result.status is PipelineStatus.SUCCESS else 1
        if args.command == "status":
            status = PipelineStatus(args.status) if args.status else None
            for run in store.list_runs(pipeline_id=args.pipeline_id, status=status, limit=args.limit):
                _print_run(run, result=store.get_result(run.run_id) if args.include_result else None)
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
    except (DefinitionValidationError, ContractValidationError, ValueError, KeyError) as exc:
        print(f"pipeline error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
