"""Operational CLI for the isolated Job runtime database."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Sequence

from qrp_atlas.config.settings import AppSettings, ConfigError
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry

from qrp_atlas.pipeline.job_adapter import (
    ContractDeploymentSelection,
    contract_runtime_definition,
    definitions_from_contract_selections,
    load_contract_selections,
    make_in_process_contract_executor,
    runtime_definition_from_production_job,
)
from qrp_atlas.pipeline.production_jobs import (
    DEFAULT_PRODUCTION_JOBS_PATH,
    load_and_validate_production_jobs,
    resolve_instance_dependencies,
)
from qrp_atlas.orchestration.definitions import DEFAULT_DEFINITIONS_PATH, DefinitionValidationError, definitions_by_id, load_definitions
from qrp_atlas.orchestration.models import JobDefinition, JobStatus
from qrp_atlas.orchestration.planning import dependency_plan, scheduled_instant_for_target_date
from qrp_atlas.orchestration.result_log import JobResultLog, ResultLogConfigurationError
from qrp_atlas.orchestration.runner import JobRunner, JobRuntimePaths
from qrp_atlas.orchestration.scheduler import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAX_CATCH_UP_MINUTES,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_SCHEDULER_ID,
    JobScheduler,
)
from qrp_atlas.orchestration.service import JobService, JobServiceFatalError
from qrp_atlas.orchestration.store import JobRuntimeStore, JobClaimFailure, utc_now


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
    parser = argparse.ArgumentParser(prog="qrp-atlas-jobs")
    parser.add_argument("--env-file", help="explicit QRP environment file")
    parser.add_argument(
        "--runtime-dir",
        help="override runtime directory; intended for controlled deployment or tests",
    )
    parser.add_argument(
        "--result-log-dir",
        help="override the configured external directory for final pipeline result JSONL logs",
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
    listing.add_argument("--production-jobs", type=Path)
    listing_alias = subparsers.add_parser("list", help="list registered source contracts or configured definitions")
    listing_alias.add_argument("--definitions", type=Path)
    listing_alias.add_argument("--contract-selections", type=Path)
    listing_alias.add_argument("--production-jobs", type=Path)
    show = subparsers.add_parser("show", help="show one registered Pipeline definition and dependencies")
    show.add_argument("job_id")
    show.add_argument("--definitions", type=Path)
    show.add_argument("--contract-selections", type=Path)
    show.add_argument("--production-jobs", type=Path)
    plan = subparsers.add_parser("plan", help="produce a non-executing dependency plan for one Pipeline")
    plan.add_argument("job_id")
    plan.add_argument("--definitions", type=Path)
    plan.add_argument("--contract-selections", type=Path)
    plan.add_argument("--production-jobs", type=Path)
    plan.add_argument("--target-date", type=_parse_trade_date)
    scan = subparsers.add_parser("scan", help="create due PENDING/BLOCKED run records; never executes commands")
    scan.add_argument("--definitions", type=Path)
    scan.add_argument("--contract-selections", type=Path)
    scan.add_argument("--production-jobs", type=Path)
    scan.add_argument("--at", type=_parse_instant, help="ISO-8601 scan instant with timezone")
    scan.add_argument("--scheduler-id", default=DEFAULT_SCHEDULER_ID)
    scan.add_argument("--max-catch-up-minutes", type=int, default=DEFAULT_MAX_CATCH_UP_MINUTES)
    scan.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    scan.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    scan.add_argument("--stale-after-seconds", type=int, default=DEFAULT_STALE_AFTER_SECONDS)
    run = subparsers.add_parser("run-pending", help="execute one pending record from an explicit definition source")
    run.add_argument("--definitions", type=Path)
    run.add_argument("--contract-selections", type=Path)
    run.add_argument("--production-jobs", type=Path)
    run.add_argument("--run-id", help="specific pending run id; defaults to the oldest pending record")
    run.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    runs = subparsers.add_parser("status", help="show isolated Job runtime records")
    runs.add_argument("--job-id", "--pipeline-id", dest="job_id")
    runs.add_argument("--status", choices=[status.value for status in JobStatus])
    runs.add_argument("--limit", type=int, default=100)
    runs.add_argument("--include-result", action="store_true")
    runs.add_argument("--run-id")
    latest = subparsers.add_parser("latest", help="show the latest run for one Pipeline")
    latest.add_argument("job_id")
    latest.add_argument("--include-result", action="store_true")
    cleanup = subparsers.add_parser("cleanup", help="fail stale RUNNING records and reclaim expired leases")
    cleanup.add_argument("--stale-after-seconds", type=int, required=True)
    retry = subparsers.add_parser("retry", help="create a new attempt while retaining failed evidence")
    retry.add_argument("run_id")
    retry.add_argument("--definitions", type=Path)
    retry.add_argument("--contract-selections", type=Path)
    retry.add_argument("--production-jobs", type=Path)
    retry.add_argument("--execute", action="store_true", help="execute the newly created retry attempt immediately")
    formal_run = subparsers.add_parser(
        "run",
        help="create and execute one formal Pipeline run by job_id through the existing runtime",
    )
    formal_run.add_argument("job_id")
    formal_run.add_argument("--trade-date", "--target-date", dest="trade_date", type=_parse_trade_date)
    formal_run.add_argument("--set", dest="parameter_assignments", action="append", default=[], metavar="NAME=VALUE")
    formal_run.add_argument("--scheduled-for", type=_parse_instant)
    formal_run.add_argument("--definitions", type=Path)
    formal_run.add_argument("--contract-selections", type=Path)
    formal_run.add_argument("--production-jobs", type=Path)
    formal_run.add_argument("--with-dependencies", action="store_true")
    formal_run.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    formal_run.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    serve = subparsers.add_parser("serve", help="continuously scan and execute due configured Pipelines")
    serve.add_argument("--definitions", type=Path)
    serve.add_argument("--contract-selections", type=Path)
    serve.add_argument("--production-jobs", type=Path)
    serve.add_argument("--scheduler-id", default=DEFAULT_SCHEDULER_ID)
    serve.add_argument("--service-name", default="job-scheduler")
    serve.add_argument("--poll-interval-seconds", type=float, default=5.0)
    serve.add_argument("--max-workers", type=int, default=4)
    serve.add_argument("--max-catch-up-minutes", type=int, default=DEFAULT_MAX_CATCH_UP_MINUTES)
    serve.add_argument("--heartbeat-interval-seconds", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    serve.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    serve.add_argument("--stale-after-seconds", type=int, default=DEFAULT_STALE_AFTER_SECONDS)
    serve.add_argument("--once", action="store_true", help="run one service cycle and exit; intended for controlled checks")
    health = subparsers.add_parser("health", help="report runtime database and scheduler-service health")
    health.add_argument("--scheduler-id", default=DEFAULT_SCHEDULER_ID)
    health.add_argument("--service-name", default="job-scheduler")
    execute_contract = subparsers.add_parser("execute-contract", help=argparse.SUPPRESS)
    execute_contract.add_argument("job_id")
    validate_jobs = subparsers.add_parser(
        "validate-job-definitions",
        help="validate source-registered production job definitions without executing them",
    )
    validate_jobs.add_argument("--production-jobs", type=Path)
    list_jobs = subparsers.add_parser(
        "list-job-definitions",
        help="list source-registered production job definitions without scheduling or executing",
    )
    list_jobs.add_argument("--production-jobs", type=Path)
    show_job = subparsers.add_parser(
        "show-job-definition",
        help="show one source-registered production job definition and its Contract mapping",
    )
    show_job.add_argument("job_id")
    show_job.add_argument("--production-jobs", type=Path)
    return parser


def _runtime_paths(args: argparse.Namespace) -> JobRuntimePaths:
    if args.runtime_dir:
        paths = JobRuntimePaths(Path(args.runtime_dir).resolve(strict=False))
    else:
        settings = AppSettings.load(env_file=args.env_file)
        paths = JobRuntimePaths.from_settings(settings)
    if args.result_log_dir:
        paths = replace(paths, result_logs_dir_override=Path(args.result_log_dir).resolve(strict=False))
    return paths


def _print_run(run, *, result: object | None = None, submitted_to_service: bool = False) -> None:
    payload = {
        "run_id": run.run_id,
        "job_id": run.job_id,
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
        "trigger_type": run.trigger_type,
        "retry_of_run_id": run.retry_of_run_id,
        "trade_date_override": run.trade_date_override.isoformat() if run.trade_date_override else None,
        "parameter_overrides": dict(run.parameter_overrides),
    }
    if result is not None:
        payload["result"] = result
    if submitted_to_service:
        payload["submitted_to_service"] = True
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
    sources = [
        value is not None
        for value in (args.definitions, args.contract_selections, getattr(args, "production_jobs", None))
    ]
    if sum(sources) > 1:
        raise DefinitionValidationError(
            "choose exactly one of --definitions, --contract-selections, --production-jobs"
        )
    if args.contract_selections is not None:
        definitions = definitions_from_contract_selections(load_contract_selections(args.contract_selections))
        environment = {"QRP_ENV_FILE": args.env_file} if args.env_file else {}
        return _apply_definition_environment(definitions, environment)
    if getattr(args, "production_jobs", None) is not None:
        environment = {"QRP_ENV_FILE": args.env_file} if args.env_file else {}
        return _apply_definition_environment(
            _definitions_from_production_jobs(args.production_jobs),
            environment,
        )
    if args.definitions is not None:
        return load_definitions(args.definitions)
    if require_source:
        raise DefinitionValidationError(
            "--definitions, --contract-selections, or --production-jobs is required"
        )
    return load_definitions(DEFAULT_DEFINITIONS_PATH)


def _definitions_from_production_jobs(path: str | Path) -> tuple[JobDefinition, ...]:
    """Map validated production job instances onto existing runtime definitions.

    Contract dependencies (pipeline_id values) are resolved to the
    corresponding production instance job_id values before mapping, so the
    runtime Scheduler and dependency plans query by job_id.
    """

    registry = default_registry()
    jobs = load_and_validate_production_jobs(path, registry=registry)
    resolved = resolve_instance_dependencies(jobs, registry=registry)
    return tuple(
        runtime_definition_from_production_job(
            job,
            registry.get(job.pipeline_id),
            dependency_job_ids=resolved[job.job_id],
        )
        for job in jobs
    )


def _print_definition(definition: JobDefinition) -> None:
    """Display configuration without exposing environment values."""

    print(
        json.dumps(
            {
                "job_id": definition.job_id,
                "pipeline_id": definition.pipeline_id,
                "name": definition.name,
                "enabled": definition.enabled,
                "schedule": definition.schedule,
                "timezone": definition.timezone,
                "definition_version": definition.definition_version,
                "dependencies": list(definition.dependencies),
                "resource_locks": list(definition.resource_locks),
                "resource_reads": list(definition.resource_reads),
                "overlap_policy": definition.overlap_policy.value,
                "timeout_seconds": definition.timeout_seconds,
                "max_retries": definition.max_retries,
                "requires_structured_result": definition.requires_structured_result,
                "manual_execution_allowed": definition.manual_execution_allowed,
                "environment_variable_names": sorted(definition.environment),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def _formal_runtime_definitions(*, environment: dict[str, str] | None = None) -> tuple[JobDefinition, ...]:
    """Adapt every registered source contract for manual inspection/execution only."""

    registry = default_registry()
    contracts = validate_contracts(registry.all())
    return tuple(
        contract_runtime_definition(
            contract,
            ContractDeploymentSelection(contract.pipeline_id, enabled=True, schedule="* * * * *"),
            environment=environment,
        )
        for contract in contracts
    )


def _apply_definition_environment(
    definitions: tuple[JobDefinition, ...],
    environment: dict[str, str],
) -> tuple[JobDefinition, ...]:
    if not environment:
        return definitions
    merged: list[JobDefinition] = []
    for item in definitions:
        merged_environment = {**item.environment, **environment}
        if item.in_process_executor is not None:
            contract = default_registry().get(item.pipeline_id or item.job_id)
            item = replace(
                item,
                environment=merged_environment,
                in_process_executor=make_in_process_contract_executor(
                    contract,
                    environment=merged_environment,
                ),
            )
        else:
            item = replace(item, environment=merged_environment)
        merged.append(item)
    return tuple(merged)


def _definitions_for_manual_run(args: argparse.Namespace, *, environment: dict[str, str]) -> tuple[JobDefinition, ...]:
    if args.definitions is not None:
        definitions = _load_definitions_for_args(args, require_source=True)
        if args.parameter_assignments:
            raise DefinitionValidationError("--set is supported only for source-registered formal Pipelines")
        return _apply_definition_environment(definitions, environment)
    if args.contract_selections is not None or getattr(args, "production_jobs", None) is not None:
        # Selections and production jobs still resolve to source-registered
        # Contracts.  Their instance-level manifests may choose
        # schedule/enablement/fixed parameters, while controlled manual
        # parameters remain part of the persisted Run.
        return _load_definitions_for_args(args, require_source=True)
    return _formal_runtime_definitions(environment=environment)


def _write_result_log(paths: JobRuntimePaths, store: JobRuntimeStore, run) -> None:
    JobResultLog(paths.result_logs_dir).write(run, store.get_result(run.run_id))


def _manual_run_environment(args: argparse.Namespace) -> dict[str, str]:
    # Business overrides belong to the durable JobRun invocation context. Do
    # not close them over in a Definition executor: a dependency would then
    # observe the target Job's parameters when the service executes the run.
    return {"QRP_ENV_FILE": args.env_file} if args.env_file else {}


def _write_result_atomically(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _execute_contract_command(args: argparse.Namespace) -> int:
    run_id = os.environ.get("QRP_JOB_RUN_ID") or os.environ.get("QRP_PIPELINE_RUN_ID")
    job_id = os.environ.get("QRP_JOB_ID") or os.environ.get("QRP_PIPELINE_ID")
    scheduled_for = os.environ.get("QRP_JOB_SCHEDULED_FOR") or os.environ.get("QRP_PIPELINE_SCHEDULED_FOR")
    attempt = os.environ.get("QRP_JOB_ATTEMPT") or os.environ.get("QRP_PIPELINE_ATTEMPT")
    result_path = os.environ.get("QRP_JOB_RESULT_PATH") or os.environ.get("QRP_PIPELINE_RESULT_PATH")
    if not all((run_id, job_id, scheduled_for, attempt, result_path)):
        _print_error("EXECUTION_CONTEXT_MISSING")
        return 2
    if job_id != args.job_id:
        _print_error("JOB_ID_MISMATCH", detail=args.job_id)
        return 2
    try:
        scheduled = _parse_instant(scheduled_for)
        parsed_attempt = int(attempt)
        if parsed_attempt < 1:
            raise ValueError("attempt must be positive")
        trade_date_raw = os.environ.get("QRP_JOB_TRADE_DATE") or os.environ.get("QRP_PIPELINE_TRADE_DATE")
        trade_date = _parse_trade_date(trade_date_raw) if trade_date_raw else None
        raw_parameters = json.loads(
            os.environ.get("QRP_JOB_PARAMETER_OVERRIDES")
            or os.environ.get("QRP_PIPELINE_PARAMETER_OVERRIDES", "{}")
        )
        if not isinstance(raw_parameters, dict) or any(not isinstance(key, str) for key in raw_parameters):
            raise ValueError("INVALID_PARAMETER_ASSIGNMENT")
        contract = default_registry().get(args.job_id)
        settings = AppSettings.load(env_file=args.env_file or os.environ.get("QRP_ENV_FILE"))
        result = execute_pipeline_contract(
            contract,
            PipelineInvocation(
                run_id=run_id,
                pipeline_id=args.job_id,
                scheduled_for=scheduled,
                attempt=parsed_attempt,
                settings=settings,
                trade_date_override=trade_date,
                parameter_overrides=raw_parameters,
                audit_context={"runtime": "job_runtime"},
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
            print(f"Job error: {exc}", file=sys.stderr)
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
            print(f"Job error: {exc}", file=sys.stderr)
            return 2
    if args.command == "execute-contract":
        return _execute_contract_command(args)
    if args.command in {"validate-job-definitions", "list-job-definitions", "show-job-definition"}:
        try:
            jobs = load_and_validate_production_jobs(
                args.production_jobs or DEFAULT_PRODUCTION_JOBS_PATH
            )
            if args.command == "validate-job-definitions":
                print(f"valid production job definitions: {len(jobs)}")
                return 0
            by_id = {job.job_id: job for job in jobs}
            if args.command == "list-job-definitions":
                for job in jobs:
                    print(
                        json.dumps(
                            {
                                "job_id": job.job_id,
                                "pipeline_id": job.pipeline_id,
                                "enabled": job.enabled,
                                "schedule": job.schedule,
                                "timezone": job.timezone,
                                "parameters": dict(job.parameters),
                                "name": job.name,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    )
                return 0
            job = by_id.get(args.job_id)
            if job is None:
                raise DefinitionValidationError(f"unknown production job: {args.job_id}")
            print(
                json.dumps(
                    {
                        "job_id": job.job_id,
                        "pipeline_id": job.pipeline_id,
                        "enabled": job.enabled,
                        "schedule": job.schedule,
                        "timezone": job.timezone,
                        "parameters": dict(job.parameters),
                        "name": job.name,
                        "description": job.description,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        except (DefinitionValidationError, ContractValidationError, ValueError, KeyError) as exc:
            print(f"Job error: {exc}", file=sys.stderr)
            return 2
    if args.command in {"list", "show", "plan"}:
        try:
            if (
                args.definitions is not None
                or args.contract_selections is not None
                or getattr(args, "production_jobs", None) is not None
            ):
                definitions = _load_definitions_for_args(args, require_source=True)
                by_id = definitions_by_id(definitions)
                if args.command == "list":
                    for definition in definitions:
                        _print_definition(definition)
                    return 0
                definition = by_id.get(args.job_id)
                if definition is None:
                    raise DefinitionValidationError(f"unknown pipeline definition: {args.job_id}")
                if args.command == "show":
                    _print_definition(definition)
                    return 0
            else:
                definitions = _formal_runtime_definitions()
                by_id = definitions_by_id(definitions)
                if args.command == "list":
                    for definition in definitions:
                        _print_definition(definition)
                    return 0
                if args.command == "show":
                    definition = by_id.get(args.job_id)
                    if definition is None:
                        raise DefinitionValidationError(f"unknown Job definition: {args.job_id}")
                    _print_definition(definition)
                    return 0
                definition = by_id.get(args.job_id)
                if definition is None:
                    raise DefinitionValidationError(f"unknown Job definition: {args.job_id}")
            planned = dependency_plan(definitions, definition.job_id)
            for item in planned:
                payload: dict[str, object] = {
                    "job_id": item.job_id,
                    "definition_version": item.definition_version,
                    "dependencies": list(item.dependencies),
                    "enabled": item.enabled,
                    "schedule": item.schedule,
                    "timezone": item.timezone,
                    "manual_execution_allowed": item.manual_execution_allowed,
                }
                if args.target_date is not None:
                    payload["scheduled_at"] = scheduled_instant_for_target_date(item, args.target_date).isoformat()
                    payload["target_date"] = args.target_date.isoformat()
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0
        except (DefinitionValidationError, ContractValidationError, ValueError, KeyError) as exc:
            print(f"Job error: {exc}", file=sys.stderr)
            return 2
    try:
        paths = _runtime_paths(args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    store = JobRuntimeStore(paths.database_path)
    try:
        if args.command == "init":
            store.initialize()
            print(paths.database_path)
            return 0
        if args.command in {"validate-definitions", "list-definitions", "scan", "run-pending", "retry", "serve"}:
            definitions = (
                load_definitions(args.definitions)
                if args.command == "validate-definitions"
                else _load_definitions_for_args(
                    args,
                    require_source=args.command in {"run-pending", "retry", "serve"},
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
                            "job_id": definition.job_id,
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
            result = JobScheduler(
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
                if target.status is not JobStatus.PENDING:
                    _print_error("RUN_NOT_PENDING", detail=target.status.value)
                    return 2
            else:
                pending = store.list_runs(status=JobStatus.PENDING, limit=1_000)
                target = min(pending, key=lambda run: (run.scheduled_at, run.attempt), default=None)
                if target is None:
                    print(json.dumps({"status": "IDLE", "reason": "NO_PENDING_RUN"}, sort_keys=True))
                    return 0
            definition = definitions_by_id(definitions).get(target.job_id)
            if definition is None:
                _print_error("DEFINITION_MISSING", detail=target.job_id)
                return 2
            if definition.definition_version != target.definition_version:
                _print_error(
                    "DEFINITION_VERSION_MISMATCH",
                    detail=f"run={target.definition_version}, definition={definition.definition_version}",
                )
                return 2
            if store.has_active_service_lease():
                _print_run(target, submitted_to_service=True)
                return 0
            try:
                JobResultLog(paths.result_logs_dir).validate()
                result = JobRunner(
                    store,
                    paths,
                    heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                    lease_seconds=args.lease_seconds,
                ).run(target.run_id, definition)
            except JobClaimFailure as exc:
                _print_error(exc.code, detail=exc.detail)
                return 1
            _print_run(result)
            _write_result_log(paths, store, result)
            return 0 if result.status is JobStatus.SUCCESS else 1
        if args.command == "run":
            environment = _manual_run_environment(args)
            parameter_overrides = _parse_parameter_assignments(args.parameter_assignments)
            definitions = _definitions_for_manual_run(args, environment=environment)
            by_id = definitions_by_id(definitions)
            if args.job_id not in by_id:
                raise DefinitionValidationError(f"unknown formal pipeline: {args.job_id}")
            planned = dependency_plan(definitions, args.job_id, include_dependencies=args.with_dependencies)
            if any(not item.manual_execution_allowed for item in planned):
                denied = next(item.job_id for item in planned if not item.manual_execution_allowed)
                raise DefinitionValidationError(f"manual execution is disabled for {denied}")
            store.initialize()
            service_owns_execution = store.has_active_service_lease()
            if not service_owns_execution:
                JobResultLog(paths.result_logs_dir).validate()
            scheduler = JobScheduler(
                store,
                definitions,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_seconds=args.lease_seconds,
                stale_after_seconds=max(DEFAULT_STALE_AFTER_SECONDS, args.lease_seconds + 1),
            )
            exit_code = 0
            for definition in planned:
                scheduler.refresh_blocked_runs()
                scheduled_for = (
                    args.scheduled_for
                    # A manual request is due on submission.  The business
                    # date is persisted separately and resolved by the
                    # Contract; it must not make an active service wait for
                    # the Pipeline's cron occurrence.
                    or utc_now()
                )
                eligibility = scheduler.eligibility(definition, scheduled_for)
                target, _ = store.create_scheduled_run(
                    definition,
                    scheduled_at=scheduled_for,
                    trigger_type="MANUAL",
                    status=eligibility[0],
                    error_summary=eligibility[1],
                    trade_date_override=args.trade_date,
                    parameter_overrides=(
                        parameter_overrides if definition.job_id == args.job_id else {}
                    ),
                )
                if target.status is JobStatus.PENDING:
                    if not service_owns_execution:
                        result = JobRunner(
                            store,
                            paths,
                            heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                            lease_seconds=args.lease_seconds,
                        ).run(target.run_id, definition)
                        _write_result_log(paths, store, result)
                        target = result
                _print_run(
                    target,
                    result=store.get_result(target.run_id),
                    submitted_to_service=service_owns_execution and target.status is JobStatus.PENDING,
                )
                if not service_owns_execution and target.status is not JobStatus.SUCCESS:
                    exit_code = 1
            return exit_code
        if args.command == "status":
            status = JobStatus(args.status) if args.status else None
            if args.run_id is not None:
                run = store.get_run(args.run_id)
                if run is None:
                    _print_error("RUN_NOT_FOUND", detail=args.run_id)
                    return 2
                _print_run(run, result=store.get_result(run.run_id) if args.include_result else None)
                return 0
            for run in store.list_runs(job_id=args.job_id, status=status, limit=args.limit):
                _print_run(run, result=store.get_result(run.run_id) if args.include_result else None)
            return 0
        if args.command == "latest":
            run = next(iter(store.list_runs(job_id=args.job_id, limit=1)), None)
            if run is None:
                _print_error("RUN_NOT_FOUND", detail=args.job_id)
                return 2
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
            definition = definitions_by_id(definitions).get(previous.job_id)
            if definition is None or definition.definition_version != previous.definition_version:
                print("matching definition/version is required to retry", file=sys.stderr)
                return 2
            retry_run = store.retry_run(args.run_id, max_retries=definition.max_retries)
            if not args.execute:
                _print_run(retry_run)
                return 0
            if store.has_active_service_lease():
                _print_run(retry_run, submitted_to_service=True)
                return 0
            JobResultLog(paths.result_logs_dir).validate()
            result = JobRunner(
                store,
                paths,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                lease_seconds=DEFAULT_LEASE_SECONDS,
            ).run(retry_run.run_id, definition)
            _write_result_log(paths, store, result)
            _print_run(result, result=store.get_result(result.run_id))
            return 0 if result.status is JobStatus.SUCCESS else 1
        if args.command == "serve":
            service = JobService(
                store,
                paths,
                definitions,
                scheduler_id=args.scheduler_id,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                lease_seconds=args.lease_seconds,
                stale_after_seconds=args.stale_after_seconds,
                max_catch_up_minutes=args.max_catch_up_minutes,
                poll_interval_seconds=args.poll_interval_seconds,
                max_workers=args.max_workers,
                service_name=args.service_name,
            )
            if args.once:
                service.start()
                try:
                    cycle = service.run_once()
                finally:
                    service.stop()
                print(
                    json.dumps(
                        {
                            "status": "SERVED_ONCE",
                            "created_runs": len(cycle.created_runs),
                            "executed_runs": len(cycle.executed_runs),
                            "stale_runs_recovered": cycle.stale_runs_recovered,
                            "expired_locks_reclaimed": cycle.expired_locks_reclaimed,
                        },
                        sort_keys=True,
                    )
                )
                return 0 if all(run.status is JobStatus.SUCCESS for run in cycle.executed_runs) else 1

            def report_cycle_error(exc: Exception) -> None:
                _print_error("JOB_SCHEDULER_CYCLE_ERROR", detail=f"{type(exc).__name__}: {exc}")

            sigterm = getattr(signal, "SIGTERM", None)
            previous_sigterm = signal.getsignal(sigterm) if sigterm is not None else None
            if sigterm is not None:
                signal.signal(sigterm, lambda *_: service.request_stop())
            try:
                service.run_forever(on_cycle_error=report_cycle_error)
            except JobServiceFatalError as exc:
                _print_error("JOB_SCHEDULER_FATAL", detail=str(exc))
                return 1
            except KeyboardInterrupt:
                service.request_stop()
            finally:
                if sigterm is not None and previous_sigterm is not None:
                    signal.signal(sigterm, previous_sigterm)
            return 0
        if args.command == "health":
            now = utc_now()
            lease = store.get_service_lease(args.service_name)
            cursor = store.get_scheduler_cursor(args.scheduler_id)
            active = lease is not None and lease.lease_expires_at > now
            pending = len(store.list_runs(status=JobStatus.PENDING, limit=10_000))
            running = len(store.list_runs(status=JobStatus.RUNNING, limit=10_000))
            print(
                json.dumps(
                    {
                        "status": "HEALTHY" if active else "STOPPED",
                        "service_name": args.service_name,
                        "scheduler_id": args.scheduler_id,
                        "service_owner_id": lease.owner_id if lease else None,
                        "service_heartbeat_at": lease.heartbeat_at.isoformat() if lease else None,
                        "service_lease_expires_at": lease.lease_expires_at.isoformat() if lease else None,
                        "last_error": lease.last_error if lease else None,
                        "last_scanned_at": cursor.last_scanned_at.isoformat() if cursor else None,
                        "pending_runs": pending,
                        "running_runs": running,
                    },
                    sort_keys=True,
                )
            )
            return 0 if active else 1
    except (OSError, sqlite3.Error) as exc:
        _print_error("JOB_RUNTIME_UNAVAILABLE", detail=f"{type(exc).__name__}: {exc}")
        return 1
    except (
        DefinitionValidationError,
        ContractValidationError,
        ResultLogConfigurationError,
        JobServiceFatalError,
        JobClaimFailure,
        ValueError,
        KeyError,
    ) as exc:
        print(f"Job error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
