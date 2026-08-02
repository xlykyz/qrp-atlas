"""Adapt formal source contracts to the existing runtime definitions.

The deployment manifest represented here intentionally contains only pipeline
selection, enablement, and cron schedule.  It cannot override business semantics.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import PipelineContract, PipelineInvocation
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import PipelineRegistry, default_registry

from qrp_atlas.orchestration.cron import CronExpression, CronExpressionError
from qrp_atlas.orchestration.definitions import DefinitionValidationError
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.orchestration.models import JobDefinition, JobExecutionResult, JobRun, JobStatus


DEFAULT_PIPELINE_TIMEZONE = "Asia/Shanghai"


@dataclass(frozen=True, slots=True)
class ContractDeploymentSelection:
    pipeline_id: str
    enabled: bool
    schedule: str


def load_contract_selections(path: str | Path) -> tuple[ContractDeploymentSelection, ...]:
    """Load a deployment-only manifest with no business arguments or paths."""

    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DefinitionValidationError(f"cannot read contract selections {manifest_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise DefinitionValidationError(f"invalid JSON in contract selections {manifest_path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise DefinitionValidationError("contract selection schema_version must be 1")
    entries = payload.get("pipelines")
    if not isinstance(entries, list):
        raise DefinitionValidationError("contract selections pipelines must be a list")
    result: list[ContractDeploymentSelection] = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"pipeline_id", "enabled", "schedule"}:
            raise DefinitionValidationError(
                "each contract deployment selection must contain only pipeline_id, enabled, schedule"
            )
        pipeline_id = entry["pipeline_id"]
        enabled = entry["enabled"]
        schedule = entry["schedule"]
        if not isinstance(pipeline_id, str) or not pipeline_id.strip():
            raise DefinitionValidationError("selection pipeline_id must be a non-empty string")
        if not isinstance(enabled, bool):
            raise DefinitionValidationError("selection enabled must be a boolean")
        if not isinstance(schedule, str) or not schedule.strip():
            raise DefinitionValidationError("selection schedule must be a non-empty string")
        try:
            CronExpression.parse(schedule)
        except CronExpressionError as exc:
            raise DefinitionValidationError(f"invalid schedule for {pipeline_id}: {exc}") from exc
        result.append(ContractDeploymentSelection(pipeline_id.strip(), enabled, schedule.strip()))
    ids = [item.pipeline_id for item in result]
    if len(ids) != len(set(ids)):
        raise DefinitionValidationError("contract deployment selection pipeline_id values must be unique")
    return tuple(result)


def definitions_from_contract_selections(
    selections: Iterable[ContractDeploymentSelection],
    *,
    registry: PipelineRegistry | None = None,
    timezone: str = DEFAULT_PIPELINE_TIMEZONE,
) -> tuple[JobDefinition, ...]:
    """Create existing runtime definitions without accepting business overrides."""

    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise DefinitionValidationError(f"invalid pipeline timezone: {timezone}") from exc
    effective_registry = registry or default_registry()
    try:
        validate_contracts(effective_registry.all())
    except ContractValidationError as exc:
        raise DefinitionValidationError(str(exc)) from exc
    definitions: list[JobDefinition] = []
    for selection in selections:
        try:
            contract = effective_registry.get(selection.pipeline_id)
        except KeyError as exc:
            raise DefinitionValidationError(f"selection references unknown formal pipeline: {selection.pipeline_id}") from exc
        definitions.append(contract_runtime_definition(contract, selection, timezone=timezone))
    return tuple(definitions)


def contract_runtime_definition(
    contract: PipelineContract,
    selection: ContractDeploymentSelection,
    *,
    timezone: str = DEFAULT_PIPELINE_TIMEZONE,
    environment: dict[str, str] | None = None,
) -> JobDefinition:
    """Map source-owned semantics onto an in-process formal runtime definition."""

    return JobDefinition(
        job_id=contract.pipeline_id,
        name=contract.name,
        enabled=selection.enabled,
        schedule=selection.schedule,
        timezone=timezone,
        command=(
            sys.executable,
            "-m",
            "qrp_atlas.jobs_cli",
            "execute-contract",
            contract.pipeline_id,
        ),
        working_directory=None,
        dependencies=contract.dependencies,
        timeout_seconds=contract.performance.hard_timeout_seconds,
        max_retries=contract.execution.max_retries,
        overlap_policy=contract.execution.overlap_policy,
        resource_locks=contract.resource_locks,
        resource_reads=contract.resource_reads,
        performance_budget={
            "normal_budget_seconds": contract.performance.normal_budget_seconds,
            "warning_threshold_seconds": contract.performance.warning_threshold_seconds,
            "baseline_source": contract.performance.baseline_source,
        },
        freshness_checks=tuple(
            {
                "input_id": item.input_id,
                "check_id": item.freshness.check_id,
                "maximum_lag_trading_days": item.freshness.maximum_lag_trading_days,
            }
            for item in contract.inputs
        ),
        definition_version=contract.contract_version,
        inherit_environment=True,
        environment=environment or {},
        requires_structured_result=True,
        manual_execution_allowed=contract.manual_execution_allowed,
        in_process_executor=make_in_process_contract_executor(contract, environment=environment),
    )


def make_in_process_contract_executor(
    contract: PipelineContract,
    *,
    environment: dict[str, str] | None = None,
):
    """Build the callback used by ``pipeline serve`` for formal Contracts.

    The callback intentionally receives the claimed runtime record rather than
    opening a second runtime process.  Business code can therefore create its
    own DuckDB connection while remaining inside the single serving process.
    """

    configured_environment = dict(environment or {})

    def execute(claimed: JobRun) -> Any:
        if not isinstance(claimed.execution_control, ExecutionControl):
            raise TypeError("claimed JobRun must carry the runner-provided ExecutionControl")
        execution_control = claimed.execution_control
        process_environment = os.environ.copy()
        process_environment.update(configured_environment)
        settings = AppSettings.load(
            env_file=process_environment.get("QRP_ENV_FILE"),
            environ=process_environment,
        )
        invocation = PipelineInvocation(
            run_id=claimed.run_id,
            pipeline_id=contract.pipeline_id,
            scheduled_for=claimed.scheduled_at,
            attempt=claimed.attempt,
            settings=settings,
            trade_date_override=claimed.trade_date_override,
            parameter_overrides=claimed.parameter_overrides,
            audit_context={"runtime": "job_runtime", "execution_mode": "in_process"},
            execution_control=execution_control,
        )
        result = execute_pipeline_contract(contract, invocation)
        payload = result.as_dict()
        error_summary = None
        if result.status.value == "FAILED":
            diagnostics = payload.get("diagnostics")
            if isinstance(diagnostics, list):
                for diagnostic in diagnostics:
                    if isinstance(diagnostic, dict):
                        code = diagnostic.get("code")
                        message = diagnostic.get("message")
                        if isinstance(code, str) and isinstance(message, str):
                            error_summary = f"{code}: {message}"[:500]
                            break
            error_summary = error_summary or "Pipeline Contract returned FAILED"
        return JobExecutionResult(
            status=(
                JobStatus.SUCCESS
                if result.status.value in {"SUCCESS", "NOOP"}
                else JobStatus.FAILED
            ),
            payload=payload,
            error_summary=error_summary,
        )

    return execute


def runtime_definition_from_production_job(
    job: Any,
    contract: PipelineContract,
    *,
    environment: dict[str, str] | None = None,
    dependency_job_ids: tuple[str, ...] | None = None,
) -> JobDefinition:
    """Map one production job instance onto the existing runtime definition.

    The instance contributes only identity (``job_id``), the referenced
    ``pipeline_id``, enablement, schedule, timezone, fixed parameters and
    display name.  Every business rule (locks, dependencies, outputs,
    transaction, timeout, retry, completion, quality, date policy) comes
    from the referenced Contract.

    ``dependency_job_ids`` carries the instance-resolved dependencies: the
    Contract declares business dependencies as pipeline_id values, but the
    runtime Scheduler and dependency plans query by job_id.  When omitted,
    the raw Contract dependencies are kept (compatible fallback).
    """

    return JobDefinition(
        job_id=job.job_id,
        pipeline_id=contract.pipeline_id,
        name=job.name or contract.name,
        enabled=job.enabled,
        schedule=job.schedule,
        timezone=job.timezone,
        command=(
            sys.executable,
            "-m",
            "qrp_atlas.jobs_cli",
            "execute-contract",
            job.job_id,
        ),
        working_directory=None,
        dependencies=(
            dependency_job_ids if dependency_job_ids is not None else contract.dependencies
        ),
        timeout_seconds=contract.performance.hard_timeout_seconds,
        max_retries=contract.execution.max_retries,
        overlap_policy=contract.execution.overlap_policy,
        resource_locks=contract.resource_locks,
        resource_reads=contract.resource_reads,
        performance_budget={
            "normal_budget_seconds": contract.performance.normal_budget_seconds,
            "warning_threshold_seconds": contract.performance.warning_threshold_seconds,
            "baseline_source": contract.performance.baseline_source,
        },
        freshness_checks=tuple(
            {
                "input_id": item.input_id,
                "check_id": item.freshness.check_id,
                "maximum_lag_trading_days": item.freshness.maximum_lag_trading_days,
            }
            for item in contract.inputs
        ),
        definition_version=contract.contract_version,
        inherit_environment=True,
        environment=environment or {},
        requires_structured_result=True,
        manual_execution_allowed=contract.manual_execution_allowed,
        fixed_parameters=dict(job.parameters),
        in_process_executor=make_in_process_contract_executor(
            contract,
            environment=environment,
        ),
    )
