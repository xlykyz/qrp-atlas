"""Adapt formal source contracts to the existing runtime definitions.

The deployment manifest represented here intentionally contains only pipeline
selection, enablement, and cron schedule.  It cannot override business semantics.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import PipelineContract, PipelineInvocation
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import PipelineRegistry, default_registry

from .cron import CronExpression, CronExpressionError
from .definitions import DefinitionValidationError
from .models import PipelineDefinition, PipelineRun


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
) -> tuple[PipelineDefinition, ...]:
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
    definitions: list[PipelineDefinition] = []
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
) -> PipelineDefinition:
    """Map source-owned semantics onto an in-process formal runtime definition."""

    return PipelineDefinition(
        pipeline_id=contract.pipeline_id,
        name=contract.name,
        enabled=selection.enabled,
        schedule=selection.schedule,
        timezone=timezone,
        command=(
            sys.executable,
            "-m",
            "qrp_atlas.pipeline.runtime",
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

    def execute(claimed: PipelineRun) -> Any:
        process_environment = os.environ.copy()
        process_environment.update(configured_environment)
        raw_parameters = json.loads(process_environment.get("QRP_PIPELINE_PARAMETER_OVERRIDES", "{}"))
        if not isinstance(raw_parameters, dict) or any(not isinstance(key, str) for key in raw_parameters):
            raise ValueError("INVALID_PARAMETER_ASSIGNMENT")
        trade_date_raw = process_environment.get("QRP_PIPELINE_TRADE_DATE")
        trade_date = date.fromisoformat(trade_date_raw) if trade_date_raw else None
        settings = AppSettings.load(
            env_file=process_environment.get("QRP_ENV_FILE"),
            environ=process_environment,
        )
        invocation = PipelineInvocation(
            run_id=claimed.run_id,
            pipeline_id=claimed.pipeline_id,
            scheduled_for=claimed.scheduled_at,
            attempt=claimed.attempt,
            settings=settings,
            trade_date_override=trade_date,
            parameter_overrides=raw_parameters,
            audit_context={"runtime": "pipeline_runtime", "execution_mode": "in_process"},
        )
        return execute_pipeline_contract(contract, invocation)

    return execute
