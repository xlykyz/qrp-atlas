"""Adapt formal source contracts to the existing runtime definitions.

The deployment manifest represented here intentionally contains only pipeline
selection, enablement, and cron schedule.  It cannot override business semantics.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import PipelineContract
from qrp_atlas.pipeline.registry import PipelineRegistry, default_registry

from .cron import CronExpression, CronExpressionError
from .definitions import DefinitionValidationError
from .models import PipelineDefinition


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
    """Map source-owned semantics onto the existing process runner definition."""

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
    )
