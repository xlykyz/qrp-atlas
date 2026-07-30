"""Fail-closed validation for source-level Pipeline contracts."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable

from .contracts import (
    InputKind,
    PipelineContract,
    PipelineKind,
    TransactionMode,
    WriteMode,
    parse_parameter_overrides,
)


class ContractValidationError(ValueError):
    """Raised when a Pipeline cannot be admitted to formal scheduling."""


PIPELINE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ERROR_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")

# These resource keys preserve the physical-DuckDB lock contract without embedding
# production paths in source or test fixtures.
MANAGED_WRITER_LOCKS: dict[str, str] = {
    "quant_db": "quant_db_writer",
    "system_b_episode_db": "system_b_episode_writer",
    "system_b_pools_db": "system_b_pools_writer",
}


def validate_contracts(contracts: Iterable[PipelineContract]) -> tuple[PipelineContract, ...]:
    """Validate identities, per-contract semantics, and the dependency DAG."""

    values = tuple(contracts)
    errors: list[str] = []
    ids = [contract.pipeline_id for contract in values]
    if len(ids) != len(set(ids)):
        errors.append("pipeline_id values must be globally unique")
    for contract in values:
        errors.extend(_validate_contract(contract, known_ids=set(ids)))
    errors.extend(_validate_dependency_graph(values))
    if errors:
        raise ContractValidationError("invalid formal Pipeline contracts:\n- " + "\n- ".join(errors))
    return values


def _validate_contract(contract: PipelineContract, *, known_ids: set[str]) -> list[str]:
    prefix = f"{contract.pipeline_id or '<missing pipeline_id>'}: "
    errors: list[str] = []
    if not PIPELINE_ID_PATTERN.fullmatch(contract.pipeline_id):
        errors.append(prefix + "pipeline_id must be stable snake_case")
    for field_name in ("name", "description", "contract_version"):
        if not getattr(contract, field_name).strip():
            errors.append(prefix + f"{field_name} must be non-empty")
    if not callable(contract.executor):
        errors.append(prefix + "executor must be callable")
    policy = contract.target_date_policy
    for field_name in ("policy_id", "description", "trading_calendar_id"):
        if not getattr(policy, field_name).strip():
            errors.append(prefix + f"target date policy {field_name} must be non-empty")
    if not callable(policy.resolver) or not callable(policy.validate_explicit_date):
        errors.append(prefix + "target date policy must provide resolver and explicit-date validator")
    parameter_names: set[str] = set()
    for parameter in contract.parameters:
        if not PIPELINE_ID_PATTERN.fullmatch(parameter.name) or parameter.name in parameter_names:
            errors.append(prefix + "parameter names must be unique snake_case")
        parameter_names.add(parameter.name)
        if not parameter.description.strip():
            errors.append(prefix + f"parameter {parameter.name} description must be non-empty")
        if parameter.required and parameter.default is not None:
            errors.append(prefix + f"required parameter {parameter.name} must not declare a default")
    try:
        parse_parameter_overrides(contract, {})
    except ValueError as exc:
        if str(exc) != "REQUIRED_PARAMETER_MISSING":
            errors.append(prefix + f"parameter defaults are invalid: {exc}")
    if len(contract.dependencies) != len(set(contract.dependencies)):
        errors.append(prefix + "dependencies must not contain duplicates")
    if contract.pipeline_id in contract.dependencies:
        errors.append(prefix + "pipeline must not depend on itself")
    missing_dependencies = sorted(set(contract.dependencies) - known_ids)
    if missing_dependencies:
        errors.append(prefix + "missing dependencies: " + ", ".join(missing_dependencies))
    if len(contract.resource_locks) != len(set(contract.resource_locks)):
        errors.append(prefix + "resource_locks must not contain duplicates")
    if any(not item.strip() for item in contract.resource_locks):
        errors.append(prefix + "resource_locks must be non-empty strings")
    if len(contract.resource_reads) != len(set(contract.resource_reads)):
        errors.append(prefix + "resource_reads must not contain duplicates")
    if any(not item.strip() for item in contract.resource_reads):
        errors.append(prefix + "resource_reads must be non-empty strings")
    if set(contract.resource_reads) & set(contract.resource_locks):
        errors.append(prefix + "resource_reads must not overlap resource_locks")
    if contract.execution.max_retries < 0:
        errors.append(prefix + "max_retries must be non-negative")
    if not isinstance(contract.manual_execution_allowed, bool):
        errors.append(prefix + "manual_execution_allowed must be a boolean")

    input_ids: set[str] = set()
    for item in contract.inputs:
        if not item.input_id.strip() or item.input_id in input_ids:
            errors.append(prefix + "input_id values must be non-empty and unique")
        input_ids.add(item.input_id)
        for field_name in ("source", "target_date_semantics", "missing_error_code"):
            if not getattr(item, field_name).strip():
                errors.append(prefix + f"input {item.input_id} {field_name} must be non-empty")
        if not item.required_fields or any(not field.strip() for field in item.required_fields):
            errors.append(prefix + f"input {item.input_id} requires explicit minimum structure")
        if not callable(item.structure_check):
            errors.append(prefix + f"input {item.input_id} structure_check must be callable")
        freshness = item.freshness
        if freshness.maximum_lag_trading_days < 0:
            errors.append(prefix + f"input {item.input_id} freshness lag must be non-negative")
        for field_name in ("check_id", "target_date_semantics", "error_code"):
            if not getattr(freshness, field_name).strip():
                errors.append(prefix + f"input {item.input_id} freshness {field_name} must be non-empty")
        if not callable(freshness.checker):
            errors.append(prefix + f"input {item.input_id} freshness checker must be callable")
        if item.kind is InputKind.UPSTREAM_PIPELINE:
            if not item.upstream_pipeline_id:
                errors.append(prefix + f"upstream input {item.input_id} requires upstream_pipeline_id")
            elif item.upstream_pipeline_id not in contract.dependencies:
                errors.append(prefix + f"upstream input {item.input_id} must be declared as a dependency")

    output_ids: set[str] = set()
    write_outputs = []
    for item in contract.outputs:
        if not item.output_id.strip() or item.output_id in output_ids:
            errors.append(prefix + "output_id values must be non-empty and unique")
        output_ids.add(item.output_id)
        for field_name in ("physical_resource", "location", "object_name", "target_date_semantics"):
            if not getattr(item, field_name).strip():
                errors.append(prefix + f"output {item.output_id} {field_name} must be non-empty")
        if item.write_mode is not WriteMode.READ_ONLY:
            write_outputs.append(item)
            if not item.unique_key or any(not field.strip() for field in item.unique_key):
                errors.append(prefix + f"write output {item.output_id} requires a logical unique key")
            expected_lock = MANAGED_WRITER_LOCKS.get(item.physical_resource)
            if expected_lock is not None and expected_lock not in contract.resource_locks:
                errors.append(prefix + f"writes {item.physical_resource} and must declare {expected_lock}")
        completion = item.completion
        if not completion.marker.strip() or not completion.error_code.strip() or not callable(completion.checker):
            errors.append(prefix + f"output {item.output_id} requires an executable completion contract")
        if not item.quality_checks:
            errors.append(prefix + f"output {item.output_id} requires at least one quality check")
        elif any(not callable(check) for check in item.quality_checks):
            errors.append(prefix + f"output {item.output_id} quality checks must be callable")

    if write_outputs and not contract.resource_locks:
        errors.append(prefix + "write pipelines require resource locks")
    if write_outputs and contract.transaction.mode is TransactionMode.READ_ONLY:
        errors.append(prefix + "write pipelines cannot declare READ_ONLY transactions")
    if not write_outputs and contract.transaction.mode is not TransactionMode.READ_ONLY:
        errors.append(prefix + "read-only pipelines must declare a READ_ONLY transaction")
    if contract.transaction.mode is TransactionMode.STAGING_ATOMIC_REPLACE and not contract.idempotency.uses_staging:
        errors.append(prefix + "staging transaction requires idempotency.uses_staging")
    for field_name in (
        "idempotency_key",
        "repeat_run_semantics",
        "existing_target_handling",
        "failure_recovery",
        "atomic_replace_boundary",
    ):
        if not getattr(contract.idempotency, field_name).strip():
            errors.append(prefix + f"idempotency {field_name} must be non-empty")
    for field_name in ("boundary", "failure_visibility"):
        if not getattr(contract.transaction, field_name).strip():
            errors.append(prefix + f"transaction {field_name} must be non-empty")

    performance = contract.performance
    if performance.warning_threshold_seconds <= 0:
        errors.append(prefix + "performance warning_threshold_seconds must be positive")
    if performance.normal_budget_seconds < performance.warning_threshold_seconds:
        errors.append(prefix + "performance normal budget must be at least its warning threshold")
    if performance.hard_timeout_seconds < performance.normal_budget_seconds:
        errors.append(prefix + "performance timeout must be at least its normal budget")
    for field_name in ("benchmark_scope", "baseline_source"):
        value = getattr(performance, field_name)
        if not value.strip() or "guess" in value.lower():
            errors.append(prefix + f"performance {field_name} must cite measured evidence")

    if contract.kind is PipelineKind.DAG:
        if not contract.dependencies:
            errors.append(prefix + "top-level DAG requires dependencies")
        if write_outputs:
            errors.append(prefix + "top-level DAG must aggregate dependencies, not duplicate writes")
    return errors


def _validate_dependency_graph(contracts: tuple[PipelineContract, ...]) -> list[str]:
    graph = {item.pipeline_id: item.dependencies for item in contracts}
    errors: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(pipeline_id: str) -> None:
        if pipeline_id in visiting:
            start = path.index(pipeline_id)
            errors.append("dependency cycle: " + " -> ".join(path[start:] + [pipeline_id]))
            return
        if pipeline_id in visited or pipeline_id not in graph:
            return
        visiting.add(pipeline_id)
        path.append(pipeline_id)
        for dependency_id in graph[pipeline_id]:
            visit(dependency_id)
        path.pop()
        visiting.remove(pipeline_id)
        visited.add(pipeline_id)

    for pipeline_id in graph:
        visit(pipeline_id)
    return errors
