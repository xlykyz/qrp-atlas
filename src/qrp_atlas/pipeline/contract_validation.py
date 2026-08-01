"""Fail-closed validation for source-level Pipeline contracts."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable
from numbers import Integral, Real

from .contracts import (
    CompletionContract,
    ExecutionPolicy,
    FreshnessContract,
    IdempotencyContract,
    InputKind,
    InputContract,
    NonTradingDayPolicy,
    ParameterContract,
    ParameterType,
    PipelineContract,
    PipelineKind,
    PerformanceBudget,
    TransactionMode,
    TransactionContract,
    TargetDatePolicy,
    WriteMode,
    OutputContract,
    parse_parameter_overrides,
)
from qrp_atlas.orchestration.models import OverlapPolicy


class ContractValidationError(ValueError):
    """Raised when a Pipeline cannot be admitted to formal scheduling."""


PIPELINE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ERROR_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
DUCKDB_RESOURCE_PATTERN = re.compile(r"^duckdb://([^#\s]+)#([^#\s]+)$")

# These resource keys preserve the physical-DuckDB lock contract without embedding
# production paths in source or test fixtures.
MANAGED_WRITER_LOCKS: dict[str, str] = {
    "quant_db": "quant_db_writer",
    "system_b_episode_db": "system_b_episode_writer",
    "system_b_pools_db": "system_b_pools_writer",
}


def is_valid_error_code(value: object) -> bool:
    """Return whether a runtime or declared error code has the formal shape."""

    return isinstance(value, str) and ERROR_CODE_PATTERN.fullmatch(value) is not None


def _is_non_empty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_non_negative_integer(value: object) -> bool:
    if not isinstance(value, Integral) or isinstance(value, bool):
        return False
    try:
        if value < 0:
            return False
        json.dumps(value, allow_nan=False)
    except Exception:
        return False
    return True


def _is_finite_non_negative_number(value: object) -> bool:
    if not isinstance(value, Real) or isinstance(value, bool):
        return False
    try:
        if value < 0:
            return False
        json.dumps(value, allow_nan=False)
        numeric_value = float(value)
    except Exception:
        return False
    return math.isfinite(numeric_value)


def _validate_resource_values(
    values: object,
    *,
    field_name: str,
    prefix: str,
    reject_scoped_locks: bool,
) -> list[str]:
    errors: list[str] = []
    if not isinstance(values, tuple):
        return [prefix + f"{field_name} must be a tuple of strings"]
    seen: list[object] = []
    for resource in values:
        if any(resource == previous for previous in seen):
            errors.append(prefix + f"{field_name} must not contain duplicates")
        seen.append(resource)
        if not _is_non_empty_text(resource):
            errors.append(prefix + f"{field_name} must be non-empty strings")
            continue
        if resource.startswith("duckdb://"):
            if DUCKDB_RESOURCE_PATTERN.fullmatch(resource) is None:
                errors.append(prefix + f"{field_name} contains malformed DuckDB resource: {resource}")
            elif reject_scoped_locks:
                errors.append(
                    prefix
                    + "table-scoped DuckDB resources are read-only declarations; "
                    + "writes require a database-wide writer lock"
                )
    return errors


def _validate_declared_error_code(value: object, *, field_name: str, prefix: str) -> str | None:
    if not is_valid_error_code(value):
        return prefix + f"{field_name} must match ERROR_CODE_PATTERN"
    return None


def validate_contracts(contracts: Iterable[PipelineContract]) -> tuple[PipelineContract, ...]:
    """Validate identities, per-contract semantics, and the dependency DAG."""

    try:
        values = tuple(contracts)
    except Exception as exc:
        raise ContractValidationError("invalid formal Pipeline contracts: contracts must be iterable") from exc
    errors: list[str] = []
    valid_contracts: list[PipelineContract] = []
    for index, contract in enumerate(values):
        if not isinstance(contract, PipelineContract):
            errors.append(f"contract[{index}] must be a PipelineContract")
        else:
            valid_contracts.append(contract)
    ids = [contract.pipeline_id for contract in valid_contracts if isinstance(contract.pipeline_id, str)]
    if len(ids) != len(set(ids)):
        errors.append("pipeline_id values must be globally unique")
    known_ids = set(ids)
    for contract in valid_contracts:
        errors.extend(_validate_contract(contract, known_ids=known_ids))
    errors.extend(_validate_dependency_graph(tuple(valid_contracts)))
    if errors:
        raise ContractValidationError("invalid formal Pipeline contracts:\n- " + "\n- ".join(errors))
    return values


def _typed_tuple_items(
    value: object,
    *,
    field_name: str,
    expected_type: type,
    prefix: str,
) -> tuple[tuple[object, ...], list[str]]:
    if not isinstance(value, tuple):
        return (), [prefix + f"{field_name} must be a tuple"]
    errors: list[str] = []
    valid_items: list[object] = []
    for index, item in enumerate(value):
        if not isinstance(item, expected_type):
            errors.append(prefix + f"{field_name}[{index}] must be a {expected_type.__name__}")
        else:
            valid_items.append(item)
    return tuple(valid_items), errors


def _validate_contract(contract: PipelineContract, *, known_ids: set[str]) -> list[str]:
    pipeline_id = contract.pipeline_id if isinstance(contract.pipeline_id, str) else ""
    prefix = f"{pipeline_id or '<missing pipeline_id>'}: "
    errors: list[str] = []
    if not isinstance(contract.pipeline_id, str) or PIPELINE_ID_PATTERN.fullmatch(contract.pipeline_id) is None:
        errors.append(prefix + "pipeline_id must be stable snake_case")
    for field_name in ("name", "description", "contract_version"):
        if not _is_non_empty_text(getattr(contract, field_name, None)):
            errors.append(prefix + f"{field_name} must be non-empty")
    if not callable(contract.executor):
        errors.append(prefix + "executor must be callable")

    if not isinstance(contract.kind, PipelineKind):
        errors.append(prefix + "kind must be a PipelineKind")
    policy = contract.target_date_policy
    if not isinstance(policy, TargetDatePolicy):
        errors.append(prefix + "target_date_policy must be a TargetDatePolicy")
    else:
        for field_name in ("policy_id", "description", "trading_calendar_id"):
            if not _is_non_empty_text(getattr(policy, field_name, None)):
                errors.append(prefix + f"target date policy {field_name} must be non-empty")
        if not isinstance(policy.non_trading_day_policy, NonTradingDayPolicy):
            errors.append(prefix + "target date policy non_trading_day_policy must be a NonTradingDayPolicy")
        if not callable(policy.resolver) or not callable(policy.validate_explicit_date):
            errors.append(prefix + "target date policy must provide resolver and explicit-date validator")

    parameters, parameter_errors = _typed_tuple_items(
        contract.parameters,
        field_name="parameters",
        expected_type=ParameterContract,
        prefix=prefix,
    )
    inputs, input_errors = _typed_tuple_items(
        contract.inputs,
        field_name="inputs",
        expected_type=InputContract,
        prefix=prefix,
    )
    outputs, output_errors = _typed_tuple_items(
        contract.outputs,
        field_name="outputs",
        expected_type=OutputContract,
        prefix=prefix,
    )
    errors.extend(parameter_errors)
    errors.extend(input_errors)
    errors.extend(output_errors)
    parameter_names: set[str] = set()
    parameter_structure_valid = not parameter_errors
    for parameter in parameters:
        if (
            not isinstance(parameter.name, str)
            or PIPELINE_ID_PATTERN.fullmatch(parameter.name) is None
            or parameter.name in parameter_names
        ):
            errors.append(prefix + "parameter names must be unique snake_case")
        if isinstance(parameter.name, str):
            parameter_names.add(parameter.name)
        if not isinstance(parameter.parameter_type, ParameterType):
            errors.append(prefix + f"parameter {parameter.name} parameter_type must be a ParameterType")
            parameter_structure_valid = False
        if not isinstance(parameter.required, bool):
            errors.append(prefix + f"parameter {parameter.name} required must be a boolean")
            parameter_structure_valid = False
        if not _is_non_empty_text(parameter.description):
            errors.append(prefix + f"parameter {parameter.name} description must be non-empty")
        if parameter.required is True and parameter.default is not None:
            errors.append(prefix + f"required parameter {parameter.name} must not declare a default")
    if parameter_structure_valid:
        try:
            parse_parameter_overrides(contract, {})
        except ValueError as exc:
            if str(exc) != "REQUIRED_PARAMETER_MISSING":
                errors.append(prefix + f"parameter defaults are invalid: {exc}")
        except Exception as exc:
            errors.append(prefix + f"parameter defaults are invalid: {type(exc).__name__}")
    dependencies = contract.dependencies if isinstance(contract.dependencies, tuple) else ()
    if not isinstance(contract.dependencies, tuple):
        errors.append(prefix + "dependencies must be a tuple of strings")
    if len([item for item in dependencies if isinstance(item, str)]) != len(
        {item for item in dependencies if isinstance(item, str)}
    ):
        errors.append(prefix + "dependencies must not contain duplicates")
    if any(not isinstance(item, str) or not item.strip() for item in dependencies):
        errors.append(prefix + "dependencies must be non-empty strings")
    if contract.pipeline_id in dependencies:
        errors.append(prefix + "pipeline must not depend on itself")
    missing_dependencies = sorted({item for item in dependencies if isinstance(item, str)} - known_ids)
    if missing_dependencies:
        errors.append(prefix + "missing dependencies: " + ", ".join(missing_dependencies))
    errors.extend(
        _validate_resource_values(
            contract.resource_locks,
            field_name="resource_locks",
            prefix=prefix,
            reject_scoped_locks=True,
        )
    )
    errors.extend(
        _validate_resource_values(
            contract.resource_reads,
            field_name="resource_reads",
            prefix=prefix,
            reject_scoped_locks=False,
        )
    )
    resource_locks = contract.resource_locks if isinstance(contract.resource_locks, tuple) else ()
    resource_reads = contract.resource_reads if isinstance(contract.resource_reads, tuple) else ()
    if {item for item in resource_reads if isinstance(item, str)} & {
        item for item in resource_locks if isinstance(item, str)
    }:
        errors.append(prefix + "resource_reads must not overlap resource_locks")
    if not isinstance(contract.manual_execution_allowed, bool):
        errors.append(prefix + "manual_execution_allowed must be a boolean")

    execution = contract.execution
    if not isinstance(execution, ExecutionPolicy):
        errors.append(prefix + "execution must be an ExecutionPolicy")
    else:
        if not isinstance(execution.overlap_policy, OverlapPolicy):
            errors.append(prefix + "execution overlap_policy must be an OverlapPolicy")
        if not _is_non_negative_integer(execution.max_retries):
            errors.append(prefix + "max_retries must be a non-negative integer")

    input_ids: set[str] = set()
    for item in inputs:
        if not _is_non_empty_text(item.input_id) or item.input_id in input_ids:
            errors.append(prefix + "input_id values must be non-empty and unique")
        if isinstance(item.input_id, str):
            input_ids.add(item.input_id)
        if not isinstance(item.kind, InputKind):
            errors.append(prefix + f"input {item.input_id} kind must be an InputKind")
        for field_name in ("source", "target_date_semantics"):
            if not _is_non_empty_text(getattr(item, field_name, None)):
                errors.append(prefix + f"input {item.input_id} {field_name} must be non-empty")
        error = _validate_declared_error_code(
            item.missing_error_code,
            field_name=f"input {item.input_id} missing_error_code",
            prefix=prefix,
        )
        if error:
            errors.append(error)
        if (
            not isinstance(item.required_fields, tuple)
            or not item.required_fields
            or any(not _is_non_empty_text(field) for field in item.required_fields)
        ):
            errors.append(prefix + f"input {item.input_id} requires explicit minimum structure")
        if not callable(item.structure_check):
            errors.append(prefix + f"input {item.input_id} structure_check must be callable")
        freshness = item.freshness
        if not isinstance(freshness, FreshnessContract):
            errors.append(prefix + f"input {item.input_id} freshness must be a FreshnessContract")
        else:
            if not _is_non_negative_integer(freshness.maximum_lag_trading_days):
                errors.append(prefix + f"input {item.input_id} freshness lag must be non-negative")
            for field_name in ("check_id", "target_date_semantics"):
                if not _is_non_empty_text(getattr(freshness, field_name, None)):
                    errors.append(prefix + f"input {item.input_id} freshness {field_name} must be non-empty")
            if not isinstance(freshness.non_trading_day_policy, NonTradingDayPolicy):
                errors.append(
                    prefix
                    + f"input {item.input_id} freshness non_trading_day_policy must be a NonTradingDayPolicy"
                )
            error = _validate_declared_error_code(
                freshness.error_code,
                field_name=f"input {item.input_id} freshness error_code",
                prefix=prefix,
            )
            if error:
                errors.append(error)
            if not callable(freshness.checker):
                errors.append(prefix + f"input {item.input_id} freshness checker must be callable")
        upstream_pipeline_id = item.upstream_pipeline_id
        upstream_pipeline_id_valid = upstream_pipeline_id is None
        if upstream_pipeline_id is not None:
            if not isinstance(upstream_pipeline_id, str):
                errors.append(
                    prefix
                    + f"upstream input {item.input_id} upstream_pipeline_id must be a string or null"
                )
            elif PIPELINE_ID_PATTERN.fullmatch(upstream_pipeline_id) is None:
                errors.append(
                    prefix
                    + f"upstream input {item.input_id} upstream_pipeline_id must be a stable pipeline identifier"
                )
            else:
                upstream_pipeline_id_valid = True
        if item.kind is InputKind.UPSTREAM_PIPELINE:
            if upstream_pipeline_id is None:
                errors.append(prefix + f"upstream input {item.input_id} requires upstream_pipeline_id")
            elif upstream_pipeline_id_valid and upstream_pipeline_id not in dependencies:
                errors.append(prefix + f"upstream input {item.input_id} must be declared as a dependency")

    output_ids: set[str] = set()
    write_outputs: list[OutputContract] = []
    for item in outputs:
        if not _is_non_empty_text(item.output_id) or item.output_id in output_ids:
            errors.append(prefix + "output_id values must be non-empty and unique")
        if isinstance(item.output_id, str):
            output_ids.add(item.output_id)
        for field_name in ("physical_resource", "location", "object_name", "target_date_semantics"):
            if not _is_non_empty_text(getattr(item, field_name, None)):
                errors.append(prefix + f"output {item.output_id} {field_name} must be non-empty")
        write_mode_valid = isinstance(item.write_mode, WriteMode)
        if not write_mode_valid:
            errors.append(prefix + f"output {item.output_id} write_mode must be a WriteMode")
        is_write_output = write_mode_valid and item.write_mode is not WriteMode.READ_ONLY
        if is_write_output:
            write_outputs.append(item)
        if (
            not isinstance(item.unique_key, tuple)
            or any(not _is_non_empty_text(field) for field in item.unique_key)
            or (is_write_output and not item.unique_key)
        ):
            errors.append(prefix + f"output {item.output_id} unique_key must be a tuple of non-empty strings")
        if not isinstance(item.allow_empty, bool):
            errors.append(prefix + f"output {item.output_id} allow_empty must be a boolean")
        if is_write_output:
            expected_lock = (
                MANAGED_WRITER_LOCKS.get(item.physical_resource)
                if isinstance(item.physical_resource, str)
                else None
            )
            if (
                expected_lock is not None
                and expected_lock not in resource_locks
            ):
                errors.append(prefix + f"writes {item.physical_resource} and must declare {expected_lock}")
        completion = item.completion
        if not isinstance(completion, CompletionContract):
            errors.append(prefix + f"output {item.output_id} completion must be a CompletionContract")
        else:
            error = _validate_declared_error_code(
                completion.error_code,
                field_name=f"output {item.output_id} completion error_code",
                prefix=prefix,
            )
            if error:
                errors.append(error)
            if not _is_non_empty_text(completion.marker) or not callable(completion.checker):
                errors.append(prefix + f"output {item.output_id} requires an executable completion contract")
        quality_checks = item.quality_checks
        if not isinstance(quality_checks, tuple):
            errors.append(prefix + f"output {item.output_id} quality_checks must be a tuple")
        elif not quality_checks:
            errors.append(prefix + f"output {item.output_id} requires at least one quality check")
        elif any(not callable(check) for check in quality_checks):
            errors.append(prefix + f"output {item.output_id} quality checks must be callable")

    idempotency = contract.idempotency
    idempotency_valid = isinstance(idempotency, IdempotencyContract)
    if not idempotency_valid:
        errors.append(prefix + "idempotency must be an IdempotencyContract")
    else:
        if not isinstance(idempotency.uses_staging, bool):
            errors.append(prefix + "idempotency uses_staging must be a boolean")
        for field_name in (
            "idempotency_key",
            "repeat_run_semantics",
            "existing_target_handling",
            "failure_recovery",
            "atomic_replace_boundary",
        ):
            if not _is_non_empty_text(getattr(idempotency, field_name, None)):
                errors.append(prefix + f"idempotency {field_name} must be non-empty")

    transaction = contract.transaction
    transaction_valid = isinstance(transaction, TransactionContract)
    transaction_mode_valid = False
    if not transaction_valid:
        errors.append(prefix + "transaction must be a TransactionContract")
    else:
        transaction_mode_valid = isinstance(transaction.mode, TransactionMode)
        if not transaction_mode_valid:
            errors.append(prefix + "transaction mode must be a TransactionMode")
        for field_name in ("boundary", "failure_visibility"):
            if not _is_non_empty_text(getattr(transaction, field_name, None)):
                errors.append(prefix + f"transaction {field_name} must be non-empty")

    if write_outputs and not resource_locks:
        errors.append(prefix + "write pipelines require resource locks")
    if write_outputs and transaction_mode_valid and transaction.mode is TransactionMode.READ_ONLY:
        errors.append(prefix + "write pipelines cannot declare READ_ONLY transactions")
    if not write_outputs and transaction_mode_valid and transaction.mode is not TransactionMode.READ_ONLY:
        errors.append(prefix + "read-only pipelines must declare a READ_ONLY transaction")
    if (
        transaction_mode_valid
        and transaction.mode is TransactionMode.STAGING_ATOMIC_REPLACE
        and idempotency_valid
        and idempotency.uses_staging is not True
    ):
        errors.append(prefix + "staging transaction requires idempotency.uses_staging")

    performance = contract.performance
    if not isinstance(performance, PerformanceBudget):
        errors.append(prefix + "performance must be a PerformanceBudget")
    else:
        warning_threshold_valid = _is_finite_non_negative_number(performance.warning_threshold_seconds)
        if not warning_threshold_valid:
            errors.append(prefix + "performance warning_threshold_seconds must be finite and non-negative")
        elif performance.warning_threshold_seconds <= 0:
            errors.append(prefix + "performance warning_threshold_seconds must be positive")
        normal_budget_valid = _is_finite_non_negative_number(performance.normal_budget_seconds)
        if not normal_budget_valid:
            errors.append(prefix + "performance normal budget must be finite and non-negative")
        elif warning_threshold_valid and performance.normal_budget_seconds < performance.warning_threshold_seconds:
            errors.append(prefix + "performance normal budget must be at least its warning threshold")
        hard_timeout_valid = _is_non_negative_integer(performance.hard_timeout_seconds)
        if not hard_timeout_valid:
            errors.append(prefix + "performance timeout must be a non-negative integer")
        elif normal_budget_valid and performance.hard_timeout_seconds < performance.normal_budget_seconds:
            errors.append(prefix + "performance timeout must be at least its normal budget")
        for field_name in ("benchmark_scope", "baseline_source"):
            value = getattr(performance, field_name, None)
            if not _is_non_empty_text(value) or "guess" in value.lower():
                errors.append(prefix + f"performance {field_name} must cite measured evidence")

    if isinstance(contract.kind, PipelineKind) and contract.kind is PipelineKind.DAG:
        if not contract.dependencies:
            errors.append(prefix + "top-level DAG requires dependencies")
        if write_outputs:
            errors.append(prefix + "top-level DAG must aggregate dependencies, not duplicate writes")
    return errors


def _validate_dependency_graph(contracts: tuple[PipelineContract, ...]) -> list[str]:
    graph = {
        item.pipeline_id: tuple(
            dependency for dependency in item.dependencies if isinstance(dependency, str)
        )
        for item in contracts
        if isinstance(item.pipeline_id, str) and isinstance(item.dependencies, tuple)
    }
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
