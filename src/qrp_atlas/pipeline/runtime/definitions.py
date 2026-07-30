"""Loading and validation for Git-versioned Pipeline definition manifests."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .cron import CronExpression, CronExpressionError
from .models import OverlapPolicy, PipelineDefinition


class DefinitionValidationError(ValueError):
    """Raised when a definition manifest is not safe to schedule."""


DEFAULT_DEFINITIONS_PATH = Path(__file__).with_name("default_definitions.json")


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DefinitionValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise DefinitionValidationError(f"{field_name} must be a list of non-empty strings")
    return tuple(value)


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise DefinitionValidationError(f"{field_name} must be an object")
    return value


def parse_definition(payload: Mapping[str, Any]) -> PipelineDefinition:
    """Parse one JSON definition without resolving relative working directories."""

    pipeline_id = _required_string(payload.get("pipeline_id"), "pipeline_id")
    name = _required_string(payload.get("name"), "name")
    if not isinstance(payload.get("enabled"), bool):
        raise DefinitionValidationError("enabled must be a boolean")
    schedule = _required_string(payload.get("schedule"), "schedule")
    try:
        CronExpression.parse(schedule)
    except CronExpressionError as exc:
        raise DefinitionValidationError(f"invalid schedule for {pipeline_id}: {exc}") from exc
    timezone = _required_string(payload.get("timezone"), "timezone")
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise DefinitionValidationError(f"invalid timezone for {pipeline_id}: {timezone}") from exc
    command = _string_tuple(payload.get("command"), "command")
    working_directory_value = payload.get("working_directory")
    if working_directory_value is not None and not isinstance(working_directory_value, str):
        raise DefinitionValidationError("working_directory must be a string or null")
    working_directory = Path(working_directory_value) if working_directory_value else None
    if working_directory is not None and not working_directory.is_absolute():
        raise DefinitionValidationError("working_directory must be an absolute path")
    dependencies = _string_tuple(payload.get("dependencies", []), "dependencies")
    timeout_seconds = payload.get("timeout_seconds")
    if timeout_seconds is not None and (not isinstance(timeout_seconds, int) or timeout_seconds <= 0):
        raise DefinitionValidationError("timeout_seconds must be a positive integer or null")
    max_retries = payload.get("max_retries", 0)
    if not isinstance(max_retries, int) or max_retries < 0:
        raise DefinitionValidationError("max_retries must be a non-negative integer")
    try:
        overlap_policy = OverlapPolicy(payload.get("overlap_policy", OverlapPolicy.FORBID))
    except ValueError as exc:
        raise DefinitionValidationError("overlap_policy must be FORBID or ALLOW") from exc
    resource_locks = _string_tuple(payload.get("resource_locks", []), "resource_locks")
    resource_reads = _string_tuple(payload.get("resource_reads", []), "resource_reads")
    if set(resource_locks) & set(resource_reads):
        raise DefinitionValidationError("resource_reads must not overlap resource_locks")
    freshness_checks_raw = payload.get("freshness_checks", [])
    if not isinstance(freshness_checks_raw, list) or any(
        not isinstance(item, dict) for item in freshness_checks_raw
    ):
        raise DefinitionValidationError("freshness_checks must be a list of objects")
    inherit_environment = payload.get("inherit_environment", False)
    if not isinstance(inherit_environment, bool):
        raise DefinitionValidationError("inherit_environment must be a boolean")
    environment = _mapping(payload.get("environment", {}), "environment")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in environment.items()):
        raise DefinitionValidationError("environment must map strings to strings")
    requires_structured_result = payload.get("requires_structured_result", False)
    if not isinstance(requires_structured_result, bool):
        raise DefinitionValidationError("requires_structured_result must be a boolean")
    manual_execution_allowed = payload.get("manual_execution_allowed", True)
    if not isinstance(manual_execution_allowed, bool):
        raise DefinitionValidationError("manual_execution_allowed must be a boolean")
    return PipelineDefinition(
        pipeline_id=pipeline_id,
        name=name,
        enabled=payload["enabled"],
        schedule=schedule,
        timezone=timezone,
        command=command,
        working_directory=working_directory,
        dependencies=dependencies,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        overlap_policy=overlap_policy,
        resource_locks=resource_locks,
        resource_reads=resource_reads,
        performance_budget=_mapping(payload.get("performance_budget", {}), "performance_budget"),
        freshness_checks=tuple(freshness_checks_raw),
        definition_version=_required_string(payload.get("definition_version", "1"), "definition_version"),
        inherit_environment=inherit_environment,
        environment=environment,
        requires_structured_result=requires_structured_result,
        manual_execution_allowed=manual_execution_allowed,
    )


def load_definitions(path: str | Path = DEFAULT_DEFINITIONS_PATH) -> tuple[PipelineDefinition, ...]:
    """Load a repository-controlled JSON manifest and validate cross references."""

    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DefinitionValidationError(f"cannot read definitions file {manifest_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise DefinitionValidationError(f"invalid JSON in {manifest_path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise DefinitionValidationError("manifest schema_version must be 1")
    entries = payload.get("definitions")
    if not isinstance(entries, list):
        raise DefinitionValidationError("manifest definitions must be a list")
    definitions = tuple(parse_definition(entry) for entry in entries if isinstance(entry, dict))
    if len(definitions) != len(entries):
        raise DefinitionValidationError("every definition must be an object")
    ids = {definition.pipeline_id for definition in definitions}
    if len(ids) != len(definitions):
        raise DefinitionValidationError("pipeline_id values must be unique")
    for definition in definitions:
        missing = set(definition.dependencies) - ids
        if missing:
            raise DefinitionValidationError(
                f"{definition.pipeline_id} references missing dependencies: {', '.join(sorted(missing))}"
            )
    _validate_acyclic_dependencies(definitions)
    return definitions


def _validate_acyclic_dependencies(definitions: tuple[PipelineDefinition, ...]) -> None:
    """Reject every directed dependency cycle with a readable cycle path."""

    graph = {definition.pipeline_id: definition.dependencies for definition in definitions}
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(pipeline_id: str) -> None:
        if pipeline_id in visiting:
            cycle_start = path.index(pipeline_id)
            cycle = path[cycle_start:] + [pipeline_id]
            raise DefinitionValidationError(f"pipeline dependency cycle detected: {' -> '.join(cycle)}")
        if pipeline_id in visited:
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


def definitions_by_id(definitions: tuple[PipelineDefinition, ...]) -> dict[str, PipelineDefinition]:
    return {definition.pipeline_id: definition for definition in definitions}
