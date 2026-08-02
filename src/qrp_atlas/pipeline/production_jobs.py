"""Production JobDefinition (job instance) layer for formal Pipeline contracts.

A ``PipelineContract`` is the single source of business semantics (inputs,
outputs, target-date policy, parameters, dependencies, resource locks,
transaction, overlap, idempotency, completion, quality, timeout, retry,
performance budget, executor).  A production JobDefinition is only a
scheduling instance: stable ``job_id``, the referenced ``pipeline_id``,
enablement, schedule, explicit timezone, fixed parameters, and optional
display information.  One Contract may be referenced by many job instances;
every business rule is resolved from the referenced Contract at runtime.

The manifest lives in the repository deployment area, is version-controlled,
contains no credentials and no machine-specific absolute paths, and every
example is disabled.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from qrp_atlas.orchestration.cron import CronExpression, CronExpressionError
from qrp_atlas.orchestration.definitions import DefinitionValidationError
from qrp_atlas.pipeline.contracts import ContractError, parse_parameter_overrides
from qrp_atlas.pipeline.registry import PipelineRegistry, default_registry

DEFAULT_PRODUCTION_JOBS_PATH = Path("deploy/pipeline/production-job-definitions.json")


@dataclass(frozen=True, slots=True)
class ProductionJobDefinition:
    """A disabled-by-default scheduling instance referencing one Contract."""

    job_id: str
    pipeline_id: str
    enabled: bool
    schedule: str
    timezone: str
    parameters: Mapping[str, str] = field(default_factory=dict)
    name: str | None = None
    description: str | None = None


def load_production_jobs(
    path: str | Path = DEFAULT_PRODUCTION_JOBS_PATH,
) -> tuple[ProductionJobDefinition, ...]:
    """Load and structurally validate the production job manifest.

    Structure-only checks (types, uniqueness, timezone, cron) run here;
    Contract cross-checks (pipeline existence, fixed-parameter validation)
    run in :func:`validate_production_jobs`.
    """

    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DefinitionValidationError(
            f"cannot read production job definitions {manifest_path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise DefinitionValidationError(
            f"invalid JSON in production job definitions {manifest_path}: {exc}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise DefinitionValidationError("production job definition schema_version must be 1")
    entries = payload.get("jobs")
    if not isinstance(entries, list):
        raise DefinitionValidationError("production job definitions jobs must be a list")

    jobs: list[ProductionJobDefinition] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise DefinitionValidationError(f"jobs[{index}] must be an object")
        job_id = entry.get("job_id")
        pipeline_id = entry.get("pipeline_id")
        enabled = entry.get("enabled")
        schedule = entry.get("schedule")
        timezone = entry.get("timezone")
        parameters = entry.get("parameters", {})
        name = entry.get("name")
        description = entry.get("description")
        if not isinstance(job_id, str) or not job_id.strip():
            raise DefinitionValidationError(f"jobs[{index}] job_id must be a non-empty string")
        if not isinstance(pipeline_id, str) or not pipeline_id.strip():
            raise DefinitionValidationError(
                f"jobs[{index}] pipeline_id must be a non-empty string"
            )
        if not isinstance(enabled, bool):
            raise DefinitionValidationError(f"jobs[{index}] enabled must be a boolean")
        if not isinstance(schedule, str) or not schedule.strip():
            raise DefinitionValidationError(f"jobs[{index}] schedule must be a non-empty string")
        if not isinstance(timezone, str) or not timezone.strip():
            raise DefinitionValidationError(f"jobs[{index}] timezone must be a non-empty string")
        if not isinstance(parameters, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in parameters.items()
        ):
            raise DefinitionValidationError(
                f"jobs[{index}] parameters must map strings to strings"
            )
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise DefinitionValidationError(f"jobs[{index}] name must be a string or null")
        if description is not None and (
            not isinstance(description, str) or not description.strip()
        ):
            raise DefinitionValidationError(
                f"jobs[{index}] description must be a string or null"
            )
        try:
            CronExpression.parse(schedule)
        except CronExpressionError as exc:
            raise DefinitionValidationError(
                f"jobs[{index}] invalid schedule for {job_id}: {exc}"
            ) from exc
        try:
            ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise DefinitionValidationError(
                f"jobs[{index}] invalid timezone for {job_id}: {timezone}"
            ) from exc
        jobs.append(
            ProductionJobDefinition(
                job_id=job_id.strip(),
                pipeline_id=pipeline_id.strip(),
                enabled=enabled,
                schedule=schedule.strip(),
                timezone=timezone.strip(),
                parameters=dict(parameters),
                name=name.strip() if isinstance(name, str) else None,
                description=description.strip() if isinstance(description, str) else None,
            )
        )

    ids = [job.job_id for job in jobs]
    if len(ids) != len(set(ids)):
        raise DefinitionValidationError("production job_id values must be unique")
    return tuple(jobs)


def validate_production_jobs(
    jobs: tuple[ProductionJobDefinition, ...],
    *,
    registry: PipelineRegistry | None = None,
) -> tuple[ProductionJobDefinition, ...]:
    """Cross-check every job against the formal Contract registry.

    Fail-closed rules:
    - pipeline_id must exist in the Contract registry;
    - fixed parameters may only use parameters declared by the Contract;
    - required Contract parameters missing from the job fail;
    - parameter values are parsed and typed exactly like runtime overrides
      (one interpretation source: ``parse_parameter_overrides``);
    - one pipeline_id may be referenced by many distinct job_id values.
    """

    effective_registry = registry or default_registry()
    for job in jobs:
        try:
            contract = effective_registry.get(job.pipeline_id)
        except KeyError as exc:
            raise DefinitionValidationError(
                f"{job.job_id} references unknown formal pipeline: {job.pipeline_id}"
            ) from exc
        try:
            parse_parameter_overrides(contract, job.parameters)
        except ContractError as exc:
            raise DefinitionValidationError(
                f"{job.job_id} fixed parameters are invalid: {exc.code}: {exc.detail}"
            ) from exc
    return jobs


def load_and_validate_production_jobs(
    path: str | Path = DEFAULT_PRODUCTION_JOBS_PATH,
    *,
    registry: PipelineRegistry | None = None,
) -> tuple[ProductionJobDefinition, ...]:
    """Load the manifest and run all structure and Contract checks."""

    jobs = load_production_jobs(path)
    return validate_production_jobs(jobs, registry=registry)
