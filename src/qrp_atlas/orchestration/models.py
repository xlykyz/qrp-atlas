"""Versioned Job definitions and durable runtime records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from collections.abc import Callable
from typing import Any, Mapping
from datetime import date


class JobStatus(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"


class OverlapPolicy(StrEnum):
    FORBID = "FORBID"
    ALLOW = "ALLOW"


_ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset(
        {JobStatus.BLOCKED, JobStatus.RUNNING, JobStatus.CANCELLED, JobStatus.SKIPPED}
    ),
    JobStatus.BLOCKED: frozenset(
        {JobStatus.PENDING, JobStatus.CANCELLED, JobStatus.SKIPPED}
    ),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCESS,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
            JobStatus.CANCELLED,
        }
    ),
    JobStatus.SUCCESS: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.TIMED_OUT: frozenset(),
    JobStatus.CANCELLED: frozenset(),
    JobStatus.SKIPPED: frozenset(),
}


def assert_status_transition(current: JobStatus, target: JobStatus) -> None:
    """Reject state changes outside the run state machine."""

    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"illegal Job status transition: {current} -> {target}")


@dataclass(frozen=True, slots=True)
class JobDefinition:
    job_id: str
    name: str
    enabled: bool
    schedule: str
    timezone: str
    command: tuple[str, ...]
    working_directory: Path | None
    dependencies: tuple[str, ...]
    timeout_seconds: int | None
    max_retries: int
    overlap_policy: OverlapPolicy
    resource_locks: tuple[str, ...]
    resource_reads: tuple[str, ...] = ()
    # Optional business-capability identifier of the formal PipelineContract
    # behind this instance.  Source-registered production jobs set it to the
    # Contract pipeline_id; legacy JSON definitions leave it unset.
    pipeline_id: str | None = None
    performance_budget: Mapping[str, Any] = field(default_factory=dict)
    freshness_checks: tuple[Mapping[str, Any], ...] = ()
    definition_version: str = "1"
    # Fixed parameter defaults declared by a production job instance.  They
    # are merged into every scheduled Run's parameter_overrides (per-run
    # manual overrides win) and therefore become part of the durable run
    # record.  Legacy JSON definitions leave this empty.
    fixed_parameters: Mapping[str, str] = field(default_factory=dict)
    inherit_environment: bool = False
    environment: Mapping[str, str] = field(default_factory=dict)
    requires_structured_result: bool = False
    manual_execution_allowed: bool = True
    # Source-registered formal Contracts execute in the owning serve process.
    # JSON/argv definitions leave this unset and continue through subprocess.
    in_process_executor: Callable[["JobRun"], object] | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True, slots=True)
class JobRun:
    run_id: str
    job_id: str
    definition_version: str
    scheduled_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    status: JobStatus
    attempt: int
    exit_code: int | None
    timed_out: bool
    trigger_type: str
    stdout_path: Path | None
    stderr_path: Path | None
    error_summary: str | None
    heartbeat_at: datetime | None
    # Business-capability pipeline_id persisted at run creation; may be None
    # for legacy JSON definitions.  job_id and pipeline_id remain distinct:
    # one Contract can own multiple production job instances.
    pipeline_id: str | None = None
    wall_duration_ms: int | None = None
    user_cpu_ms: int | None = None
    system_cpu_ms: int | None = None
    peak_rss_kb: int | None = None
    retry_of_run_id: str | None = None
    trade_date_override: date | None = None
    parameter_overrides: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)
    execution_control: Any = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class JobExecutionResult:
    """Adapter-neutral outcome returned by an in-process Job executor."""

    status: JobStatus
    payload: Mapping[str, object] | None = None
    error_summary: str | None = None


@dataclass(frozen=True, slots=True)
class JobStageRun:
    run_id: str
    stage_name: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    status: JobStatus
    input_rows: int | None
    output_rows: int | None
    metadata: Mapping[str, Any]
