"""Versioned pipeline definitions and runtime records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping


class PipelineStatus(StrEnum):
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


_ALLOWED_TRANSITIONS: dict[PipelineStatus, frozenset[PipelineStatus]] = {
    PipelineStatus.PENDING: frozenset(
        {PipelineStatus.BLOCKED, PipelineStatus.RUNNING, PipelineStatus.CANCELLED, PipelineStatus.SKIPPED}
    ),
    PipelineStatus.BLOCKED: frozenset(
        {PipelineStatus.PENDING, PipelineStatus.CANCELLED, PipelineStatus.SKIPPED}
    ),
    PipelineStatus.RUNNING: frozenset(
        {
            PipelineStatus.SUCCESS,
            PipelineStatus.FAILED,
            PipelineStatus.TIMED_OUT,
            PipelineStatus.CANCELLED,
        }
    ),
    PipelineStatus.SUCCESS: frozenset(),
    PipelineStatus.FAILED: frozenset(),
    PipelineStatus.TIMED_OUT: frozenset(),
    PipelineStatus.CANCELLED: frozenset(),
    PipelineStatus.SKIPPED: frozenset(),
}


def assert_status_transition(current: PipelineStatus, target: PipelineStatus) -> None:
    """Reject state changes outside the run state machine."""

    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"illegal pipeline status transition: {current} -> {target}")


@dataclass(frozen=True, slots=True)
class PipelineDefinition:
    pipeline_id: str
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
    performance_budget: Mapping[str, Any] = field(default_factory=dict)
    freshness_checks: tuple[Mapping[str, Any], ...] = ()
    definition_version: str = "1"
    inherit_environment: bool = False
    environment: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PipelineRun:
    run_id: str
    pipeline_id: str
    definition_version: str
    scheduled_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    status: PipelineStatus
    attempt: int
    exit_code: int | None
    timed_out: bool
    trigger_type: str
    stdout_path: Path | None
    stderr_path: Path | None
    error_summary: str | None
    heartbeat_at: datetime | None
    wall_duration_ms: int | None = None
    user_cpu_ms: int | None = None
    system_cpu_ms: int | None = None
    peak_rss_kb: int | None = None
    retry_of_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class StageRun:
    run_id: str
    stage_name: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    status: PipelineStatus
    input_rows: int | None
    output_rows: int | None
    metadata: Mapping[str, Any]
