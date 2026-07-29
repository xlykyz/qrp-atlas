"""Create due Pipeline run records; execution belongs exclusively to the Runner."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from .cron import CronExpression
from .models import OverlapPolicy, PipelineDefinition, PipelineRun, PipelineStatus
from .store import PipelineRuntimeStore


DEFAULT_SCHEDULER_ID = "default"
DEFAULT_MAX_CATCH_UP_MINUTES = 360
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5.0
DEFAULT_LEASE_SECONDS = 30
DEFAULT_STALE_AFTER_SECONDS = 180


@dataclass(frozen=True, slots=True)
class SchedulerScanResult(Sequence[PipelineRun]):
    """Durable scan outcome, including the cursor interval actually handled."""

    created_runs: tuple[PipelineRun, ...]
    scheduler_id: str
    requested_start_at: datetime | None
    scan_start_at: datetime | None
    scanned_through_at: datetime
    catch_up_limited: bool
    stale_runs_recovered: int
    expired_locks_reclaimed: int

    def __getitem__(self, index: int | slice) -> PipelineRun | tuple[PipelineRun, ...]:
        return self.created_runs[index]

    def __len__(self) -> int:
        return len(self.created_runs)


class PipelineScheduler:
    """Cursor-backed, non-executing scanner for Git-versioned definitions."""

    def __init__(
        self,
        store: PipelineRuntimeStore,
        definitions: tuple[PipelineDefinition, ...],
        *,
        scheduler_id: str = DEFAULT_SCHEDULER_ID,
        max_catch_up_minutes: int = DEFAULT_MAX_CATCH_UP_MINUTES,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    ) -> None:
        if not scheduler_id.strip():
            raise ValueError("scheduler_id must be non-empty")
        if max_catch_up_minutes <= 0:
            raise ValueError("max_catch_up_minutes must be positive")
        if not stale_after_seconds > lease_seconds > heartbeat_interval_seconds > 0:
            raise ValueError(
                "stale_after_seconds must be greater than lease_seconds, which must be greater than "
                "heartbeat_interval_seconds"
            )
        self.store = store
        self.definitions = definitions
        self.scheduler_id = scheduler_id
        self.max_catch_up_minutes = max_catch_up_minutes
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.lease_seconds = lease_seconds
        self.stale_after_seconds = stale_after_seconds

    def scan(self, *, now: datetime | None = None) -> SchedulerScanResult:
        """Persist all due minutes since the previous successful scan.

        The first scan intentionally considers only its current UTC minute.
        Later scans replay ``(cursor, current]``. A bounded catch-up advances
        only after that bounded interval has been atomically committed, and
        reports the omitted earlier start explicitly to the caller.
        """

        instant = (now or datetime.now(UTC)).astimezone(UTC).replace(second=0, microsecond=0)
        self.store.initialize()
        stale_runs, expired_locks = self.store.recover_stale(
            stale_after_seconds=self.stale_after_seconds,
            now=instant,
        )
        self._refresh_blocked_runs()
        compiled_definitions = tuple(
            (definition, CronExpression.parse(definition.schedule), ZoneInfo(definition.timezone))
            for definition in self.definitions
            if definition.enabled
        )
        for _ in range(3):
            cursor = self.store.get_scheduler_cursor(self.scheduler_id)
            if cursor is not None and cursor.last_scanned_at >= instant:
                return SchedulerScanResult(
                    created_runs=(),
                    scheduler_id=self.scheduler_id,
                    requested_start_at=None,
                    scan_start_at=None,
                    scanned_through_at=instant,
                    catch_up_limited=False,
                    stale_runs_recovered=stale_runs,
                    expired_locks_reclaimed=expired_locks,
                )
            expected_cursor = cursor.last_scanned_at if cursor is not None else None
            requested_start = instant if expected_cursor is None else expected_cursor + timedelta(minutes=1)
            scan_start = requested_start
            catch_up_limited = False
            interval_minutes = int((instant - requested_start).total_seconds() // 60) + 1
            if interval_minutes > self.max_catch_up_minutes:
                scan_start = instant - timedelta(minutes=self.max_catch_up_minutes - 1)
                catch_up_limited = True
            candidates: list[tuple[PipelineDefinition, datetime, PipelineStatus, str | None]] = []
            for scheduled_at in _minutes_between(scan_start, instant):
                for definition, cron, timezone in compiled_definitions:
                    local_time = scheduled_at.astimezone(timezone)
                    if not cron.matches(local_time):
                        continue
                    status, reason = self._eligibility(definition, scheduled_at)
                    candidates.append((definition, scheduled_at, status, reason))
            created = self.store.commit_scheduler_scan(
                scheduler_id=self.scheduler_id,
                expected_last_scanned_at=expected_cursor,
                scanned_through_at=instant,
                candidates=candidates,
                now=instant,
            )
            if created is not None:
                return SchedulerScanResult(
                    created_runs=tuple(created),
                    scheduler_id=self.scheduler_id,
                    requested_start_at=requested_start,
                    scan_start_at=scan_start,
                    scanned_through_at=instant,
                    catch_up_limited=catch_up_limited,
                    stale_runs_recovered=stale_runs,
                    expired_locks_reclaimed=expired_locks,
                )
        raise RuntimeError("scheduler cursor changed repeatedly; scan was not committed")

    def _refresh_blocked_runs(self) -> None:
        definitions = {definition.pipeline_id: definition for definition in self.definitions}
        for run in self.store.list_runs(status=PipelineStatus.BLOCKED, limit=10_000):
            definition = definitions.get(run.pipeline_id)
            if definition is None or definition.definition_version != run.definition_version:
                continue
            status, _ = self._eligibility(definition, run.scheduled_at)
            if status is PipelineStatus.PENDING:
                self.store.unblock_run(run.run_id)

    def eligibility(self, definition: PipelineDefinition, scheduled_at: datetime) -> tuple[PipelineStatus, str | None]:
        """Expose the runtime's existing dependency and overlap gate for manual runs."""

        return self._eligibility(definition, scheduled_at)

    def _eligibility(
        self,
        definition: PipelineDefinition,
        scheduled_at: datetime,
    ) -> tuple[PipelineStatus, str | None]:
        failed_dependencies: list[str] = []
        for dependency_id in definition.dependencies:
            dependency_run = self.store.latest_run_before(dependency_id, scheduled_at)
            if dependency_run is None:
                failed_dependencies.append(f"dependency {dependency_id} has no completed run")
            elif dependency_run.status is not PipelineStatus.SUCCESS:
                failed_dependencies.append(
                    f"dependency {dependency_id} latest status is {dependency_run.status.value}"
                )
        if failed_dependencies:
            return PipelineStatus.BLOCKED, "; ".join(failed_dependencies)
        if definition.overlap_policy is OverlapPolicy.FORBID and self.store.has_active_pipeline_run(
            definition.pipeline_id
        ):
            return PipelineStatus.BLOCKED, "overlap_policy=FORBID has an active run"
        locked = [
            resource_name
            for resource_name in definition.resource_locks
            if self.store.has_active_resource_lock(resource_name)
        ]
        if locked:
            return PipelineStatus.BLOCKED, f"active resource locks: {', '.join(locked)}"
        return PipelineStatus.PENDING, None


def _minutes_between(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current <= end:
        yield current
        current += timedelta(minutes=1)
