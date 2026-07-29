"""Create due Pipeline run records; execution belongs exclusively to the Runner."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from .cron import CronExpression
from .models import OverlapPolicy, PipelineDefinition, PipelineRun, PipelineStatus
from .store import PipelineRuntimeStore


class PipelineScheduler:
    """A stateless scanner backed by SQLite uniqueness and resource leases."""

    def __init__(self, store: PipelineRuntimeStore, definitions: tuple[PipelineDefinition, ...]) -> None:
        self.store = store
        self.definitions = definitions

    def scan(self, *, now: datetime | None = None) -> list[PipelineRun]:
        """Create one PENDING/BLOCKED first attempt for each due definition.

        A scan is intentionally non-executing. SQLite's unique
        ``pipeline_id, scheduled_at, attempt`` constraint makes duplicate scans,
        including concurrent scheduler processes, idempotent.
        """

        instant = (now or datetime.now(UTC)).astimezone(UTC)
        self.store.initialize()
        self._refresh_blocked_runs()
        created: list[PipelineRun] = []
        for definition in self.definitions:
            if not definition.enabled:
                continue
            local_time = instant.astimezone(ZoneInfo(definition.timezone)).replace(second=0, microsecond=0)
            if not CronExpression.parse(definition.schedule).matches(local_time):
                continue
            scheduled_at = local_time.astimezone(UTC)
            status, reason = self._eligibility(definition, scheduled_at)
            run, inserted = self.store.create_scheduled_run(
                definition,
                scheduled_at=scheduled_at,
                status=status,
                error_summary=reason,
            )
            if inserted:
                created.append(run)
        return created

    def _refresh_blocked_runs(self) -> None:
        definitions = {definition.pipeline_id: definition for definition in self.definitions}
        for run in self.store.list_runs(status=PipelineStatus.BLOCKED, limit=10_000):
            definition = definitions.get(run.pipeline_id)
            if definition is None or definition.definition_version != run.definition_version:
                continue
            status, _ = self._eligibility(definition, run.scheduled_at)
            if status is PipelineStatus.PENDING:
                self.store.unblock_run(run.run_id)

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
