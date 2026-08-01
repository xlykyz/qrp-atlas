"""Pure dependency planning helpers for the Job command surface."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .cron import CronExpression
from .definitions import DefinitionValidationError, definitions_by_id
from .models import JobDefinition


def dependency_plan(
    definitions: tuple[JobDefinition, ...], job_id: str, *, include_dependencies: bool = True
) -> tuple[JobDefinition, ...]:
    """Return a stable topological order ending at ``job_id``.

    Definition loading already rejects invalid graphs.  Keeping this traversal
    independently defensive makes CLI planning safe for programmatic callers.
    """

    by_id = definitions_by_id(definitions)
    if job_id not in by_id:
        raise DefinitionValidationError(f"unknown Job definition: {job_id}")
    if not include_dependencies:
        return (by_id[job_id],)

    ordered: list[JobDefinition] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(current_id: str) -> None:
        if current_id in visiting:
            raise DefinitionValidationError(f"pipeline dependency cycle detected at {current_id}")
        if current_id in visited:
            return
        current = by_id.get(current_id)
        if current is None:
            raise DefinitionValidationError(f"missing Job dependency: {current_id}")
        visiting.add(current_id)
        for dependency_id in current.dependencies:
            visit(dependency_id)
        visiting.remove(current_id)
        visited.add(current_id)
        ordered.append(current)

    visit(job_id)
    return tuple(ordered)


def scheduled_instant_for_target_date(definition: JobDefinition, target_date: date) -> datetime:
    """Locate the first configured schedule occurrence for a local business date.

    This only maps a user-selected date onto the Definition's declared target
    time.  It intentionally does not apply trading-calendar or holiday logic;
    formal contracts resolve that business meaning inside their own executor.
    """

    timezone = ZoneInfo(definition.timezone)
    cron = CronExpression.parse(definition.schedule)
    current = datetime.combine(target_date, time.min, tzinfo=timezone)
    end = current + timedelta(days=1)
    while current < end:
        if cron.matches(current):
            return current.astimezone(UTC)
        current += timedelta(minutes=1)
    raise DefinitionValidationError(
        f"{definition.job_id} has no configured schedule occurrence on {target_date.isoformat()}"
    )
