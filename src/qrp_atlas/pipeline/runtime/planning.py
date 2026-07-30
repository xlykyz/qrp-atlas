"""Pure dependency planning helpers for the Pipeline command surface."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .cron import CronExpression
from .definitions import DefinitionValidationError, definitions_by_id
from .models import PipelineDefinition


def dependency_plan(
    definitions: tuple[PipelineDefinition, ...], pipeline_id: str, *, include_dependencies: bool = True
) -> tuple[PipelineDefinition, ...]:
    """Return a stable topological order ending at ``pipeline_id``.

    Definition loading already rejects invalid graphs.  Keeping this traversal
    independently defensive makes CLI planning safe for programmatic callers.
    """

    by_id = definitions_by_id(definitions)
    if pipeline_id not in by_id:
        raise DefinitionValidationError(f"unknown pipeline definition: {pipeline_id}")
    if not include_dependencies:
        return (by_id[pipeline_id],)

    ordered: list[PipelineDefinition] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(current_id: str) -> None:
        if current_id in visiting:
            raise DefinitionValidationError(f"pipeline dependency cycle detected at {current_id}")
        if current_id in visited:
            return
        current = by_id.get(current_id)
        if current is None:
            raise DefinitionValidationError(f"missing pipeline dependency: {current_id}")
        visiting.add(current_id)
        for dependency_id in current.dependencies:
            visit(dependency_id)
        visiting.remove(current_id)
        visited.add(current_id)
        ordered.append(current)

    visit(pipeline_id)
    return tuple(ordered)


def scheduled_instant_for_target_date(definition: PipelineDefinition, target_date: date) -> datetime:
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
        f"{definition.pipeline_id} has no configured schedule occurrence on {target_date.isoformat()}"
    )
