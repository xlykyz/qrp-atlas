"""Small, dependency-free cron matcher for the Job scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class CronExpressionError(ValueError):
    """Raised when a five-field cron expression is invalid."""


def _field_values(source: str, minimum: int, maximum: int) -> frozenset[int]:
    values: set[int] = set()
    for part in source.split(","):
        step = 1
        base = part
        if "/" in part:
            base, step_text = part.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise CronExpressionError(f"invalid step {step_text!r}") from exc
            if step <= 0:
                raise CronExpressionError("cron step must be positive")
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            try:
                start, end = int(start_text), int(end_text)
            except ValueError as exc:
                raise CronExpressionError(f"invalid range {base!r}") from exc
        else:
            try:
                start = end = int(base)
            except ValueError as exc:
                raise CronExpressionError(f"invalid cron value {base!r}") from exc
        if start < minimum or end > maximum or start > end:
            raise CronExpressionError(f"cron value outside {minimum}-{maximum}: {part!r}")
        values.update(range(start, end + 1, step))
    if not values:
        raise CronExpressionError("cron field cannot be empty")
    return frozenset(values)


@dataclass(frozen=True, slots=True)
class CronExpression:
    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    day_is_wildcard: bool
    weekday_is_wildcard: bool

    @classmethod
    def parse(cls, expression: str) -> "CronExpression":
        fields = expression.split()
        if len(fields) != 5:
            raise CronExpressionError("only five-field cron expressions are supported")
        minute, hour, day, month, weekday = fields
        return cls(
            expression=expression,
            minutes=_field_values(minute, 0, 59),
            hours=_field_values(hour, 0, 23),
            days=_field_values(day, 1, 31),
            months=_field_values(month, 1, 12),
            weekdays=frozenset(value % 7 for value in _field_values(weekday, 0, 7)),
            day_is_wildcard=day == "*",
            weekday_is_wildcard=weekday == "*",
        )

    def matches(self, instant: datetime) -> bool:
        if instant.tzinfo is None:
            raise ValueError("cron matching requires a timezone-aware datetime")
        cron_weekday = (instant.weekday() + 1) % 7
        if instant.minute not in self.minutes or instant.hour not in self.hours or instant.month not in self.months:
            return False
        day_matches = instant.day in self.days
        weekday_matches = cron_weekday in self.weekdays
        if not self.day_is_wildcard and not self.weekday_is_wildcard:
            return day_matches or weekday_matches
        if not self.day_is_wildcard:
            return day_matches
        if not self.weekday_is_wildcard:
            return weekday_matches
        return True
