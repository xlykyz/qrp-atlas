"""Source-level contracts for formally managed QRP pipelines.

The contract deliberately contains production semantics.  Schedules and enablement
remain deployment selections and are adapted to the existing runtime separately.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
import threading
import time
from typing import TYPE_CHECKING, Any

from qrp_atlas.pipeline.runtime.models import OverlapPolicy

if TYPE_CHECKING:
    from qrp_atlas.config.settings import AppSettings


class ContractError(ValueError):
    """A stable contract error code with optional diagnostic detail."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = code
        self.detail = detail
        super().__init__(code)


@dataclass(slots=True)
class ExecutionControl:
    """Cooperative cancellation and deadline state shared by one invocation."""

    deadline: datetime | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    deadline_monotonic: float | None = field(default=None, repr=False)
    _cancel_reason: str | None = field(default=None, init=False, repr=False)
    _reason_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def cancel(self, reason: str) -> None:
        with self._reason_lock:
            if self._cancel_reason is None:
                self._cancel_reason = reason[:500]
            self.cancel_event.set()

    @property
    def cancel_reason(self) -> str | None:
        with self._reason_lock:
            return self._cancel_reason

    def remaining_seconds(self) -> float | None:
        if self.deadline_monotonic is not None:
            return max(0.0, self.deadline_monotonic - time.monotonic())
        if self.deadline is None:
            return None
        if self.deadline.tzinfo is None:
            raise ValueError("execution deadline must be timezone-aware")
        return max(0.0, (self.deadline - datetime.now(self.deadline.tzinfo)).total_seconds())

    def bounded_timeout(self, requested: float | None = None) -> float | None:
        """Return a wait/network timeout that cannot exceed this invocation's deadline."""

        remaining = self.remaining_seconds()
        if requested is not None and requested < 0:
            raise ValueError("requested timeout must be non-negative")
        if remaining is None:
            return requested
        return remaining if requested is None else min(remaining, requested)

    def wait(self, event: threading.Event, timeout: float | None = None) -> bool:
        """Wait cooperatively and re-check cancellation/deadline on wake-up."""

        self.check()
        woke = event.wait(self.bounded_timeout(timeout))
        self.check()
        return woke

    def check(self) -> None:
        """Raise a stable ContractError before unsafe work continues."""

        remaining = self.remaining_seconds()
        if remaining is not None and remaining <= 0:
            self.cancel("execution deadline exceeded")
            raise ContractError("EXECUTION_TIMED_OUT", "execution deadline exceeded")
        if self.cancel_event.is_set():
            raise ContractError("EXECUTION_CANCELLED", self.cancel_reason or "execution cancelled")


class PipelineKind(StrEnum):
    ATOMIC = "ATOMIC"
    DAG = "DAG"


class InputKind(StrEnum):
    TABLE = "TABLE"
    FILE = "FILE"
    EXTERNAL_API = "EXTERNAL_API"
    UPSTREAM_PIPELINE = "UPSTREAM_PIPELINE"


class WriteMode(StrEnum):
    APPEND = "APPEND"
    UPSERT = "UPSERT"
    REPLACE_TARGET_DATE = "REPLACE_TARGET_DATE"
    REPLACE_TARGET_RANGE = "REPLACE_TARGET_RANGE"
    FULL_REBUILD = "FULL_REBUILD"
    READ_ONLY = "READ_ONLY"


class TransactionMode(StrEnum):
    DATABASE_TRANSACTION = "DATABASE_TRANSACTION"
    STAGING_ATOMIC_REPLACE = "STAGING_ATOMIC_REPLACE"
    READ_ONLY = "READ_ONLY"


class NonTradingDayPolicy(StrEnum):
    REJECT = "REJECT"
    PREVIOUS_TRADING_DAY = "PREVIOUS_TRADING_DAY"
    NEXT_TRADING_DAY = "NEXT_TRADING_DAY"
    ALLOW_CALENDAR_DATE = "ALLOW_CALENDAR_DATE"


class ResultStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    NOOP = "NOOP"


class DiagnosticLevel(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class ParameterType(StrEnum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"


@dataclass(frozen=True, slots=True)
class TargetWindow:
    """Exactly one target business date or inclusive business-date range."""

    target_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None

    def __post_init__(self) -> None:
        single_date = self.target_date is not None
        date_range = self.start_date is not None or self.end_date is not None
        if single_date == date_range:
            raise ContractError(
                "INVALID_TARGET_WINDOW",
                "target window must contain either target_date or start_date/end_date",
            )
        if date_range and (self.start_date is None or self.end_date is None):
            raise ContractError("INVALID_TARGET_WINDOW", "target date range requires both start_date and end_date")
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            raise ContractError("INVALID_TARGET_WINDOW", "target date range start_date must not be after end_date")

    @classmethod
    def for_date(cls, target_date: date) -> "TargetWindow":
        return cls(target_date=target_date)

    def as_dict(self) -> dict[str, str | None]:
        return {
            "target_date": self.target_date.isoformat() if self.target_date else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
        }


@dataclass(frozen=True, slots=True)
class PipelineInvocation:
    """Values available before a contract resolves its business date."""

    run_id: str
    pipeline_id: str
    scheduled_for: datetime
    attempt: int
    settings: "AppSettings"
    parameter_overrides: Mapping[str, Any] = field(default_factory=dict)
    trade_date_override: date | None = None
    audit_context: Mapping[str, str] = field(default_factory=dict)
    execution_control: ExecutionControl = field(default_factory=ExecutionControl)


@dataclass(frozen=True, slots=True)
class PipelineRunContext:
    """Uniform context passed to every contract check and executor."""

    run_id: str
    pipeline_id: str
    scheduled_for: datetime
    attempt: int
    settings: "AppSettings"
    parameter_overrides: Mapping[str, Any]
    target_window: TargetWindow
    audit_context: Mapping[str, str]
    execution_control: ExecutionControl = field(default_factory=ExecutionControl)


TargetDateResolver = Callable[[PipelineInvocation], TargetWindow]
TargetDateValidator = Callable[[date, PipelineInvocation], bool]


@dataclass(frozen=True, slots=True)
class TargetDatePolicy:
    policy_id: str
    description: str
    trading_calendar_id: str
    non_trading_day_policy: NonTradingDayPolicy
    resolver: TargetDateResolver
    validate_explicit_date: TargetDateValidator


@dataclass(frozen=True, slots=True)
class ParameterContract:
    name: str
    parameter_type: ParameterType
    description: str
    required: bool = False
    default: Any = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    check_id: str
    passed: bool
    error_code: str | None = None
    detail: str | None = None
    observed: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def success(cls, check_id: str, **observed: Any) -> "CheckResult":
        return cls(check_id=check_id, passed=True, observed=observed)

    @classmethod
    def failure(
        cls,
        check_id: str,
        error_code: str,
        detail: str,
        **observed: Any,
    ) -> "CheckResult":
        return cls(
            check_id=check_id,
            passed=False,
            error_code=error_code,
            detail=detail,
            observed=observed,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "passed": self.passed,
            "error_code": self.error_code,
            "detail": self.detail,
            "observed": dict(self.observed),
        }


ContractCheck = Callable[[PipelineRunContext], CheckResult]


@dataclass(frozen=True, slots=True)
class FreshnessContract:
    check_id: str
    target_date_semantics: str
    maximum_lag_trading_days: int
    non_trading_day_policy: NonTradingDayPolicy
    error_code: str
    checker: ContractCheck


@dataclass(frozen=True, slots=True)
class InputContract:
    input_id: str
    kind: InputKind
    source: str
    required_fields: tuple[str, ...]
    target_date_semantics: str
    missing_error_code: str
    structure_check: ContractCheck
    freshness: FreshnessContract
    upstream_pipeline_id: str | None = None


@dataclass(frozen=True, slots=True)
class CompletionContract:
    marker: str
    error_code: str
    checker: ContractCheck


@dataclass(frozen=True, slots=True)
class OutputContract:
    output_id: str
    physical_resource: str
    location: str
    object_name: str
    unique_key: tuple[str, ...]
    write_mode: WriteMode
    target_date_semantics: str
    completion: CompletionContract
    quality_checks: tuple[ContractCheck, ...]
    allow_empty: bool


@dataclass(frozen=True, slots=True)
class IdempotencyContract:
    idempotency_key: str
    repeat_run_semantics: str
    existing_target_handling: str
    failure_recovery: str
    uses_staging: bool
    atomic_replace_boundary: str


@dataclass(frozen=True, slots=True)
class TransactionContract:
    mode: TransactionMode
    boundary: str
    failure_visibility: str


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    overlap_policy: OverlapPolicy
    max_retries: int


@dataclass(frozen=True, slots=True)
class PerformanceBudget:
    normal_budget_seconds: float
    warning_threshold_seconds: float
    hard_timeout_seconds: int
    benchmark_scope: str
    baseline_source: str


@dataclass(frozen=True, slots=True)
class PipelineDiagnostic:
    code: str
    level: DiagnosticLevel
    message: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "level": self.level.value,
            "message": self.message,
            "detail": dict(self.detail),
        }


@dataclass(frozen=True, slots=True)
class PipelineMetrics:
    rows_read: int = 0
    rows_written: int = 0
    assets_processed: int = 0
    dates_processed: int = 0
    database_write_seconds: float = 0.0
    stage_durations_seconds: Mapping[str, float] = field(default_factory=dict)
    api_requests: int | None = None
    batches: int | None = None
    retries: int = 0
    peak_rss_kb: int | None = None
    temporary_disk_bytes: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "assets_processed": self.assets_processed,
            "dates_processed": self.dates_processed,
            "database_write_seconds": self.database_write_seconds,
            "stage_durations_seconds": dict(self.stage_durations_seconds),
            "api_requests": self.api_requests,
            "batches": self.batches,
            "retries": self.retries,
            "peak_rss_kb": self.peak_rss_kb,
            "temporary_disk_bytes": self.temporary_disk_bytes,
        }


@dataclass(frozen=True, slots=True)
class OutputResult:
    output_id: str
    rows_written: int
    location: str
    completed: bool
    detail: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "output_id": self.output_id,
            "rows_written": self.rows_written,
            "location": self.location,
            "completed": self.completed,
            "detail": dict(self.detail),
        }


@dataclass(frozen=True, slots=True)
class PerformanceResult:
    duration_seconds: float
    warning_threshold_seconds: float
    normal_budget_seconds: float
    within_warning_threshold: bool
    within_normal_budget: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "duration_seconds": self.duration_seconds,
            "warning_threshold_seconds": self.warning_threshold_seconds,
            "normal_budget_seconds": self.normal_budget_seconds,
            "within_warning_threshold": self.within_warning_threshold,
            "within_normal_budget": self.within_normal_budget,
        }


@dataclass(frozen=True, slots=True)
class BusinessExecution:
    """Business-layer outcome before common completion and performance checks."""

    status: ResultStatus
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)
    outputs: tuple[OutputResult, ...] = ()
    diagnostics: tuple[PipelineDiagnostic, ...] = ()
    noop_reason: str | None = None

    @classmethod
    def success(
        cls,
        *,
        metrics: PipelineMetrics | None = None,
        outputs: tuple[OutputResult, ...] = (),
        diagnostics: tuple[PipelineDiagnostic, ...] = (),
    ) -> "BusinessExecution":
        return cls(
            status=ResultStatus.SUCCESS,
            metrics=metrics or PipelineMetrics(),
            outputs=outputs,
            diagnostics=diagnostics,
        )

    @classmethod
    def noop(
        cls,
        reason: str,
        *,
        metrics: PipelineMetrics | None = None,
        diagnostics: tuple[PipelineDiagnostic, ...] = (),
    ) -> "BusinessExecution":
        return cls(
            status=ResultStatus.NOOP,
            metrics=metrics or PipelineMetrics(),
            diagnostics=diagnostics,
            noop_reason=reason,
        )


PipelineExecutor = Callable[[PipelineRunContext], BusinessExecution]


@dataclass(frozen=True, slots=True)
class PipelineContract:
    pipeline_id: str
    name: str
    description: str
    contract_version: str
    kind: PipelineKind
    executor: PipelineExecutor
    target_date_policy: TargetDatePolicy
    parameters: tuple[ParameterContract, ...]
    inputs: tuple[InputContract, ...]
    outputs: tuple[OutputContract, ...]
    dependencies: tuple[str, ...]
    resource_locks: tuple[str, ...]
    idempotency: IdempotencyContract
    transaction: TransactionContract
    execution: ExecutionPolicy
    performance: PerformanceBudget
    manual_execution_allowed: bool = True
    resource_reads: tuple[str, ...] = ()

    def describe(self) -> dict[str, Any]:
        """Return a machine-readable, secret-free view of the source contract."""

        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "description": self.description,
            "contract_version": self.contract_version,
            "kind": self.kind.value,
            "executor": _callable_name(self.executor),
            "target_date_policy": {
                "policy_id": self.target_date_policy.policy_id,
                "description": self.target_date_policy.description,
                "trading_calendar_id": self.target_date_policy.trading_calendar_id,
                "non_trading_day_policy": self.target_date_policy.non_trading_day_policy.value,
                "resolver": _callable_name(self.target_date_policy.resolver),
                "validate_explicit_date": _callable_name(self.target_date_policy.validate_explicit_date),
            },
            "parameters": [
                {
                    "name": item.name,
                    "parameter_type": item.parameter_type.value,
                    "description": item.description,
                    "required": item.required,
                    "default": _describe_parameter_value(item.default),
                }
                for item in self.parameters
            ],
            "inputs": [
                {
                    "input_id": item.input_id,
                    "kind": item.kind.value,
                    "source": item.source,
                    "required_fields": list(item.required_fields),
                    "target_date_semantics": item.target_date_semantics,
                    "missing_error_code": item.missing_error_code,
                    "structure_check": _callable_name(item.structure_check),
                    "upstream_pipeline_id": item.upstream_pipeline_id,
                    "freshness": {
                        "check_id": item.freshness.check_id,
                        "target_date_semantics": item.freshness.target_date_semantics,
                        "maximum_lag_trading_days": item.freshness.maximum_lag_trading_days,
                        "non_trading_day_policy": item.freshness.non_trading_day_policy.value,
                        "error_code": item.freshness.error_code,
                        "checker": _callable_name(item.freshness.checker),
                    },
                }
                for item in self.inputs
            ],
            "outputs": [
                {
                    "output_id": item.output_id,
                    "physical_resource": item.physical_resource,
                    "location": item.location,
                    "object_name": item.object_name,
                    "unique_key": list(item.unique_key),
                    "write_mode": item.write_mode.value,
                    "target_date_semantics": item.target_date_semantics,
                    "completion": {
                        "marker": item.completion.marker,
                        "error_code": item.completion.error_code,
                        "checker": _callable_name(item.completion.checker),
                    },
                    "quality_checks": [_callable_name(check) for check in item.quality_checks],
                    "allow_empty": item.allow_empty,
                }
                for item in self.outputs
            ],
            "dependencies": list(self.dependencies),
            "resource_locks": list(self.resource_locks),
            "resource_reads": list(self.resource_reads),
            "idempotency": {
                "idempotency_key": self.idempotency.idempotency_key,
                "repeat_run_semantics": self.idempotency.repeat_run_semantics,
                "existing_target_handling": self.idempotency.existing_target_handling,
                "failure_recovery": self.idempotency.failure_recovery,
                "uses_staging": self.idempotency.uses_staging,
                "atomic_replace_boundary": self.idempotency.atomic_replace_boundary,
            },
            "transaction": {
                "mode": self.transaction.mode.value,
                "boundary": self.transaction.boundary,
                "failure_visibility": self.transaction.failure_visibility,
            },
            "execution": {
                "overlap_policy": self.execution.overlap_policy.value,
                "max_retries": self.execution.max_retries,
            },
            "manual_execution_allowed": self.manual_execution_allowed,
            "performance": {
                "normal_budget_seconds": self.performance.normal_budget_seconds,
                "warning_threshold_seconds": self.performance.warning_threshold_seconds,
                "hard_timeout_seconds": self.performance.hard_timeout_seconds,
                "benchmark_scope": self.performance.benchmark_scope,
                "baseline_source": self.performance.baseline_source,
            },
        }


@dataclass(frozen=True, slots=True)
class PipelineResult:
    run_id: str
    pipeline_id: str
    status: ResultStatus
    target_window: TargetWindow
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    attempt: int
    metrics: PipelineMetrics
    outputs: tuple[OutputResult, ...]
    input_checks: tuple[CheckResult, ...]
    freshness_checks: tuple[CheckResult, ...]
    completion_checks: tuple[CheckResult, ...]
    performance: PerformanceResult
    diagnostics: tuple[PipelineDiagnostic, ...]
    noop_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "target_window": self.target_window.as_dict(),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "attempt": self.attempt,
            "rows_read": self.metrics.rows_read,
            "rows_written": self.metrics.rows_written,
            "metrics": self.metrics.as_dict(),
            "outputs": [item.as_dict() for item in self.outputs],
            "input_checks": [item.as_dict() for item in self.input_checks],
            "freshness_checks": [item.as_dict() for item in self.freshness_checks],
            "completion_checks": [item.as_dict() for item in self.completion_checks],
            "performance": self.performance.as_dict(),
            "diagnostics": [item.as_dict() for item in self.diagnostics],
            "noop_reason": self.noop_reason,
        }


def _callable_name(value: Callable[..., object]) -> str:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if isinstance(module, str) and isinstance(qualname, str):
        return f"{module}.{qualname}"
    return repr(value)


def parse_parameter_overrides(
    contract: PipelineContract,
    raw_values: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Parse only source-declared parameters before executor invocation."""

    definitions = {item.name: item for item in contract.parameters}
    if len(definitions) != len(contract.parameters):
        raise ContractError("DUPLICATE_PARAMETER_DEFINITION", "parameter names must be unique")
    unknown = set(raw_values) - set(definitions)
    if unknown:
        raise ContractError("UNKNOWN_PARAMETER", ", ".join(sorted(unknown)))
    parsed: dict[str, Any] = {}
    for definition in contract.parameters:
        if definition.name in raw_values:
            parsed[definition.name] = _parse_parameter_value(definition, raw_values[definition.name])
        elif definition.required:
            raise ContractError("REQUIRED_PARAMETER_MISSING", definition.name)
        else:
            parsed[definition.name] = _parse_parameter_value(definition, definition.default)
    return parsed


def _parse_parameter_value(definition: ParameterContract, value: Any) -> Any:
    try:
        if definition.parameter_type is ParameterType.STRING:
            if not isinstance(value, str):
                raise ValueError
            return value
        if definition.parameter_type is ParameterType.INTEGER:
            if isinstance(value, bool):
                raise ValueError
            return int(value)
        if definition.parameter_type is ParameterType.BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, str) and value.strip().lower() in {"true", "1", "yes", "on"}:
                return True
            if isinstance(value, str) and value.strip().lower() in {"false", "0", "no", "off"}:
                return False
            raise ValueError
        if definition.parameter_type is ParameterType.DATE:
            if isinstance(value, date):
                return value
            if isinstance(value, str):
                return date.fromisoformat(value)
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ContractError("INVALID_PARAMETER", definition.name) from exc
    raise ContractError("UNKNOWN_PARAMETER_TYPE", definition.name)


def _describe_parameter_value(value: Any) -> Any:
    return value.isoformat() if isinstance(value, date) else value
