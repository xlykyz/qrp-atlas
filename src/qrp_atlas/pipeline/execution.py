"""Common outer lifecycle for source-registered Pipeline contracts."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from numbers import Integral, Real

from qrp_atlas.orchestration.execution_control import ExecutionControlError

from .contract_validation import is_valid_error_code
from .contracts import (
    BusinessExecution,
    CheckResult,
    ContractCheck,
    ContractError,
    DiagnosticLevel,
    ExecutionControl,
    OutputResult,
    PipelineContract,
    PipelineDiagnostic,
    PipelineInvocation,
    PipelineMetrics,
    PipelineResult,
    PipelineRunContext,
    PerformanceResult,
    parse_parameter_overrides,
    ResultStatus,
    TargetWindow,
)


def execute_pipeline_contract(contract: PipelineContract, invocation: PipelineInvocation) -> PipelineResult:
    """Execute common preflight, business logic, completion, and result assembly.

    Dependency state, claiming, locking, timeout, heartbeat, and retry remain the
    responsibility of the generic Job orchestrator.  This function intentionally never
    implements those runtime concerns a second time.
    """

    started_at = datetime.now(UTC)
    started = time.monotonic()
    target_window = _fallback_target_window(invocation)
    input_checks: tuple[CheckResult, ...] = ()
    freshness_checks: tuple[CheckResult, ...] = ()
    completion_checks: tuple[CheckResult, ...] = ()
    diagnostics: tuple[PipelineDiagnostic, ...] = ()
    business = BusinessExecution(status=ResultStatus.FAILED)

    try:
        invocation.execution_control.check()
        target_window = _resolve_target_window(contract, invocation)
        invocation.execution_control.check()
        parameters = parse_parameter_overrides(contract, invocation.parameter_overrides)
        invocation.execution_control.check()
        context = PipelineRunContext(
            run_id=invocation.run_id,
            pipeline_id=contract.pipeline_id,
            scheduled_for=invocation.scheduled_for,
            attempt=invocation.attempt,
            settings=invocation.settings,
            parameter_overrides=parameters,
            target_window=target_window,
            audit_context=invocation.audit_context,
            execution_control=invocation.execution_control,
        )
        input_checks = tuple(
            _run_check(
                item.structure_check,
                context,
                control=invocation.execution_control,
                fallback_code=item.missing_error_code,
            )
            for item in contract.inputs
        )
        if failed := _first_failure(input_checks):
            return _failure_result(
                invocation,
                target_window,
                started_at,
                started,
                input_checks=input_checks,
                diagnostic=_diagnostic_for_check(failed),
                contract=contract,
            )

        freshness_checks = tuple(
            _run_check(
                item.freshness.checker,
                context,
                control=invocation.execution_control,
                fallback_code=item.freshness.error_code,
            )
            for item in contract.inputs
        )
        if failed := _first_failure(freshness_checks):
            return _failure_result(
                invocation,
                target_window,
                started_at,
                started,
                input_checks=input_checks,
                freshness_checks=freshness_checks,
                diagnostic=_diagnostic_for_check(failed),
                contract=contract,
            )

        invocation.execution_control.check()
        business = contract.executor(context)
        if not isinstance(business, BusinessExecution):
            raise ContractError("INVALID_EXECUTOR_RETURN", "executor must return BusinessExecution")
        invocation.execution_control.check()
        try:
            _validate_business_execution(contract, business)
        except ContractError:
            business = BusinessExecution(status=ResultStatus.FAILED)
            raise
        diagnostics = business.diagnostics
        if business.status is ResultStatus.FAILED:
            return _failure_result(
                invocation,
                target_window,
                started_at,
                started,
                input_checks=input_checks,
                freshness_checks=freshness_checks,
                business=business,
                diagnostic=PipelineDiagnostic(
                    code="BUSINESS_EXECUTION_FAILED",
                    level=DiagnosticLevel.ERROR,
                    message="business executor returned FAILED",
                ),
                contract=contract,
            )

        if business.status is ResultStatus.SUCCESS:
            completion_checks = tuple(
                _run_check(
                    output.completion.checker,
                    context,
                    control=invocation.execution_control,
                    fallback_code=output.completion.error_code,
                )
                for output in contract.outputs
            ) + tuple(
                result
                for output in contract.outputs
                for result in (
                    _run_check(
                        check,
                        context,
                        control=invocation.execution_control,
                        fallback_code="OUTPUT_QUALITY_CHECK_FAILED",
                    )
                    for check in output.quality_checks
                )
            )
            if failed := _first_failure(completion_checks):
                return _failure_result(
                    invocation,
                    target_window,
                    started_at,
                    started,
                    input_checks=input_checks,
                    freshness_checks=freshness_checks,
                    completion_checks=completion_checks,
                    business=business,
                    diagnostic=_diagnostic_for_check(failed),
                    contract=contract,
                )

        invocation.execution_control.check()
        return _result(
            invocation,
            target_window,
            started_at,
            started,
            contract,
            status=business.status,
            business=business,
            input_checks=input_checks,
            freshness_checks=freshness_checks,
            completion_checks=completion_checks,
            diagnostics=diagnostics + _performance_diagnostics(contract, started),
        )
    except ExecutionControlError as exc:
        return _failure_result(
            invocation,
            target_window,
            started_at,
            started,
            input_checks=input_checks,
            freshness_checks=freshness_checks,
            completion_checks=completion_checks,
            business=business,
            diagnostic=PipelineDiagnostic(
                code=exc.code,
                level=DiagnosticLevel.ERROR,
                message="execution control stopped the Pipeline contract",
                detail={"reason": exc.detail or exc.code},
            ),
            contract=contract,
        )
    except ContractError as exc:
        return _failure_result(
            invocation,
            target_window,
            started_at,
            started,
            input_checks=input_checks,
            freshness_checks=freshness_checks,
            completion_checks=completion_checks,
            business=business,
            diagnostic=PipelineDiagnostic(
                code=exc.code if is_valid_error_code(exc.code) else "INVALID_ERROR_CODE",
                level=DiagnosticLevel.ERROR,
                message="formal Pipeline contract rejected the execution",
                detail=_contract_error_detail(exc),
            ),
            contract=contract,
        )
    except Exception as exc:
        return _failure_result(
            invocation,
            target_window,
            started_at,
            started,
            input_checks=input_checks,
            freshness_checks=freshness_checks,
            completion_checks=completion_checks,
            business=business,
            diagnostic=PipelineDiagnostic(
                code="UNHANDLED_EXCEPTION",
                level=DiagnosticLevel.ERROR,
                message=f"unhandled {type(exc).__name__} during Pipeline execution",
            ),
            contract=contract,
        )


def _resolve_target_window(contract: PipelineContract, invocation: PipelineInvocation) -> TargetWindow:
    invocation.execution_control.check()
    if invocation.trade_date_override is not None:
        if not contract.target_date_policy.validate_explicit_date(invocation.trade_date_override, invocation):
            raise ContractError("TARGET_DATE_OVERRIDE_INVALID")
        invocation.execution_control.check()
        return TargetWindow.for_date(invocation.trade_date_override)
    window = contract.target_date_policy.resolver(invocation)
    invocation.execution_control.check()
    if not isinstance(window, TargetWindow):
        raise ContractError("INVALID_TARGET_DATE_RESOLUTION")
    return window


def _fallback_target_window(invocation: PipelineInvocation) -> TargetWindow:
    return TargetWindow.for_date(invocation.trade_date_override or invocation.scheduled_for.date())


def _run_check(
    check: ContractCheck,
    context: PipelineRunContext,
    *,
    control: ExecutionControl,
    fallback_code: str,
) -> CheckResult:
    control.check()
    try:
        result = check(context)
    except ContractError as exc:
        if isinstance(exc.code, str) and exc.code in {"EXECUTION_TIMED_OUT", "EXECUTION_CANCELLED"}:
            raise
        error_code = fallback_code if is_valid_error_code(fallback_code) else "INVALID_ERROR_CODE"
        if not is_valid_error_code(exc.code):
            error_code = "INVALID_ERROR_CODE"
        return CheckResult.failure(
            "CHECK_CONTRACT_ERROR",
            error_code,
            f"contract check rejected execution with {exc.code}",
        )
    except Exception as exc:
        return CheckResult.failure(
            "CHECK_EXCEPTION",
            fallback_code if is_valid_error_code(fallback_code) else "INVALID_ERROR_CODE",
            f"contract check raised {type(exc).__name__}",
        )
    if not isinstance(result, CheckResult):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            fallback_code if is_valid_error_code(fallback_code) else "INVALID_ERROR_CODE",
            "contract check must return CheckResult",
        )
    control.check()
    if not isinstance(result.passed, bool):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_RESULT",
            "contract check passed must be a boolean",
        )
    if not isinstance(result.check_id, str) or not result.check_id.strip():
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_ID",
            "contract check must return a non-empty check_id",
        )
    if result.error_code is not None and not is_valid_error_code(result.error_code):
        return CheckResult.failure(
            "CHECK_INVALID_ERROR_CODE",
            "INVALID_ERROR_CODE",
            "contract check returned a malformed error code",
        )
    if result.passed and result.error_code is not None:
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_RESULT",
            "successful contract checks must not include an error code",
        )
    if result.detail is not None and not isinstance(result.detail, str):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_RESULT",
            "contract check detail must be a string or null",
        )
    if not isinstance(result.observed, Mapping):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_RESULT",
            "contract check observed data must be a mapping",
        )
    if not _is_json_safe_mapping(result.observed):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            "INVALID_CHECK_RESULT",
            "contract check observed data must be JSON serializable",
        )
    if not result.passed and result.error_code is None:
        return CheckResult.failure(
            result.check_id,
            fallback_code if is_valid_error_code(fallback_code) else "INVALID_ERROR_CODE",
            result.detail or "contract check failed",
        )
    return result


def _validate_business_execution(contract: PipelineContract, business: BusinessExecution) -> None:
    _validate_business_payload(business)
    _validate_noop_reason(business)
    if business.status is ResultStatus.SUCCESS:
        output_ids = [item.output_id for item in business.outputs]
        if len(output_ids) != len(set(output_ids)):
            raise ContractError("DUPLICATE_OUTPUT_RESULT")
        by_id = {item.output_id: item for item in business.outputs}
        expected_ids = {item.output_id for item in contract.outputs}
        missing = expected_ids - set(by_id)
        if missing:
            raise ContractError("MISSING_OUTPUT_RESULT", ", ".join(sorted(missing)))
        unexpected = set(by_id) - expected_ids
        if unexpected:
            raise ContractError("UNKNOWN_OUTPUT_RESULT", ", ".join(sorted(unexpected)))
        incomplete = [item.output_id for item in business.outputs if not item.completed]
        if incomplete:
            raise ContractError("INCOMPLETE_OUTPUT_RESULT", ", ".join(sorted(incomplete)))
        if business.metrics.rows_written != sum(item.rows_written for item in business.outputs):
            raise ContractError("ROWS_WRITTEN_MISMATCH")
        expected_locations = {item.output_id: item.location for item in contract.outputs}
        if any(item.location != expected_locations[item.output_id] for item in business.outputs):
            raise ContractError("OUTPUT_LOCATION_MISMATCH")
        empty_disallowed = [
            output.output_id
            for output in contract.outputs
            if not output.allow_empty and by_id[output.output_id].rows_written == 0
        ]
        if empty_disallowed:
            raise ContractError("EMPTY_OUTPUT_NOT_ALLOWED", ", ".join(sorted(empty_disallowed)))


def _validate_noop_reason(business: BusinessExecution) -> None:
    if business.status is ResultStatus.NOOP:
        if not isinstance(business.noop_reason, str) or not business.noop_reason.strip():
            raise ContractError("NOOP_REASON_REQUIRED", "NOOP result requires a non-empty reason")
    elif business.noop_reason is not None:
        raise ContractError("NOOP_REASON_FORBIDDEN", "noop_reason is only valid for NOOP results")


def _validate_business_payload(business: object) -> None:
    if not isinstance(business, BusinessExecution):
        raise ContractError("INVALID_EXECUTOR_RETURN", "executor must return BusinessExecution")
    if not isinstance(business.status, ResultStatus):
        raise ContractError("INVALID_BUSINESS_STATUS")
    _validate_metrics(business.metrics)
    _validate_diagnostics(business.diagnostics)
    if not isinstance(business.outputs, tuple):
        raise ContractError("INVALID_OUTPUT_RESULT", "outputs must be a tuple")
    for output in business.outputs:
        _validate_output_result(output)
    if business.noop_reason is not None and not isinstance(business.noop_reason, str):
        raise ContractError("INVALID_BUSINESS_PAYLOAD", "noop_reason must be a string or null")


def _validate_metrics(metrics: PipelineMetrics) -> None:
    if not isinstance(metrics, PipelineMetrics):
        raise ContractError("INVALID_PIPELINE_METRICS", "metrics must be PipelineMetrics")
    integer_fields = (
        "rows_read",
        "rows_written",
        "assets_processed",
        "dates_processed",
        "retries",
    )
    for field_name in integer_fields:
        value = getattr(metrics, field_name)
        if not _is_json_safe_non_negative_integer(value):
            raise ContractError("INVALID_PIPELINE_METRICS", field_name)
    if not _is_json_safe_finite_non_negative_number(metrics.database_write_seconds):
        raise ContractError("INVALID_PIPELINE_METRICS", "database_write_seconds")
    if not isinstance(metrics.stage_durations_seconds, Mapping):
        raise ContractError("INVALID_PIPELINE_METRICS", "stage_durations_seconds")
    try:
        stage_durations = dict(metrics.stage_durations_seconds)
    except Exception as exc:
        raise ContractError("INVALID_PIPELINE_METRICS", "stage_durations_seconds") from exc
    if not _is_strict_json_serializable(stage_durations):
        raise ContractError("INVALID_PIPELINE_METRICS", "stage_durations_seconds")
    for stage_name, duration in stage_durations.items():
        if not isinstance(stage_name, str) or not stage_name.strip():
            raise ContractError("INVALID_PIPELINE_METRICS", "stage name")
        if not _is_json_safe_finite_non_negative_number(duration):
            raise ContractError("INVALID_PIPELINE_METRICS", stage_name)
    for field_name in ("api_requests", "batches", "peak_rss_kb", "temporary_disk_bytes"):
        value = getattr(metrics, field_name)
        if value is not None and not _is_json_safe_non_negative_integer(value):
            raise ContractError("INVALID_PIPELINE_METRICS", field_name)


def _validate_output_result(output: OutputResult) -> None:
    if not isinstance(output, OutputResult):
        raise ContractError("INVALID_OUTPUT_RESULT", "outputs must contain OutputResult values")
    if not isinstance(output.output_id, str) or not output.output_id.strip():
        raise ContractError("INVALID_OUTPUT_RESULT", "output_id")
    if not _is_json_safe_non_negative_integer(output.rows_written):
        raise ContractError("INVALID_OUTPUT_METRICS", output.output_id)
    if not isinstance(output.location, str) or not output.location.strip():
        raise ContractError("INVALID_OUTPUT_RESULT", output.output_id)
    if not isinstance(output.completed, bool):
        raise ContractError("INVALID_OUTPUT_RESULT", output.output_id)
    if not isinstance(output.detail, Mapping) or not _is_json_safe_mapping(output.detail):
        raise ContractError("INVALID_OUTPUT_RESULT", output.output_id)


def _validate_diagnostics(diagnostics: tuple[PipelineDiagnostic, ...]) -> None:
    if not isinstance(diagnostics, tuple):
        raise ContractError("INVALID_DIAGNOSTIC", "diagnostics must be a tuple")
    for diagnostic in diagnostics:
        if not isinstance(diagnostic, PipelineDiagnostic):
            raise ContractError("INVALID_DIAGNOSTIC")
        if not is_valid_error_code(diagnostic.code):
            raise ContractError("INVALID_ERROR_CODE", "diagnostic code")
        if not isinstance(diagnostic.level, DiagnosticLevel):
            raise ContractError("INVALID_DIAGNOSTIC", "level")
        if not isinstance(diagnostic.message, str) or not diagnostic.message.strip():
            raise ContractError("INVALID_DIAGNOSTIC", "message")
        if not isinstance(diagnostic.detail, Mapping) or not _is_json_safe_mapping(diagnostic.detail):
            raise ContractError("INVALID_DIAGNOSTIC", "detail")


def _is_strict_json_serializable(value: object) -> bool:
    try:
        json.dumps(value, allow_nan=False)
    except Exception:
        return False
    return True


def _is_json_safe_non_negative_integer(value: object) -> bool:
    if not isinstance(value, Integral) or isinstance(value, bool):
        return False
    try:
        if value < 0:
            return False
        return _is_strict_json_serializable(value)
    except Exception:
        return False


def _is_json_safe_finite_non_negative_number(value: object) -> bool:
    if not isinstance(value, Real) or isinstance(value, bool):
        return False
    if not _is_strict_json_serializable(value):
        return False
    try:
        return value >= 0 and math.isfinite(float(value))
    except Exception:
        return False


def _first_failure(checks: tuple[CheckResult, ...]) -> CheckResult | None:
    return next((item for item in checks if not item.passed), None)


def _diagnostic_for_check(check: CheckResult) -> PipelineDiagnostic:
    code = check.error_code or "CONTRACT_CHECK_FAILED"
    if not is_valid_error_code(code):
        code = "INVALID_ERROR_CODE"
    return PipelineDiagnostic(
        code=code,
        level=DiagnosticLevel.ERROR,
        message=check.detail or f"contract check {check.check_id} failed",
        detail={"check_id": check.check_id, "observed": dict(check.observed)},
    )


def _failure_result(
    invocation: PipelineInvocation,
    target_window: TargetWindow,
    started_at: datetime,
    started: float,
    *,
    contract: PipelineContract,
    input_checks: tuple[CheckResult, ...] = (),
    freshness_checks: tuple[CheckResult, ...] = (),
    completion_checks: tuple[CheckResult, ...] = (),
    business: BusinessExecution | None = None,
    diagnostic: PipelineDiagnostic,
) -> PipelineResult:
    outcome = _safe_business_outcome(business)
    return _result(
        invocation,
        target_window,
        started_at,
        started,
        contract,
        status=ResultStatus.FAILED,
        business=outcome,
        input_checks=input_checks,
        freshness_checks=freshness_checks,
        completion_checks=completion_checks,
        diagnostics=_sanitize_diagnostics(outcome.diagnostics)
        + (diagnostic,)
        + _performance_diagnostics(contract, started),
    )


def _result(
    invocation: PipelineInvocation,
    target_window: TargetWindow,
    started_at: datetime,
    started: float,
    contract: PipelineContract,
    *,
    status: ResultStatus,
    business: BusinessExecution,
    input_checks: tuple[CheckResult, ...],
    freshness_checks: tuple[CheckResult, ...],
    completion_checks: tuple[CheckResult, ...],
    diagnostics: tuple[PipelineDiagnostic, ...],
) -> PipelineResult:
    completed_at = datetime.now(UTC)
    duration = max(0.0, time.monotonic() - started)
    performance = _performance_result(contract, duration)
    return PipelineResult(
        run_id=invocation.run_id,
        pipeline_id=invocation.pipeline_id,
        status=status,
        target_window=target_window,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration,
        attempt=invocation.attempt,
        metrics=business.metrics,
        outputs=business.outputs,
        input_checks=input_checks,
        freshness_checks=freshness_checks,
        completion_checks=completion_checks,
        performance=performance,
        diagnostics=_sanitize_diagnostics(diagnostics),
        noop_reason=business.noop_reason if status is ResultStatus.NOOP else None,
    )


def _performance_result(contract: PipelineContract, duration: float) -> PerformanceResult:
    budget = contract.performance
    return PerformanceResult(
        duration_seconds=duration,
        warning_threshold_seconds=budget.warning_threshold_seconds,
        normal_budget_seconds=budget.normal_budget_seconds,
        within_warning_threshold=duration <= budget.warning_threshold_seconds,
        within_normal_budget=duration <= budget.normal_budget_seconds,
    )


def _performance_diagnostics(contract: PipelineContract, started: float) -> tuple[PipelineDiagnostic, ...]:
    duration = max(0.0, time.monotonic() - started)
    budget = contract.performance
    if duration > budget.normal_budget_seconds:
        return (
            PipelineDiagnostic(
                code="PERFORMANCE_BUDGET_EXCEEDED",
                level=DiagnosticLevel.WARNING,
                message="pipeline duration exceeded its normal performance budget",
                detail={"duration_seconds": duration, "baseline_source": budget.baseline_source},
            ),
        )
    if duration > budget.warning_threshold_seconds:
        return (
            PipelineDiagnostic(
                code="PERFORMANCE_WARNING_THRESHOLD_EXCEEDED",
                level=DiagnosticLevel.WARNING,
                message="pipeline duration exceeded its warning threshold",
                detail={"duration_seconds": duration, "baseline_source": budget.baseline_source},
            ),
        )
    return ()


def _contract_error_detail(exc: ContractError) -> dict[str, object]:
    detail: dict[str, object] = {}
    if exc.detail is not None:
        detail["contract_error_detail"] = exc.detail if isinstance(exc.detail, str) else repr(exc.detail)
    if not is_valid_error_code(exc.code):
        detail["invalid_error_code"] = repr(exc.code)
    return detail


def _safe_business_outcome(business: object) -> BusinessExecution:
    """Keep only a structurally safe payload when assembling a FAILED result."""

    if not isinstance(business, BusinessExecution):
        return BusinessExecution(status=ResultStatus.FAILED)
    try:
        _validate_business_payload(business)
        _validate_noop_reason(business)
    except Exception:
        return BusinessExecution(status=ResultStatus.FAILED)
    return business


def _is_json_safe_mapping(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        json.dumps(dict(value), allow_nan=False)
    except Exception:
        return False
    return True


def _sanitize_diagnostics(diagnostics: object) -> tuple[PipelineDiagnostic, ...]:
    if not isinstance(diagnostics, tuple):
        return (
            PipelineDiagnostic(
                code="INVALID_DIAGNOSTIC",
                level=DiagnosticLevel.ERROR,
                message="business executor emitted invalid diagnostics",
            ),
        )
    sanitized: list[PipelineDiagnostic] = []
    for diagnostic in diagnostics:
        if (
            isinstance(diagnostic, PipelineDiagnostic)
            and is_valid_error_code(diagnostic.code)
            and isinstance(diagnostic.level, DiagnosticLevel)
            and isinstance(diagnostic.message, str)
            and diagnostic.message.strip()
            and isinstance(diagnostic.detail, Mapping)
            and _is_json_safe_mapping(diagnostic.detail)
        ):
            sanitized.append(diagnostic)
        else:
            sanitized.append(
                PipelineDiagnostic(
                    code="INVALID_DIAGNOSTIC",
                    level=DiagnosticLevel.ERROR,
                    message="business executor emitted an invalid diagnostic",
                )
            )
    return tuple(sanitized)
