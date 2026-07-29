"""Common outer lifecycle for source-registered Pipeline contracts."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from .contracts import (
    BusinessExecution,
    CheckResult,
    ContractCheck,
    ContractError,
    DiagnosticLevel,
    PipelineContract,
    PipelineDiagnostic,
    PipelineInvocation,
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
    responsibility of ``pipeline.runtime``.  This function intentionally never
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
        target_window = _resolve_target_window(contract, invocation)
        parameters = parse_parameter_overrides(contract, invocation.parameter_overrides)
        context = PipelineRunContext(
            run_id=invocation.run_id,
            pipeline_id=contract.pipeline_id,
            scheduled_for=invocation.scheduled_for,
            attempt=invocation.attempt,
            settings=invocation.settings,
            parameter_overrides=parameters,
            target_window=target_window,
            audit_context=invocation.audit_context,
        )
        input_checks = tuple(
            _run_check(item.structure_check, context, fallback_code=item.missing_error_code)
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
            _run_check(item.freshness.checker, context, fallback_code=item.freshness.error_code)
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

        business = contract.executor(context)
        if not isinstance(business, BusinessExecution):
            raise ContractError("INVALID_EXECUTOR_RETURN", "executor must return BusinessExecution")
        _validate_business_execution(contract, business)
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
                _run_check(output.completion.checker, context, fallback_code=output.completion.error_code)
                for output in contract.outputs
            ) + tuple(
                result
                for output in contract.outputs
                for result in (
                    _run_check(check, context, fallback_code="OUTPUT_QUALITY_CHECK_FAILED")
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
                code=exc.code,
                level=DiagnosticLevel.ERROR,
                message="formal Pipeline contract rejected the execution",
                detail={"contract_error_detail": exc.detail} if exc.detail else {},
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
    if invocation.trade_date_override is not None:
        if not contract.target_date_policy.validate_explicit_date(invocation.trade_date_override, invocation):
            raise ContractError("TARGET_DATE_OVERRIDE_INVALID")
        return TargetWindow.for_date(invocation.trade_date_override)
    window = contract.target_date_policy.resolver(invocation)
    if not isinstance(window, TargetWindow):
        raise ContractError("INVALID_TARGET_DATE_RESOLUTION")
    return window


def _fallback_target_window(invocation: PipelineInvocation) -> TargetWindow:
    return TargetWindow.for_date(invocation.trade_date_override or invocation.scheduled_for.date())


def _run_check(check: ContractCheck, context: PipelineRunContext, *, fallback_code: str) -> CheckResult:
    try:
        result = check(context)
    except Exception as exc:
        return CheckResult.failure(
            "CHECK_EXCEPTION",
            fallback_code,
            f"contract check raised {type(exc).__name__}",
        )
    if not isinstance(result, CheckResult):
        return CheckResult.failure(
            "CHECK_INVALID_RETURN",
            fallback_code,
            "contract check must return CheckResult",
        )
    if not result.passed and result.error_code is None:
        return CheckResult.failure(result.check_id, fallback_code, result.detail or "contract check failed")
    return result


def _validate_business_execution(contract: PipelineContract, business: BusinessExecution) -> None:
    if business.status is ResultStatus.NOOP and not business.noop_reason:
        raise ContractError("NOOP_REASON_REQUIRED", "NOOP result requires a reason")
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
        if any(item.rows_written < 0 for item in business.outputs):
            raise ContractError("INVALID_OUTPUT_METRICS")
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
    numeric_values = (
        business.metrics.rows_read,
        business.metrics.rows_written,
        business.metrics.assets_processed,
        business.metrics.dates_processed,
        business.metrics.database_write_seconds,
        business.metrics.retries,
    )
    if any(value < 0 for value in numeric_values):
        raise ContractError("INVALID_PIPELINE_METRICS")


def _first_failure(checks: tuple[CheckResult, ...]) -> CheckResult | None:
    return next((item for item in checks if not item.passed), None)


def _diagnostic_for_check(check: CheckResult) -> PipelineDiagnostic:
    return PipelineDiagnostic(
        code=check.error_code or "CONTRACT_CHECK_FAILED",
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
    outcome = business or BusinessExecution(status=ResultStatus.FAILED)
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
        diagnostics=outcome.diagnostics + (diagnostic,) + _performance_diagnostics(contract, started),
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
        diagnostics=diagnostics,
        noop_reason=business.noop_reason,
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
