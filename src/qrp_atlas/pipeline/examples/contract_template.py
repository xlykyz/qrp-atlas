"""Test-only example showing the minimum formal Pipeline implementation.

This module is deliberately absent from ``contract_catalog.CONTRACT_MODULES``.
It performs no I/O and never becomes discoverable by the production CLI.
"""

from __future__ import annotations

from ..contracts import (
    BusinessExecution,
    CheckResult,
    CompletionContract,
    ExecutionPolicy,
    FreshnessContract,
    IdempotencyContract,
    InputContract,
    InputKind,
    NonTradingDayPolicy,
    OutputContract,
    PerformanceBudget,
    PipelineContract,
    PipelineKind,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from ..runtime.models import OverlapPolicy


def _fixture_target_date(invocation) -> TargetWindow:
    return TargetWindow.for_date(invocation.scheduled_for.date())


def _fixture_calendar_accepts(_target_date, _invocation) -> bool:
    return True


def _fixture_input_structure(context) -> CheckResult:
    return CheckResult.success("fixture_input_structure", target=context.target_window.as_dict())


def _fixture_input_freshness(context) -> CheckResult:
    return CheckResult.success("fixture_input_freshness", target=context.target_window.as_dict())


def _fixture_completion(context) -> CheckResult:
    return CheckResult.success("fixture_output_completion", target=context.target_window.as_dict())


def _fixture_quality(context) -> CheckResult:
    return CheckResult.success("fixture_output_quality", target=context.target_window.as_dict())


def execute_template(_context) -> BusinessExecution:
    return BusinessExecution.noop("TEMPLATE_NOT_DEPLOYED")


CONTRACT_TEMPLATE_EXAMPLE = PipelineContract(
    pipeline_id="contract_template_example",
    name="Pipeline contract template example",
    description="Test-only source template; it never accesses data or external services.",
    contract_version="1",
    kind=PipelineKind.ATOMIC,
    executor=execute_template,
    target_date_policy=TargetDatePolicy(
        policy_id="fixture_scheduled_date",
        description="Uses the runtime scheduled instant only in this test-only template.",
        trading_calendar_id="fixture_exchange_calendar",
        non_trading_day_policy=NonTradingDayPolicy.REJECT,
        resolver=_fixture_target_date,
        validate_explicit_date=_fixture_calendar_accepts,
    ),
    parameters=(),
    inputs=(
        InputContract(
            input_id="fixture_input",
            kind=InputKind.FILE,
            source="tmp_path fixture supplied by contract tests",
            required_fields=("trade_date", "value"),
            target_date_semantics="fixture target date",
            missing_error_code="INPUT_MISSING",
            structure_check=_fixture_input_structure,
            freshness=FreshnessContract(
                check_id="fixture_input_freshness",
                target_date_semantics="fixture target date",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.REJECT,
                error_code="INPUT_STALE",
                checker=_fixture_input_freshness,
            ),
        ),
    ),
    outputs=(
        OutputContract(
            output_id="fixture_output",
            physical_resource="contract_template_fixture",
            location="tmp_path / contract-template",
            object_name="fixture_output",
            unique_key=("trade_date",),
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="fixture target date",
            completion=CompletionContract(
                marker="fixture completion check",
                error_code="COMPLETION_MISSING",
                checker=_fixture_completion,
            ),
            quality_checks=(_fixture_quality,),
            allow_empty=False,
        ),
    ),
    dependencies=(),
    resource_locks=("contract_template_fixture_writer",),
    idempotency=IdempotencyContract(
        idempotency_key="fixture_output.trade_date",
        repeat_run_semantics="same target date replaces the fixture output atomically",
        existing_target_handling="replace target date",
        failure_recovery="rerun the same target date after fixture cleanup",
        uses_staging=True,
        atomic_replace_boundary="fixture target date",
    ),
    transaction=TransactionContract(
        mode=TransactionMode.STAGING_ATOMIC_REPLACE,
        boundary="fixture target date",
        failure_visibility="unfinished staging is never considered a fixture output",
    ),
    execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=0),
    performance=PerformanceBudget(
        normal_budget_seconds=1.0,
        warning_threshold_seconds=0.5,
        hard_timeout_seconds=5,
        benchmark_scope="unit fixture: one target date and one synthetic record",
        baseline_source="tests/pipeline/test_pipeline_contract.py::test_template_contract_executes_without_io",
    ),
)
