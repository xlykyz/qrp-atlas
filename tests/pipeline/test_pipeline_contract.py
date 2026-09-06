"""Public acceptance tests for the formal Pipeline development contract."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.examples.contract_template import CONTRACT_TEMPLATE_EXAMPLE as CONTRACT_TEMPLATE
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.cninfo_contracts import CNINFO_CONTRACTS
from qrp_atlas.pipeline.irm_qa_contracts import IRM_CONTRACTS
from qrp_atlas.pipeline.membership_contracts import MEMBERSHIP_CONTRACTS
from qrp_atlas.pipeline.pit_fundamentals_contracts import PIT_FUNDAMENTALS_CONTRACTS
from qrp_atlas.pipeline.research_report_contracts import RESEARCH_REPORT_CONTRACTS
from qrp_atlas.pipeline.research_industry_contracts import RESEARCH_INDUSTRY_CONTRACTS
from qrp_atlas.pipeline.system_b_contracts import SYSTEM_B_CONTRACTS
from qrp_atlas.pipeline.theme_contracts import THEME_M4_PRODUCTION_CONTRACT
from qrp_atlas.pipeline.theme_m5_contracts import THEME_M5_PRODUCTION_CONTRACT
from qrp_atlas.pipeline.dc_hot_contracts import DC_HOT_CONTRACTS
from qrp_atlas.pipeline.ths_hot_contracts import THS_HOT_CONTRACTS
from qrp_atlas.pipeline.market_m6_contracts import MARKET_M6_PRODUCTION_CONTRACT
from qrp_atlas.pipeline.contracts import (
    BusinessExecution,
    CheckResult,
    ContractError,
    DiagnosticLevel,
    InputKind,
    InputContract,
    OutputResult,
    ParameterContract,
    ParameterType,
    PipelineDiagnostic,
    PipelineInvocation,
    PipelineMetrics,
    PipelineKind,
    ResultStatus,
)
from qrp_atlas.pipeline.registry import PipelineRegistry
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.jobs_cli import main as pipeline_cli
from qrp_atlas.pipeline.job_adapter import (
    ContractDeploymentSelection,
    definitions_from_contract_selections,
    load_contract_selections,
    make_in_process_contract_executor,
)
from qrp_atlas.orchestration.models import JobRun, JobStatus
from qrp_atlas.orchestration.store import JobRuntimeStore
from qrp_atlas.pipeline.testing import ContractTestHarness, assert_contract_result_matches_context


def settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
        },
        project_root=tmp_path / "repo",
    )


def assert_failed_result_is_json_safe(result) -> None:
    assert result.status is ResultStatus.FAILED
    json.dumps(result.as_dict(), allow_nan=False)


def claimed_run(*, job_id: str, execution_control: object = None) -> JobRun:
    return JobRun(
        run_id="pipeline-contract-test-run",
        job_id=job_id,
        definition_version="test",
        scheduled_at=datetime(2026, 7, 29, tzinfo=UTC),
        started_at=None,
        finished_at=None,
        status=JobStatus.RUNNING,
        attempt=1,
        exit_code=None,
        timed_out=False,
        trigger_type="manual",
        stdout_path=None,
        stderr_path=None,
        error_summary=None,
        heartbeat_at=None,
        execution_control=execution_control,
    )


def test_template_contract_executes_without_io(tmp_path: Path) -> None:
    result = ContractTestHarness(CONTRACT_TEMPLATE, settings(tmp_path)).run(trade_date=date(2026, 7, 29))

    assert_contract_result_matches_context(result, CONTRACT_TEMPLATE)
    assert result.status is ResultStatus.NOOP
    assert result.noop_reason == "TEMPLATE_NOT_DEPLOYED"
    assert result.target_window.target_date == date(2026, 7, 29)
    assert not (tmp_path / "home").exists()
    assert not (tmp_path / "data").exists()


def test_execution_control_is_passed_unchanged_to_run_context(tmp_path: Path) -> None:
    observed: list[ExecutionControl] = []

    def executor(context) -> BusinessExecution:
        observed.append(context.execution_control)
        return BusinessExecution.noop("TEST_NOOP")

    contract = replace(CONTRACT_TEMPLATE, executor=executor)
    validate_contracts((contract,))
    control = ExecutionControl()
    result = execute_pipeline_contract(
        contract,
        PipelineInvocation(
            run_id="execution-control-test",
            pipeline_id=contract.pipeline_id,
            scheduled_for=datetime(2026, 7, 29, tzinfo=UTC),
            attempt=1,
            settings=settings(tmp_path),
            execution_control=control,
        ),
    )

    assert result.status is ResultStatus.NOOP
    assert observed == [control]


@pytest.mark.parametrize("status", (ResultStatus.SUCCESS, ResultStatus.FAILED))
def test_runtime_rejects_noop_reason_for_success_and_failed_results(
    tmp_path: Path,
    status: ResultStatus,
) -> None:
    def executor(_context) -> BusinessExecution:
        if status is ResultStatus.SUCCESS:
            return BusinessExecution(
                status=status,
                metrics=PipelineMetrics(rows_written=1),
                outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
                noop_reason="invalid reason",
            )
        return BusinessExecution(status=status, noop_reason="invalid reason")

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.noop_reason is None
    assert result.diagnostics[-1].code == "NOOP_REASON_FORBIDDEN"


def test_runtime_rejects_noop_without_a_non_empty_reason(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution(status=ResultStatus.NOOP, noop_reason="  ")

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.noop_reason is None
    assert result.diagnostics[-1].code == "NOOP_REASON_REQUIRED"


@pytest.mark.parametrize("invalid_control", (None, object()))
def test_in_process_executor_rejects_missing_or_wrong_execution_control(
    invalid_control: object,
) -> None:
    executor = make_in_process_contract_executor(CONTRACT_TEMPLATE)

    with pytest.raises(TypeError, match="ExecutionControl"):
        executor(claimed_run(job_id=CONTRACT_TEMPLATE.pipeline_id, execution_control=invalid_control))


def test_in_process_executor_preserves_runner_execution_control_identity(tmp_path: Path) -> None:
    observed: list[ExecutionControl] = []

    def executor(context) -> BusinessExecution:
        observed.append(context.execution_control)
        return BusinessExecution.noop("TEST_NOOP")

    contract = replace(CONTRACT_TEMPLATE, executor=executor)
    control = ExecutionControl()
    in_process_executor = make_in_process_contract_executor(
        contract,
        environment={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
        },
    )

    result = in_process_executor(claimed_run(job_id=contract.pipeline_id, execution_control=control))

    assert result.status is JobStatus.SUCCESS
    assert observed == [control]


def test_contract_validator_requires_canonical_lock_for_managed_database() -> None:
    output = replace(CONTRACT_TEMPLATE.outputs[0], physical_resource="quant_db")
    invalid = replace(CONTRACT_TEMPLATE, outputs=(output,))

    with pytest.raises(ContractValidationError, match="quant_db_writer"):
        validate_contracts((invalid,))


def test_contract_validator_rejects_table_scoped_duckdb_lock() -> None:
    output = replace(CONTRACT_TEMPLATE.outputs[0], physical_resource="quant_db")
    scoped = replace(
        CONTRACT_TEMPLATE,
        outputs=(output,),
        resource_locks=("duckdb://quant_db#fixture_output",),
    )

    with pytest.raises(ContractValidationError, match="database-wide writer lock"):
        validate_contracts((scoped,))


def test_contract_validator_accepts_scoped_duckdb_reads_and_validates_manual_execution_flag() -> None:
    contract = replace(
        CONTRACT_TEMPLATE,
        resource_reads=("duckdb://quant_db#fixture_input",),
        manual_execution_allowed=False,
    )

    assert validate_contracts((contract,)) == (contract,)
    assert contract.describe()["resource_reads"] == ["duckdb://quant_db#fixture_input"]
    assert contract.describe()["manual_execution_allowed"] is False


def test_contract_validator_rejects_malformed_duckdb_read_and_manual_flag() -> None:
    malformed_read = replace(CONTRACT_TEMPLATE, resource_reads=("duckdb://quant_db",))
    with pytest.raises(ContractValidationError, match="malformed DuckDB resource"):
        validate_contracts((malformed_read,))

    malformed_flag = replace(CONTRACT_TEMPLATE, manual_execution_allowed=1)
    with pytest.raises(ContractValidationError, match="manual_execution_allowed"):
        validate_contracts((malformed_flag,))


def test_contract_validator_rejects_malformed_declared_error_code() -> None:
    input_contract = replace(CONTRACT_TEMPLATE.inputs[0], missing_error_code="input missing")
    invalid = replace(CONTRACT_TEMPLATE, inputs=(input_contract,))

    with pytest.raises(ContractValidationError, match="ERROR_CODE_PATTERN"):
        validate_contracts((invalid,))


@pytest.mark.parametrize("upstream_pipeline_id", ("", "   ", "Bad-ID"))
def test_contract_validator_rejects_invalid_upstream_pipeline_id(upstream_pipeline_id: str) -> None:
    input_contract = replace(
        CONTRACT_TEMPLATE.inputs[0],
        upstream_pipeline_id=upstream_pipeline_id,
    )
    invalid = replace(CONTRACT_TEMPLATE, inputs=(input_contract,))

    with pytest.raises(ContractValidationError, match="stable pipeline identifier"):
        validate_contracts((invalid,))


def test_contract_validator_accepts_valid_upstream_pipeline_id_with_dependency() -> None:
    upstream = replace(CONTRACT_TEMPLATE, pipeline_id="upstream_pipeline")
    downstream_input = replace(
        CONTRACT_TEMPLATE.inputs[0],
        kind=InputKind.UPSTREAM_PIPELINE,
        upstream_pipeline_id="upstream_pipeline",
    )
    downstream = replace(
        CONTRACT_TEMPLATE,
        pipeline_id="downstream_pipeline",
        inputs=(downstream_input,),
        dependencies=("upstream_pipeline",),
    )

    assert validate_contracts((upstream, downstream)) == (upstream, downstream)


@pytest.mark.parametrize(
    ("performance_update", "message"),
    (
        ({"warning_threshold_seconds": "30"}, "warning_threshold_seconds"),
        ({"normal_budget_seconds": "60"}, "normal budget"),
        ({"hard_timeout_seconds": "120"}, "performance timeout"),
        ({"warning_threshold_seconds": math.nan}, "warning_threshold_seconds"),
        ({"normal_budget_seconds": math.inf}, "normal budget"),
        ({"hard_timeout_seconds": -math.inf}, "performance timeout"),
    ),
)
def test_contract_validator_aggregates_invalid_performance_values(
    performance_update: dict[str, object],
    message: str,
) -> None:
    invalid = replace(
        CONTRACT_TEMPLATE,
        performance=replace(CONTRACT_TEMPLATE.performance, **performance_update),
    )

    with pytest.raises(ContractValidationError, match=message):
        validate_contracts((invalid,))


@pytest.mark.parametrize(
    "field_name",
    ("warning_threshold_seconds", "normal_budget_seconds", "hard_timeout_seconds"),
)
def test_contract_validator_rejects_huge_performance_values(field_name: str) -> None:
    huge_value = 10**10000
    invalid = replace(
        CONTRACT_TEMPLATE,
        performance=replace(CONTRACT_TEMPLATE.performance, **{field_name: huge_value}),
    )

    with pytest.raises(ContractValidationError):
        validate_contracts((invalid,))


@pytest.mark.parametrize("field_name", ("parameters", "inputs", "outputs"))
def test_contract_validator_requires_tuple_collections(field_name: str) -> None:
    invalid = replace(CONTRACT_TEMPLATE, **{field_name: []})

    with pytest.raises(ContractValidationError):
        validate_contracts((invalid,))


@pytest.mark.parametrize(
    "invalid",
    (
        object(),
        replace(CONTRACT_TEMPLATE, parameters=(None,)),
        replace(CONTRACT_TEMPLATE, inputs=(None,)),
        replace(CONTRACT_TEMPLATE, outputs=(None,)),
        replace(CONTRACT_TEMPLATE, inputs=(replace(CONTRACT_TEMPLATE.inputs[0], freshness=None),)),
        replace(CONTRACT_TEMPLATE, outputs=(replace(CONTRACT_TEMPLATE.outputs[0], completion=None),)),
    ),
)
def test_contract_validator_rejects_invalid_nested_contract_objects(invalid: object) -> None:
    with pytest.raises(ContractValidationError):
        validate_contracts((invalid,))


@pytest.mark.parametrize(
    "invalid",
    (
        replace(CONTRACT_TEMPLATE, kind="ATOMIC"),
        replace(
            CONTRACT_TEMPLATE,
            parameters=(
                ParameterContract(
                    name="batch_size",
                    parameter_type="INTEGER",
                    description="Fixture batch size",
                ),
            ),
        ),
        replace(
            CONTRACT_TEMPLATE,
            parameters=(
                ParameterContract(
                    name="batch_size",
                    parameter_type=ParameterType.INTEGER,
                    description="Fixture batch size",
                    required=1,
                ),
            ),
        ),
        replace(CONTRACT_TEMPLATE, inputs=(replace(CONTRACT_TEMPLATE.inputs[0], kind="TABLE"),)),
        replace(
            CONTRACT_TEMPLATE,
            inputs=(
                replace(
                    CONTRACT_TEMPLATE.inputs[0],
                    required_fields=["trade_date"],
                ),
            ),
        ),
        replace(
            CONTRACT_TEMPLATE,
            inputs=(
                replace(
                    CONTRACT_TEMPLATE.inputs[0],
                    freshness=replace(
                        CONTRACT_TEMPLATE.inputs[0].freshness,
                        non_trading_day_policy="REJECT",
                    ),
                ),
            ),
        ),
        replace(
            CONTRACT_TEMPLATE,
            outputs=(replace(CONTRACT_TEMPLATE.outputs[0], write_mode="UPSERT"),),
        ),
        replace(CONTRACT_TEMPLATE, outputs=(replace(CONTRACT_TEMPLATE.outputs[0], allow_empty=1),)),
        replace(
            CONTRACT_TEMPLATE,
            outputs=(replace(CONTRACT_TEMPLATE.outputs[0], unique_key=["trade_date"]),),
        ),
        replace(
            CONTRACT_TEMPLATE,
            outputs=(
                replace(
                    CONTRACT_TEMPLATE.outputs[0],
                    quality_checks=[CONTRACT_TEMPLATE.outputs[0].completion.checker],
                ),
            ),
        ),
        replace(
            CONTRACT_TEMPLATE,
            transaction=replace(CONTRACT_TEMPLATE.transaction, mode="READ_ONLY"),
        ),
        replace(
            CONTRACT_TEMPLATE,
            execution=replace(CONTRACT_TEMPLATE.execution, overlap_policy="FORBID"),
        ),
        replace(
            CONTRACT_TEMPLATE,
            idempotency=replace(CONTRACT_TEMPLATE.idempotency, uses_staging=1),
        ),
    ),
)
def test_contract_validator_rejects_invalid_mechanical_field_types(invalid: object) -> None:
    with pytest.raises(ContractValidationError):
        validate_contracts((invalid,))


def test_runtime_rejects_malformed_check_error_code(tmp_path: Path) -> None:
    def malformed(_context) -> CheckResult:
        return CheckResult.failure("fixture_input_structure", "not-valid", "malformed")

    input_contract = replace(CONTRACT_TEMPLATE.inputs[0], structure_check=malformed)
    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, inputs=(input_contract,)),
        settings(tmp_path),
    ).run()

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[-1].code == "INVALID_ERROR_CODE"


def test_runtime_rejects_unhashable_check_error_code_without_native_exception(tmp_path: Path) -> None:
    def malformed(_context) -> CheckResult:
        raise ContractError([], "malformed error code")

    input_contract = replace(CONTRACT_TEMPLATE.inputs[0], structure_check=malformed)
    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, inputs=(input_contract,)),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_ERROR_CODE"


def test_runtime_rejects_non_finite_metric(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(
                rows_written=1,
                stage_durations_seconds={"load": math.nan},
            ),
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
            diagnostics=(
                PipelineDiagnostic(
                    code="bad-code",
                    level=DiagnosticLevel.WARNING,
                    message="malformed diagnostic",
                ),
            ),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_PIPELINE_METRICS"
    assert all(diagnostic.code != "bad-code" for diagnostic in result.diagnostics)


@pytest.mark.parametrize("metric_field", ("rows_read", "database_write_seconds"))
def test_runtime_rejects_huge_metrics_as_strict_json_invalid(
    tmp_path: Path,
    metric_field: str,
) -> None:
    huge_value = 10**10000

    def executor(_context) -> BusinessExecution:
        metrics = PipelineMetrics(
            rows_written=1,
            **{metric_field: huge_value},
        )
        return BusinessExecution.success(
            metrics=metrics,
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_PIPELINE_METRICS"


def test_runtime_rejects_huge_stage_duration_as_strict_json_invalid(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(
                rows_written=1,
                stage_durations_seconds={"load": 10**10000},
            ),
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_PIPELINE_METRICS"


def test_runtime_rejects_huge_output_rows_as_strict_json_invalid(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(),
            outputs=(OutputResult("fixture_output", 10**10000, "tmp_path / contract-template", True),),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_OUTPUT_METRICS"


def test_runtime_rejects_malformed_business_diagnostic_code(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=1),
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
            diagnostics=(
                PipelineDiagnostic(
                    code="bad-code",
                    level=DiagnosticLevel.WARNING,
                    message="malformed diagnostic",
                ),
            ),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.diagnostics[-1].code == "INVALID_ERROR_CODE"
    assert all(diagnostic.code != "bad-code" for diagnostic in result.diagnostics)


def test_runtime_discards_malformed_output_detail_before_result_serialization(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=1),
            outputs=(
                OutputResult(
                    "fixture_output",
                    1,
                    "tmp_path / contract-template",
                    True,
                    detail={"invalid": math.inf},
                ),
            ),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.outputs == ()


def test_runtime_discards_malformed_diagnostic_detail_before_result_serialization(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=1),
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
            diagnostics=(
                PipelineDiagnostic(
                    code="BUSINESS_NOTE",
                    level=DiagnosticLevel.WARNING,
                    message="malformed diagnostic detail",
                    detail={"invalid": math.nan},
                ),
            ),
        )

    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, executor=executor),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.outputs == ()


def test_runtime_replaces_malformed_check_observed_before_result_serialization(tmp_path: Path) -> None:
    def malformed(_context) -> CheckResult:
        return CheckResult.success("fixture_input_structure", invalid=math.nan)

    input_contract = replace(CONTRACT_TEMPLATE.inputs[0], structure_check=malformed)
    result = ContractTestHarness(
        replace(CONTRACT_TEMPLATE, inputs=(input_contract,)),
        settings(tmp_path),
    ).run()

    assert_failed_result_is_json_safe(result)
    assert result.input_checks[0].error_code == "INVALID_CHECK_RESULT"


def test_input_freshness_failure_prevents_executor(tmp_path: Path) -> None:
    called = False

    def stale(_context) -> CheckResult:
        return CheckResult.failure("fixture_input_freshness", "INPUT_STALE", "fixture is stale")

    def executor(_context) -> BusinessExecution:
        nonlocal called
        called = True
        return BusinessExecution.success(
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
        )

    input_contract = replace(CONTRACT_TEMPLATE.inputs[0], freshness=replace(CONTRACT_TEMPLATE.inputs[0].freshness, checker=stale))
    contract = replace(CONTRACT_TEMPLATE, inputs=(input_contract,), executor=executor)
    result = ContractTestHarness(contract, settings(tmp_path)).run()

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[-1].code == "INPUT_STALE"
    assert not called


def test_success_requires_output_completion_and_records_metrics(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=3),
            outputs=(OutputResult("fixture_output", 3, "tmp_path / contract-template", True),),
        )

    result = ContractTestHarness(replace(CONTRACT_TEMPLATE, executor=executor), settings(tmp_path)).run()

    assert result.status is ResultStatus.SUCCESS
    assert all(check.passed for check in result.completion_checks)
    assert result.metrics.rows_read == 0
    assert result.metrics.rows_written == 3


def test_invalid_explicit_trade_date_returns_stable_failure(tmp_path: Path) -> None:
    def reject_date(_target_date, _invocation) -> bool:
        return False

    contract = replace(
        CONTRACT_TEMPLATE,
        target_date_policy=replace(CONTRACT_TEMPLATE.target_date_policy, validate_explicit_date=reject_date),
    )
    result = ContractTestHarness(contract, settings(tmp_path)).run(trade_date=date(2026, 7, 29))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[-1].code == "TARGET_DATE_OVERRIDE_INVALID"


def test_empty_output_keeps_its_stable_error_code_and_detail(tmp_path: Path) -> None:
    def executor(_context) -> BusinessExecution:
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=0),
            outputs=(OutputResult("fixture_output", 0, "tmp_path / contract-template", True),),
        )

    result = ContractTestHarness(replace(CONTRACT_TEMPLATE, executor=executor), settings(tmp_path)).run()

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[-1].code == "EMPTY_OUTPUT_NOT_ALLOWED"
    assert result.diagnostics[-1].detail == {"contract_error_detail": "fixture_output"}


def test_contract_owns_parameter_parsing_and_rejects_unknown_values(tmp_path: Path) -> None:
    observed: list[object] = []

    def executor(context) -> BusinessExecution:
        observed.append(context.parameter_overrides["batch_size"])
        return BusinessExecution.success(
            metrics=PipelineMetrics(rows_written=1),
            outputs=(OutputResult("fixture_output", 1, "tmp_path / contract-template", True),),
        )

    contract = replace(
        CONTRACT_TEMPLATE,
        executor=executor,
        parameters=(
            ParameterContract(
                name="batch_size",
                parameter_type=ParameterType.INTEGER,
                description="Fixture batch size",
                default=10,
            ),
        ),
    )
    harness = ContractTestHarness(contract, settings(tmp_path))
    assert harness.run(parameter_overrides={"batch_size": "4"}).status is ResultStatus.SUCCESS
    assert observed == [4]
    invalid = harness.run(parameter_overrides={"unexpected": "4"})
    assert invalid.status is ResultStatus.FAILED
    assert invalid.diagnostics[-1].code == "UNKNOWN_PARAMETER"


def test_dependency_cycles_and_missing_dependencies_fail_closed() -> None:
    first = replace(CONTRACT_TEMPLATE, pipeline_id="first_pipeline", dependencies=("second_pipeline",))
    second = replace(CONTRACT_TEMPLATE, pipeline_id="second_pipeline", dependencies=("first_pipeline",))
    with pytest.raises(ContractValidationError, match="dependency cycle"):
        validate_contracts((first, second))

    missing = replace(CONTRACT_TEMPLATE, pipeline_id="missing_dependency", dependencies=("not_registered",))
    with pytest.raises(ContractValidationError, match="missing dependencies"):
        validate_contracts((missing,))


def test_harness_validates_dependency_contracts_with_the_contract_under_test(tmp_path: Path) -> None:
    upstream = replace(CONTRACT_TEMPLATE, pipeline_id="upstream_pipeline")
    downstream = replace(CONTRACT_TEMPLATE, pipeline_id="downstream_pipeline", dependencies=("upstream_pipeline",))

    result = ContractTestHarness(
        downstream,
        settings(tmp_path),
        dependency_contracts=(upstream,),
    ).run()

    assert result.status is ResultStatus.NOOP


def test_top_level_dag_cannot_repeat_atomic_writes() -> None:
    dag = replace(CONTRACT_TEMPLATE, kind=PipelineKind.DAG, dependencies=("atomic_pipeline",))
    atomic = replace(CONTRACT_TEMPLATE, pipeline_id="atomic_pipeline")

    with pytest.raises(ContractValidationError, match="must aggregate dependencies"):
        validate_contracts((dag, atomic))


def test_deployment_selection_has_only_identity_enabled_and_schedule(tmp_path: Path) -> None:
    selections = tmp_path / "selections.json"
    selections.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pipelines": [
                    {
                        "pipeline_id": "contract_template_example",
                        "enabled": False,
                        "schedule": "15 16 * * 1-5",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    registry = PipelineRegistry()
    registry.register(CONTRACT_TEMPLATE)

    definitions = definitions_from_contract_selections(load_contract_selections(selections), registry=registry)
    assert definitions[0].command[-1] == "contract_template_example"
    assert definitions[0].dependencies == CONTRACT_TEMPLATE.dependencies
    assert definitions[0].resource_locks == CONTRACT_TEMPLATE.resource_locks
    assert definitions[0].requires_structured_result

    selections.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pipelines": [
                    {
                        "pipeline_id": "contract_template_example",
                        "enabled": False,
                        "schedule": "15 16 * * 1-5",
                        "timeout_seconds": 60,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="only pipeline_id, enabled, schedule"):
        load_contract_selections(selections)


def test_default_registry_contains_only_admitted_contracts_and_never_the_template(tmp_path: Path, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    assert pipeline_cli(["validate-contracts"]) == 0
    assert capsys.readouterr().out == "valid contracts: 34\n"
    assert pipeline_cli(["list-contracts"]) == 0
    contracts = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {contract["pipeline_id"] for contract in contracts} == {
        "market_daily_update",
        "adj_factor_daily",
        "etf_daily_update",
        "etf_adj_factor_update",
        "daily_basic_update",
        "index_daily_update",
        "index_basic_update",
        "stock_basic_update",
        "limit_step_ingest",
        "ths_daily_ingest",
        "stk_high_shock_ingest",
        "zt_dt_pool_daily",
        "suspend_d_ingest",
        THEME_M4_PRODUCTION_CONTRACT.pipeline_id,
        THEME_M5_PRODUCTION_CONTRACT.pipeline_id,
        MARKET_M6_PRODUCTION_CONTRACT.pipeline_id,
        *(contract.pipeline_id for contract in CNINFO_CONTRACTS),
        *(contract.pipeline_id for contract in IRM_CONTRACTS),
        *(contract.pipeline_id for contract in MEMBERSHIP_CONTRACTS),
        *(contract.pipeline_id for contract in PIT_FUNDAMENTALS_CONTRACTS),
        *(contract.pipeline_id for contract in SYSTEM_B_CONTRACTS),
        "system_b_asset_rank_daily",
        *(contract.pipeline_id for contract in RESEARCH_REPORT_CONTRACTS),
        *(contract.pipeline_id for contract in RESEARCH_INDUSTRY_CONTRACTS),
        *(contract.pipeline_id for contract in DC_HOT_CONTRACTS),
        *(contract.pipeline_id for contract in THS_HOT_CONTRACTS),
    }
    assert pipeline_cli(["--runtime-dir", str(runtime_dir), "run", "contract_template_example"]) == 2
    assert "unknown formal pipeline" in capsys.readouterr().err
    assert not (runtime_dir / "job_runtime.sqlite3").exists()


def test_cli_contract_validation_is_config_free(capsys) -> None:
    assert pipeline_cli(["validate-contracts"]) == 0
    assert capsys.readouterr().out == "valid contracts: 34\n"
