"""Public acceptance tests for the formal Pipeline development contract."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.pipeline.examples.contract_template import CONTRACT_TEMPLATE_EXAMPLE as CONTRACT_TEMPLATE
from qrp_atlas.pipeline.contract_validation import ContractValidationError, validate_contracts
from qrp_atlas.pipeline.contracts import (
    BusinessExecution,
    CheckResult,
    InputContract,
    OutputResult,
    ParameterContract,
    ParameterType,
    PipelineMetrics,
    PipelineKind,
    ResultStatus,
)
from qrp_atlas.pipeline.registry import PipelineRegistry
from qrp_atlas.pipeline.runtime.cli import main as pipeline_cli
from qrp_atlas.pipeline.runtime.contract_adapter import (
    ContractDeploymentSelection,
    definitions_from_contract_selections,
    load_contract_selections,
)
from qrp_atlas.pipeline.runtime.store import PipelineRuntimeStore
from qrp_atlas.pipeline.testing import ContractTestHarness, assert_contract_result_matches_context


def settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
        },
        project_root=tmp_path / "repo",
    )


def test_template_contract_executes_without_io(tmp_path: Path) -> None:
    result = ContractTestHarness(CONTRACT_TEMPLATE, settings(tmp_path)).run(trade_date=date(2026, 7, 29))

    assert_contract_result_matches_context(result, CONTRACT_TEMPLATE)
    assert result.status is ResultStatus.NOOP
    assert result.noop_reason == "TEMPLATE_NOT_DEPLOYED"
    assert result.target_window.target_date == date(2026, 7, 29)
    assert not (tmp_path / "home").exists()
    assert not (tmp_path / "data").exists()


def test_contract_validator_requires_canonical_lock_for_managed_database() -> None:
    output = replace(CONTRACT_TEMPLATE.outputs[0], physical_resource="quant_db")
    invalid = replace(CONTRACT_TEMPLATE, outputs=(output,))

    with pytest.raises(ContractValidationError, match="quant_db_writer"):
        validate_contracts((invalid,))


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


def test_default_registry_contains_only_admitted_market_contracts_and_never_the_template(tmp_path: Path, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    assert pipeline_cli(["validate-contracts"]) == 0
    assert capsys.readouterr().out == "valid contracts: 6\n"
    assert pipeline_cli(["list-contracts"]) == 0
    contracts = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {contract["pipeline_id"] for contract in contracts} == {
        "market_daily_update",
        "adj_factor_daily",
        "daily_basic_update",
        "index_daily_update",
        "zt_dt_pool_daily",
        "suspend_d_ingest",
    }
    assert pipeline_cli(["--runtime-dir", str(runtime_dir), "run", "contract_template_example"]) == 2
    assert "unknown formal pipeline" in capsys.readouterr().err
    assert not (runtime_dir / "pipeline_runtime.sqlite3").exists()


def test_cli_contract_validation_is_config_free(capsys) -> None:
    assert pipeline_cli(["validate-contracts"]) == 0
    assert capsys.readouterr().out == "valid contracts: 6\n"
