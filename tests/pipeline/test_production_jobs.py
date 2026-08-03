"""专项测试：Production JobDefinition 与多 Schedule 实例支持。"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from qrp_atlas.orchestration.cron import CronExpression
from qrp_atlas.orchestration.definitions import DefinitionValidationError
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.orchestration.models import JobRun, JobStatus
from qrp_atlas.orchestration.planning import dependency_plan
from qrp_atlas.orchestration.scheduler import JobScheduler
from qrp_atlas.orchestration.store import JobRuntimeStore
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import BusinessExecution
from qrp_atlas.pipeline.job_adapter import runtime_definition_from_production_job
from qrp_atlas.pipeline.production_jobs import (
    DEFAULT_PRODUCTION_JOBS_PATH,
    ProductionJobDefinition,
    load_production_jobs,
    resolve_instance_dependencies,
    validate_production_jobs,
)
from qrp_atlas.pipeline.registry import default_registry

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _job(
    job_id: str,
    pipeline_id: str = "research_stock_report_ingest",
    *,
    enabled: bool = False,
    schedule: str = "0 7 * * 1-5",
    timezone: str = "Asia/Shanghai",
    parameters: dict[str, str] | None = None,
) -> ProductionJobDefinition:
    return ProductionJobDefinition(
        job_id=job_id,
        pipeline_id=pipeline_id,
        enabled=enabled,
        schedule=schedule,
        timezone=timezone,
        parameters=parameters or {},
    )


def _manifest(tmp_path: Path, jobs: list[dict]) -> Path:
    path = tmp_path / "production-jobs.json"
    path.write_text(json.dumps({"schema_version": 1, "jobs": jobs}, ensure_ascii=False), encoding="utf-8")
    return path


def _registry():
    return default_registry()


def test_one_contract_can_be_referenced_by_two_job_definitions() -> None:
    jobs = (
        _job("research-stock-report-morning"),
        _job("research-stock-report-evening", schedule="0 19 * * 1-5"),
    )
    validated = validate_production_jobs(jobs, registry=_registry())
    assert {job.job_id for job in validated} == {
        "research-stock-report-morning",
        "research-stock-report-evening",
    }
    assert {job.pipeline_id for job in validated} == {"research_stock_report_ingest"}


def test_two_instances_keep_independent_job_ids() -> None:
    contract = _registry().get("research_stock_report_ingest")
    morning = runtime_definition_from_production_job(
        _job("research-stock-report-morning"), contract
    )
    evening = runtime_definition_from_production_job(
        _job("research-stock-report-evening", schedule="0 19 * * 1-5"), contract
    )
    assert morning.job_id == "research-stock-report-morning"
    assert evening.job_id == "research-stock-report-evening"
    assert morning.pipeline_id == evening.pipeline_id == "research_stock_report_ingest"
    # 业务语义仍然来自 Contract，而非实例层
    assert morning.resource_locks == contract.resource_locks
    assert morning.timeout_seconds == contract.performance.hard_timeout_seconds
    assert morning.definition_version == contract.contract_version


def test_duplicate_job_id_is_rejected(tmp_path: Path) -> None:
    path = _manifest(
        tmp_path,
        [
            {"job_id": "dup", "pipeline_id": "research_stock_report_ingest", "enabled": False,
             "schedule": "0 7 * * 1-5", "timezone": "Asia/Shanghai"},
            {"job_id": "dup", "pipeline_id": "research_stock_report_ingest", "enabled": False,
             "schedule": "0 19 * * 1-5", "timezone": "Asia/Shanghai"},
        ],
    )
    with pytest.raises(DefinitionValidationError, match="job_id values must be unique"):
        load_production_jobs(path)


def test_unknown_pipeline_id_is_rejected() -> None:
    with pytest.raises(DefinitionValidationError, match="unknown formal pipeline"):
        validate_production_jobs((_job("ghost", pipeline_id="does_not_exist"),), registry=_registry())


def test_invalid_timezone_is_rejected(tmp_path: Path) -> None:
    path = _manifest(
        tmp_path,
        [{"job_id": "bad-tz", "pipeline_id": "research_stock_report_ingest", "enabled": False,
          "schedule": "0 7 * * 1-5", "timezone": "Mars/Olympus"}],
    )
    with pytest.raises(DefinitionValidationError, match="invalid timezone"):
        load_production_jobs(path)


def test_invalid_schedule_is_rejected(tmp_path: Path) -> None:
    path = _manifest(
        tmp_path,
        [{"job_id": "bad-cron", "pipeline_id": "research_stock_report_ingest", "enabled": False,
          "schedule": "not a cron", "timezone": "Asia/Shanghai"}],
    )
    with pytest.raises(DefinitionValidationError, match="invalid schedule"):
        load_production_jobs(path)


def test_undeclared_parameter_is_rejected() -> None:
    # index_component_ingest 声明了 index_codes/start_date/end_date
    with pytest.raises(DefinitionValidationError, match="UNKNOWN_PARAMETER"):
        validate_production_jobs(
            (_job("bad-param", pipeline_id="index_component_ingest", parameters={"nope": "x"}),),
            registry=_registry(),
        )


def test_missing_required_parameter_is_rejected() -> None:
    # index_component_ingest 的 start_date 必填
    with pytest.raises(DefinitionValidationError, match="REQUIRED_PARAMETER_MISSING"):
        validate_production_jobs(
            (
                _job(
                    "missing-required",
                    pipeline_id="index_component_ingest",
                    parameters={"index_codes": "000300.SH"},
                ),
            ),
            registry=_registry(),
        )


def test_invalid_parameter_type_is_rejected() -> None:
    with pytest.raises(DefinitionValidationError, match="INVALID_PARAMETER"):
        validate_production_jobs(
            (
                _job(
                    "bad-type",
                    pipeline_id="index_component_ingest",
                    parameters={"index_codes": "000300.SH", "start_date": "not-a-date",
                                "end_date": "2026-07-31"},
                ),
            ),
            registry=_registry(),
        )


def test_disabled_job_is_not_submitted_by_scheduler(tmp_path: Path) -> None:
    store = JobRuntimeStore(tmp_path / "runtime" / "job_runtime.sqlite3")
    store.initialize()
    contract = _registry().get("research_stock_report_ingest")
    disabled = runtime_definition_from_production_job(_job("disabled-instance"), contract)
    enabled = runtime_definition_from_production_job(
        _job("enabled-instance", schedule="* * * * *", enabled=True), contract
    )
    scheduler = JobScheduler(store, (disabled, enabled))
    result = scheduler.scan(now=datetime(2026, 8, 3, 12, 0, tzinfo=UTC))

    created = [run for run in result if run.status is JobStatus.PENDING]
    assert [run.job_id for run in created] == ["enabled-instance"]
    runs = store.list_runs(limit=100)
    assert [run.job_id for run in runs] == ["enabled-instance"]


def test_enabled_job_resolves_to_correct_contract() -> None:
    real = _registry().get("research_stock_report_ingest")
    seen: dict[str, object] = {}

    def fake_executor(context):
        seen["pipeline_id"] = context.pipeline_id
        seen["parameters"] = dict(context.parameter_overrides)
        return BusinessExecution.success()

    contract = replace(real, executor=fake_executor, inputs=(), outputs=())
    job = _job("research-stock-report-morning", parameters={"incremental": "true"})
    definition = runtime_definition_from_production_job(job, contract)
    assert definition.job_id == "research-stock-report-morning"
    assert definition.pipeline_id == "research_stock_report_ingest"
    assert definition.fixed_parameters == {"incremental": "true"}
    assert definition.in_process_executor is not None

    claimed = JobRun(
        run_id="run-1",
        job_id=definition.job_id,
        definition_version=definition.definition_version,
        scheduled_at=datetime(2026, 8, 3, 12, 0, tzinfo=UTC),
        started_at=None,
        finished_at=None,
        status=JobStatus.RUNNING,
        attempt=1,
        exit_code=None,
        timed_out=False,
        trigger_type="MANUAL",
        stdout_path=None,
        stderr_path=None,
        error_summary=None,
        heartbeat_at=None,
        pipeline_id=definition.pipeline_id,
        parameter_overrides={"incremental": "true"},
        execution_control=ExecutionControl(),
    )
    definition.in_process_executor(claimed)

    assert seen["pipeline_id"] == "research_stock_report_ingest"  # 而非 job_id
    # 固定参数经 Contract 解析（"true" → True）后注入执行上下文
    assert seen["parameters"].get("incremental") is True


def test_run_keeps_both_job_id_and_pipeline_id(tmp_path: Path) -> None:
    store = JobRuntimeStore(tmp_path / "runtime" / "job_runtime.sqlite3")
    store.initialize()
    contract = _registry().get("research_stock_report_ingest")
    definition = runtime_definition_from_production_job(
        _job("research-stock-report-morning", parameters={"incremental": "true"}), contract
    )
    run, _ = store.create_scheduled_run(
        definition, scheduled_at=datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
    )
    assert run.job_id == "research-stock-report-morning"
    assert run.pipeline_id == "research_stock_report_ingest"
    # 固定参数随 run 持久化
    assert run.parameter_overrides == {"incremental": "true"}
    stored = store.get_run(run.run_id)
    assert stored is not None
    assert stored.job_id == "research-stock-report-morning"
    assert stored.pipeline_id == "research_stock_report_ingest"


def test_two_instances_produce_distinct_run_history(tmp_path: Path) -> None:
    store = JobRuntimeStore(tmp_path / "runtime" / "job_runtime.sqlite3")
    store.initialize()
    contract = _registry().get("research_stock_report_ingest")
    morning = runtime_definition_from_production_job(_job("research-stock-report-morning"), contract)
    evening = runtime_definition_from_production_job(
        _job("research-stock-report-evening", schedule="0 19 * * 1-5"), contract
    )
    scheduled_at = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
    store.create_scheduled_run(morning, scheduled_at=scheduled_at)
    store.create_scheduled_run(evening, scheduled_at=scheduled_at)

    morning_runs = store.list_runs(job_id="research-stock-report-morning", limit=10)
    evening_runs = store.list_runs(job_id="research-stock-report-evening", limit=10)
    assert len(morning_runs) == 1
    assert len(evening_runs) == 1
    assert morning_runs[0].run_id != evening_runs[0].run_id
    assert {run.job_id for run in morning_runs} == {"research-stock-report-morning"}
    assert {run.job_id for run in evening_runs} == {"research-stock-report-evening"}
    assert {run.pipeline_id for run in morning_runs + evening_runs} == {"research_stock_report_ingest"}


def test_existing_registry_has_no_regression() -> None:
    contracts = validate_contracts(default_registry().all())
    assert len(contracts) == 16


def test_example_definitions_are_all_disabled() -> None:
    jobs = load_production_jobs(PROJECT_ROOT / "deploy" / "pipeline" / "production-job-definitions.json")
    assert len(jobs) == 2
    assert {job.job_id for job in jobs} == {
        "research-stock-report-morning",
        "research-stock-report-evening",
    }
    assert all(job.enabled is False for job in jobs)
    assert all(job.pipeline_id == "research_stock_report_ingest" for job in jobs)


def test_manifest_reload_is_stable(tmp_path: Path) -> None:
    path = _manifest(
        tmp_path,
        [
            {"job_id": "research-stock-report-morning", "pipeline_id": "research_stock_report_ingest",
             "enabled": False, "schedule": "0 7 * * 1-5", "timezone": "Asia/Shanghai",
             "parameters": {"incremental": "true"}},
            {"job_id": "research-stock-report-evening", "pipeline_id": "research_stock_report_ingest",
             "enabled": False, "schedule": "0 19 * * 1-5", "timezone": "Asia/Shanghai"},
        ],
    )
    first = load_production_jobs(path)
    second = load_production_jobs(path)
    assert first == second
    assert first == load_production_jobs(path)


def test_dependency_resolution_maps_pipeline_to_instance() -> None:
    jobs = (
        _job("market-daily-close", pipeline_id="market_daily_update"),
        _job("daily-basic-close", pipeline_id="daily_basic_update"),
    )
    resolved = resolve_instance_dependencies(jobs, registry=_registry())
    # daily_basic_update.dependencies = ("market_daily_update",) → 解析为实例 job_id
    assert resolved["market-daily-close"] == ()
    assert resolved["daily-basic-close"] == ("market-daily-close",)

    contract = _registry().get("daily_basic_update")
    downstream = runtime_definition_from_production_job(
        jobs[1], contract, dependency_job_ids=resolved["daily-basic-close"]
    )
    assert downstream.dependencies == ("market-daily-close",)
    assert downstream.pipeline_id == "daily_basic_update"


def test_downstream_unblocks_after_upstream_success(tmp_path: Path) -> None:
    store = JobRuntimeStore(tmp_path / "runtime" / "job_runtime.sqlite3")
    store.initialize()
    registry = _registry()
    upstream_contract = registry.get("market_daily_update")
    downstream_contract = registry.get("daily_basic_update")
    upstream = runtime_definition_from_production_job(
        _job("market-daily-close", pipeline_id="market_daily_update", enabled=True),
        upstream_contract,
    )
    downstream = runtime_definition_from_production_job(
        _job("daily-basic-close", pipeline_id="daily_basic_update", enabled=True),
        downstream_contract,
        dependency_job_ids=("market-daily-close",),
    )
    scheduled_at = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
    up_run, _ = store.create_scheduled_run(upstream, scheduled_at=scheduled_at)
    down_run, _ = store.create_scheduled_run(
        downstream,
        scheduled_at=scheduled_at,
        status=JobStatus.BLOCKED,
        error_summary="dependency market-daily-close has no completed run",
    )
    assert store.get_run(down_run.run_id).status is JobStatus.BLOCKED

    store.claim_run(
        up_run.run_id,
        job_id=upstream.job_id,
        definition_version=upstream.definition_version,
        overlap_policy=upstream.overlap_policy,
        resource_locks=upstream.resource_locks,
        lease_seconds=60,
    )
    store.finish_run(
        up_run.run_id,
        status=JobStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=None,
        system_cpu_ms=None,
        peak_rss_kb=None,
        result_payload={"status": "SUCCESS"},
    )
    scheduler = JobScheduler(store, (upstream, downstream))
    scheduler.refresh_blocked_runs()

    assert store.get_run(down_run.run_id).status is JobStatus.PENDING


def test_dependency_plan_outputs_both_instances() -> None:
    registry = _registry()
    upstream = runtime_definition_from_production_job(
        _job("market-daily-close", pipeline_id="market_daily_update"),
        registry.get("market_daily_update"),
    )
    downstream = runtime_definition_from_production_job(
        _job("daily-basic-close", pipeline_id="daily_basic_update"),
        registry.get("daily_basic_update"),
        dependency_job_ids=("market-daily-close",),
    )
    plan = dependency_plan((upstream, downstream), "daily-basic-close")
    assert [item.job_id for item in plan] == ["market-daily-close", "daily-basic-close"]


def test_missing_upstream_instance_fails_validation() -> None:
    jobs = (_job("daily-basic-close", pipeline_id="daily_basic_update"),)
    with pytest.raises(DefinitionValidationError, match="has no production job instance"):
        validate_production_jobs(jobs, registry=_registry())


def test_ambiguous_upstream_instance_fails_validation() -> None:
    jobs = (
        _job("market-daily-close", pipeline_id="market_daily_update"),
        _job("market-daily-extra", pipeline_id="market_daily_update"),
        _job("daily-basic-close", pipeline_id="daily_basic_update"),
    )
    with pytest.raises(DefinitionValidationError, match="ambiguous"):
        validate_production_jobs(jobs, registry=_registry())
