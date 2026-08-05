from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, PipelineRunContext, ResultStatus, TargetWindow
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline import system_b_contracts as subject


def _settings(tmp_path: Path, *, pool_path: Path | None = None) -> AppSettings:
    environ = {
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "QRP_RUNTIME_ENV": "test",
            "TUSHARE_TOKEN": "test-token",
        }
    if pool_path is not None:
        environ["QRP_POOL_DB_PATH"] = str(pool_path)
    return AppSettings.load(
        environ=environ,
        project_root=tmp_path / "repo",
    )


def _initialise_database(settings: AppSettings) -> None:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(connection)
    finally:
        connection.close()


def test_system_b_daily_chain_is_formally_registered_and_valid() -> None:
    registry = default_registry()
    validate_contracts(registry.all())
    expected = {
        "system_b_state_readiness",
        "system_b_state_daily",
        "system_b_episode_rebuild",
        "system_b_pool_height",
        "system_b_pool_capacity",
        "system_b_pool_recognition",
    }
    assert {contract.pipeline_id for contract in subject.SYSTEM_B_CONTRACTS} == expected
    assert expected <= {contract.pipeline_id for contract in registry.all()}
    assert subject.SYSTEM_B_STATE_READINESS.dependencies == (
        "market_daily_update",
        "adj_factor_daily",
        "suspend_d_ingest",
    )
    assert subject.SYSTEM_B_STATE_DAILY.dependencies == ("system_b_state_readiness",)
    assert subject.SYSTEM_B_EPISODE_REBUILD.dependencies == ("system_b_state_daily",)
    assert all(contract.dependencies == ("system_b_episode_rebuild",) for contract in subject.SYSTEM_B_CONTRACTS[3:])


def test_readiness_and_state_are_explicit_noops_on_closed_calendar_date(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    scheduled_for = datetime(2026, 8, 8, 9, 0, tzinfo=UTC)

    for contract in (subject.SYSTEM_B_STATE_READINESS, subject.SYSTEM_B_STATE_DAILY):
        result = execute_pipeline_contract(
            contract,
            PipelineInvocation(
                run_id=f"noop-{contract.pipeline_id}",
                pipeline_id=contract.pipeline_id,
                scheduled_for=scheduled_for,
                attempt=1,
                settings=settings,
            ),
        )
        assert result.status is ResultStatus.NOOP
        assert result.noop_reason == "non_trading_day"


def test_pool_output_quality_checks_are_bound_to_pool_database(tmp_path: Path) -> None:
    pool_path = tmp_path / "pool.duckdb"
    settings = _settings(tmp_path, pool_path=pool_path)
    connection = duckdb.connect(str(pool_path))
    try:
        from qrp_atlas.contracts import SYSTEM_B_POOL_MEMBERSHIP

        connection.execute(SYSTEM_B_POOL_MEMBERSHIP.duckdb_create_sql())
    finally:
        connection.close()
    context = PipelineRunContext(
        run_id="quality-check",
        pipeline_id=subject.SYSTEM_B_POOL_HEIGHT.pipeline_id,
        scheduled_for=datetime(2026, 8, 7),
        attempt=1,
        settings=settings,
        parameter_overrides={},
        target_window=TargetWindow.for_date(date(2026, 8, 7)),
        audit_context={},
    )
    quality = subject.SYSTEM_B_POOL_HEIGHT.outputs[0].quality_checks[0](context)
    assert quality.passed
    assert quality.observed["duplicate_groups"] == 0
