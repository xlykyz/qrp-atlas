"""Tests for Theme M4 pipeline contracts: registration, freshness check, and execution."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.contracts.schema import init_stock_collections_database
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import (
    PipelineInvocation,
    PipelineRunContext,
    ResultStatus,
    TargetWindow,
)
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.theme_contracts import THEME_M4_PRODUCTION_CONTRACT
from qrp_atlas.stock_collections.service import StockCollectionService


def _settings(tmp_path: Path) -> AppSettings:
    environ = {
        "QRP_HOME": str(tmp_path / "home"),
        "QRP_DATA_DIR": str(tmp_path / "data"),
        "QRP_RUNTIME_ENV": "test",
        "TUSHARE_TOKEN": "test-token",
    }
    return AppSettings.load(
        environ=environ,
        project_root=tmp_path / "repo",
    )


def _setup_db(settings: AppSettings, target_date: date, *, missing_snapshot: bool = False) -> None:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(con)
        init_stock_collections_database(con)

        # 1. Trading calendar
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)", [target_date])

        # 2. Stock info
        con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000001.SZ', 'Stock A', '2020-01-01')")

        # 3. Market snapshot (unless testing freshness failure)
        if not missing_snapshot:
            con.execute(
                "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [target_date, "000001.SZ", "Stock A", 10.0, 10.0, 9.8, 10.0, 1000, 10000, 2.0, False],
            )
            con.execute(
                "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, ?, 100.0, 1.5)",
                [target_date, "881101.TI"],
            )

        # 4. Stock collection and theme
        sc = StockCollectionService(con, clock=lambda: datetime(2026, 8, 3, 0, 0, tzinfo=UTC))
        thm, coll = sc.create_canonical_theme(
            theme_name="芯片",
            source_key="CHIP",
            effective_from=target_date,
            available_trade_date=target_date,
        )
        sc.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=target_date,
            available_trade_date=target_date,
        )
    finally:
        con.close()


def test_theme_m4_contract_registration() -> None:
    try:
        registry = default_registry()
        validate_contracts(registry.all())
    except ModuleNotFoundError:
        from qrp_atlas.pipeline.registry import PipelineRegistry
        registry = PipelineRegistry()
        registry.register(THEME_M4_PRODUCTION_CONTRACT)
        validate_contracts(registry.all())

    assert registry.get("theme_m4_production") is not None
    assert THEME_M4_PRODUCTION_CONTRACT.pipeline_id == "theme_m4_production"
    assert THEME_M4_PRODUCTION_CONTRACT.resource_locks == ("quant_db_writer",)

    # 验证全部 4 张正式业务事实表均已声明在 outputs 中
    output_ids = {o.output_id for o in THEME_M4_PRODUCTION_CONTRACT.outputs}
    assert output_ids == {
        "theme_custom_index_daily",
        "theme_custom_index_state",
        "theme_custom_index_episode",
        "theme_m4_observation",
    }
    table_names = {o.object_name for o in THEME_M4_PRODUCTION_CONTRACT.outputs}
    assert table_names == {
        "theme_custom_index_daily",
        "theme_custom_index_state",
        "theme_custom_index_episode",
        "theme_m4_observation",
    }


def test_theme_m4_freshness_check_fails_when_inputs_missing(tmp_path: Path) -> None:
    app_settings = _settings(tmp_path)
    target = date(2026, 8, 3)
    _setup_db(app_settings, target, missing_snapshot=True)

    invocation = PipelineInvocation(
        run_id="test-run-missing",
        pipeline_id="theme_m4_production",
        scheduled_for=datetime(2026, 8, 3, 17, 0, tzinfo=UTC),
        attempt=1,
        settings=app_settings,
        trade_date_override=target,
    )
    result = execute_pipeline_contract(
        THEME_M4_PRODUCTION_CONTRACT,
        invocation,
    )
    assert result.status == ResultStatus.FAILED
    assert any(fc.error_code == "THEME_M4_INPUTS_STALE" for fc in result.freshness_checks)


def test_theme_m4_execution_success_and_knowledge_date_propagation(tmp_path: Path) -> None:
    app_settings = _settings(tmp_path)
    target = date(2026, 8, 3)
    _setup_db(app_settings, target, missing_snapshot=False)

    kd = date(2026, 8, 4)
    invocation = PipelineInvocation(
        run_id="test-run-success",
        pipeline_id="theme_m4_production",
        scheduled_for=datetime(2026, 8, 3, 17, 0, tzinfo=UTC),
        attempt=1,
        settings=app_settings,
        trade_date_override=target,
        parameter_overrides={"knowledge_date": kd.isoformat()},
    )
    result = execute_pipeline_contract(
        THEME_M4_PRODUCTION_CONTRACT,
        invocation,
    )
    assert result.status == ResultStatus.SUCCESS
    assert any(diag.message == f"Knowledge date: {kd.isoformat()}" for diag in result.diagnostics)
