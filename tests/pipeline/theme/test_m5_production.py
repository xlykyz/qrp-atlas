"""Acceptance tests for Task04-B2 M5 production and contract semantics."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config import AppSettings
from qrp_atlas.contracts import DC_HOT, THS_HOT, init_database
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import ResultStatus
from qrp_atlas.pipeline.dc_hot_contracts import DC_HOT_INGEST
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.testing import ContractTestHarness
from qrp_atlas.pipeline.ths_hot_contracts import THS_HOT_INGEST
from qrp_atlas.pipeline.theme.m5_service import ThemeM5PipelineService
from qrp_atlas.pipeline.theme_m5_contracts import THEME_M5_PRODUCTION_CONTRACT
from qrp_atlas.stock_collections.service import StockCollectionService


TARGET = date(2026, 3, 2)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "QRP_RUNTIME_ENV": "test",
            "TUSHARE_TOKEN": "test-token",
        },
        project_root=tmp_path / "repo",
    )


def _source_rows(
    *,
    table: str,
    trade_date: date,
    source: str,
    list_name: str,
    snapshots: tuple[tuple[str, tuple[str, ...]], ...],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    fillers = [f"{900000 + index:06d}.SZ" for index in range(1, 101)]
    for seq, (started_at, hot_tickers) in enumerate(snapshots, start=1):
        ordered = list(dict.fromkeys(hot_tickers)) + [ticker for ticker in fillers if ticker not in hot_tickers]
        for rank, ticker in enumerate(ordered[:100], start=1):
            row: dict[str, object] = {
                "trade_date": trade_date,
                "source": source,
                "list_name": list_name,
                "ticker": ticker,
                "name": ticker,
                "rank_position": rank,
                "pct_change": 1.0,
                "current_price": 10.0,
                "source_rank_time": started_at,
                "snapshot_seq": seq,
                "snapshot_started_at": started_at,
                "snapshot_completed_at": started_at,
            }
            if table == THS_HOT.name:
                row.update({"hot": 100.0 - rank, "concept": "fixture", "rank_reason": "fixture"})
            rows.append(row)
    columns = [column for column in (THS_HOT if table == THS_HOT.name else DC_HOT).column_names() if column != "created_at"]
    return pd.DataFrame(rows, columns=columns)


def _insert_source(con: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    con.register("_m5_fixture_rows", frame)
    try:
        columns = list(frame.columns)
        con.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM _m5_fixture_rows"
        )
    finally:
        con.unregister("_m5_fixture_rows")


def _setup_database(
    settings: AppSettings,
    *,
    include_ths: bool = True,
    future_member: bool = False,
) -> tuple[str, str, str, str]:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(con)
        for ticker in ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"):
            con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES (?, ?, ?)", [ticker, ticker, date(2020, 1, 1)])
        service = StockCollectionService(
            con,
            clock=lambda: datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        )
        created: list[tuple[str, str]] = []
        for source_key, name in (("ALPHA", "Alpha"), ("BETA", "Beta"), ("EMPTY", "Empty"), ("ZERO", "Zero")):
            theme, collection = service.create_canonical_theme(
                theme_name=name,
                source_key=source_key,
                effective_from=TARGET,
                available_trade_date=TARGET,
            )
            created.append((theme.theme_id, collection.collection_id))
        for ticker in ("000001.SZ", "000002.SZ", "000003.SZ"):
            service.add_member(
                theme_id=created[0][0],
                collection_id=created[0][1],
                asset_id=ticker,
                effective_from=TARGET,
                available_trade_date=TARGET,
            )
        for ticker in ("000001.SZ", "000004.SZ"):
            service.add_member(
                theme_id=created[1][0],
                collection_id=created[1][1],
                asset_id=ticker,
                effective_from=TARGET,
                available_trade_date=TARGET,
            )
        service.add_member(
            theme_id=created[3][0],
            collection_id=created[3][1],
            asset_id="000005.SZ",
            effective_from=TARGET,
            available_trade_date=TARGET + timedelta(days=1) if future_member else TARGET,
        )
        dc = _source_rows(
            table=DC_HOT.name,
            trade_date=TARGET,
            source="EASTMONEY",
            list_name="POPULARITY",
            snapshots=(
                ("2026-03-02 09:30:00", ("000001.SZ", "000002.SZ")),
                ("2026-03-02 10:00:00", ("000001.SZ", "000003.SZ")),
            ),
        )
        _insert_source(con, DC_HOT.name, dc)
        if include_ths:
            ths = _source_rows(
                table=THS_HOT.name,
                trade_date=TARGET,
                source="THS",
                list_name="HOT_STOCK",
                snapshots=(("2026-03-02 11:00:00", ("000001.SZ", "000002.SZ")),),
            )
            _insert_source(con, THS_HOT.name, ths)
    finally:
        con.close()
    return (*created[0], *created[3])


def test_m5_contract_is_registered_and_formally_validated() -> None:
    registry = default_registry()
    validate_contracts(registry.all())
    assert registry.get("theme_m5_production") is THEME_M5_PRODUCTION_CONTRACT
    assert THEME_M5_PRODUCTION_CONTRACT.resource_locks == ("quant_db_writer",)
    assert THEME_M5_PRODUCTION_CONTRACT.outputs[0].unique_key == ("theme_id", "trade_date")
    assert THEME_M5_PRODUCTION_CONTRACT.outputs[0].allow_empty is False


def test_m5_maps_all_snapshots_to_overlapping_themes_and_keeps_zero_rows(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    alpha_theme, _, _, _ = _setup_database(settings)

    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        report = ThemeM5PipelineService(con).run_m5_daily(TARGET, production_run_id="run-m5-test")
        rows = con.execute(
            """
            SELECT theme_id, theme_member_count, theme_hot_stock_count,
                   theme_hot_stock_ratio, theme_hot_list_appearance_count,
                   theme_hot_source_count
            FROM theme_m5_observation WHERE trade_date = ? ORDER BY theme_id
            """,
            [TARGET],
        ).fetchall()
    finally:
        con.close()

    assert report.total_observation_rows == 4
    by_theme = {row[0]: row[1:] for row in rows}
    assert by_theme[alpha_theme] == (3, 3, 1.0, 6, 2)
    # The fourth Theme has a member admitted tomorrow and is still emitted as a
    # zero-member row for D rather than leaking the future membership.
    empty = by_theme["THM:QRP:EMPTY"]
    assert empty == (0, 0, None, 0, 0)
    zero = by_theme["THM:QRP:ZERO"]
    assert zero == (1, 0, 0.0, 0, 0)


def test_m5_zero_hit_member_has_zero_ratio_and_rerun_is_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _setup_database(settings)
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        first = ThemeM5PipelineService(con).run_m5_daily(TARGET, production_run_id="run-one")
        con.execute("DELETE FROM dc_hot WHERE ticker = '000002.SZ'")
        con.execute("DELETE FROM ths_hot WHERE ticker = '000002.SZ'")
        # Source rows would now be incomplete, so restore the fixture before the
        # idempotent repeat.  The repeat must have the same business facts.
        dc = _source_rows(
            table=DC_HOT.name,
            trade_date=TARGET,
            source="EASTMONEY",
            list_name="POPULARITY",
            snapshots=(("2026-03-02 09:30:00", ("000001.SZ", "000002.SZ")), ("2026-03-02 10:00:00", ("000001.SZ", "000003.SZ"))),
        )
        ths = _source_rows(
            table=THS_HOT.name,
            trade_date=TARGET,
            source="THS",
            list_name="HOT_STOCK",
            snapshots=(("2026-03-02 11:00:00", ("000001.SZ", "000002.SZ")),),
        )
        con.execute("DELETE FROM dc_hot")
        con.execute("DELETE FROM ths_hot")
        _insert_source(con, DC_HOT.name, dc)
        _insert_source(con, THS_HOT.name, ths)
        second = ThemeM5PipelineService(con).run_m5_daily(TARGET, production_run_id="run-two")
        count = con.execute("SELECT COUNT(*) FROM theme_m5_observation WHERE trade_date = ?", [TARGET]).fetchone()[0]
        alpha = con.execute(
            "SELECT theme_hot_stock_ratio FROM theme_m5_observation WHERE theme_id = 'THM:QRP:ALPHA' AND trade_date = ?",
            [TARGET],
        ).fetchone()[0]
    finally:
        con.close()
    assert first.input_snapshot_id == second.input_snapshot_id
    assert count == 4
    assert alpha == 1.0


def test_m5_missing_official_source_fails_contract_and_preserves_existing_observation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _setup_database(settings)
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        ThemeM5PipelineService(con).run_m5_daily(TARGET, production_run_id="before-missing")
        before = con.execute(
            "SELECT COUNT(*), MIN(production_run_id) FROM theme_m5_observation WHERE trade_date = ?",
            [TARGET],
        ).fetchone()
        con.execute("DELETE FROM ths_hot")
    finally:
        con.close()

    result = ContractTestHarness(
        THEME_M5_PRODUCTION_CONTRACT,
        settings,
        dependency_contracts=(DC_HOT_INGEST, THS_HOT_INGEST),
    ).run(trade_date=TARGET)
    assert result.status is ResultStatus.FAILED
    assert any(check.error_code == "THEME_M5_THS_HOT_INPUT_INCOMPLETE" for check in result.freshness_checks)

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        after = con.execute(
            "SELECT COUNT(*), MIN(production_run_id) FROM theme_m5_observation WHERE trade_date = ?",
            [TARGET],
        ).fetchone()
    finally:
        con.close()
    assert after == before


def test_m5_future_membership_is_not_visible_on_d_day(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _setup_database(settings, future_member=True)
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        ThemeM5PipelineService(con).run_m5_daily(TARGET)
        count = con.execute(
            "SELECT theme_member_count FROM theme_m5_observation WHERE theme_id = 'THM:QRP:ZERO' AND trade_date = ?",
            [TARGET],
        ).fetchone()[0]
    finally:
        con.close()
    assert count == 0
