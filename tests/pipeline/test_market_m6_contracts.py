"""Integration and pipeline contract tests for M6 Market Sentiment facts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CLOSE,
    CONSECUTIVE_LIMIT_UP_COUNT,
    DAILY_MARKET_SNAPSHOT,
    INPUT_SNAPSHOT_ID,
    IS_LIMIT_DOWN,
    IS_LIMIT_UP,
    LIMIT_DOWN_COUNT,
    LIMIT_UP_COUNT,
    M6_CALCULATION_VERSION,
    MARKET_M6_OBSERVATION,
    MARKET_M6_OBSERVATION_TABLE,
    MARKET_SCOPE,
    MARKET_SCOPE_ALL_MARKET,
    MARKET_SCOPE_BSE,
    MARKET_SCOPE_CHINEXT,
    MARKET_SCOPE_MAIN_BOARD,
    MARKET_SCOPE_STAR_MARKET,
    MARKET_SCOPES,
    MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
    PRE_LIMIT_UP_PREMIUM,
    PRODUCTION_RUN_ID,
    STOCK_INFO,
    SUSPEND_D,
    TICKER,
    TRADE_DATE,
    TRADING_CALENDAR,
    init_database,
)
from qrp_atlas.pipeline.contracts import ContractError
from qrp_atlas.pipeline.market_m6 import (
    MarketM6PipelineService,
    MarketM6QueryService,
    resolve_canonical_market_scope,
)
from qrp_atlas.pipeline.market_m6_contracts import (
    MARKET_M6_PRODUCTION_CONTRACT,
    execute_market_m6_production,
)


def _init_test_db(con: duckdb.DuckDBPyConnection) -> None:
    init_database(con)


def _seed_base_data(con: duckdb.DuckDBPyConnection) -> None:
    # 1. trading_calendar
    con.execute(
        """
        INSERT INTO trading_calendar (trade_date, is_open) VALUES
            ('2026-08-07', TRUE),
            ('2026-08-08', FALSE),
            ('2026-08-09', FALSE),
            ('2026-08-10', TRUE)
        """
    )

    # 2. stock_info across all 4 sub-markets
    con.execute(
        """
        INSERT INTO stock_info (ticker, market, exchange) VALUES
            ('000001.SZ', '主板', 'SZSE'),
            ('000002.SZ', '主板', 'SZSE'),
            ('000003.SZ', '主板', 'SZSE'),
            ('300750.SZ', '创业板', 'SZSE'),
            ('688981.SH', '科创板', 'SSE'),
            ('830799.BJ', '北交所', 'BSE')
        """
    )

    # 3. daily_market_snapshot on D-1 (2026-08-07)
    # 000001.SZ: limit up (close 10.0)
    # 000002.SZ: limit up (close 10.0)
    # 000003.SZ: limit up (close 10.0)
    # 300750.SZ: limit up (close 20.0)
    # 688981.SH: normal (close 50.0)
    # 830799.BJ: normal (close 15.0)
    con.execute(
        """
        INSERT INTO daily_market_snapshot (trade_date, ticker, close, is_limit_up, is_limit_down, volume) VALUES
            ('2026-08-07', '000001.SZ', 10.0, TRUE, FALSE, 1000),
            ('2026-08-07', '000002.SZ', 10.0, TRUE, FALSE, 1000),
            ('2026-08-07', '000003.SZ', 10.0, TRUE, FALSE, 1000),
            ('2026-08-07', '300750.SZ', 20.0, TRUE, FALSE, 1000),
            ('2026-08-07', '688981.SH', 50.0, FALSE, FALSE, 1000),
            ('2026-08-07', '830799.BJ', 15.0, FALSE, FALSE, 1000)
        """
    )

    # 4. daily_market_snapshot on D (2026-08-10)
    # 000001.SZ: limit up (close 11.0, streak 2)
    # 000002.SZ: traded, not limit up, limit down (close 9.0, premium -10%)
    # 000003.SZ: suspended on D (volume 0) -> dropped from premium denominator
    # 300750.SZ: limit up (close 24.0, premium +20%, streak 2)
    # 688981.SH: limit down (close 40.0)
    # 830799.BJ: normal (close 16.0)
    con.execute(
        """
        INSERT INTO daily_market_snapshot (trade_date, ticker, close, is_limit_up, is_limit_down, volume) VALUES
            ('2026-08-10', '000001.SZ', 11.0, TRUE, FALSE, 1000),
            ('2026-08-10', '000002.SZ', 9.0, FALSE, TRUE, 1000),
            ('2026-08-10', '000003.SZ', 10.0, FALSE, FALSE, 0),
            ('2026-08-10', '300750.SZ', 24.0, TRUE, FALSE, 1000),
            ('2026-08-10', '688981.SH', 40.0, FALSE, TRUE, 1000),
            ('2026-08-10', '830799.BJ', 16.0, FALSE, FALSE, 1000)
        """
    )


def test_market_scope_resolver() -> None:
    assert resolve_canonical_market_scope("主板", "SSE") == MARKET_SCOPE_MAIN_BOARD
    assert resolve_canonical_market_scope("中小板", "SZSE") == MARKET_SCOPE_MAIN_BOARD
    assert resolve_canonical_market_scope("创业板", "SZSE") == MARKET_SCOPE_CHINEXT
    assert resolve_canonical_market_scope("科创板", "SSE") == MARKET_SCOPE_STAR_MARKET
    assert resolve_canonical_market_scope("北交所", "BSE") == MARKET_SCOPE_BSE
    assert resolve_canonical_market_scope("未知", "BSE") == MARKET_SCOPE_BSE
    assert resolve_canonical_market_scope("未知", "SZSE") is None


def test_m6_pipeline_contract_metadata() -> None:
    contract = MARKET_M6_PRODUCTION_CONTRACT
    assert contract.pipeline_id == "market_m6_production"
    assert contract.resource_locks == ("quant_db_writer",)
    assert len(contract.inputs) == 1
    assert len(contract.outputs) == 1
    out = contract.outputs[0]
    assert out.object_name == MARKET_M6_OBSERVATION_TABLE
    assert out.unique_key == ("trade_date", "market_scope")
    assert not out.allow_empty


def test_m6_production_success_and_semantics() -> None:
    con = duckdb.connect()
    try:
        _init_test_db(con)
        _seed_base_data(con)

        service = MarketM6PipelineService(con)
        target_date = date(2026, 8, 10)
        df = service.run_m6_daily(target_date, production_run_id="test-run-1")

        assert len(df) == 5
        assert set(df[MARKET_SCOPE].tolist()) == set(MARKET_SCOPES)

        # Query persisted data
        persisted = con.execute(
            f"SELECT * FROM {MARKET_M6_OBSERVATION_TABLE} WHERE trade_date = ? ORDER BY market_scope",
            [target_date],
        ).fetchdf()
        assert len(persisted) == 5

        res_dict = persisted.set_index(MARKET_SCOPE).to_dict(orient="index")

        # MAIN_BOARD:
        # 000001.SZ is limit up (streak 2); 000002.SZ is limit down; 000003.SZ is suspended.
        # limit_up_count: 1
        # limit_down_count: 1
        # consecutive_limit_up_count: 1
        # max_consecutive_limit_up_height: 2
        # pre_limit_up_premium: 000001 (+0.10), 000002 (-0.10) -> (0.10 - 0.10) / 2 = 0.0
        mb = res_dict[MARKET_SCOPE_MAIN_BOARD]
        assert mb[LIMIT_UP_COUNT] == 1
        assert mb[LIMIT_DOWN_COUNT] == 1
        assert mb[CONSECUTIVE_LIMIT_UP_COUNT] == 1
        assert mb[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 2
        assert pytest.approx(mb[PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.0

        # CHINEXT:
        # 300750.SZ is limit up (streak 2); close 24 vs 20 (+20%)
        # limit_up_count: 1
        # consecutive_limit_up_count: 1
        # max_height: 2
        # premium: 0.20
        cyb = res_dict[MARKET_SCOPE_CHINEXT]
        assert cyb[LIMIT_UP_COUNT] == 1
        assert cyb[CONSECUTIVE_LIMIT_UP_COUNT] == 1
        assert cyb[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 2
        assert pytest.approx(cyb[PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.20

        # STAR_MARKET:
        # 688981.SH is limit down; no limit up; no D-1 limit up
        # limit_up: 0, limit_down: 1, consecutive: 0, max_height: 0, premium: NULL
        kcb = res_dict[MARKET_SCOPE_STAR_MARKET]
        assert kcb[LIMIT_UP_COUNT] == 0
        assert kcb[LIMIT_DOWN_COUNT] == 1
        assert kcb[CONSECUTIVE_LIMIT_UP_COUNT] == 0
        assert kcb[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 0
        assert pd.isna(kcb[PRE_LIMIT_UP_PREMIUM])
        assert con.execute(
            f"SELECT pre_limit_up_premium IS NULL FROM {MARKET_M6_OBSERVATION_TABLE} WHERE trade_date = ? AND market_scope = ?",
            [target_date, MARKET_SCOPE_STAR_MARKET],
        ).fetchone()[0] is True

        # BSE:
        # 830799.BJ is normal (no limit up/down); no D-1 limit up
        # limit_up: 0, limit_down: 0, consecutive: 0, max_height: 0, premium: NULL
        bse = res_dict[MARKET_SCOPE_BSE]
        assert bse[LIMIT_UP_COUNT] == 0
        assert bse[LIMIT_DOWN_COUNT] == 0
        assert bse[CONSECUTIVE_LIMIT_UP_COUNT] == 0
        assert bse[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 0
        assert pd.isna(bse[PRE_LIMIT_UP_PREMIUM])
        assert con.execute(
            f"SELECT pre_limit_up_premium IS NULL FROM {MARKET_M6_OBSERVATION_TABLE} WHERE trade_date = ? AND market_scope = ?",
            [target_date, MARKET_SCOPE_BSE],
        ).fetchone()[0] is True

        # ALL_MARKET:
        # limit_up: 2 (000001, 300750)
        # limit_down: 2 (000002, 688981)
        # consecutive_limit_up: 2 (000001, 300750)
        # max_height: 2
        # premium: directly equal-weighted across 3 stocks (000001, 000002, 300750)
        # = (0.10 - 0.10 + 0.20) / 3 = 0.20 / 3 ≈ 0.066667
        all_m = res_dict[MARKET_SCOPE_ALL_MARKET]
        assert all_m[LIMIT_UP_COUNT] == 2
        assert all_m[LIMIT_DOWN_COUNT] == 2
        assert all_m[CONSECUTIVE_LIMIT_UP_COUNT] == 2
        assert all_m[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 2
        assert pytest.approx(all_m[PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.20 / 3.0

        # Rerun idempotency
        service.run_m6_daily(target_date, production_run_id="test-run-2")
        count_after = con.execute(
            f"SELECT COUNT(*) FROM {MARKET_M6_OBSERVATION_TABLE} WHERE trade_date = ?",
            [target_date],
        ).fetchone()[0]
        assert count_after == 5
    finally:
        con.close()


def test_m6_production_fail_closed_on_missing_snapshot() -> None:
    con = duckdb.connect()
    try:
        _init_test_db(con)
        # Only seed calendar and stock_info, no daily_market_snapshot
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES ('2026-08-10', TRUE)")
        con.execute("INSERT INTO stock_info (ticker, market, exchange) VALUES ('000001.SZ', '主板', 'SZSE')")

        service = MarketM6PipelineService(con)
        with pytest.raises(ContractError) as exc_info:
            service.run_m6_daily(date(2026, 8, 10))
        assert exc_info.value.code == "M6_SNAPSHOT_EMPTY"

        # Verify nothing written
        cnt = con.execute(f"SELECT COUNT(*) FROM {MARKET_M6_OBSERVATION_TABLE}").fetchone()[0]
        assert cnt == 0
    finally:
        con.close()


def test_m6_production_fail_closed_on_unresolved_market_scope() -> None:
    con = duckdb.connect()
    try:
        _init_test_db(con)
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES ('2026-08-10', TRUE)")
        # Stock with unmappable market
        con.execute("INSERT INTO stock_info (ticker, market, exchange) VALUES ('999999.ZZ', '未知板块', 'ZZ')")
        con.execute("INSERT INTO daily_market_snapshot (trade_date, ticker, close, is_limit_up, is_limit_down, volume) VALUES ('2026-08-10', '999999.ZZ', 10.0, FALSE, FALSE, 100)")

        service = MarketM6PipelineService(con)
        with pytest.raises(ContractError) as exc_info:
            service.run_m6_daily(date(2026, 8, 10))
        assert exc_info.value.code == "M6_CANONICAL_MARKET_UNRESOLVED"
    finally:
        con.close()


def test_m6_query_and_audit_report() -> None:
    con = duckdb.connect()
    try:
        _init_test_db(con)
        _seed_base_data(con)

        target_date = date(2026, 8, 10)
        service = MarketM6PipelineService(con)
        service.run_m6_daily(target_date, production_run_id="run-audit-1")

        query_service = MarketM6QueryService(con)

        # 1. Fetch range
        df = query_service.get_m6_observations(target_date, target_date)
        assert len(df) == 5

        # 2. Audit reproducible
        report = query_service.audit_m6_observation(target_date)
        assert report.is_reproducible
        assert len(report.discrepancies) == 0
        assert report.discrepancy_reason is None

        # 3. Tamper with one value and verify audit catches it
        con.execute(
            f"UPDATE {MARKET_M6_OBSERVATION_TABLE} SET limit_up_count = 999 WHERE trade_date = ? AND market_scope = ?",
            [target_date, MARKET_SCOPE_MAIN_BOARD],
        )
        tampered_report = query_service.audit_m6_observation(target_date)
        assert not tampered_report.is_reproducible
        assert len(tampered_report.discrepancies) > 0
        assert any(d["market_scope"] == MARKET_SCOPE_MAIN_BOARD for d in tampered_report.discrepancies)
    finally:
        con.close()
