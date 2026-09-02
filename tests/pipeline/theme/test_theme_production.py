"""Tests for Theme M4 production pipeline: full vs daily equality, targeted replay, and lineage audit."""

from datetime import date
import duckdb
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts.schema import init_database, init_stock_collections_database
from qrp_atlas.pipeline.theme.query import ThemeQueryService
from qrp_atlas.pipeline.theme.service import ThemePipelineService
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_database(con)
    init_stock_collections_database(con)

    # 1. Populate trading calendar (prior dates + 10 production days)
    prior_dates = [date(2026, 7, i) for i in range(20, 31)]
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    for d in prior_dates + dates:
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)", [d])

    # 2. Populate stock info
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000001.SZ', 'Stock A', '2020-01-01')")
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('600519.SH', 'Stock B', '2020-01-01')")

    # 3. Populate market snapshots & THS daily for all dates
    for idx, d in enumerate(dates):
        # Stock A return 2% (+0.02, limit up on day 4)
        pct_a = 9.9 if idx == 3 else 2.0
        close_a = 10.0 * ((1 + 0.02) ** (idx + 1))
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [d, "000001.SZ", "Stock A", 10.0, close_a, 9.8, close_a, 1000, 10000, pct_a],
        )

        # Stock B return 3% (+0.03)
        close_b = 20.0 * ((1 + 0.03) ** (idx + 1))
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [d, "600519.SH", "Stock B", 20.0, close_b, 19.8, close_b, 1000, 20000, 3.0],
        )

        # THS Board (881101.TI) return 1.5%
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, ?, 100.0, 1.5)",
            [d, "881101.TI"],
        )

    # 4. Create Theme and Add Members
    sc_service = StockCollectionService(con)
    thm, coll = sc_service.create_canonical_theme(
        theme_name="高算力芯片",
        source_key="AI_CHIP",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    sc_service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    sc_service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="600519.SH",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    yield con
    con.close()


def test_full_replay_vs_daily_production_exact_value_equality(db):
    """Assert full replay and sequential daily production produce 100% numerically identical outputs."""
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)

    # 1. Run full historical replay
    report_full = service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1])
    assert report_full.total_index_rows == 10
    full_indices = query_service.get_theme_index_history("THM:QRP:AI_CHIP", dates[0], dates[-1])
    full_m4 = query_service.get_m4_observations(dates[-1])

    # Save full replay values
    full_levels = full_indices["index_level"].tolist()
    full_returns = full_indices["theme_daily_return"].tolist()

    # 2. Simulate sequential daily production on clean state
    db.execute("DELETE FROM theme_custom_index_daily")
    db.execute("DELETE FROM theme_custom_index_state")
    db.execute("DELETE FROM theme_custom_index_episode")
    db.execute("DELETE FROM theme_m4_observation")

    for d in dates:
        service.run_m4_daily(trade_date=d)

    daily_indices = query_service.get_theme_index_history("THM:QRP:AI_CHIP", dates[0], dates[-1])
    daily_m4 = query_service.get_m4_observations(dates[-1])

    daily_levels = daily_indices["index_level"].tolist()
    daily_returns = daily_indices["theme_daily_return"].tolist()

    # Numerical exact equality assertion
    np.testing.assert_allclose(full_levels, daily_levels, rtol=1e-7)
    np.testing.assert_allclose(full_returns, daily_returns, rtol=1e-7)
    assert len(daily_indices) == len(full_indices)
    assert len(daily_m4) == len(full_m4)


def test_lineage_audit_and_input_snapshot_traceability(db):
    """Verify audit service traces inputs without N+1 queries."""
    d = date(2026, 8, 6)  # Day 4 (Stock A has limit up)
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)

    service.rebuild_m4_facts(start_date=date(2026, 8, 3), end_date=d)

    audit = query_service.audit_m4_observation("THM:QRP:AI_CHIP", d)

    assert audit.theme_id == "THM:QRP:AI_CHIP"
    assert audit.trade_date == d
    assert audit.theme_limit_up_count == 1
    assert "000001.SZ" in audit.limit_up_assets
    assert audit.effective_members == 2
    assert audit.total_members == 2
    assert audit.production_run_id is not None
    assert audit.input_snapshot_id is not None
    assert len(audit.comparison_boards) >= 1
