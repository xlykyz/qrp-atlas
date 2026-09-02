"""Integration tests for Theme custom index and M4 pipeline production and auditability."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import duckdb
import pytest

from qrp_atlas.contracts import (
    DAILY_MARKET_SNAPSHOT,
    STOCK_INFO,
    SUSPEND_D,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_M4_OBSERVATION_TABLE,
    THS_DAILY,
    TRADING_CALENDAR,
    init_database,
)
from qrp_atlas.pipeline.theme.query import ThemeQueryService
from qrp_atlas.pipeline.theme.service import ThemePipelineService
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def integrated_db():
    con = duckdb.connect(":memory:")
    init_database(con)
    yield con
    con.close()


def test_theme_m4_production_replay_idempotency_and_audit(integrated_db):
    """Full end-to-end integration test for Theme Custom Index & M4 Observations pipeline."""
    con = integrated_db
    collection_service = StockCollectionService(con)
    pipeline_service = ThemePipelineService(con)
    query_service = ThemeQueryService(con)

    # 1. Setup Calendar (10 trading days: 2026-08-01 to 2026-08-14)
    start_date = date(2026, 8, 3)
    calendar_dates = [start_date + timedelta(days=i) for i in range(10)]
    for d in calendar_dates:
        con.execute(
            "INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)",
            [d],
        )

    # 2. Setup Stocks
    # Stock A: Old stock (listed 2020-01-01)
    # Stock B: Old stock (listed 2020-01-01)
    # Stock C: New stock (listed on 2026-08-05, Day 3 of calendar)
    con.execute(
        "INSERT INTO stock_info (ticker, name, list_date, is_active) VALUES (?, ?, ?, true)",
        ["000001.SZ", "平安银行", date(2020, 1, 1)],
    )
    con.execute(
        "INSERT INTO stock_info (ticker, name, list_date, is_active) VALUES (?, ?, ?, true)",
        ["000002.SZ", "万科A", date(2020, 1, 1)],
    )
    con.execute(
        "INSERT INTO stock_info (ticker, name, list_date, is_active) VALUES (?, ?, ?, true)",
        ["300001.SZ", "新股C", date(2026, 8, 5)],
    )

    # 3. Setup Suspensions (Stock B suspended on Day 4: 2026-08-06)
    con.execute(
        "INSERT INTO suspend_d (trade_date, ticker, suspend_type) VALUES (?, ?, ?)",
        [calendar_dates[3], "000002.SZ", "S"],
    )

    # 4. Setup Market Snapshots for all 10 days
    # Daily returns:
    # A: constant +3% every day, limit-up on Day 5 (2026-08-07)
    # B: constant +1% every day (except suspended on Day 4)
    # C: constant +5% every day (listed on Day 3)
    for i, d in enumerate(calendar_dates):
        # A
        is_lu_a = (i == 4)  # Limit up on Day 5
        con.execute(
            """
            INSERT INTO daily_market_snapshot
            (trade_date, ticker, open, high, low, close, pre_close, pct_change, is_limit_up)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [d, "000001.SZ", 10.0, 10.5, 9.9, 10.3, 10.0, 3.0, is_lu_a],
        )
        # B
        con.execute(
            """
            INSERT INTO daily_market_snapshot
            (trade_date, ticker, open, high, low, close, pre_close, pct_change, is_limit_up)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [d, "000002.SZ", 20.0, 20.3, 19.8, 20.2, 20.0, 1.0, False],
        )
        # C (listed on Day 3 onwards)
        if i >= 2:
            con.execute(
                """
                INSERT INTO daily_market_snapshot
                (trade_date, ticker, open, high, low, close, pre_close, pct_change, is_limit_up)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [d, "300001.SZ", 50.0, 52.5, 49.5, 52.5, 50.0, 5.0, False],
            )

    # 5. Setup External Board (THS_DAILY) for comparison universe
    for d in calendar_dates:
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, pct_change) VALUES (?, ?, ?)",
            [d, "881101.TI", 2.0],  # Semi-conductor +2.0%
        )

    # 6. Create Theme 'AI_COMPUTE' and add members
    theme, coll = collection_service.create_canonical_theme(
        theme_id="TH_AI_COMPUTE",
        canonical_name="AI算力",
        source_key="AI_COMPUTE",
        effective_from=calendar_dates[0],
        available_trade_date=calendar_dates[0],
    )
    cid = coll.collection_id

    collection_service.add_member(
        theme_id="TH_AI_COMPUTE",
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=calendar_dates[0],
        available_trade_date=calendar_dates[0],
    )
    collection_service.add_member(
        theme_id="TH_AI_COMPUTE",
        collection_id=cid,
        asset_id="000002.SZ",
        effective_from=calendar_dates[0],
        available_trade_date=calendar_dates[0],
    )
    collection_service.add_member(
        theme_id="TH_AI_COMPUTE",
        collection_id=cid,
        asset_id="300001.SZ",
        effective_from=calendar_dates[2],
        available_trade_date=calendar_dates[2],
    )

    # 7. Execute Full Replay
    counts = pipeline_service.rebuild_m4_facts(
        start_date=calendar_dates[0],
        end_date=calendar_dates[-1],
        knowledge_date=calendar_dates[-1],
    )
    assert counts["theme_custom_index_daily"] == 10
    assert counts["theme_custom_index_state"] == 10
    assert counts["theme_m4_observation"] == 10

    # 8. Verify Table Contents
    daily_rows = con.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}").fetchone()[0]
    assert daily_rows == 10

    state_rows = con.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE}").fetchone()[0]
    assert state_rows == 10

    obs_rows = con.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE}").fetchone()[0]
    assert obs_rows == 10

    # 9. Test Idempotency (Running rebuild again produces exactly the same counts)
    counts_repeat = pipeline_service.rebuild_m4_facts(
        start_date=calendar_dates[0],
        end_date=calendar_dates[-1],
        knowledge_date=calendar_dates[-1],
    )
    assert counts_repeat == counts
    assert con.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE}").fetchone()[0] == 10

    # 10. Auditability verification for Day 4 (when Stock B is suspended and C is new listing)
    audit_day_4 = query_service.audit_m4_observation(
        theme_id="TH_AI_COMPUTE",
        trade_date=calendar_dates[3],
        knowledge_date=calendar_dates[-1],
    )
    assert audit_day_4.theme_id == "TH_AI_COMPUTE"
    assert audit_day_4.total_members == 3  # A, B, C
    assert audit_day_4.effective_members == 1  # Only A
    assert len(audit_day_4.excluded_members) == 2
    ex_reasons = {e["asset_id"]: e["reason"] for e in audit_day_4.excluded_members}
    assert ex_reasons["000002.SZ"] == "SUSPENDED"
    assert ex_reasons["300001.SZ"] == "NEW_LISTING_LE_5"

    # 11. Auditability verification for Day 5 (Limit-up of Stock A)
    audit_day_5 = query_service.audit_m4_observation(
        theme_id="TH_AI_COMPUTE",
        trade_date=calendar_dates[4],
        knowledge_date=calendar_dates[-1],
    )
    assert audit_day_5.theme_limit_up_count == 1
    assert audit_day_5.qualification_status == "NOT_CONFIGURED"
