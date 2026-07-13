"""Tests for point-in-time historical query services (task 03-C)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.pit_queries import (
    IndustryMembershipConflictError,
    query_financial_as_of,
    query_index_components_as_of,
    query_industry_as_of,
    summarize_index_components,
)
from qrp_atlas.contracts import (
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INCOME_STATEMENT,
    INDEX_COMPONENT_HISTORY,
    INDUSTRY_MEMBERSHIP_HISTORY,
    init_database,
)


def _seed_calendar(con: duckdb.DuckDBPyConnection) -> None:
    rows = []
    d = date(2024, 1, 1)
    while d <= date(2024, 12, 31):
        if d.weekday() < 5:
            rows.append((d, True, d.year, d.month, (d.month - 1) // 3 + 1))
        d = date.fromordinal(d.toordinal() + 1)
    con.executemany("INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?)", rows)


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    con.register("tmp_q", df)
    cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM tmp_q")
    con.unregister("tmp_q")


@pytest.fixture
def pit_db(tmp_path: Path) -> Path:
    path = tmp_path / "pit_query.duckdb"
    con = duckdb.connect(str(path))
    try:
        init_database(con)
        _seed_calendar(con)

        # Financial: same business entity with old then new revision.
        income = pd.DataFrame(
            [
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "f_ann_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 3, 18),
                    "report_type": "1",
                    "update_flag": "0",
                    "comp_type": "2",
                    "end_type": "4",
                    "basic_eps": 2.0,
                    "diluted_eps": 2.0,
                    "total_revenue": 100.0,
                    "revenue": 100.0,
                    "operate_profit": 50.0,
                    "total_profit": 50.0,
                    "n_income": 40.0,
                    "n_income_attr_p": 40.0,
                    "ebit": 51.0,
                    "ebitda": 52.0,
                    "source": "tushare",
                    "source_record_id": "inc_old",
                    "revision_id": "rev_income_old",
                    "ingested_at": datetime(2024, 3, 18, 1, 0, 0),
                },
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "f_ann_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 4, 1),
                    "report_type": "1",
                    "update_flag": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "basic_eps": 2.1,
                    "diluted_eps": 2.1,
                    "total_revenue": 110.0,
                    "revenue": 110.0,
                    "operate_profit": 55.0,
                    "total_profit": 55.0,
                    "n_income": 45.0,
                    "n_income_attr_p": 45.0,
                    "ebit": 56.0,
                    "ebitda": 57.0,
                    "source": "tushare",
                    "source_record_id": "inc_new",
                    "revision_id": "rev_income_new",
                    "ingested_at": datetime(2024, 4, 1, 1, 0, 0),
                },
                # different report_type should not mix
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "f_ann_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 3, 18),
                    "report_type": "2",
                    "update_flag": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "basic_eps": 1.0,
                    "diluted_eps": 1.0,
                    "total_revenue": 10.0,
                    "revenue": 10.0,
                    "operate_profit": 5.0,
                    "total_profit": 5.0,
                    "n_income": 4.0,
                    "n_income_attr_p": 4.0,
                    "ebit": 5.0,
                    "ebitda": 5.0,
                    "source": "tushare",
                    "source_record_id": "inc_rt2",
                    "revision_id": "rev_income_rt2",
                    "ingested_at": datetime(2024, 3, 18, 1, 0, 0),
                },
                {
                    "ticker": "600519.SH",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 4, 3),
                    "f_ann_date": date(2024, 4, 3),
                    "published_at": None,
                    "available_trade_date": date(2024, 4, 8),
                    "report_type": "1",
                    "update_flag": "1",
                    "comp_type": "1",
                    "end_type": "4",
                    "basic_eps": 50.0,
                    "diluted_eps": 50.0,
                    "total_revenue": 1000.0,
                    "revenue": 1000.0,
                    "operate_profit": 500.0,
                    "total_profit": 500.0,
                    "n_income": 400.0,
                    "n_income_attr_p": 400.0,
                    "ebit": 510.0,
                    "ebitda": 520.0,
                    "source": "tushare",
                    "source_record_id": "inc_mt",
                    "revision_id": "rev_income_mt",
                    "ingested_at": datetime(2024, 4, 8, 1, 0, 0),
                },
            ]
        )
        _insert_df(con, INCOME_STATEMENT.name, income)

        fina = pd.DataFrame(
            [
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 3, 18),
                    "update_flag": "0",
                    "eps": 2.0,
                    "bps": 20.0,
                    "cfps": 1.0,
                    "roe": 10.0,
                    "roa": None,
                    "grossprofit_margin": None,
                    "netprofit_margin": 30.0,
                    "debt_to_assets": 90.0,
                    "current_ratio": None,
                    "quick_ratio": None,
                    "source": "tushare",
                    "source_record_id": "fi_old",
                    "revision_id": "rev_fi_old",
                    "ingested_at": datetime(2024, 3, 18, 1, 0, 0),
                },
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 4, 1),
                    "update_flag": "1",
                    "eps": 2.2,
                    "bps": 21.0,
                    "cfps": 1.1,
                    "roe": 11.0,
                    "roa": None,
                    "grossprofit_margin": None,
                    "netprofit_margin": 31.0,
                    "debt_to_assets": 89.0,
                    "current_ratio": None,
                    "quick_ratio": None,
                    "source": "tushare",
                    "source_record_id": "fi_new",
                    "revision_id": "rev_fi_new",
                    "ingested_at": datetime(2024, 4, 1, 1, 0, 0),
                },
            ]
        )
        _insert_df(con, FINANCIAL_INDICATOR.name, fina)

        # also empty-capable other two tables with one row each
        bal = pd.DataFrame(
            [
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "f_ann_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 3, 18),
                    "report_type": "1",
                    "update_flag": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "total_assets": 1000.0,
                    "total_liab": 900.0,
                    "total_cur_assets": None,
                    "total_nca": None,
                    "total_cur_liab": None,
                    "total_ncl": None,
                    "total_hldr_eqy_exc_min_int": 100.0,
                    "total_hldr_eqy_inc_min_int": 100.0,
                    "money_cap": 50.0,
                    "accounts_receiv": None,
                    "inventories": None,
                    "source": "tushare",
                    "source_record_id": "bs1",
                    "revision_id": "rev_bs1",
                    "ingested_at": datetime(2024, 3, 18, 1, 0, 0),
                }
            ]
        )
        _insert_df(con, BALANCE_SHEET.name, bal)
        cf = bal.rename(columns={}).copy()
        # rebuild cashflow row simply
        cf = pd.DataFrame(
            [
                {
                    "ticker": "000001.SZ",
                    "report_period": date(2023, 12, 31),
                    "announcement_date": date(2024, 3, 15),
                    "f_ann_date": date(2024, 3, 15),
                    "published_at": None,
                    "available_trade_date": date(2024, 3, 18),
                    "report_type": "1",
                    "update_flag": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "n_cashflow_act": 10.0,
                    "n_cashflow_inv_act": -2.0,
                    "n_cash_flows_fnc_act": -1.0,
                    "n_incr_cash_cash_equ": 7.0,
                    "c_cash_equ_end_period": 20.0,
                    "free_cashflow": 8.0,
                    "source": "tushare",
                    "source_record_id": "cf1",
                    "revision_id": "rev_cf1",
                    "ingested_at": datetime(2024, 3, 18, 1, 0, 0),
                }
            ]
        )
        _insert_df(con, CASHFLOW_STATEMENT.name, cf)

        # Industry: open interval + exit sample + revision + overlap asset
        industry = pd.DataFrame(
            [
                # A open path L1/L2/L3
                {
                    "asset_id": "A",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801730.SI",
                    "industry_name": "电力设备",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2020, 1, 3),
                    "source": "tushare",
                    "source_record_id": "ia1",
                    "revision_id": "rev_ia1",
                    "ingested_at": datetime(2020, 1, 3, 1, 0, 0),
                },
                {
                    "asset_id": "A",
                    "classification_system": "sw2021",
                    "industry_level": 2,
                    "industry_code": "801737.SI",
                    "industry_name": "电池",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2020, 1, 3),
                    "source": "tushare",
                    "source_record_id": "ia2",
                    "revision_id": "rev_ia2",
                    "ingested_at": datetime(2020, 1, 3, 1, 0, 0),
                },
                {
                    "asset_id": "A",
                    "classification_system": "sw2021",
                    "industry_level": 3,
                    "industry_code": "857371.SI",
                    "industry_name": "锂电池",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2020, 1, 3),
                    "source": "tushare",
                    "source_record_id": "ia3",
                    "revision_id": "rev_ia3",
                    "ingested_at": datetime(2020, 1, 3, 1, 0, 0),
                },
                # B membership with exit on 2022-07-28 (half-open: invalid on exit day)
                {
                    "asset_id": "B",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801730.SI",
                    "industry_name": "电力设备",
                    "effective_from": date(2019, 1, 2),
                    "effective_to": date(2022, 7, 28),
                    "available_trade_date": date(2019, 1, 3),
                    "source": "tushare",
                    "source_record_id": "ib1",
                    "revision_id": "rev_ib1",
                    "ingested_at": datetime(2019, 1, 3, 1, 0, 0),
                },
                # C revised industry name later
                {
                    "asset_id": "C",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801120.SI",
                    "industry_name": "食品饮料-旧",
                    "effective_from": date(2018, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2018, 1, 3),
                    "source": "tushare",
                    "source_record_id": "ic_old",
                    "revision_id": "rev_ic_old",
                    "ingested_at": datetime(2018, 1, 3, 1, 0, 0),
                },
                {
                    "asset_id": "C",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801120.SI",
                    "industry_name": "食品饮料-新",
                    "effective_from": date(2018, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2021, 6, 1),
                    "source": "tushare",
                    "source_record_id": "ic_new",
                    "revision_id": "rev_ic_new",
                    "ingested_at": datetime(2021, 6, 1, 1, 0, 0),
                },
                # E append-only exit revision:
                # old open version remains; later version fills effective_to.
                {
                    "asset_id": "E",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801180.SI",
                    "industry_name": "房地产",
                    "effective_from": date(2015, 1, 5),
                    "effective_to": None,
                    "available_trade_date": date(2015, 1, 6),
                    "source": "tushare",
                    "source_record_id": "ie_open",
                    "revision_id": "rev_ie_open",
                    "ingested_at": datetime(2015, 1, 6, 1, 0, 0),
                },
                {
                    "asset_id": "E",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801180.SI",
                    "industry_name": "房地产",
                    "effective_from": date(2015, 1, 5),
                    "effective_to": date(2020, 6, 1),
                    # Available before exit so as-of queries can observe closed-rev selection
                    # while membership is still valid under half-open semantics.
                    "available_trade_date": date(2020, 5, 20),
                    "source": "tushare",
                    "source_record_id": "ie_closed",
                    "revision_id": "rev_ie_closed",
                    "ingested_at": datetime(2020, 5, 20, 1, 0, 0),
                },
                # D intentional overlap (two different codes same level active)
                {
                    "asset_id": "D",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801730.SI",
                    "industry_name": "电力设备",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2020, 1, 3),
                    "source": "tushare",
                    "source_record_id": "id1",
                    "revision_id": "rev_id1",
                    "ingested_at": datetime(2020, 1, 3, 1, 0, 0),
                },
                {
                    "asset_id": "D",
                    "classification_system": "sw2021",
                    "industry_level": 1,
                    "industry_code": "801880.SI",
                    "industry_name": "汽车",
                    "effective_from": date(2020, 1, 2),
                    "effective_to": None,
                    "available_trade_date": date(2020, 1, 3),
                    "source": "tushare",
                    "source_record_id": "id2",
                    "revision_id": "rev_id2",
                    "ingested_at": datetime(2020, 1, 3, 2, 0, 0),
                },
            ]
        )
        _insert_df(con, INDUSTRY_MEMBERSHIP_HISTORY.name, industry)

        # Index snapshots for two indices with different calendars
        index_rows = []
        for asset, w1, w2 in [("X", 1.0, 2.0), ("Y", 99.0, 98.0)]:
            index_rows.extend(
                [
                    {
                        "index_code": "000300.SH",
                        "asset_id": asset,
                        "snapshot_date": date(2024, 1, 31),
                        "weight": w1,
                        "effective_from": date(2024, 1, 31),
                        "effective_to": None,
                        "available_trade_date": date(2024, 2, 1),
                        "source": "tushare",
                        "source_record_id": f"i300_{asset}_1",
                        "revision_id": f"rev_i300_{asset}_1",
                        "ingested_at": datetime(2024, 2, 1, 1, 0, 0),
                    },
                    {
                        "index_code": "000300.SH",
                        "asset_id": asset,
                        "snapshot_date": date(2024, 2, 29),
                        "weight": w2,
                        "effective_from": date(2024, 2, 29),
                        "effective_to": None,
                        "available_trade_date": date(2024, 3, 1),
                        "source": "tushare",
                        "source_record_id": f"i300_{asset}_2",
                        "revision_id": f"rev_i300_{asset}_2",
                        "ingested_at": datetime(2024, 3, 1, 1, 0, 0),
                    },
                ]
            )
        # revision on first snapshot for X, available later
        index_rows.append(
            {
                "index_code": "000300.SH",
                "asset_id": "X",
                "snapshot_date": date(2024, 1, 31),
                "weight": 1.5,
                "effective_from": date(2024, 1, 31),
                "effective_to": None,
                "available_trade_date": date(2024, 2, 15),
                "source": "tushare",
                "source_record_id": "i300_X_1b",
                "revision_id": "rev_i300_X_1b",
                "ingested_at": datetime(2024, 2, 15, 1, 0, 0),
            }
        )
        # other index
        for asset, w in [("P", 10.0), ("Q", 90.0)]:
            index_rows.append(
                {
                    "index_code": "000905.SH",
                    "asset_id": asset,
                    "snapshot_date": date(2024, 1, 15),
                    "weight": w,
                    "effective_from": date(2024, 1, 15),
                    "effective_to": None,
                    "available_trade_date": date(2024, 1, 16),
                    "source": "tushare",
                    "source_record_id": f"i905_{asset}",
                    "revision_id": f"rev_i905_{asset}",
                    "ingested_at": datetime(2024, 1, 16, 1, 0, 0),
                }
            )
        _insert_df(con, INDEX_COMPONENT_HISTORY.name, pd.DataFrame(index_rows))
    finally:
        con.close()
    return path


# ── Financial ─────────────────────────────────────────────────────────────


def test_financial_table_whitelist(pit_db: Path):
    with pytest.raises(ValueError, match="unsupported financial table"):
        query_financial_as_of(as_of_date="2024-03-18", table="not_a_table", db_path=pit_db)


def test_financial_available_boundary_and_revision_switch(pit_db: Path):
    before = query_financial_as_of(
        as_of_date="2024-03-15",
        table="income_statement",
        tickers="000001.SZ",
        db_path=pit_db,
    )
    assert before.empty

    day = query_financial_as_of(
        as_of_date="2024-03-18",
        table="income_statement",
        tickers="000001.SZ",
        db_path=pit_db,
    )
    # report_type 1 old + report_type 2
    assert len(day) == 2
    rt1 = day[day["report_type"] == "1"].iloc[0]
    assert rt1["revision_id"] == "rev_income_old"
    assert rt1["n_income"] == 40.0

    after = query_financial_as_of(
        as_of_date="2024-04-01",
        table="income_statement",
        tickers="000001.SZ",
        db_path=pit_db,
    )
    rt1b = after[after["report_type"] == "1"].iloc[0]
    assert rt1b["revision_id"] == "rev_income_new"
    assert rt1b["n_income"] == 45.0


def test_financial_business_grouping_and_multi_ticker(pit_db: Path):
    out = query_financial_as_of(
        as_of_date="2024-04-08",
        table="income_statement",
        db_path=pit_db,
    )
    assert set(out["ticker"]) == {"000001.SZ", "600519.SH"}
    # 000001 has two business groups (report_type 1 and 2)
    a = out[out["ticker"] == "000001.SZ"]
    assert set(a["report_type"]) == {"1", "2"}
    # stable full ordering by business keys
    ordered = out.sort_values(
        ["ticker", "report_period", "report_type", "comp_type", "end_type"],
        kind="mergesort",
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(out.reset_index(drop=True), ordered)


def test_financial_indicator_without_report_type(pit_db: Path):
    old = query_financial_as_of(
        as_of_date="2024-03-18",
        table="financial_indicator",
        tickers=["000001.SZ"],
        db_path=pit_db,
    )
    assert len(old) == 1
    assert old.iloc[0]["eps"] == 2.0
    new = query_financial_as_of(
        as_of_date="2024-04-01",
        table="financial_indicator",
        tickers=["000001.SZ"],
        db_path=pit_db,
    )
    assert new.iloc[0]["eps"] == 2.2
    assert "report_type" not in new.columns


def test_financial_all_four_tables_and_empty(pit_db: Path):
    for table in (
        "income_statement",
        "balance_sheet",
        "cashflow_statement",
        "financial_indicator",
    ):
        out = query_financial_as_of(as_of_date="2024-03-18", table=table, tickers="000001.SZ", db_path=pit_db)
        assert len(out) >= 1
    empty = query_financial_as_of(
        as_of_date="2024-03-18",
        table="income_statement",
        tickers="999999.SZ",
        db_path=pit_db,
    )
    assert empty.empty


def test_financial_custom_db_path_readonly(pit_db: Path, tmp_path: Path):
    # Query uses custom db and does not require default DB.
    out = query_financial_as_of(as_of_date="2024-03-18", table="balance_sheet", db_path=pit_db)
    assert len(out) == 1
    # reopen read-write to ensure file still intact / not corrupted by query path
    con = duckdb.connect(str(pit_db))
    try:
        n = con.execute("select count(*) from balance_sheet").fetchone()[0]
        assert n == 1
    finally:
        con.close()


# ── Industry ──────────────────────────────────────────────────────────────


def test_industry_interval_and_future_exit_mask(pit_db: Path):
    # before effective
    before = query_industry_as_of(as_of_date="2018-12-31", asset_ids="B", db_path=pit_db)
    assert before.empty

    # effective_from day before available_trade_date is still unavailable
    before_avail = query_industry_as_of(as_of_date="2019-01-02", asset_ids="B", db_path=pit_db)
    assert before_avail.empty
    # first available day
    on_from = query_industry_as_of(as_of_date="2019-01-03", asset_ids="B", db_path=pit_db)
    assert len(on_from) == 1

    # during open before exit
    mid = query_industry_as_of(as_of_date="2022-07-27", asset_ids="B", db_path=pit_db)
    assert len(mid) == 1
    # future exit masked
    assert pd.isna(mid.iloc[0]["effective_to"])

    # on effective_to day: half-open, no longer valid
    on_to = query_industry_as_of(as_of_date="2022-07-28", asset_ids="B", db_path=pit_db)
    assert on_to.empty

    # audit mode can retain future exit date
    mid_audit = query_industry_as_of(
        as_of_date="2022-07-27",
        asset_ids="B",
        mask_future_effective_to=False,
        db_path=pit_db,
    )
    assert str(mid_audit.iloc[0]["effective_to"].date()) == "2022-07-28"


def test_industry_full_path_revision_and_overlap(pit_db: Path):
    path = query_industry_as_of(
        as_of_date="2024-01-02",
        asset_ids="A",
        include_full_path=True,
        db_path=pit_db,
    )
    assert len(path) == 1
    assert path.iloc[0]["l1_code"] == "801730.SI"
    assert path.iloc[0]["l2_code"] == "801737.SI"
    assert path.iloc[0]["l3_code"] == "857371.SI"

    old = query_industry_as_of(as_of_date="2020-01-02", asset_ids="C", industry_level=1, db_path=pit_db)
    assert old.iloc[0]["industry_name"] == "食品饮料-旧"
    new = query_industry_as_of(as_of_date="2021-06-01", asset_ids="C", industry_level=1, db_path=pit_db)
    assert new.iloc[0]["industry_name"] == "食品饮料-新"

    with pytest.raises(IndustryMembershipConflictError):
        query_industry_as_of(as_of_date="2020-06-01", asset_ids="D", industry_level=1, db_path=pit_db)


def test_industry_stable_sort(pit_db: Path):
    out = query_industry_as_of(as_of_date="2024-01-02", asset_ids=["A", "C"], db_path=pit_db)
    keys = list(zip(out["asset_id"], out["industry_level"], out["industry_code"]))
    assert keys == sorted(keys)



def test_industry_append_only_exit_revision_not_reopen(pit_db: Path):
    """Later exit revision must supersede older open version after it is available.

    Historical open row remains stored, but as-of queries after the exit revision
    becomes available must not fall back to the stale open version.
    """
    # Before exit revision is available: still the open version.
    before_rev = query_industry_as_of(
        as_of_date="2020-05-19",
        asset_ids="E",
        industry_level=1,
        db_path=pit_db,
        mask_future_effective_to=False,
    )
    assert len(before_rev) == 1
    assert before_rev.iloc[0]["revision_id"] == "rev_ie_open"
    assert pd.isna(before_rev.iloc[0]["effective_to"])

    # After closed revision available and still before exit: select closed revision.
    pre_exit = query_industry_as_of(
        as_of_date="2020-05-29",
        asset_ids="E",
        industry_level=1,
        db_path=pit_db,
        mask_future_effective_to=False,
    )
    assert len(pre_exit) == 1
    assert pre_exit.iloc[0]["revision_id"] == "rev_ie_closed"
    assert str(pd.Timestamp(pre_exit.iloc[0]["effective_to"]).date()) == "2020-06-01"

    # Research default masks future effective_to so exit does not leak.
    pre_exit_masked = query_industry_as_of(
        as_of_date="2020-05-29",
        asset_ids="E",
        industry_level=1,
        db_path=pit_db,
    )
    assert len(pre_exit_masked) == 1
    assert pre_exit_masked.iloc[0]["revision_id"] == "rev_ie_closed"
    assert pd.isna(pre_exit_masked.iloc[0]["effective_to"])

    # Exit day and thereafter are invalid under half-open semantics.
    for d in ["2020-06-01", "2020-06-02", "2020-12-31", "2021-01-04", "2024-01-02"]:
        out = query_industry_as_of(as_of_date=d, asset_ids="E", industry_level=1, db_path=pit_db)
        assert out.empty


def test_empty_sequence_filters_do_not_full_scan(pit_db: Path):
    # empty sequences mean "no matches", never "all rows"
    assert query_financial_as_of(
        as_of_date="2024-03-18",
        table="income_statement",
        tickers=[],
        db_path=pit_db,
    ).empty
    assert query_industry_as_of(
        as_of_date="2024-01-02",
        asset_ids=[],
        db_path=pit_db,
    ).empty
    assert query_industry_as_of(
        as_of_date="2024-01-02",
        industry_level=[],
        db_path=pit_db,
    ).empty


# ── Index ─────────────────────────────────────────────────────────────────


def test_index_snapshot_selection_boundaries(pit_db: Path):
    empty = query_index_components_as_of(as_of_date="2024-01-31", index_code="000300.SH", db_path=pit_db)
    assert empty.empty  # snapshot exists but available only on 2024-02-01

    first = query_index_components_as_of(as_of_date="2024-02-01", index_code="000300.SH", db_path=pit_db)
    assert set(first["asset_id"]) == {"X", "Y"}
    assert str(pd.Timestamp(first["snapshot_date"].iloc[0]).date()) == "2024-01-31"
    assert float(first.loc[first["asset_id"] == "X", "weight"].iloc[0]) == 1.0

    # between snapshots, still first snapshot; revision on X becomes available 2024-02-15
    mid = query_index_components_as_of(as_of_date="2024-02-20", index_code="000300.SH", db_path=pit_db)
    assert str(pd.Timestamp(mid["snapshot_date"].iloc[0]).date()) == "2024-01-31"
    assert float(mid.loc[mid["asset_id"] == "X", "weight"].iloc[0]) == 1.5

    second = query_index_components_as_of(as_of_date="2024-03-01", index_code="000300.SH", db_path=pit_db)
    assert str(pd.Timestamp(second["snapshot_date"].iloc[0]).date()) == "2024-02-29"
    assert float(second.loc[second["asset_id"] == "X", "weight"].iloc[0]) == 2.0


def test_index_multi_index_isolation_and_summary(pit_db: Path):
    csi = query_index_components_as_of(as_of_date="2024-02-01", index_code="000300.SH", db_path=pit_db)
    zz = query_index_components_as_of(as_of_date="2024-02-01", index_code="000905.SH", db_path=pit_db)
    assert set(csi["asset_id"]).isdisjoint(set(zz["asset_id"]))
    assert set(zz["asset_id"]) == {"P", "Q"}
    summary = summarize_index_components(csi)
    assert summary["component_count"] == 2
    assert abs(summary["weight_sum"] - 100.0) < 1e-9
    # stable sort
    assert list(csi["asset_id"]) == sorted(csi["asset_id"])


def test_query_services_do_not_mutate_inputs(pit_db: Path):
    # no caller-owned mutable frame is required, but ensure repeated queries stable
    a = query_financial_as_of(as_of_date="2024-03-18", table="income_statement", db_path=pit_db)
    b = query_financial_as_of(as_of_date="2024-03-18", table="income_statement", db_path=pit_db)
    pd.testing.assert_frame_equal(a, b)
