"""Tests for task 03-B PIT financial / industry / index pipelines."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ALL_TABLES,
    AVAILABLE_TRADE_DATE,
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INCOME_STATEMENT,
    INDEX_COMPONENT_HISTORY,
    INDUSTRY_MEMBERSHIP_HISTORY,
    REVISION_ID,
    SOURCE_MAPPINGS,
    TUSHARE_BALANCESHEET,
    TUSHARE_CASHFLOW,
    TUSHARE_FINA_INDICATOR,
    TUSHARE_INCOME,
    TUSHARE_INDEX_MEMBER_ALL,
    TUSHARE_INDEX_WEIGHT,
    init_database,
)
from qrp_atlas.pipeline.fundamentals.clean import clean_financial
from qrp_atlas.pipeline.fundamentals.load_duckdb import load_financial
from qrp_atlas.pipeline.fundamentals.run import run_fundamentals, run_one as run_financial_one
from qrp_atlas.pipeline.index_component.clean import clean_index_component
from qrp_atlas.pipeline.index_component.load_duckdb import load_index_component
from qrp_atlas.pipeline.index_component.run import run_index_component
from qrp_atlas.pipeline.industry_membership.clean import clean_industry_membership
from qrp_atlas.pipeline.industry_membership.load_duckdb import load_industry_membership
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver, stable_hash


PIT_TABLES = (
    INCOME_STATEMENT,
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INDUSTRY_MEMBERSHIP_HISTORY,
    INDEX_COMPONENT_HISTORY,
)


class FakePro:
    """Minimal fake tushare client for offline pipeline tests."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def _record(self, name: str, **kwargs) -> None:
        self.calls.append((name, kwargs))

    def income_vip(self, period: str):
        self._record("income_vip", period=period)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240315",
                    "f_ann_date": "20240315",
                    "end_date": period,
                    "report_type": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "update_flag": "1",
                    "basic_eps": 2.25,
                    "diluted_eps": 2.25,
                    "total_revenue": 1.0e11,
                    "revenue": 1.0e11,
                    "operate_profit": 5.0e10,
                    "total_profit": 5.0e10,
                    "n_income": 4.0e10,
                    "n_income_attr_p": 4.0e10,
                    "ebit": 5.1e10,
                    "ebitda": 5.2e10,
                },
                {
                    "ts_code": "600519.SH",
                    "ann_date": "20240403",
                    "f_ann_date": "20240403",
                    "end_date": period,
                    "report_type": "1",
                    "comp_type": "1",
                    "end_type": "4",
                    "update_flag": "0",
                    "basic_eps": 59.1,
                    "diluted_eps": 59.1,
                    "total_revenue": 1.5e11,
                    "revenue": 1.4e11,
                    "operate_profit": 9.0e10,
                    "total_profit": 9.1e10,
                    "n_income": 7.5e10,
                    "n_income_attr_p": 7.4e10,
                    "ebit": 9.2e10,
                    "ebitda": 9.3e10,
                },
            ]
        )

    def balancesheet_vip(self, period: str):
        self._record("balancesheet_vip", period=period)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240315",
                    "f_ann_date": "20240315",
                    "end_date": period,
                    "report_type": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "update_flag": "1",
                    "total_assets": 5.0e12,
                    "total_liab": 4.5e12,
                    "total_cur_assets": None,
                    "total_nca": None,
                    "total_cur_liab": None,
                    "total_ncl": None,
                    "total_hldr_eqy_exc_min_int": 4.0e11,
                    "total_hldr_eqy_inc_min_int": 4.0e11,
                    "money_cap": 1.0e11,
                    "accounts_receiv": None,
                    "inventories": None,
                }
            ]
        )

    def cashflow_vip(self, period: str):
        self._record("cashflow_vip", period=period)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240315",
                    "f_ann_date": "20240315",
                    "end_date": period,
                    "report_type": "1",
                    "comp_type": "2",
                    "end_type": "4",
                    "update_flag": "1",
                    "n_cashflow_act": 1.0e10,
                    "n_cashflow_inv_act": -2.0e9,
                    "n_cash_flows_fnc_act": -3.0e9,
                    "n_incr_cash_cash_equ": 5.0e9,
                    "c_cash_equ_end_period": 2.0e10,
                    "free_cashflow": 8.0e9,
                }
            ]
        )

    def fina_indicator_vip(self, period: str):
        self._record("fina_indicator_vip", period=period)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240315",
                    "end_date": period,
                    "update_flag": "1",
                    "eps": 2.25,
                    "bps": 20.0,
                    "cfps": 1.1,
                    "roe": 12.0,
                    "roa": None,
                    "grossprofit_margin": None,
                    "netprofit_margin": 30.0,
                    "debt_to_assets": 90.0,
                    "current_ratio": None,
                    "quick_ratio": None,
                }
            ]
        )

    def index_member_all(self, **kwargs):
        self._record("index_member_all", **kwargs)
        return pd.DataFrame(
            [
                {
                    "l1_code": "801730.SI",
                    "l1_name": "电力设备",
                    "l2_code": "801737.SI",
                    "l2_name": "电池",
                    "l3_code": "857371.SI",
                    "l3_name": "锂电池",
                    "ts_code": "300750.SZ",
                    "name": "宁德时代",
                    "in_date": "20180528",
                    "out_date": None,
                    "is_new": "Y",
                },
                {
                    "l1_code": "801880.SI",
                    "l1_name": "汽车",
                    "l2_code": "801881.SI",
                    "l2_name": "汽车零部件",
                    "l3_code": "857001.SI",
                    "l3_name": "其他汽车零部件",
                    "ts_code": "300750.SZ",
                    "name": "宁德时代",
                    "in_date": "20180601",
                    "out_date": "20201231",
                    "is_new": "N",
                },
            ]
        )

    def index_weight(self, index_code: str, start_date: str, end_date: str):
        self._record("index_weight", index_code=index_code, start_date=start_date, end_date=end_date)
        start = int(start_date)
        end = int(end_date)
        frames = []
        # Two indices intentionally use different snapshot calendars.
        calendars = {
            "000300.SH": [
                ("20240131", 5.5, 0.8),
                ("20240229", 5.7, 0.7),
                ("20240329", 5.9, 0.6),
                ("20240430", 6.0, 0.5),
            ],
            "000905.SH": [
                ("20240115", 1.1, 0.4),
                ("20240215", 1.2, 0.3),
                ("20240315", 1.3, 0.2),
                ("20240415", 1.4, 0.1),
            ],
        }
        rows = calendars.get(
            index_code,
            [
                ("20240131", 1.0, 1.0),
                ("20240229", 1.0, 1.0),
            ],
        )
        for snap, w1, w2 in rows:
            if start <= int(snap) <= end:
                frames.extend(
                    [
                        {
                            "index_code": index_code,
                            "con_code": "600519.SH",
                            "trade_date": snap,
                            "weight": w1,
                        },
                        {
                            "index_code": index_code,
                            "con_code": "000001.SZ",
                            "trade_date": snap,
                            "weight": w2,
                        },
                    ]
                )
        return pd.DataFrame(frames)


def _open_dates() -> list[date]:
    start = date(2024, 1, 1)
    end = date(2024, 12, 31)
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _seed_calendar(con: duckdb.DuckDBPyConnection, open_dates: list[date]) -> None:
    rows = [(d, True, d.year, d.month, (d.month - 1) // 3 + 1) for d in open_dates]
    con.executemany("INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?)", rows)


@pytest.fixture
def resolver() -> NextTradeDateResolver:
    return NextTradeDateResolver(_open_dates())


@pytest.fixture
def tmp_db(tmp_path):
    path = tmp_path / "pit.duckdb"
    con = duckdb.connect(str(path))
    try:
        init_database(con)
        _seed_calendar(con, _open_dates())
    finally:
        con.close()
    return path


def test_six_tables_in_contracts_and_exports():
    names = {t.name for t in ALL_TABLES}
    for table in PIT_TABLES:
        assert table.name in names
        assert table.primary_key == (REVISION_ID,)
        for col in table.columns:
            if col.name in table.primary_key:
                assert col.nullable is False
        assert AVAILABLE_TRADE_DATE in table.column_names()


def test_mapping_alignment():
    assert SOURCE_MAPPINGS["tushare_income"] is TUSHARE_INCOME
    assert TUSHARE_INCOME["ts_code"] == "ticker"
    assert TUSHARE_INCOME["end_date"] == "report_period"
    assert TUSHARE_INCOME["ann_date"] == "announcement_date"
    assert TUSHARE_BALANCESHEET["total_assets"] == "total_assets"
    assert TUSHARE_CASHFLOW["n_cashflow_act"] == "n_cashflow_act"
    assert TUSHARE_FINA_INDICATOR["roe"] == "roe"
    assert TUSHARE_INDEX_MEMBER_ALL["ts_code"] == "asset_id"
    assert TUSHARE_INDEX_WEIGHT["con_code"] == "asset_id"
    assert TUSHARE_INDEX_WEIGHT["trade_date"] == "snapshot_date"


def test_stable_hash_is_reproducible():
    a = stable_hash(["income_statement", "000001.SZ", "2023-12-31", "1", "2024-03-15", "1"])
    b = stable_hash(["income_statement", "000001.SZ", "2023-12-31", "1", "2024-03-15", "1"])
    c = stable_hash(["income_statement", "000001.SZ", "2023-12-31", "1", "2024-03-15", "0"])
    assert a == b
    assert a != c
    assert len(a) == 16


def test_next_trade_date_and_weekend(resolver: NextTradeDateResolver):
    assert resolver.next_trade_date(date(2024, 3, 15)) == date(2024, 3, 18)
    assert resolver.next_trade_date(date(2024, 3, 16)) == date(2024, 3, 18)
    assert resolver.next_trade_date(date(2024, 3, 17)) == date(2024, 3, 18)
    assert resolver.next_trade_date(date(2024, 3, 18)) == date(2024, 3, 19)


def test_next_trade_date_raises_when_calendar_exhausted():
    open_dates = [date(2024, 3, 15), date(2024, 3, 18), date(2024, 3, 19)]
    resolver = NextTradeDateResolver(open_dates, on_calendar_exhausted="raise")

    with pytest.raises(ValueError, match="No open trade date found after 2024-03-19"):
        resolver.next_trade_date(date(2024, 3, 19))

    with pytest.raises(ValueError, match="No open trade date found after 2024-03-20"):
        resolver.next_trade_date(date(2024, 3, 20))

    # Normal mapping still works inside the calendar.
    assert resolver.next_trade_date(date(2024, 3, 15)) == date(2024, 3, 18)
    assert resolver.next_trade_date(date(2024, 3, 16)) == date(2024, 3, 18)


def test_financial_same_content_idempotent(resolver: NextTradeDateResolver):
    raw = FakePro().income_vip("20231231")
    c1 = clean_financial(raw, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    c2 = clean_financial(raw, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 2, 2))
    assert set(c1[REVISION_ID]) == set(c2[REVISION_ID])
    first_available = c1.iloc[0][AVAILABLE_TRADE_DATE]
    if hasattr(first_available, "date"):
        first_available = first_available.date()
    assert first_available == date(2024, 3, 18)


def test_financial_content_change_appends_revision(resolver: NextTradeDateResolver):
    raw = FakePro().income_vip("20231231")
    c1 = clean_financial(raw, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    raw2 = raw.copy()
    raw2.loc[raw2["ts_code"] == "000001.SZ", "n_income"] = 4.1e10
    c2 = clean_financial(raw2, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 2))
    old_ids = set(c1.loc[c1["ticker"] == "000001.SZ", REVISION_ID])
    new_ids = set(c2.loc[c2["ticker"] == "000001.SZ", REVISION_ID])
    assert old_ids.isdisjoint(new_ids)


def test_financial_four_tables_fake_pipeline(tmp_db, resolver: NextTradeDateResolver):
    client = FakePro()
    results = run_fundamentals(
        tables=("income_statement", "balance_sheet", "cashflow_statement", "financial_indicator"),
        periods=["20231231"],
        tickers=["000001.SZ", "600519.SH"],
        mode="period",
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    assert all(r["inserted"] > 0 for r in results)
    con = duckdb.connect(str(tmp_db), read_only=True)
    try:
        assert con.execute("select count(*) from income_statement").fetchone()[0] == 2
        assert con.execute("select count(*) from balance_sheet").fetchone()[0] == 1
        assert con.execute("select count(*) from cashflow_statement").fetchone()[0] == 1
        assert con.execute("select count(*) from financial_indicator").fetchone()[0] == 1
        row = con.execute(
            "select available_trade_date from income_statement where ticker='000001.SZ'"
        ).fetchone()
        assert str(row[0]) == "2024-03-18"
    finally:
        con.close()

    results2 = run_fundamentals(
        tables=("income_statement",),
        periods=["20231231"],
        tickers=["000001.SZ", "600519.SH"],
        mode="period",
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    assert results2[0]["inserted"] == 0


def test_financial_append_new_revision_in_duckdb(tmp_db, resolver: NextTradeDateResolver):
    raw = FakePro().income_vip("20231231")
    cleaned = clean_financial(raw, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert load_financial(cleaned, "income_statement", db_path=tmp_db, init=True) == 2
    raw2 = raw.copy()
    raw2.loc[0, "n_income"] = 999.0
    cleaned2 = clean_financial(raw2, "income_statement", trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 2))
    inserted = load_financial(cleaned2, "income_statement", db_path=tmp_db, init=False)
    assert inserted == 1
    con = duckdb.connect(str(tmp_db), read_only=True)
    try:
        n = con.execute("select count(*) from income_statement where ticker='000001.SZ'").fetchone()[0]
        assert n == 2
    finally:
        con.close()


def test_industry_history_pipeline(tmp_db, resolver: NextTradeDateResolver):
    raw = FakePro().index_member_all(ts_code="300750.SZ")
    cleaned = clean_industry_membership(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert len(cleaned) == 6
    assert set(cleaned["industry_level"]) == {1, 2, 3}
    assert cleaned["effective_to"].notna().any()
    inserted = load_industry_membership(cleaned, db_path=tmp_db, init=True)
    assert inserted == 6
    assert load_industry_membership(cleaned, db_path=tmp_db, init=False) == 0


def test_index_snapshot_model_no_batch_intervals(tmp_db, resolver: NextTradeDateResolver):
    raw = FakePro().index_weight("000300.SH", "20240101", "20240331")
    cleaned = clean_index_component(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert len(cleaned) == 6  # 3 snapshots * 2 constituents
    # Snapshot model: effective_from = snapshot_date, effective_to always empty.
    assert cleaned["effective_to"].isna().all()
    assert (
        cleaned["effective_from"].map(lambda x: x.date() if hasattr(x, "date") else x).tolist()
        == cleaned["snapshot_date"].map(lambda x: x.date() if hasattr(x, "date") else x).tolist()
    )
    inserted = load_index_component(cleaned, db_path=tmp_db, init=True)
    assert inserted == 6
    assert load_index_component(cleaned, db_path=tmp_db, init=False) == 0


def test_index_multi_index_isolation(resolver: NextTradeDateResolver):
    client = FakePro()
    raw = pd.concat(
        [
            client.index_weight("000300.SH", "20240101", "20240229"),
            client.index_weight("000905.SH", "20240101", "20240229"),
        ],
        ignore_index=True,
    )
    cleaned = clean_index_component(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    snaps_300 = set(
        cleaned.loc[cleaned["index_code"] == "000300.SH", "snapshot_date"].astype(str)
    )
    snaps_905 = set(
        cleaned.loc[cleaned["index_code"] == "000905.SH", "snapshot_date"].astype(str)
    )
    assert snaps_300 == {"2024-01-31", "2024-02-29"}
    assert snaps_905 == {"2024-01-15", "2024-02-15"}
    assert snaps_300.isdisjoint(snaps_905)
    # No cross-index effective_to construction; all remain empty.
    assert cleaned["effective_to"].isna().all()


def test_index_batched_backfill_snapshot_model(tmp_db, resolver: NextTradeDateResolver):
    client = FakePro()
    r1 = run_index_component(
        index_codes=["000300.SH"],
        start_date="20240101",
        end_date="20240229",
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    r2 = run_index_component(
        index_codes=["000300.SH"],
        start_date="20240301",
        end_date="20240430",
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    assert r1["inserted"] == 4  # Jan+Feb * 2 members
    assert r2["inserted"] == 4  # Mar+Apr * 2 members

    # rerun first batch remains idempotent and independent
    r1b = run_index_component(
        index_codes=["000300.SH"],
        start_date="20240101",
        end_date="20240229",
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    assert r1b["inserted"] == 0

    con = duckdb.connect(str(tmp_db), read_only=True)
    try:
        snaps = [
            str(r[0])
            for r in con.execute(
                "select distinct snapshot_date from index_component_history order by 1"
            ).fetchall()
        ]
        assert snaps == ["2024-01-31", "2024-02-29", "2024-03-29", "2024-04-30"]
        assert con.execute("select count(*) from index_component_history").fetchone()[0] == 8
        assert con.execute(
            "select count(*) from index_component_history where effective_to is not null"
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_run_entries_use_db_path_calendar_not_default(tmp_path, monkeypatch):
    """When db_path is passed without resolver, calendar is read from that DB only."""
    from qrp_atlas.pipeline import pit_utils as pu
    from qrp_atlas.pipeline.fundamentals import run as frun

    custom_db = tmp_path / "custom.duckdb"
    con = duckdb.connect(str(custom_db))
    try:
        init_database(con)
        # Custom calendar: after 2024-03-15 the next open day is 2024-03-20 (not Mon 18).
        rows = [
            (date(2024, 3, 14), True, 2024, 3, 1),
            (date(2024, 3, 15), True, 2024, 3, 1),
            (date(2024, 3, 20), True, 2024, 3, 1),
            (date(2024, 4, 3), True, 2024, 4, 2),
            (date(2024, 4, 8), True, 2024, 4, 2),
        ]
        con.executemany("INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        con.close()

    real_loader = pu.load_open_trade_dates

    def guarded_loader(db_path=None):
        if db_path is None:
            raise AssertionError("attempted to load default trading_calendar")
        return real_loader(db_path)

    monkeypatch.setattr(pu, "load_open_trade_dates", guarded_loader)
    monkeypatch.setattr(frun, "NextTradeDateResolver", pu.NextTradeDateResolver)

    result = frun.run_one(
        "income_statement",
        periods=["20231231"],
        tickers=["000001.SZ"],
        mode="period",
        client=FakePro(),
        db_path=str(custom_db),
        resolver=None,
    )
    assert result["inserted"] == 1
    con = duckdb.connect(str(custom_db), read_only=True)
    try:
        avail = con.execute(
            "select available_trade_date from income_statement where ticker='000001.SZ'"
        ).fetchone()[0]
        assert str(avail) == "2024-03-20"
    finally:
        con.close()


def test_duckdb_create_sql_for_pit_tables(tmp_path):
    db = tmp_path / "schema.duckdb"
    con = duckdb.connect(str(db))
    try:
        for table in PIT_TABLES:
            con.execute(table.duckdb_create_sql())
            con.execute(table.duckdb_create_sql())
        names = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert {t.name for t in PIT_TABLES} <= names
    finally:
        con.close()
