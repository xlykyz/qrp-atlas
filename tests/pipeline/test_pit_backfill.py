"""Offline tests for PIT historical backfill orchestrator."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import init_database
from qrp_atlas.pipeline.pit_backfill.batches import (
    financial_batches,
    index_batches,
    industry_batches,
    iter_month_ranges,
    iter_quarter_ends,
    precheck_batches,
    summarize_plan,
)
from qrp_atlas.pipeline.pit_backfill.manifest import (
    STAGE_CLEAN,
    STAGE_FETCH,
    STAGE_LOAD,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    BatchRecord,
    ManifestStore,
)
from qrp_atlas.pipeline.pit_backfill.runner import BackfillConfig, PitBackfillRunner
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver


class FakePro:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def _record(self, name: str, **kwargs):
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
                    "total_revenue": 1.0e11,
                    "n_income": 4.0e10,
                    "n_income_attr_p": 4.0e10,
                }
            ]
        )

    def balancesheet_vip(self, period: str):
        self._record("balancesheet_vip", period=period)
        return pd.DataFrame()

    def cashflow_vip(self, period: str):
        self._record("cashflow_vip", period=period)
        return pd.DataFrame()

    def fina_indicator_vip(self, period: str):
        self._record("fina_indicator_vip", period=period)
        return pd.DataFrame()

    def index_member_all(self, **kwargs):
        self._record("index_member_all", **kwargs)
        assert "is_new" not in kwargs
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "l1_code": "801780.SI",
                    "l1_name": "银行",
                    "l2_code": "801782.SI",
                    "l2_name": "银行II",
                    "l3_code": "851781.SI",
                    "l3_name": "国有银行",
                    "in_date": "20140101",
                    "out_date": None,
                    "is_new": "Y",
                },
                {
                    "ts_code": "600000.SH",
                    "l1_code": "801780.SI",
                    "l1_name": "银行",
                    "l2_code": "801782.SI",
                    "l2_name": "银行II",
                    "l3_code": "851781.SI",
                    "l3_name": "国有银行",
                    "in_date": "20100101",
                    "out_date": "20151231",
                    "is_new": "N",
                },
            ]
        )

    def index_weight(self, **kwargs):
        self._record("index_weight", **kwargs)
        start = kwargs.get("start_date", "20240101")
        if start < "20150101":
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "index_code": kwargs["index_code"],
                    "con_code": "000001.SZ",
                    "trade_date": start[:6] + "15" if len(start) == 8 else "20240115",
                    "weight": 1.2,
                },
                {
                    "index_code": kwargs["index_code"],
                    "con_code": "600519.SH",
                    "trade_date": start[:6] + "15" if len(start) == 8 else "20240115",
                    "weight": 2.3,
                },
            ]
        )

    def index_classify(self, **kwargs):
        self._record("index_classify", **kwargs)
        return pd.DataFrame(
            [
                {"index_code": "801010.SI", "industry_name": "农林牧渔", "level": "L1", "src": "SW2021"},
                {"index_code": "801780.SI", "industry_name": "银行", "level": "L1", "src": "SW2021"},
            ]
        )


class _nullcontext:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture
def open_dates():
    d0 = date(2009, 1, 1)
    days = []
    cur = d0
    while cur <= date(2026, 12, 31):
        if cur.weekday() < 5:
            days.append(cur)
        cur = date.fromordinal(cur.toordinal() + 1)
    return days


@pytest.fixture
def tmp_env(tmp_path, open_dates):
    db = tmp_path / "quant.db"
    con = duckdb.connect(str(db))
    try:
        init_database(con)
        con.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter) VALUES (?, ?, ?, ?, ?)",
            [(d, True, d.year, d.month, (d.month - 1) // 3 + 1) for d in open_dates],
        )
    finally:
        con.close()
    return {
        "db": db,
        "raw_dir": tmp_path / "raw",
        "cleaned_dir": tmp_path / "cleaned",
        "state_dir": tmp_path / "state",
        "log_path": tmp_path / "backfill.log",
        "open_dates": open_dates,
        "tmp": tmp_path,
    }


def test_quarter_and_month_generation():
    qs = iter_quarter_ends(date(2010, 3, 31), date(2011, 6, 30))
    assert qs[0] == date(2010, 3, 31)
    assert qs[-1] == date(2011, 6, 30)
    assert len(qs) == 6

    ms = iter_month_ranges(date(2010, 1, 15), date(2010, 3, 10))
    assert ms[0] == (date(2010, 1, 15), date(2010, 1, 31))
    assert ms[-1] == (date(2010, 3, 1), date(2010, 3, 10))
    assert len(ms) == 3


def test_financial_batch_shape():
    batches = financial_batches(tables=["income_statement", "balance_sheet"])
    assert len(batches) == 66 * 2
    assert batches[0].batch_id.startswith("fundamentals:income_statement:")
    assert batches[0].period == "20100331"


def test_index_batch_shape():
    batches = index_batches(index_codes=["000300.SH", "000905.SH"])
    months = iter_month_ranges()
    assert len(batches) == len(months) * 2
    assert batches[0].start_date == "20100101"


def test_precheck_batches_three():
    b = precheck_batches(l1_code="801010.SI")
    assert len(b) == 3
    assert {x.dataset for x in b} == {"fundamentals", "industry", "index"}


def test_manifest_stage_resume(tmp_path):
    path = tmp_path / "m.jsonl"
    store = ManifestStore(path)
    rec = BatchRecord(batch_id="a", dataset="fundamentals", key="income_statement")
    rec.fetch_status = STATUS_SUCCESS
    rec.clean_status = STATUS_FAILED
    rec.load_status = STATUS_PENDING
    rec.recompute_status()
    store.upsert(rec)
    assert store.should_process("a", resume=True, stages=(STAGE_FETCH, STAGE_CLEAN, STAGE_LOAD)) is True
    assert store.should_process("a", resume=True, stages=(STAGE_FETCH,)) is False
    assert store.should_process("a", resume=True, stages=(STAGE_CLEAN,)) is True

    rec2 = BatchRecord(batch_id="b", dataset="industry", key="x", status=STATUS_RUNNING)
    rec2.fetch_status = STATUS_RUNNING
    store.upsert(rec2)
    n = store.reset_running_to_pending()
    assert n >= 1
    assert store.get("b").fetch_status == STATUS_PENDING


def test_legacy_manifest_migration(tmp_path):
    path = tmp_path / "legacy.jsonl"
    # write old-style success record without stage fields
    path.write_text(
        '{"batch_id":"x","dataset":"fundamentals","key":"income_statement","status":"success",'
        '"fetched_rows":10,"cleaned_rows":9,"inserted_rows":9,"raw_path":null}\n',
        encoding="utf-8",
    )
    store = ManifestStore(path)
    rec = store.get("x")
    assert rec is not None
    assert rec.fetch_status == STATUS_SUCCESS
    assert rec.clean_status == STATUS_SUCCESS
    assert rec.load_status == STATUS_SUCCESS
    assert rec.status == STATUS_SUCCESS


def test_runner_stages_decoupled(tmp_env, open_dates, monkeypatch):
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.preflight",
        lambda *a, **k: {"free_gb": 100, "backup_path": None, "db_path": str(tmp_env["db"])},
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.pipeline_db_lock",
        lambda *a, **k: _nullcontext(),
    )

    client = FakePro()
    common = dict(
        mode="precheck",
        db_path=tmp_env["db"],
        raw_dir=tmp_env["raw_dir"],
        cleaned_dir=tmp_env["cleaned_dir"],
        state_dir=tmp_env["state_dir"],
        log_path=tmp_env["log_path"],
        client=client,
        min_interval=0.01,
        skip_preflight=True,
        create_backup=False,
        l1_codes=["801780.SI"],
    )

    # Stage 1: fetch only
    r1 = PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=False, **common)).run()
    assert r1["ok"] is True
    assert r1["request_count"] >= 1
    # raw exists, cleaned should not for success batches until clean
    raw_files = list(Path(tmp_env["raw_dir"]).glob("*.parquet"))
    assert raw_files
    # fetch calls happened
    assert any(name == "income_vip" for name, _ in client.calls)

    # Stage 2: clean only offline
    calls_before = len(client.calls)
    r2 = PitBackfillRunner(
        BackfillConfig(stages=(STAGE_CLEAN,), resume=True, offline_only=True, **common)
    ).run()
    assert r2["ok"] is True
    assert len(client.calls) == calls_before  # no new network
    cleaned_files = list(Path(tmp_env["cleaned_dir"]).glob("*.parquet"))
    assert cleaned_files

    # Stage 3: load only
    r3 = PitBackfillRunner(
        BackfillConfig(stages=(STAGE_LOAD,), resume=True, offline_only=True, **common)
    ).run()
    assert r3["ok"] is True
    assert r3["totals"]["inserted_rows"] >= 0

    # Full resume should skip all
    r4 = PitBackfillRunner(BackfillConfig(stages="all", resume=True, **common)).run()
    assert r4["totals"]["processed"] == 0

    # Re-load is idempotent inserted=0
    r5 = PitBackfillRunner(
        BackfillConfig(stages=(STAGE_LOAD,), resume=False, offline_only=True, **common)
    ).run()
    assert r5["totals"]["inserted_rows"] == 0


def test_plan_summary_counts():
    batches = financial_batches(tables=["income_statement"], periods=["20231231"]) + industry_batches(
        ["801010.SI"]
    )
    s = summarize_plan(batches)
    assert s["total_batches"] == 2
    assert s["planned_requests"] == 2
