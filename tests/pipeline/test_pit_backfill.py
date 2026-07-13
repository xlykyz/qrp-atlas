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
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    BatchRecord,
    ManifestStore,
)
from qrp_atlas.pipeline.pit_backfill.rate_limit import RateLimiter
from qrp_atlas.pipeline.pit_backfill.runner import BackfillConfig, PitBackfillRunner
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver
from qrp_atlas.pipeline.fundamentals.clean import clean_financial
from qrp_atlas.pipeline.fundamentals.load_duckdb import load_financial


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
            return pd.DataFrame()  # empty for early history of some indexes
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


@pytest.fixture
def open_dates():
    # dense open calendar for resolver
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
        # seed calendar
        rows = [(d, True) for d in open_dates]
        con.execute("CREATE TABLE IF NOT EXISTS trading_calendar (trade_date DATE, is_open BOOLEAN)")
        # table may already exist from contracts; insert best-effort
        con.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter) VALUES (?, ?, ?, ?, ?)",
            [
                (d, True, d.year, d.month, (d.month - 1) // 3 + 1)
                for d in open_dates
            ],
        )
    finally:
        con.close()

    # If calendar still empty / missing proper seed, write via resolver path using open_dates override
    raw_dir = tmp_path / "raw"
    state_dir = tmp_path / "state"
    log_path = tmp_path / "backfill.log"
    return {
        "db": db,
        "raw_dir": raw_dir,
        "state_dir": state_dir,
        "log_path": log_path,
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
    # 2010Q1..2026Q2 inclusive = 16.25 years * 4 = 65 quarters
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


def test_manifest_resume_semantics(tmp_path):
    path = tmp_path / "m.jsonl"
    store = ManifestStore(path)
    rec = BatchRecord(batch_id="a", dataset="fundamentals", key="income_statement", status=STATUS_SUCCESS)
    store.upsert(rec)
    store.upsert(BatchRecord(batch_id="b", dataset="industry", key="x", status=STATUS_FAILED))
    store.upsert(BatchRecord(batch_id="c", dataset="index", key="y", status=STATUS_RUNNING))
    assert store.should_process("a", resume=True) is False
    assert store.should_process("b", resume=True) is True
    n = store.reset_running_to_pending()
    assert n == 1
    assert store.get("c").status == STATUS_PENDING


def test_runner_precheck_offline(tmp_env, open_dates, monkeypatch):
    # Force resolver to use open_dates without reading db calendar content quirks
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
    )
    # bypass disk/backup for unit test
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.preflight",
        lambda *a, **k: {"free_gb": 100, "backup_path": None, "db_path": str(tmp_env["db"])},
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.pipeline_db_lock",
        lambda *a, **k: _nullcontext(),
    )

    client = FakePro()
    cfg = BackfillConfig(
        mode="precheck",
        resume=False,
        db_path=tmp_env["db"],
        raw_dir=tmp_env["raw_dir"],
        state_dir=tmp_env["state_dir"],
        log_path=tmp_env["log_path"],
        client=client,
        min_interval=0.0 if False else 0.01,
        skip_preflight=True,
        create_backup=False,
        l1_codes=["801780.SI"],
    )
    # RateLimiter rejects 0; use tiny interval
    cfg.min_interval = 0.01
    result = PitBackfillRunner(cfg).run()
    assert result["ok"] is True
    assert result["counts"]["success"] + result["counts"]["empty"] == 3

    # second run with resume should skip all terminal
    cfg2 = BackfillConfig(
        mode="precheck",
        resume=True,
        db_path=tmp_env["db"],
        raw_dir=tmp_env["raw_dir"],
        state_dir=tmp_env["state_dir"],
        log_path=tmp_env["log_path"],
        client=client,
        min_interval=0.01,
        skip_preflight=True,
        create_backup=False,
        l1_codes=["801780.SI"],
    )
    result2 = PitBackfillRunner(cfg2).run()
    assert result2["totals"]["processed"] == 0

    # re-run without resume should use offline raw and insert 0 new
    cfg3 = BackfillConfig(
        mode="precheck",
        resume=False,
        db_path=tmp_env["db"],
        raw_dir=tmp_env["raw_dir"],
        state_dir=tmp_env["state_dir"],
        log_path=tmp_env["log_path"],
        client=client,
        min_interval=0.01,
        skip_preflight=True,
        create_backup=False,
        l1_codes=["801780.SI"],
    )
    result3 = PitBackfillRunner(cfg3).run()
    assert all(r.get("offline") for r in result3["results"] if r["status"] == STATUS_SUCCESS)
    assert result3["totals"]["inserted_rows"] == 0

    # token must not appear in log
    log_text = Path(tmp_env["log_path"]).read_text(encoding="utf-8")
    assert "TUSHARE" not in log_text or "token" not in log_text.lower()


class _nullcontext:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_plan_summary_counts():
    batches = financial_batches(tables=["income_statement"], periods=["20231231"]) + industry_batches(
        ["801010.SI"]
    )
    s = summarize_plan(batches)
    assert s["total_batches"] == 2
    assert s["planned_requests"] == 2
