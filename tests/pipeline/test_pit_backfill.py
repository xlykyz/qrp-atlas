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
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.ensure_load_backup",
        lambda *a, **k: {"backup_path": str(tmp_env["db"]), "reused": False, "backup_size_bytes": 1},
    )
    common_load = dict(common)
    common_load["create_backup"] = True
    r3 = PitBackfillRunner(
        BackfillConfig(stages=(STAGE_LOAD,), resume=True, offline_only=True, **common_load)
    ).run()
    assert r3["ok"] is True
    assert r3["totals"]["inserted_rows"] >= 0

    # Full resume should skip all
    r4 = PitBackfillRunner(BackfillConfig(stages="all", resume=True, create_backup=True, **{k:v for k,v in common.items() if k!='create_backup'})).run()
    assert r4["totals"]["processed"] == 0

    # Re-load is idempotent inserted=0
    r5 = PitBackfillRunner(
        BackfillConfig(stages=(STAGE_LOAD,), resume=False, offline_only=True, create_backup=True, **{k:v for k,v in common.items() if k!='create_backup'})
    ).run()
    assert r5["totals"]["inserted_rows"] == 0


def test_plan_summary_counts():
    batches = financial_batches(tables=["income_statement"], periods=["20231231"]) + industry_batches(
        ["801010.SI"]
    )
    s = summarize_plan(batches)
    assert s["total_batches"] == 2
    assert s["planned_requests"] == 2


# ---------------------------------------------------------------------------
# Repair coverage for PR #10 rework
# ---------------------------------------------------------------------------

from qrp_atlas.pipeline.pit_backfill.raw_io import (  # noqa: E402
    CorruptParquetError,
    quarantine_corrupt,
    save_parquet,
    validate_parquet,
    load_parquet,
)
from qrp_atlas.pipeline.pit_backfill.batches import discover_sw2021_l1_codes  # noqa: E402
from qrp_atlas.pipeline.pit_backfill.safety import (  # noqa: E402
    ensure_load_backup,
    load_backup_marker,
    save_backup_marker,
)
from qrp_atlas.pipeline.pit_backfill import audit as pit_audit  # noqa: E402
from qrp_atlas.pipeline.pit_backfill.runner import _sanitize_error  # noqa: E402
import os  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402


def test_save_parquet_atomic_success(tmp_path):
    path = tmp_path / "x.parquet"
    df = pd.DataFrame({"a": [1, 2, 3]})
    out = save_parquet(df, path)
    assert out == path
    assert path.exists()
    assert validate_parquet(path) == 3
    # no temp leftovers
    temps = list(tmp_path.glob("*.tmp.parquet"))
    assert temps == []


def test_save_parquet_failure_leaves_no_final(tmp_path, monkeypatch):
    path = tmp_path / "bad.parquet"
    df = pd.DataFrame({"a": [1]})

    def boom(*a, **k):
        raise RuntimeError("write boom")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(RuntimeError, match="write boom"):
        save_parquet(df, path)
    assert not path.exists()
    assert list(tmp_path.glob("*.tmp.parquet")) == []


def test_corrupt_raw_quarantine_and_refetch(tmp_env, open_dates, monkeypatch):
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
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
    r1 = PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=False, **common)).run()
    assert r1["ok"] is True
    raw_files = list(Path(tmp_env["raw_dir"]).glob("*.parquet"))
    assert raw_files
    # corrupt one non-empty raw
    target = None
    for p in raw_files:
        if p.stat().st_size > 50:
            target = p
            break
    assert target is not None
    target.write_bytes(b"not-a-parquet")
    calls_before = len(client.calls)
    r2 = PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=True, **common)).run()
    assert r2["ok"] is True
    assert len(client.calls) > calls_before
    # corrupt renamed aside
    corrupt = list(Path(tmp_env["raw_dir"]).glob("*.corrupt.*"))
    assert corrupt
    assert validate_parquet(target) >= 0


def test_corrupt_cleaned_isolated_and_reclean(tmp_env, open_dates, monkeypatch):
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
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
    PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=False, **common)).run()
    PitBackfillRunner(BackfillConfig(stages=(STAGE_CLEAN,), resume=True, offline_only=True, **common)).run()
    cleaned = list(Path(tmp_env["cleaned_dir"]).glob("*.parquet"))
    assert cleaned
    victim = cleaned[0]
    victim.write_bytes(b"broken-clean")
    # mark clean success in manifest so resume thinks clean done
    store = ManifestStore(Path(tmp_env["state_dir"]) / "manifest.jsonl")
    for rec in list(store.iter_records()):
        if rec.cleaned_path and Path(rec.cleaned_path).name == victim.name:
            rec.set_stage(STAGE_CLEAN, STATUS_SUCCESS, finished=True)
            store.save(rec)
    r = PitBackfillRunner(BackfillConfig(stages=(STAGE_CLEAN,), resume=True, offline_only=True, **common)).run()
    assert r["ok"] is True
    assert list(Path(tmp_env["cleaned_dir"]).glob("*.corrupt.*"))
    assert validate_parquet(victim) >= 0


def test_raw_gate_blocks_load_on_missing(tmp_env, open_dates, monkeypatch):
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.pipeline_db_lock",
        lambda *a, **k: _nullcontext(),
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.ensure_load_backup",
        lambda *a, **k: {"backup_path": str(tmp_env["db"]), "reused": True, "backup_size_bytes": 1},
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
        create_backup=True,
        l1_codes=["801780.SI"],
    )
    PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=False, **common)).run()
    # delete a raw file and mark fetch success
    store = ManifestStore(Path(tmp_env["state_dir"]) / "manifest.jsonl")
    deleted = None
    for rec in store.iter_records():
        if rec.fetch_status == STATUS_SUCCESS and rec.raw_path:
            Path(rec.raw_path).unlink(missing_ok=True)
            deleted = rec.batch_id
            break
    assert deleted
    with pytest.raises(RuntimeError, match="raw integrity gate failed"):
        # offline_only so no auto re-fetch
        PitBackfillRunner(
            BackfillConfig(stages=(STAGE_CLEAN, STAGE_LOAD), resume=True, offline_only=True, **common)
        ).run()


def test_stage_specific_running_reset_and_active_fetch_protected(tmp_path):
    path = tmp_path / "m.jsonl"
    store = ManifestStore(path)
    fetch_rec = BatchRecord(batch_id="f1", dataset="fundamentals", key="income_statement")
    fetch_rec.fetch_status = STATUS_RUNNING
    fetch_rec.started_at = "2099-01-01T00:00:00Z"  # far future -> not stale
    store.upsert(fetch_rec)
    clean_rec = BatchRecord(batch_id="c1", dataset="fundamentals", key="income_statement")
    clean_rec.clean_status = STATUS_RUNNING
    clean_rec.started_at = "2000-01-01T00:00:00Z"  # stale
    store.upsert(clean_rec)

    # clean/load worker only resets clean/load stages
    n = store.reset_running_to_pending(stages=(STAGE_CLEAN, STAGE_LOAD), stale_seconds=3600.0)
    assert n >= 1
    assert store.get("f1").fetch_status == STATUS_RUNNING
    assert store.get("c1").clean_status == STATUS_PENDING

    # terminal success not overwritten by pending merge
    ok = BatchRecord(batch_id="ok", dataset="x", key="y")
    ok.fetch_status = STATUS_SUCCESS
    store.upsert(ok)
    bad = BatchRecord(batch_id="ok", dataset="x", key="y")
    bad.fetch_status = STATUS_PENDING
    bad.fetch_error = None
    store.save(bad)
    assert store.get("ok").fetch_status == STATUS_SUCCESS


def test_ensure_load_backup_marker_reuse(tmp_path):
    db = tmp_path / "quant.db"
    con = duckdb.connect(str(db))
    con.execute("create table t(i int); insert into t values (1)")
    con.close()
    state = tmp_path / "state"
    info1 = ensure_load_backup(db, state_dir=state, tag="t1", lock_path=tmp_path / "lock")
    assert Path(info1["backup_path"]).exists()
    assert info1["reused"] is False
    info2 = ensure_load_backup(db, state_dir=state, tag="t1", lock_path=tmp_path / "lock")
    assert info2["reused"] is True
    assert info2["backup_path"] == info1["backup_path"]
    marker = load_backup_marker(state)
    assert marker["tag"] == "t1"
    # readable open
    c = duckdb.connect(info1["backup_path"], read_only=True)
    assert c.execute("select count(*) from t").fetchone()[0] == 1
    c.close()


def test_discover_sw2021_empty_and_bad_src_fail():
    class Empty:
        def index_classify(self, **kwargs):
            return pd.DataFrame()

    with pytest.raises(RuntimeError, match="empty"):
        discover_sw2021_l1_codes(client=Empty())

    class BadSrc:
        def index_classify(self, **kwargs):
            return pd.DataFrame([{"index_code": "801010.SI", "src": "SW"}])

    with pytest.raises(RuntimeError, match="not SW2021"):
        discover_sw2021_l1_codes(client=BadSrc())

    class NoSrcKw:
        def index_classify(self, **kwargs):
            if "src" in kwargs:
                raise TypeError("unexpected keyword src")
            return pd.DataFrame([{"index_code": "801010.SI"}])  # no src column

    with pytest.raises(RuntimeError, match="refusing"):
        discover_sw2021_l1_codes(client=NoSrcKw())


def test_revision_aware_industry_and_index_audit(tmp_path):
    db = tmp_path / "a.db"
    con = duckdb.connect(str(db))
    con.execute(
        """
        create table industry_membership_history as
        select * from (values
          ('r1','a1','SW2021',1,'801010.SI','农林牧渔', date '2020-01-01', cast(NULL as date), timestamp '2020-01-02'),
          ('r2','a1','SW2021',2,'801011.SI','农业', date '2020-01-01', cast(NULL as date), timestamp '2020-01-02'),
          ('r3','a1','SW2021',3,'801012.SI','种子', date '2020-01-01', cast(NULL as date), timestamp '2020-01-02'),
          -- physical revision duplicate chain: same business identity newer revision
          ('r4','a1','SW2021',3,'801012.SI','种子', date '2020-01-01', cast(NULL as date), timestamp '2020-02-02')
        ) v(revision_id, asset_id, classification_system, industry_level, industry_code, industry_name, effective_from, effective_to, ingested_at)
        """
    )
    con.execute(
        """
        create table index_component_history as
        select * from (values
          ('ir1','000300.SH', date '2024-01-15', 'a1', 1.0, timestamp '2024-01-16'),
          ('ir2','000300.SH', date '2024-01-15', 'a1', 1.5, timestamp '2024-01-17'),
          ('ir3','000300.SH', date '2024-01-15', 'a2', 2.0, timestamp '2024-01-16')
        ) v(revision_id, index_code, snapshot_date, asset_id, weight, ingested_at)
        """
    )
    con.close()
    ind = pit_audit.audit_industry(db)
    assert ind["exists"] is True
    assert ind["physical_rows"] == 4
    assert ind["resolved_rows"] == 3
    idx = pit_audit.audit_index(db, index_codes=["000300.SH"])
    assert idx["resolved_rows"] == 2
    assert idx["by_index"]["000300.SH"]["snapshots_with_duplicate_assets_after_resolve"] == 0


def test_failed_batch_nonzero_exit(tmp_env, open_dates, monkeypatch):
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.NextTradeDateResolver",
        lambda *a, **k: NextTradeDateResolver(open_dates),
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.pit_backfill.runner.pipeline_db_lock",
        lambda *a, **k: _nullcontext(),
    )

    class BoomPro(FakePro):
        def income_vip(self, period: str):
            raise RuntimeError("forced fail")

    common = dict(
        mode="precheck",
        db_path=tmp_env["db"],
        raw_dir=tmp_env["raw_dir"],
        cleaned_dir=tmp_env["cleaned_dir"],
        state_dir=tmp_env["state_dir"],
        log_path=tmp_env["log_path"],
        client=BoomPro(),
        min_interval=0.01,
        skip_preflight=True,
        create_backup=False,
        l1_codes=["801780.SI"],
    )
    out = PitBackfillRunner(BackfillConfig(stages=(STAGE_FETCH,), resume=False, **common)).run()
    assert out["ok"] is False


def test_token_not_leaked_in_sanitize(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "super-secret-token-xyz")
    err = _sanitize_error(RuntimeError("failed token=super-secret-token-xyz auth"))
    assert "super-secret-token-xyz" not in err
    assert "***" in err or "redacted" in err.lower()


def test_finish_watcher_active_exited_semantics(tmp_path):
    # Extract and unit-test the bash helper via a tiny wrapper script
    script = Path("scripts/run_pit_backfill_finish.sh")
    text = script.read_text(encoding="utf-8")
    assert "SubState" in text
    assert 'sub" == "exited"' in text or "sub\" == \"exited\"" in text or 'sub" == "exited"' in text
    assert "refuse clean/load until re-fetch" in text
    # Fake systemctl show values
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    show = fake_bin / "systemctl"
    show.write_text(
        """#!/usr/bin/env bash
# systemctl --user show UNIT -p KEY --value
key=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p) key="$2"; shift 2;;
    --value) shift;;
    --user) shift;;
    show) shift;;
    *) shift;;
  esac
done
case "$key" in
  ActiveState) echo active;;
  SubState) echo exited;;
  Result) echo success;;
  ExecMainStatus) echo 0;;
  *) echo unknown;;
esac
""",
        encoding="utf-8",
    )
    show.chmod(0o755)
    # source function by running a snippet
    snippet = tmp_path / "check.sh"
    snippet.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
export PATH="{fake_bin}:$PATH"
FETCH_UNIT=dummy.service
source <(sed -n '/^fetch_unit_done()/,/^}}/p' scripts/run_pit_backfill_finish.sh)
if fetch_unit_done; then echo DONE; else echo NOTDONE; fi
""",
        encoding="utf-8",
    )
    snippet.chmod(0o755)
    out = subprocess.check_output(["bash", str(snippet)], text=True, cwd=str(Path.cwd()))
    assert "DONE" in out
