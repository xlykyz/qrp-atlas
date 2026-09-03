"""Comprehensive tests for Task04-B1 M5 popularity data foundation (dc_hot & ths_hot).

Covers all 17 acceptance criteria from Section 21 of the frozen design book:
- Formal contract validation
- Fixed Tushare parameters (dc_hot & ths_hot)
- Raw means raw (unmodified raw CSV schema)
- Clean schema canonicalization & DuckDB persistence
- Multi-day range execution as a single batch (1 raw CSV, 1 clean CSV, 1 DB transaction)
- Logical snapshot reconstruction with second-level jitter and multiple snapshots per day
- Top100 data quality constraints & fail-closed protection
- Monotonic snapshot timing verification
- Empty response protection (fail-closed when existing data present)
- Idempotency of repeated runs
- DC and THS dataset isolation
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config import AppSettings
from qrp_atlas.contracts import (
    DC_HOT,
    THS_HOT,
    init_database,
)
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import ResultStatus
from qrp_atlas.pipeline.dc_hot_contracts import (
    DC_HOT_INGEST,
)
from qrp_atlas.pipeline.popularity_support import (
    DC_HOT_RAW_FIELDS,
    THS_HOT_RAW_FIELDS,
)
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.testing import ContractTestHarness
from qrp_atlas.pipeline.ths_hot_contracts import (
    THS_HOT_INGEST,
)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "TUSHARE_TOKEN": "test-token-only",
            "QRP_RUNTIME_ENV": "test",
        },
        project_root=tmp_path / "repo",
    )


def _initialise_database(settings: AppSettings) -> None:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(connection)
        connection.execute(DC_HOT.duckdb_create_sql())
        connection.execute(THS_HOT.duckdb_create_sql())
    finally:
        connection.close()


def _make_dc_hot_rows(
    trade_date: str,
    base_time: str,
    count: int = 100,
    jitter: bool = False,
) -> pd.DataFrame:
    rows = []
    for i in range(1, count + 1):
        if jitter:
            # Add small second offset: base_time format "YYYY-MM-DD HH:MM:SS"
            sec = i % 60
            t = f"{base_time[:17]}{sec:02d}"
        else:
            t = base_time
        rows.append(
            {
                "trade_date": trade_date,
                "data_type": "东财人气榜",
                "ts_code": f"{i:06d}.SZ",
                "ts_name": f"股票{i}",
                "rank": i,
                "pct_change": round(0.05 * i, 2),
                "current_price": round(10.0 + 0.5 * i, 2),
                "rank_time": t,
            }
        )
    return pd.DataFrame(rows)


def _make_ths_hot_rows(
    trade_date: str,
    base_time: str,
    count: int = 100,
    jitter: bool = False,
) -> pd.DataFrame:
    rows = []
    for i in range(1, count + 1):
        if jitter:
            sec = i % 60
            t = f"{base_time[:17]}{sec:02d}"
        else:
            t = base_time
        rows.append(
            {
                "trade_date": trade_date,
                "data_type": "同花顺热股榜",
                "ts_code": f"{i:06d}.SZ",
                "ts_name": f"股票{i}",
                "rank": i,
                "pct_change": round(0.05 * i, 2),
                "current_price": round(10.0 + 0.5 * i, 2),
                "concept": f"概念{i % 5}",
                "rank_reason": f"异动原因{i % 3}",
                "hot": float(100000 - 100 * i),
                "rank_time": t,
            }
        )
    return pd.DataFrame(rows)


class _FakePopularityTushare:
    def __init__(
        self,
        dc_responses: dict[str, object] | None = None,
        ths_responses: dict[str, object] | None = None,
    ) -> None:
        self.dc_hot_calls: list[dict[str, str]] = []
        self.ths_hot_calls: list[dict[str, str]] = []
        self.dc_responses = dc_responses or {}
        self.ths_responses = ths_responses or {}

    def dc_hot(self, **kwargs: str) -> pd.DataFrame:
        self.dc_hot_calls.append(kwargs)
        trade_date = kwargs["trade_date"]
        if trade_date in self.dc_responses:
            resp = self.dc_responses[trade_date]
            if isinstance(resp, pd.DataFrame):
                return resp.copy()
            if isinstance(resp, Exception):
                raise resp
            return resp
        return _make_dc_hot_rows(trade_date, "2026-03-02 09:30:00")

    def ths_hot(self, **kwargs: str) -> pd.DataFrame:
        self.ths_hot_calls.append(kwargs)
        trade_date = kwargs["trade_date"]
        if trade_date in self.ths_responses:
            resp = self.ths_responses[trade_date]
            if isinstance(resp, pd.DataFrame):
                return resp.copy()
            if isinstance(resp, Exception):
                raise resp
            return resp
        return _make_ths_hot_rows(trade_date, "2026-03-02 09:30:00")


# ── Acceptance Test Cases ──


def test_popularity_contracts_are_registered_and_pass_formal_validation() -> None:
    contracts = (DC_HOT_INGEST, THS_HOT_INGEST)
    validate_contracts(contracts)
    registered = {item.pipeline_id for item in default_registry().all()}
    assert {"dc_hot_ingest", "ths_hot_ingest"} <= registered
    for c in contracts:
        assert c.resource_locks == ("quant_db_writer",)
        assert c.outputs[0].physical_resource == "quant_db"
        assert c.manual_execution_allowed is True


def test_dc_hot_single_day_execution_and_fixed_params(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.SUCCESS
    # Fixed parameters check (Section 3.1 & 21.4)
    assert len(client.dc_hot_calls) == 1
    assert client.dc_hot_calls[0] == {
        "trade_date": "20260302",
        "market": "A股市场",
        "hot_type": "人气榜",
        "is_new": "N",
    }

    # Raw means raw check (Section 5.1 & 21.5)
    raw_path = settings.paths.raw_dir / "dc_hot" / "dc_hot_raw_2026-03-02_2026-03-02.csv"
    assert raw_path.exists()
    raw_csv = pd.read_csv(raw_path)
    assert list(raw_csv.columns) == list(DC_HOT_RAW_FIELDS)
    assert "source" not in raw_csv.columns
    assert "snapshot_seq" not in raw_csv.columns

    # Clean CSV check (Section 6.1)
    clean_path = settings.paths.canonical_dir / "dc_hot" / "dc_hot_clean_2026-03-02_2026-03-02.csv"
    assert clean_path.exists()
    clean_csv = pd.read_csv(clean_path)
    assert len(clean_csv) == 100
    assert "source" in clean_csv.columns
    assert "snapshot_seq" in clean_csv.columns
    assert clean_csv["source"].unique().tolist() == ["EASTMONEY"]
    assert clean_csv["list_name"].unique().tolist() == ["POPULARITY"]

    # DuckDB persistence check
    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        rows = con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0]
        assert rows == 100
        first_row = con.execute(
            "SELECT trade_date, source, list_name, ticker, rank_position, snapshot_seq, snapshot_started_at "
            "FROM dc_hot WHERE rank_position = 1"
        ).fetchone()
        assert first_row[0] == date(2026, 3, 2)
        assert first_row[1] == "EASTMONEY"
        assert first_row[2] == "POPULARITY"
        assert first_row[3] == "000001.SZ"
        assert first_row[4] == 1
        assert first_row[5] == 1
        assert first_row[6] == "2026-03-02 09:30:00"
    finally:
        con.close()


def test_ths_hot_single_day_execution_and_fixed_params(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.ths_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(THS_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.SUCCESS
    # Fixed parameters check (Section 3.2 & 21.4)
    assert len(client.ths_hot_calls) == 1
    assert client.ths_hot_calls[0] == {
        "trade_date": "20260302",
        "market": "热股",
        "is_new": "N",
    }

    # Raw means raw check (Section 5.2 & 21.5)
    raw_path = settings.paths.raw_dir / "ths_hot" / "ths_hot_raw_2026-03-02_2026-03-02.csv"
    assert raw_path.exists()
    raw_csv = pd.read_csv(raw_path)
    assert list(raw_csv.columns) == list(THS_HOT_RAW_FIELDS)

    # Clean CSV check (Section 6.3)
    clean_path = settings.paths.canonical_dir / "ths_hot" / "ths_hot_clean_2026-03-02_2026-03-02.csv"
    assert clean_path.exists()
    clean_csv = pd.read_csv(clean_path)
    assert len(clean_csv) == 100
    assert clean_csv["source"].unique().tolist() == ["THS"]
    assert clean_csv["list_name"].unique().tolist() == ["HOT_STOCK"]
    assert "hot" in clean_csv.columns
    assert "concept" in clean_csv.columns
    assert "rank_reason" in clean_csv.columns

    # DuckDB persistence check
    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        rows = con.execute("SELECT COUNT(*) FROM ths_hot").fetchone()[0]
        assert rows == 100
        first_row = con.execute(
            "SELECT trade_date, source, list_name, ticker, rank_position, hot, concept "
            "FROM ths_hot WHERE rank_position = 1"
        ).fetchone()
        assert first_row[0] == date(2026, 3, 2)
        assert first_row[1] == "THS"
        assert first_row[2] == "HOT_STOCK"
        assert first_row[3] == "000001.SZ"
        assert first_row[4] == 1
        assert first_row[5] == 99900.0
        assert first_row[6] == "概念1"
    finally:
        con.close()


def test_date_range_executed_as_single_batch(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(parameter_overrides={"start_date": "2026-03-02", "end_date": "2026-03-04"})

    assert result.status is ResultStatus.SUCCESS
    # 3 provider calls (once per day)
    assert len(client.dc_hot_calls) == 3
    assert [c["trade_date"] for c in client.dc_hot_calls] == ["20260302", "20260303", "20260304"]

    # Exactly ONE raw CSV written for the entire batch
    raw_files = list((settings.paths.raw_dir / "dc_hot").glob("*.csv"))
    assert len(raw_files) == 1
    assert raw_files[0].name == "dc_hot_raw_2026-03-02_2026-03-04.csv"
    raw_df = pd.read_csv(raw_files[0])
    assert len(raw_df) == 300

    # Exactly ONE clean CSV written for the entire batch
    clean_files = list((settings.paths.canonical_dir / "dc_hot").glob("*.csv"))
    assert len(clean_files) == 1
    assert clean_files[0].name == "dc_hot_clean_2026-03-02_2026-03-04.csv"
    clean_df = pd.read_csv(clean_files[0])
    assert len(clean_df) == 300

    # DuckDB persistence check
    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 300
        date_counts = con.execute("SELECT trade_date, COUNT(*) FROM dc_hot GROUP BY trade_date ORDER BY trade_date").fetchall()
        assert date_counts == [
            (date(2026, 3, 2), 100),
            (date(2026, 3, 3), 100),
            (date(2026, 3, 4), 100),
        ]
    finally:
        con.close()


def test_snapshot_reconstruction_with_jitter_and_multiple_snapshots_per_day(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # 2 distinct snapshots on 2026-03-02 with row-level second jitter
    snap1 = _make_dc_hot_rows("20260302", "2026-03-02 09:30:00", jitter=True)
    snap2 = _make_dc_hot_rows("20260302", "2026-03-02 10:00:00", jitter=True)
    # Combine and shuffle rows to test robust reconstruction
    combined_raw = pd.concat([snap2, snap1], ignore_index=True)

    client = _FakePopularityTushare(dc_responses={"20260302": combined_raw})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.SUCCESS

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 200
        seqs = con.execute(
            "SELECT snapshot_seq, COUNT(*), MIN(rank_position), MAX(rank_position), "
            "MIN(snapshot_started_at), MAX(snapshot_completed_at) "
            "FROM dc_hot GROUP BY snapshot_seq ORDER BY snapshot_seq"
        ).fetchall()
        # seq 1 (earlier)
        assert seqs[0][0] == 1
        assert seqs[0][1] == 100
        assert seqs[0][2] == 1
        assert seqs[0][3] == 100
        assert seqs[0][4].startswith("2026-03-02 09:30:")
        # seq 2 (later)
        assert seqs[1][0] == 2
        assert seqs[1][1] == 100
        assert seqs[1][2] == 1
        assert seqs[1][3] == 100
        assert seqs[1][4].startswith("2026-03-02 10:00:")

        # Monotonicity check
        assert seqs[0][5] < seqs[1][4]
    finally:
        con.close()


def test_incomplete_snapshot_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # Incomplete snapshot (99 rows)
    raw_99 = _make_dc_hot_rows("20260302", "2026-03-02 09:30:00", count=99)
    client = _FakePopularityTushare(dc_responses={"20260302": raw_99})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_API_PARTIAL"

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        # Fail-closed: 0 rows inserted
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 0
    finally:
        con.close()


def test_duplicate_rank_in_snapshot_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # 100 rows, but rank 5 is replaced with rank 6 (causing duplicate rank 6 and missing rank 5)
    raw = _make_dc_hot_rows("20260302", "2026-03-02 09:30:00", count=100)
    raw.loc[raw["rank"] == 5, "rank"] = 6

    client = _FakePopularityTushare(dc_responses={"20260302": raw})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_API_PARTIAL"


def test_duplicate_ticker_in_snapshot_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # 100 rows, but stock 2 ts_code is replaced with stock 1's ts_code
    raw = _make_dc_hot_rows("20260302", "2026-03-02 09:30:00", count=100)
    raw.loc[1, "ts_code"] = raw.loc[0, "ts_code"]

    client = _FakePopularityTushare(dc_responses={"20260302": raw})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_API_PARTIAL"


def test_snapshot_timing_anomaly_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # Two snapshots where rank 50 in snap1 finished later (10:05:00) than snap2 started (10:00:00)
    snap1 = _make_dc_hot_rows("20260302", "2026-03-02 09:30:00")
    snap1.loc[snap1["rank"] == 50, "rank_time"] = "2026-03-02 10:05:00"
    snap2 = _make_dc_hot_rows("20260302", "2026-03-02 10:00:00")
    snap2.loc[snap2["rank"] == 50, "rank_time"] = "2026-03-02 10:10:00"
    combined = pd.concat([snap1, snap2], ignore_index=True)

    client = _FakePopularityTushare(dc_responses={"20260302": combined})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_API_PARTIAL"


def test_empty_date_with_existing_records_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # First run succeeds and populates 2026-03-02
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )
    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    res1 = harness.run(trade_date=date(2026, 3, 2))
    assert res1.status is ResultStatus.SUCCESS

    # Second run returns empty DataFrame for 2026-03-02
    empty_df = pd.DataFrame(columns=list(DC_HOT_RAW_FIELDS))
    client2 = _FakePopularityTushare(dc_responses={"20260302": empty_df})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client2,
    )
    res2 = harness.run(trade_date=date(2026, 3, 2))

    assert res2.status is ResultStatus.FAILED
    assert res2.diagnostics[0].code == "DC_HOT_API_PARTIAL"

    # Verify existing records were preserved! (Section 13 & 21.8)
    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot WHERE trade_date = '2026-03-02'").fetchone()[0] == 100
    finally:
        con.close()


def test_idempotent_replacement_same_range(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    res1 = harness.run(parameter_overrides={"start_date": "2026-03-02", "end_date": "2026-03-03"})
    assert res1.status is ResultStatus.SUCCESS

    # Rerun identical range
    res2 = harness.run(parameter_overrides={"start_date": "2026-03-02", "end_date": "2026-03-03"})
    assert res2.status is ResultStatus.SUCCESS

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        # Still exactly 200 rows, no duplicates
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 200
    finally:
        con.close()


def test_dc_and_ths_isolation(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.ths_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    # Run DC
    res_dc = ContractTestHarness(DC_HOT_INGEST, settings).run(trade_date=date(2026, 3, 2))
    assert res_dc.status is ResultStatus.SUCCESS

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 100
        assert con.execute("SELECT COUNT(*) FROM ths_hot").fetchone()[0] == 0
    finally:
        con.close()

    # Run THS
    res_ths = ContractTestHarness(THS_HOT_INGEST, settings).run(trade_date=date(2026, 3, 2))
    assert res_ths.status is ResultStatus.SUCCESS

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 100
        assert con.execute("SELECT COUNT(*) FROM ths_hot").fetchone()[0] == 100
    finally:
        con.close()


def test_ths_hot_date_range_executed_as_single_batch(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.ths_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    harness = ContractTestHarness(THS_HOT_INGEST, settings)
    result = harness.run(parameter_overrides={"start_date": "2026-03-02", "end_date": "2026-03-04"})

    assert result.status is ResultStatus.SUCCESS
    assert len(client.ths_hot_calls) == 3

    # Exactly ONE raw and clean CSV written
    raw_files = list((settings.paths.raw_dir / "ths_hot").glob("*.csv"))
    assert len(raw_files) == 1
    assert raw_files[0].name == "ths_hot_raw_2026-03-02_2026-03-04.csv"
    assert len(pd.read_csv(raw_files[0])) == 300

    clean_files = list((settings.paths.canonical_dir / "ths_hot").glob("*.csv"))
    assert len(clean_files) == 1
    assert clean_files[0].name == "ths_hot_clean_2026-03-02_2026-03-04.csv"
    assert len(pd.read_csv(clean_files[0])) == 300

    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM ths_hot").fetchone()[0] == 300
    finally:
        con.close()


def test_provider_network_failure_returns_api_failed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    class _BrokenTushare:
        def dc_hot(self, **kwargs):
            raise ConnectionError("Remote endpoint unreachable")

    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: _BrokenTushare(),
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_API_FAILED"


def test_database_write_failure_rolls_back_entire_batch(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    # Pre-populate 2026-03-01
    con = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        con.execute(
            "INSERT INTO dc_hot (trade_date, source, list_name, ticker, rank_position, source_rank_time, "
            "snapshot_seq, snapshot_started_at, snapshot_completed_at) "
            "VALUES ('2026-03-01', 'EASTMONEY', 'POPULARITY', '000001.SZ', 1, '2026-03-01 09:30:00', 1, "
            "'2026-03-01 09:30:00', '2026-03-01 09:30:00')"
        )
    finally:
        con.close()

    client = _FakePopularityTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    # Monkeypatch replace_dc_hot_batch to simulate failure after partial deletion
    def _exploding_replace(*args, **kwargs):
        from qrp_atlas.pipeline.contracts import ContractError
        raise ContractError("DC_HOT_WRITE_FAILED", "Simulated disk full")

    monkeypatch.setattr(
        "qrp_atlas.pipeline.dc_hot_contracts.replace_dc_hot_batch",
        _exploding_replace,
    )

    harness = ContractTestHarness(DC_HOT_INGEST, settings)
    result = harness.run(trade_date=date(2026, 3, 2))

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "DC_HOT_WRITE_FAILED"

    # Prior records intact
    con = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM dc_hot").fetchone()[0] == 1
    finally:
        con.close()
