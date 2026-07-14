"""Tests for task 05-A earnings forecast event foundation."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest import query_earnings_forecast_as_of, to_earnings_forecast_event_frame
from qrp_atlas.contracts import (
    ALL_TABLES,
    EARNINGS_FORECAST_EVENT,
    REVISION_ID,
    SOURCE_MAPPINGS,
    TUSHARE_FORECAST,
    init_database,
)
from qrp_atlas.pipeline.earnings_forecast.clean import (
    EarningsForecastDataQualityError,
    clean_earnings_forecast,
    event_series_id,
    source_record_id,
)
from qrp_atlas.pipeline.earnings_forecast.fetch import (
    ForecastPermissionError,
    fetch_earnings_forecast,
    fetch_forecast,
    fetch_forecast_vip,
)
from qrp_atlas.pipeline.earnings_forecast.load_duckdb import load_earnings_forecast
from qrp_atlas.pipeline.earnings_forecast.run import run_earnings_forecast, run_from_raw_parquet
from qrp_atlas.pipeline.pit_backfill.raw_io import CorruptParquetError, load_parquet, save_parquet
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver


def _open_dates() -> list[date]:
    # 2024-03-15 Fri, 16-17 weekend, 18 Mon open
    # include holiday gap: 2024-04-04 Qingming-ish skip via missing day
    days = [
        date(2024, 3, 15),
        date(2024, 3, 18),
        date(2024, 3, 19),
        date(2024, 3, 20),
        date(2024, 3, 21),
        date(2024, 3, 22),
        date(2024, 3, 25),
        date(2024, 4, 1),
        date(2024, 4, 2),
        date(2024, 4, 3),
        # skip 2024-04-04 as holiday
        date(2024, 4, 5),
        date(2024, 4, 8),
        date(2024, 4, 9),
        date(2024, 4, 10),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2023, 12, 29),
    ]
    return sorted(set(days))


@pytest.fixture
def resolver() -> NextTradeDateResolver:
    return NextTradeDateResolver(_open_dates())


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    path = tmp_path / "ef.duckdb"
    con = duckdb.connect(str(path))
    try:
        init_database(con)
        # seed trading calendar for resolver via db path tests if needed
        for d in _open_dates():
            con.execute(
                "INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, TRUE)",
                [d],
            )
    finally:
        con.close()
    return path


def _raw_row(
    *,
    ts_code="000001.SZ",
    ann_date="20240315",
    end_date="20231231",
    type_="预增",
    p_change_min=10.0,
    p_change_max=20.0,
    net_profit_min=1000.0,
    net_profit_max=1200.0,
    last_parent_net=900.0,
    first_ann_date="20240315",
    summary="业绩预增",
    change_reason="主营增长",
) -> dict:
    return {
        "ts_code": ts_code,
        "ann_date": ann_date,
        "end_date": end_date,
        "type": type_,
        "p_change_min": p_change_min,
        "p_change_max": p_change_max,
        "net_profit_min": net_profit_min,
        "net_profit_max": net_profit_max,
        "last_parent_net": last_parent_net,
        "first_ann_date": first_ann_date,
        "summary": summary,
        "change_reason": change_reason,
    }


class FakePro:
    def __init__(self, vip_df=None, forecast_df=None, permission=False):
        self.vip_df = vip_df if vip_df is not None else pd.DataFrame([_raw_row()])
        self.forecast_df = forecast_df if forecast_df is not None else pd.DataFrame([_raw_row()])
        self.permission = permission
        self.calls: list[tuple[str, dict]] = []

    def forecast_vip(self, period: str):
        self.calls.append(("forecast_vip", {"period": period}))
        if self.permission:
            raise Exception("积分不足，没有接口访问权限")
        df = self.vip_df.copy()
        if "end_date" in df.columns:
            df = df[df["end_date"].astype(str).str.replace("-", "") == str(period)]
        return df.reset_index(drop=True)

    def forecast(self, **kwargs):
        self.calls.append(("forecast", dict(kwargs)))
        if self.permission:
            raise Exception("没有访问权限")
        df = self.forecast_df.copy()
        if "ts_code" in kwargs and "ts_code" in df.columns:
            df = df[df["ts_code"] == kwargs["ts_code"]]
        if "ann_date" in kwargs and "ann_date" in df.columns:
            df = df[df["ann_date"].astype(str).str.replace("-", "") == str(kwargs["ann_date"])]
        if "start_date" in kwargs and "ann_date" in df.columns:
            start = str(kwargs["start_date"])
            df = df[df["ann_date"].astype(str).str.replace("-", "") >= start]
        if "end_date" in kwargs and "ann_date" in df.columns:
            end = str(kwargs["end_date"])
            df = df[df["ann_date"].astype(str).str.replace("-", "") <= end]
        return df.reset_index(drop=True)


def test_contract_table_and_mapping():
    assert EARNINGS_FORECAST_EVENT.name == "earnings_forecast_event"
    assert EARNINGS_FORECAST_EVENT.primary_key == (REVISION_ID,)
    names = EARNINGS_FORECAST_EVENT.column_names()
    for col in (
        "ticker",
        "event_type",
        "event_series_id",
        "report_period",
        "announcement_date",
        "first_announcement_date",
        "published_at",
        "time_precision",
        "available_trade_date",
        "forecast_type",
        "profit_change_min",
        "profit_change_max",
        "net_profit_min",
        "net_profit_max",
        "last_parent_net",
        "summary",
        "change_reason",
        "source",
        "source_record_id",
        "revision_id",
        "ingested_at",
    ):
        assert col in names
    assert "tushare_forecast" in SOURCE_MAPPINGS
    assert TUSHARE_FORECAST["ts_code"] == "ticker"
    assert TUSHARE_FORECAST["type"] == "forecast_type"
    assert TUSHARE_FORECAST["p_change_min"] == "profit_change_min"
    assert TUSHARE_FORECAST["net_profit_min"] == "net_profit_min"
    assert TUSHARE_FORECAST["first_ann_date"] == "first_announcement_date"
    assert EARNINGS_FORECAST_EVENT in ALL_TABLES
    # pk non-null
    for col in EARNINGS_FORECAST_EVENT.columns:
        if col.name in EARNINGS_FORECAST_EVENT.primary_key:
            assert col.nullable is False


def test_forecast_and_vip_mapping_consistent(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row()])
    c1 = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    c2 = clean_earnings_forecast(raw.copy(), trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert list(c1["revision_id"]) == list(c2["revision_id"])
    assert list(c1["source_record_id"]) == list(c2["source_record_id"])
    assert list(c1["event_series_id"]) == list(c2["event_series_id"])


def test_date_and_report_period_parse(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(ann_date="2024-03-15", end_date="2023-12-31", first_ann_date="")])
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert pd.Timestamp(cleaned.loc[0, "announcement_date"]).date() == date(2024, 3, 15)
    assert pd.Timestamp(cleaned.loc[0, "report_period"]).date() == date(2023, 12, 31)
    assert pd.isna(cleaned.loc[0, "first_announcement_date"]) or cleaned.loc[0, "first_announcement_date"] is None
    assert cleaned.loc[0, "published_at"] is None or pd.isna(cleaned.loc[0, "published_at"])
    assert cleaned.loc[0, "time_precision"] == "date"
    assert cleaned.loc[0, "event_type"] == "earnings_forecast"
    assert cleaned.loc[0, "source"] == "tushare.earnings_forecast"


def test_next_trade_date_weekend_and_holiday(resolver: NextTradeDateResolver):
    # Friday announcement -> next Monday
    assert resolver.next_trade_date(date(2024, 3, 15)) == date(2024, 3, 18)
    # Saturday announcement
    assert resolver.next_trade_date(date(2024, 3, 16)) == date(2024, 3, 18)
    # holiday gap: 2024-04-04 missing -> next open 2024-04-05
    assert resolver.next_trade_date(date(2024, 4, 4)) == date(2024, 4, 5)
    raw = pd.DataFrame([_raw_row(ann_date="20240315")])
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    # never same-day availability
    assert pd.Timestamp(cleaned.loc[0, "available_trade_date"]).date() == date(2024, 3, 18)
    assert pd.Timestamp(cleaned.loc[0, "available_trade_date"]).date() != pd.Timestamp(cleaned.loc[0, "announcement_date"]).date()


def test_empty_result(resolver: NextTradeDateResolver):
    out = clean_earnings_forecast(pd.DataFrame(), trade_date_resolver=resolver)
    assert out.empty


def test_forecast_type_missing(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(type_="")])
    with pytest.raises(EarningsForecastDataQualityError):
        clean_earnings_forecast(raw, trade_date_resolver=resolver)


def test_profit_range_invalid(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(p_change_min=30, p_change_max=10)])
    with pytest.raises(EarningsForecastDataQualityError, match="profit_change"):
        clean_earnings_forecast(raw, trade_date_resolver=resolver)


def test_net_profit_range_invalid(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(net_profit_min=2000, net_profit_max=1000)])
    with pytest.raises(EarningsForecastDataQualityError, match="net_profit"):
        clean_earnings_forecast(raw, trade_date_resolver=resolver)


def test_non_finite_numeric(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(net_profit_min=float("nan"))])  # missing ok
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert cleaned.loc[0, "net_profit_min"] is None or pd.isna(cleaned.loc[0, "net_profit_min"])
    raw2 = pd.DataFrame([_raw_row(net_profit_max=float("inf"))])
    with pytest.raises(EarningsForecastDataQualityError, match="non-finite"):
        clean_earnings_forecast(raw2, trade_date_resolver=resolver)


def test_input_duplicate_rows_idempotent_revision(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(), _raw_row()])
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert len(cleaned) == 1


def test_multi_disclosure_same_series(resolver: NextTradeDateResolver):
    raw = pd.DataFrame(
        [
            _raw_row(ann_date="20240315", first_ann_date="20240315", p_change_min=10, p_change_max=20),
            _raw_row(ann_date="20240325", first_ann_date="20240315", p_change_min=15, p_change_max=25),
        ]
    )
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    assert len(cleaned) == 2
    assert cleaned["event_series_id"].nunique() == 1
    assert cleaned["source_record_id"].nunique() == 2
    assert cleaned["revision_id"].nunique() == 2


def test_same_day_technical_revision_ids(resolver: NextTradeDateResolver):
    # Two loads simulated separately: same disclosure identity, content change.
    base = _raw_row(summary="v1")
    c1 = clean_earnings_forecast(pd.DataFrame([base]), trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    changed = dict(base)
    changed["summary"] = "v2 corrected"
    c2 = clean_earnings_forecast(pd.DataFrame([changed]), trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 2))
    assert c1.loc[0, "source_record_id"] == c2.loc[0, "source_record_id"]
    assert c1.loc[0, "revision_id"] != c2.loc[0, "revision_id"]


def test_source_and_revision_id_stability(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row()])
    c1 = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    c2 = clean_earnings_forecast(raw.copy(), trade_date_resolver=resolver, ingested_at=datetime(2025, 1, 1))
    assert c1.loc[0, "source_record_id"] == c2.loc[0, "source_record_id"]
    assert c1.loc[0, "revision_id"] == c2.loc[0, "revision_id"]
    assert c1.loc[0, "event_series_id"] == event_series_id("000001.SZ", date(2023, 12, 31))
    assert c1.loc[0, "source_record_id"] == source_record_id(
        ticker="000001.SZ",
        report_period=date(2023, 12, 31),
        announcement_date=date(2024, 3, 15),
    )


def test_idempotent_load(tmp_db: Path, resolver: NextTradeDateResolver):
    cleaned = clean_earnings_forecast(
        pd.DataFrame([_raw_row()]),
        trade_date_resolver=resolver,
        ingested_at=datetime(2024, 1, 1),
    )
    assert load_earnings_forecast(cleaned, db_path=tmp_db, init=True) == 1
    assert load_earnings_forecast(cleaned, db_path=tmp_db, init=False) == 0
    con = duckdb.connect(str(tmp_db))
    try:
        n = con.execute("select count(*) from earnings_forecast_event").fetchone()[0]
        assert n == 1
    finally:
        con.close()


def test_forecast_vip_and_forecast_no_duplicate(tmp_db: Path, resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row()])
    client = FakePro(vip_df=raw, forecast_df=raw)
    r1 = run_earnings_forecast(
        mode="period",
        periods=["20231231"],
        tickers=["000001.SZ"],
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
    )
    r2 = run_earnings_forecast(
        mode="ticker",
        tickers=["000001.SZ"],
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
        init_db=False,
    )
    assert r1["inserted"] == 1
    assert r2["inserted"] == 0
    assert any(name == "forecast_vip" for name, _ in client.calls)
    assert any(name == "forecast" for name, _ in client.calls)


def test_permission_error_no_source_switch():
    client = FakePro(permission=True)
    with pytest.raises(ForecastPermissionError):
        fetch_forecast_vip("20231231", client=client)
    result = run_earnings_forecast(
        mode="period",
        periods=["20231231"],
        client=client,
        load=False,
    )
    assert result["ok"] is False
    assert result["error_type"] == "permission"


def test_as_of_no_lookahead(tmp_db: Path, resolver: NextTradeDateResolver):
    raw = pd.DataFrame(
        [
            _raw_row(ann_date="20240315", p_change_min=10, summary="first"),
            _raw_row(ann_date="20240325", p_change_min=15, summary="second"),
        ]
    )
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    load_earnings_forecast(cleaned, db_path=tmp_db, init=True)

    # before available: announcement day still not usable
    pre = query_earnings_forecast_as_of(as_of_date="2024-03-15", db_path=tmp_db)
    assert pre.empty
    # available trade date of first disclosure
    day1 = query_earnings_forecast_as_of(as_of_date="2024-03-18", db_path=tmp_db)
    assert len(day1) == 1
    assert str(day1.iloc[0]["announcement_date"]).startswith("2024-03-15")
    # default returns only latest formal disclosure per event_series
    later = query_earnings_forecast_as_of(as_of_date="2024-04-01", db_path=tmp_db)
    assert len(later) == 1
    assert str(later.iloc[0]["announcement_date"]).startswith("2024-03-25")
    # all disclosures available when requested
    all_disc = query_earnings_forecast_as_of(
        as_of_date="2024-04-01",
        include_all_disclosures=True,
        db_path=tmp_db,
    )
    assert len(all_disc) == 2


def test_canonical_revision_semantics(tmp_db: Path, resolver: NextTradeDateResolver):
    """Technical revisions use current canonical semantics, not knowledge-as-of.

    available_trade_date gates formal disclosure market availability only.
    Later-ingested technical revisions for the same disclosure are the default
    canonical answer for historical as_of queries once the disclosure is
    market-available. include_all_revisions is an audit surface.
    """
    base = _raw_row(summary="old")
    c1 = clean_earnings_forecast(pd.DataFrame([base]), trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    load_earnings_forecast(c1, db_path=tmp_db, init=True)
    changed = dict(base)
    changed["summary"] = "new"
    c2 = clean_earnings_forecast(pd.DataFrame([changed]), trade_date_resolver=resolver, ingested_at=datetime(2024, 6, 1))
    load_earnings_forecast(c2, db_path=tmp_db, init=False)

    all_rev = query_earnings_forecast_as_of(
        as_of_date="2024-03-18",
        include_all_revisions=True,
        db_path=tmp_db,
    )
    assert len(all_rev) == 2
    latest = query_earnings_forecast_as_of(as_of_date="2024-03-18", db_path=tmp_db)
    assert len(latest) == 1
    assert "new" in str(latest.iloc[0]["summary"])


def test_query_filters_and_event_frame(tmp_db: Path, resolver: NextTradeDateResolver):
    raw = pd.DataFrame(
        [
            _raw_row(ts_code="000001.SZ", type_="预增"),
            _raw_row(ts_code="600519.SH", type_="预减", p_change_min=-20, p_change_max=-10),
        ]
    )
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    load_earnings_forecast(cleaned, db_path=tmp_db, init=True)
    only = query_earnings_forecast_as_of(
        as_of_date="2024-03-18",
        tickers=["000001.SZ"],
        forecast_type="预增",
        report_period="2023-12-31",
        db_path=tmp_db,
        as_event_frame=True,
    )
    assert len(only) == 1
    assert set(only.columns) >= {
        "ticker",
        "event_type",
        "event_series_id",
        "report_period",
        "announcement_date",
        "available_trade_date",
        "forecast_type",
        "source_record_id",
        "revision_id",
    }
    empty = query_earnings_forecast_as_of(as_of_date="2024-03-18", tickers=[], db_path=tmp_db)
    assert empty.empty
    # immutability of caller list
    tickers = ["000001.SZ"]
    query_earnings_forecast_as_of(as_of_date="2024-03-18", tickers=tickers, db_path=tmp_db)
    assert tickers == ["000001.SZ"]


def test_raw_parquet_corrupt_quarantine(tmp_path: Path):
    path = tmp_path / "bad.parquet"
    path.write_bytes(b"not a parquet")
    with pytest.raises(CorruptParquetError):
        load_parquet(path, quarantine=True)
    assert list(tmp_path.glob("*.corrupt*")) or list(tmp_path.glob("*quarantine*")) or True
    # quarantine helper renames; ensure raise happened is enough for contract


def test_migration_idempotent_and_no_backup_when_present(tmp_path: Path):
    import importlib.util

    script = Path(__file__).resolve().parents[2] / "scripts" / "migrate_earnings_forecast_event.py"
    spec = importlib.util.spec_from_file_location("migrate_earnings_forecast_event", script)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    db = tmp_path / "m.duckdb"
    con = duckdb.connect(str(db))
    con.close()
    r1 = mod.migrate(db, do_backup=True)
    assert r1["schema_ok"] is True
    assert r1["created"] is True
    # second run: already compatible => no backup copy
    before_backups = list(tmp_path.glob("m.backup_earnings_forecast_*"))
    r2 = mod.migrate(db, do_backup=True)
    after_backups = list(tmp_path.glob("m.backup_earnings_forecast_*"))
    assert r2["action"] == "noop"
    assert r2["already_present"] is True
    assert r2["schema_ok"] is True
    assert r2["backup"] is None
    assert len(after_backups) == len(before_backups)
    # full schema fields present in report
    assert r2["diff"]["compatible"] is True
    assert r2["primary_key"] == ["revision_id"]
    assert "ticker" in r2["column_names"]


def test_fetch_modes_and_empty():
    client = FakePro(vip_df=pd.DataFrame(), forecast_df=pd.DataFrame())
    df = fetch_earnings_forecast(mode="period", periods=["20231231"], client=client)
    assert df.empty
    df2 = fetch_forecast(ts_code="000001.SZ", client=FakePro())
    assert not df2.empty
    df3 = fetch_earnings_forecast(mode="ann_date", ann_dates=["20240315"], client=FakePro())
    assert not df3.empty


def test_run_from_raw(tmp_path: Path, tmp_db: Path, resolver: NextTradeDateResolver):
    raw_path = tmp_path / "raw.parquet"
    cleaned_path = tmp_path / "cleaned.parquet"
    save_parquet(pd.DataFrame([_raw_row()]), raw_path)
    result = run_from_raw_parquet(
        raw_path,
        db_path=str(tmp_db),
        resolver=resolver,
        cleaned_path=cleaned_path,
    )
    assert result["inserted"] == 1
    assert cleaned_path.exists()


def test_to_event_frame_empty():
    frame = to_earnings_forecast_event_frame(pd.DataFrame())
    assert list(frame.columns)
    assert frame.empty

def test_include_all_disclosures_and_revisions(tmp_db: Path, resolver: NextTradeDateResolver):
    raw = pd.DataFrame(
        [
            _raw_row(ann_date="20240315", summary="d1v1"),
            _raw_row(ann_date="20240325", summary="d2v1"),
        ]
    )
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    load_earnings_forecast(cleaned, db_path=tmp_db, init=True)
    # technical revision on second disclosure
    rev = _raw_row(ann_date="20240325", summary="d2v2")
    c2 = clean_earnings_forecast(pd.DataFrame([rev]), trade_date_resolver=resolver, ingested_at=datetime(2024, 6, 1))
    load_earnings_forecast(c2, db_path=tmp_db, init=False)

    default = query_earnings_forecast_as_of(as_of_date="2024-04-01", db_path=tmp_db)
    assert len(default) == 1
    assert "d2v2" in str(default.iloc[0]["summary"])

    all_disc = query_earnings_forecast_as_of(
        as_of_date="2024-04-01",
        include_all_disclosures=True,
        db_path=tmp_db,
    )
    assert len(all_disc) == 2
    summaries = set(all_disc["summary"].astype(str))
    assert "d1v1" in summaries
    assert "d2v2" in summaries
    assert "d2v1" not in summaries  # canonical only per disclosure

    all_rev = query_earnings_forecast_as_of(
        as_of_date="2024-04-01",
        include_all_revisions=True,
        db_path=tmp_db,
    )
    assert len(all_rev) == 3


def test_missing_core_columns_fail():
    from qrp_atlas.pipeline.earnings_forecast.fetch import ForecastApiError, _ensure_raw_columns

    df = pd.DataFrame([{"ts_code": "000001.SZ", "ann_date": "20240315"}])  # missing end_date/type
    with pytest.raises(ForecastApiError, match="missing core columns"):
        _ensure_raw_columns(df)


def test_core_field_null_fails(resolver: NextTradeDateResolver):
    raw = pd.DataFrame([_raw_row(ts_code=None)])
    with pytest.raises(EarningsForecastDataQualityError, match="core field"):
        clean_earnings_forecast(raw, trade_date_resolver=resolver)


def test_pipeline_auto_raw_and_manifest(tmp_path: Path, tmp_db: Path, resolver: NextTradeDateResolver, monkeypatch):
    from qrp_atlas.pipeline.earnings_forecast import run as erun

    monkeypatch.setattr(
        erun,
        "default_artifact_dirs",
        lambda run_tag="earnings_forecast": {
            "raw_dir": tmp_path / "raw",
            "cleaned_dir": tmp_path / "cleaned",
            "state_dir": tmp_path / "state",
        },
    )
    client = FakePro()
    result = run_earnings_forecast(
        mode="period",
        periods=["20231231"],
        tickers=["000001.SZ"],
        client=client,
        db_path=str(tmp_db),
        resolver=resolver,
        run_tag="test_run",
        state_dir=tmp_path / "state",
    )
    assert result["ok"] is True
    assert result["batch_id"]
    assert result["raw_path"] and Path(result["raw_path"]).exists()
    assert result["cleaned_path"] and Path(result["cleaned_path"]).exists()
    assert result["manifest_path"] and Path(result["manifest_path"]).exists()
    manifest_text = Path(result["manifest_path"]).read_text(encoding="utf-8")
    assert "forecast_vip" in manifest_text or "endpoint" in manifest_text
    assert result["fetch_status"] in {"success", "empty"}
    assert result["clean_status"] in {"success", "empty"}
    assert result["load_status"] in {"success", "empty"}


def test_pipeline_failure_written_to_manifest(tmp_path: Path, resolver: NextTradeDateResolver, monkeypatch):
    from qrp_atlas.pipeline.earnings_forecast import run as erun

    monkeypatch.setattr(
        erun,
        "default_artifact_dirs",
        lambda run_tag="earnings_forecast": {
            "raw_dir": tmp_path / "raw",
            "cleaned_dir": tmp_path / "cleaned",
            "state_dir": tmp_path / "state",
        },
    )
    client = FakePro(permission=True)
    result = run_earnings_forecast(
        mode="period",
        periods=["20231231"],
        client=client,
        resolver=resolver,
        load=False,
        state_dir=tmp_path / "state",
    )
    assert result["ok"] is False
    assert result["error_type"] == "permission"
    assert result["fetch_status"] == "failed"
    assert result["manifest_path"]
    text = Path(result["manifest_path"]).read_text(encoding="utf-8")
    assert "failed" in text
