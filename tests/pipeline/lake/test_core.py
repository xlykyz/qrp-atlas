from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from qrp_atlas.contracts.mappings import TUSHARE_STOCK_BASIC
from qrp_atlas.pipeline.lake.core import CoreQualityError, CoreRunner, normalize_core, normalize_stock_identities
from qrp_atlas.pipeline.lake.provider import AcquisitionError, SDKProvider


TARGET = date(2026, 9, 30)
OBSERVED = datetime(2026, 9, 30, 8, 30, tzinfo=timezone.utc)


def daily():
    return pd.DataFrame([dict(ts_code="000001.SZ", trade_date="20260930", open=10.0,
                             high=11.0, low=9.0, close=10.5, pre_close=10.0,
                             pct_chg=5.0, vol=12.5, amount=25.5, unknown_field="keep raw")])


def normalize(name, frame, **kwargs):
    return normalize_core(name, frame, target_date=TARGET, observed_at=OBSERVED, complete=True, **kwargs)


def test_daily_reuses_units_preserves_raw_and_has_frozen_timestamp():
    raw = daily()
    before = raw.copy(deep=True)
    result = normalize("daily_market_snapshot", raw)
    assert result.iloc[0]["volume"] == 1250
    assert result.iloc[0]["amount"] == 25500
    assert result.iloc[0]["pct_change"] == 5.0
    assert pd.Timestamp(result.iloc[0]["created_at"]) == pd.Timestamp("2026-09-30 08:30:00")
    pd.testing.assert_frame_equal(raw, before)


@pytest.mark.parametrize("change,error", [
    ({"trade_date": "20260929"}, "TARGET_DATE_MISMATCH"),
    ({"close": float("nan")}, "MISSING_MARKET_VALUES"),
    ({"vol": -1}, "NEGATIVE_ACTIVITY"),
    ({"high": 9}, "INVALID_OHLC_RANGE"),
    ({"close": float("inf")}, "NONFINITE_VALUE"),
    ({"ts_code": "BAD"}, "INVALID_TICKER"),
])
def test_market_rejects_corrupt_or_wrong_partition(change, error):
    frame = daily().assign(**change)
    with pytest.raises(CoreQualityError, match=error):
        normalize("daily_market_snapshot", frame)


def test_rejects_duplicates_before_legacy_cleaner_can_drop_them():
    with pytest.raises(CoreQualityError, match="DUPLICATE_BUSINESS_KEY"):
        normalize("daily_market_snapshot", pd.concat([daily(), daily()]))


def test_empty_is_contract_specific_and_never_implied_by_no_exception():
    with pytest.raises(CoreQualityError, match="UNEXPECTED_EMPTY"):
        normalize("daily_market_snapshot", pd.DataFrame())
    assert normalize("suspend_d", pd.DataFrame()).empty
    assert normalize("daily_market_snapshot", pd.DataFrame(), is_open=False).empty
    with pytest.raises(CoreQualityError, match="INCOMPLETE_RAW_BATCH"):
        normalize_core("suspend_d", pd.DataFrame(), target_date=TARGET,
                       observed_at=OBSERVED, complete=False)


def test_basic_uses_existing_units_and_requires_expected_coverage():
    frame = pd.DataFrame([dict(ts_code="000001.SZ", trade_date="20260930", close=10,
                              pe=12, total_share=200, total_mv=3000)])
    result = normalize("daily_basic", frame, expected_tickers=["000001.SZ"])
    assert result.iloc[0]["pe_ratio"] == 12
    assert result.iloc[0]["total_shares"] == 200
    assert result.iloc[0]["total_mv"] == 3000
    with pytest.raises(CoreQualityError, match="INCOMPLETE_TICKER_COVERAGE"):
        normalize("daily_basic", frame, expected_tickers=["600000.SH"])


def test_factor_keeps_full_daily_snapshot_and_rejects_nonpositive():
    raw = pd.DataFrame([dict(ts_code="000001.SZ", trade_date="20260930", adj_factor=2)])
    assert len(normalize("adj_factor_changes", raw)) == 1
    with pytest.raises(CoreQualityError, match="INVALID_ADJUSTMENT_FACTOR"):
        normalize("adj_factor_changes", raw.assign(adj_factor=0))


def test_calendar_does_not_treat_zero_string_as_true():
    result = normalize("trading_calendar", pd.DataFrame([{"cal_date": "20260930", "is_open": "0"}]))
    assert not result.iloc[0]["is_open"]
    assert result.iloc[0]["quarter"] == 3


def stock_row(code="000001.SZ", status="L", exchange="SZSE"):
    result = dict.fromkeys(TUSHARE_STOCK_BASIC, None)
    result.update(ts_code=code, symbol=code.split(".")[0], name="测试银行", list_status=status,
                  exchange=exchange, list_date="19910403", curr_type="CNY")
    return result


def test_stock_identity_partition_and_unknown_history_are_preserved_separately():
    raw = pd.DataFrame([stock_row(), stock_row("historical-id", "D")])
    standard, history = normalize_stock_identities(raw, target_date=TARGET,
                                                   observed_at=OBSERVED, complete=True)
    assert standard.ticker.tolist() == ["000001.SZ"]
    assert history.ts_code.tolist() == ["historical-id"]
    assert history.standard_ticker.isna().all()
    assert standard.iloc[0]["is_active"]


def test_current_stock_reference_cannot_be_backdated():
    with pytest.raises(CoreQualityError, match="CURRENT_REFERENCE_CANNOT_BACKDATE"):
        normalize_core("stock_info", pd.DataFrame([stock_row()]), target_date=date(2026, 9, 29),
                       observed_at=OBSERVED, complete=True)


class MemoryRaw:
    def __init__(self):
        self.items = []

    def save_frame(self, source, dataset, frame, **metadata):
        record = SimpleNamespace(batch_id=str(len(self.items)), checksum=frame.to_json(),
                                 frame=frame.copy(), **metadata)
        self.items.append(record)
        return record


def test_acquisition_persists_successful_pages_before_failure_and_redacts_exception():
    class Client:
        def daily(self, **query):
            if query["offset"]:
                raise RuntimeError("secret=do-not-expose")
            return daily()
    raw = MemoryRaw()
    provider = SDKProvider(Client(), raw, sdk_version="test", page_size=1)
    with pytest.raises(AcquisitionError, match="PROVIDER_REQUEST_FAILED") as error:
        provider.acquire("daily_market_snapshot", {"trade_date": "20260930"})
    assert "do-not-expose" not in str(error.value)
    assert len(raw.items) == 2
    assert raw.items[0].frame.unknown_field.tolist() == ["keep raw"]
    assert raw.items[1].status == "failed"
    assert not any(item.complete for item in raw.items)


def test_acquisition_repeated_page_and_max_page_fail_closed():
    client = SimpleNamespace(daily=lambda **query: daily())
    raw = MemoryRaw()
    with pytest.raises(AcquisitionError, match="PROVIDER_PAGE_REPEATED"):
        SDKProvider(client, raw, sdk_version="test", page_size=1).acquire("daily_market_snapshot", {})
    assert len(raw.items) == 2
    with pytest.raises(AcquisitionError, match="PROVIDER_PAGINATION_INCOMPLETE"):
        SDKProvider(client, MemoryRaw(), sdk_version="test", page_size=1,
                    max_pages=1).acquire("daily_market_snapshot", {})


def test_acquisition_stores_unmodified_aggregate_only_after_all_pages():
    client = SimpleNamespace(daily=lambda **query: daily() if query["offset"] == 0 else daily().iloc[:0])
    raw = MemoryRaw()
    result = SDKProvider(client, raw, sdk_version="test", page_size=1).acquire("daily_market_snapshot", {})
    assert result.complete
    assert len(result.page_ids) == 2
    assert raw.items[-1].complete
    pd.testing.assert_frame_equal(raw.items[-1].frame, daily())


class CapturingPublisher:
    def __init__(self):
        self.calls = []

    def publish(self, schema, frame, **kwargs):
        self.calls.append((schema, frame, kwargs))
        return SimpleNamespace(publication_id=f"pub-{len(self.calls)}", snapshot_id=len(self.calls))

    def current_publication(self, dataset, partition, *, whole_table=False):
        return ""


def market_inputs():
    return {
        "trading_calendar": normalize("trading_calendar", pd.DataFrame([{"cal_date": "20260930", "is_open": 1}])),
        "stock_info": normalize("stock_info", pd.DataFrame([stock_row()])),
        "suspend_d": normalize("suspend_d", pd.DataFrame()),
    }


def test_real_raw_replay_pins_dependencies_and_rejects_stale_calendar(tmp_path):
    from qrp_atlas.pipeline.lake.raw import RawStore
    raw = RawStore(tmp_path / "raw")
    batch = raw.save_frame("tushare", "daily_market_snapshot", daily(),
                           parameters={}, complete=True, observed_at=OBSERVED)
    publisher = CapturingPublisher()
    runner = CoreRunner(raw, publisher)
    with pytest.raises(CoreQualityError, match="PINNED_INPUTS_REQUIRED"):
        runner.replay("daily_market_snapshot", batch.batch_id, target_date=TARGET)
    inputs = market_inputs()
    lineage = {"input_snapshots": {name: 5 for name in inputs}}
    runner.replay("daily_market_snapshot", batch.batch_id, target_date=TARGET,
                  inputs=inputs, lineage=lineage)
    result = publisher.calls[-1][1]
    assert result.name.tolist() == ["测试银行"]
    assert result.is_st.tolist() == [False]
    assert not result.is_limit_up.iloc[0]
    inputs["trading_calendar"]["trade_date"] = date(2026, 9, 29)
    with pytest.raises(CoreQualityError, match="TARGET_CALENDAR_MISSING"):
        runner.replay("daily_market_snapshot", batch.batch_id, target_date=TARGET,
                      inputs=inputs, lineage=lineage)


def test_daily_replay_does_not_hide_missing_stock_as_suspension(tmp_path):
    from qrp_atlas.pipeline.lake.raw import RawStore
    raw = RawStore(tmp_path / "raw")
    batch = raw.save_frame("tushare", "daily_market_snapshot", daily(),
                           parameters={}, complete=True, observed_at=OBSERVED)
    inputs = market_inputs()
    inputs["stock_info"] = normalize("stock_info", pd.DataFrame([stock_row(), stock_row("000002.SZ")]))
    lineage = {"input_snapshots": {name: 5 for name in inputs}}
    runner = CoreRunner(raw, CapturingPublisher())
    with pytest.raises(CoreQualityError, match="INCOMPLETE_TICKER_COVERAGE"):
        runner.replay("daily_market_snapshot", batch.batch_id, target_date=TARGET, inputs=inputs, lineage=lineage)
    inputs["suspend_d"] = normalize("suspend_d", pd.DataFrame([{
        "ts_code": "000002.SZ", "trade_date": "20260930", "suspend_type": "S", "suspend_timing": "全天"}]))
    runner.replay("daily_market_snapshot", batch.batch_id, target_date=TARGET, inputs=inputs, lineage=lineage)


def test_replay_rejects_wrong_dataset_raw(tmp_path):
    from qrp_atlas.pipeline.lake.raw import RawStore
    raw = RawStore(tmp_path / "raw")
    batch = raw.save_frame("tushare", "daily_basic", daily(), parameters={}, complete=True, observed_at=OBSERVED)
    with pytest.raises(CoreQualityError, match="RAW_DATASET_MISMATCH"):
        CoreRunner(raw, CapturingPublisher()).replay("daily_market_snapshot", batch.batch_id, target_date=TARGET)
