from __future__ import annotations

from dataclasses import replace
from datetime import date
import subprocess
import sys

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import get_table
from qrp_atlas.contracts.lake_plugins import LAKE_ROLLING_MEAN, LAKE_SYSTEM_B_STATE
from qrp_atlas.pipeline.lake.plugins import (
    BUILTIN_PLUGINS, PluginContext, PluginError, frame_identity, load_plugin,
)
from qrp_atlas.pipeline.system_b.service import initialize_history


@pytest.fixture
def frames():
    dates = list(pd.bdate_range("2026-01-05", periods=20).date)
    prices = [10.] * 10 + [11., 12., 8., 7., 8., 9., 10., 11., 9., 8.]
    daily = pd.DataFrame([
        {"ticker": ticker, "trade_date": day, "close": price * factor, "volume": 100}
        for ticker, factor in [("000001", 1), ("000002", 2)]
        for day, price in zip(dates, prices, strict=True)
        if not (ticker == "000002" and day == dates[11])
    ])
    return {
        "stock_info": pd.DataFrame({"ticker": ["000001", "000002"], "list_date": [dates[0]] * 2, "delist_date": [None] * 2}),
        "trading_calendar": pd.DataFrame({"trade_date": dates, "is_open": [True] * len(dates)}),
        "daily_market_snapshot": daily,
        "adj_factor_changes": pd.DataFrame({"ticker": ["000001", "000002"], "trade_date": [dates[0]] * 2, "adj_factor": [1., 1.]}),
        "suspend_d": pd.DataFrame({"ticker": ["000002"], "trade_date": [dates[11]], "suspend_type": ["停牌"]}),
    }


def context(frames, index=-1, **kwargs):
    return PluginContext(
        target_date=frames["trading_calendar"]["trade_date"].iloc[index], inputs=frames,
        input_versions={name: "snapshot-1" for name in frames},
        pit_evidence={name: "verified" for name in frames}, **kwargs,
    )


def plugin(name="system_b_state"):
    return load_plugin(name, enabled=BUILTIN_PLUGINS)


def test_core_discovery_does_not_import_extensions():
    code = "import sys; import qrp_atlas.pipeline.lake.plugins; assert not any(k.startswith('qrp_atlas.pipeline.lake.extensions.') for k in sys.modules)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    with pytest.raises(PluginError, match="not enabled"):
        load_plugin("system_b_state", enabled={})
    with pytest.raises(ModuleNotFoundError):
        load_plugin("broken", enabled={"broken": "nonexistent_qrp_extension:Plugin"})
    assert plugin("rolling_mean").spec.plugin_id == "rolling_mean"


def test_system_b_matches_existing_production_pipeline(frames, tmp_path):
    db_path = tmp_path / "old.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        for name, frame in frames.items():
            connection.execute(get_table(name).duckdb_create_sql())
            connection.register("seed", frame)
            columns = ", ".join(frame.columns)
            connection.execute(f"INSERT INTO {name} ({columns}) SELECT {columns} FROM seed")
            connection.unregister("seed")
    target = frames["trading_calendar"]["trade_date"].iloc[-1]
    report = initialize_history(source_database=db_path, output_database=db_path, end_date=target, staging_root=tmp_path / "staging")
    assert report.status == "SUCCEEDED"
    actual = plugin().run(context(frames)).frame
    assert tuple(actual.columns) == LAKE_SYSTEM_B_STATE.column_names()
    with duckdb.connect(str(db_path), read_only=True) as connection:
        expected = connection.execute("SELECT * FROM system_b_state_observation WHERE trade_date = ? ORDER BY asset_id", [target]).fetchdf()
    columns = ["asset_id", "trend_state", "previous_trend_state", "state_changed", "close", "ma5", "listing_trading_day_number", "market_fact_status", "state_basis_sequence_intact"]
    pd.testing.assert_frame_equal(actual[columns], expected[columns], check_dtype=False)


def test_checkpoint_sequential_and_revision_replay(frames):
    p = plugin()
    first = p.run(context(frames, 10))
    second = p.run(context(frames, 11, checkpoint=first.checkpoint))
    assert second.checkpoint["previous_checkpoint_id"] == first.checkpoint["checkpoint_id"]
    assert second.frame.loc[second.frame.asset_id == "000002", "market_fact_status"].item() == "EXPLICIT_NON_TRADING"
    with pytest.raises(PluginError, match="next open date"):
        p.run(context(frames, 12, checkpoint=first.checkpoint))
    revised = {name: frame.copy(deep=True) for name, frame in frames.items()}
    revised["daily_market_snapshot"].loc[0, "close"] = 3.
    with pytest.raises(PluginError, match="explicit replay"):
        p.run(context(revised, 11, checkpoint=first.checkpoint))
    with pytest.raises(PluginError, match="history start"):
        p.run(context(revised, 11, checkpoint=first.checkpoint, replay_from=frames["trading_calendar"].trade_date.iloc[10]))
    replay = p.run(context(revised, 11, checkpoint=first.checkpoint, replay_from=frames["trading_calendar"].trade_date.iloc[0]))
    assert replay.checkpoint["checkpoint_id"] != first.checkpoint["checkpoint_id"]
    assert len(replay.frame) == 24
    assert first.checkpoint["history_hashes"]["daily_market_snapshot"] != replay.checkpoint["history_hashes"]["daily_market_snapshot"]
    assert first.checkpoint["replay_from"] is None


def test_fixed_inputs_reproduce_and_future_rows_do_not_leak(frames):
    before = {name: frame_identity(frame) for name, frame in frames.items()}
    p = plugin()
    result = p.run(context(frames, 10))
    repeated = p.run(context(frames, 10))
    pd.testing.assert_frame_equal(result.frame, repeated.frame)
    assert result.checkpoint == repeated.checkpoint
    revised = {name: frame.copy(deep=True) for name, frame in frames.items()}
    revised["daily_market_snapshot"].loc[revised["daily_market_snapshot"].trade_date > context(frames, 10).target_date, "close"] = 9999.
    pd.testing.assert_frame_equal(result.frame, p.run(context(revised, 10)).frame)
    assert before == {name: frame_identity(frame) for name, frame in frames.items()}


def test_pit_missing_input_and_tampered_checkpoint_fail(frames):
    p = plugin()
    with pytest.raises(PluginError, match="PIT evidence"):
        p.run(replace(context(frames), pit_evidence={}))
    with pytest.raises(PluginError, match="fixed input"):
        p.run(replace(context(frames), input_versions={}))
    checkpoint = dict(p.run(context(frames, 10)).checkpoint)
    checkpoint["target_date"] = "2020-01-01"
    with pytest.raises(PluginError, match="identity mismatch"):
        p.run(context(frames, 11, checkpoint=checkpoint))
    with pytest.raises(PluginError, match="frozen"):
        p.run(context(frames, parameters={"window": 10}))


def test_missing_trading_fact_is_not_a_suspension(frames):
    missing = {name: frame.copy(deep=True) for name, frame in frames.items()}
    missing["suspend_d"] = missing["suspend_d"].iloc[:0]
    with pytest.raises(PluginError, match="unresolved missing"):
        plugin().run(context(missing))


def test_observation_bookkeeping_does_not_force_historical_replay(frames):
    first_frames = {name: frame.copy(deep=True) for name, frame in frames.items()}
    first_frames["stock_info"]["updated_at"] = pd.Timestamp("2026-01-19")
    first_frames["daily_market_snapshot"]["created_at"] = pd.Timestamp("2026-01-19")
    p = plugin()
    first = p.run(context(first_frames, 10))
    next_frames = {name: frame.copy(deep=True) for name, frame in first_frames.items()}
    next_frames["stock_info"]["updated_at"] = pd.Timestamp("2026-01-20")
    next_frames["daily_market_snapshot"]["created_at"] = pd.Timestamp("2026-01-20")
    request = replace(context(next_frames, 11, checkpoint=first.checkpoint),
                      input_versions={name: "new-source-snapshot" for name in next_frames})
    second = p.run(request)
    assert second.checkpoint["input_versions"] == request.input_versions
    assert second.checkpoint["previous_checkpoint_id"] == first.checkpoint["checkpoint_id"]
    # Business reference changes still invalidate the prior history prefix.
    next_frames["stock_info"].loc[0, "list_date"] = frames["trading_calendar"].trade_date.iloc[1]
    with pytest.raises(PluginError, match="explicit replay"):
        p.run(replace(request, inputs=next_frames))


def test_custom_extension_entrypoint_needs_no_executor_edit(monkeypatch, tmp_path, frames):
    import types
    from qrp_atlas.contracts.schema import ColumnSpec, TableSchema
    from qrp_atlas.pipeline.lake.plugins import PluginResult, PluginSpec
    from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation

    custom = types.ModuleType("qrp_test_custom_extension")
    class CustomPlugin:
        spec = PluginSpec("custom", "1", "1", ("daily_market_snapshot",), "custom", ("ticker", "trade_date"))
        schema = TableSchema("custom", (
            ColumnSpec("ticker", "VARCHAR", False), ColumnSpec("trade_date", "DATE", False),
            ColumnSpec("custom_value", "DOUBLE", False),
        ), spec.primary_key)

        def run(self, request):
            output = request.inputs["daily_market_snapshot"].loc[
                request.inputs["daily_market_snapshot"].trade_date == request.target_date,
                ["ticker", "trade_date", "close"],
            ].rename(columns={"close": "custom_value"})
            return PluginResult(output, None, {"producer": "custom@1", "inputs": dict(request.input_versions)})
    custom.Factory = CustomPlugin
    monkeypatch.setitem(sys.modules, custom.__name__, custom)
    loaded = load_plugin("custom", enabled={"custom": custom.__name__ + ":Factory"})
    assert loaded.spec.output_domain == "extension"
    calculated = loaded.run(context(frames))
    store = DuckLakeStore(LakeLocation(str(tmp_path / "extension.ducklake"), tmp_path, domain="extension"))
    store.initialize()
    publication = store.publish(loaded.schema, calculated.frame, partition=context(frames).target_date.isoformat(),
                                idempotency_key="custom-v1", lineage=dict(calculated.metadata))
    observed = DataPortal(store, publication.snapshot_id).read(loaded.schema.name)
    assert observed.custom_value.tolist() == [8., 16.]
    assert loaded.spec.output_dataset not in {"system_b_state", "rolling_mean"}
    # A declaration cannot redirect the publisher to an unrelated schema.
    custom.Factory = lambda: types.SimpleNamespace(spec=CustomPlugin.spec, schema=LAKE_ROLLING_MEAN, run=lambda _: None)
    with pytest.raises(PluginError, match="disagrees"):
        load_plugin("custom", enabled={"custom": custom.__name__ + ":Factory"})
    with pytest.raises(PluginError, match="core domain"):
        PluginSpec("custom", "1", "1", (), "core_table", ("ticker",), output_domain="core")


def test_rolling_registry_formula_and_window(frames):
    p = plugin("rolling_mean")
    result = p.run(context(frames, 11, parameters={"window": 5}))
    assert tuple(result.frame.columns) == LAKE_ROLLING_MEAN.column_names()
    assert list(result.frame.ticker) == ["000001"]
    assert result.frame.mean_close.iloc[0] == pytest.approx((10 + 10 + 10 + 11 + 12) / 5)
    warmup = p.run(context(frames, 1))
    assert warmup.frame.mean_close.isna().all()
    with pytest.raises(PluginError, match="positive integer"):
        p.run(context(frames, parameters={"window": True}))
    duplicates = {name: frame.copy() for name, frame in frames.items()}
    duplicates["daily_market_snapshot"] = pd.concat([frames["daily_market_snapshot"]] * 2)
    with pytest.raises(PluginError, match="duplicated"):
        p.run(context(duplicates))
