"""Real CLI adapter -> pinned core snapshots -> state checkpoint publications."""

from argparse import Namespace
from dataclasses import replace
from datetime import date
import json

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.config.lake import LakeSettings
from qrp_atlas.contracts import get_table
from qrp_atlas.pipeline.lake import cli
from qrp_atlas.pipeline.lake.plugins import BUILTIN_PLUGINS, PluginError
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def _typed(name, frame):
    """Build the full established contract, including nullable/default fields."""
    with duckdb.connect() as con:
        con.execute(get_table(name).duckdb_create_sql())
        if not frame.empty:
            con.register("seed", frame)
            columns = ",".join(frame.columns)
            con.execute(f"INSERT INTO {name} ({columns}) SELECT {columns} FROM seed")
        return con.execute(f"SELECT * FROM {name}").fetchdf()


def test_cli_state_initial_next_and_revision_replay(tmp_path):
    root = tmp_path / "lake"
    core_catalog = str(tmp_path / "core.ducklake")
    ext_catalog = str(tmp_path / "extension.ducklake")
    core = DuckLakeStore(LakeLocation(core_catalog, root))
    extension = DuckLakeStore(LakeLocation(ext_catalog, root, domain="extension"))
    core.initialize()
    extension.initialize()
    settings = replace(AppSettings.load(environ={}, project_root=tmp_path), lake=LakeSettings(
        root=root, core_read_catalog=core_catalog, extension_catalog=ext_catalog,
    ))
    enabled = tmp_path / "plugins.json"
    enabled.write_text(json.dumps({"system_b_state": BUILTIN_PLUGINS["system_b_state"]}), encoding="utf-8")
    manifest_path = tmp_path / "inputs.json"
    checkpoint_path = tmp_path / "prior-checkpoint.json"
    first_day, next_day = date(2026, 1, 5), date(2026, 1, 6)

    def publish_market(day, revision, close):
        frame = _typed("daily_market_snapshot", pd.DataFrame({
            "ticker": ["000001.SZ"], "trade_date": [day], "close": [close], "volume": [100],
        }))
        return core.publish(get_table("daily_market_snapshot"), frame, partition=str(day),
                            idempotency_key=f"market-{day}-{revision}", lineage={"test_revision": revision},
                            expected_previous=core.current_publication("daily_market_snapshot", str(day)))

    def publish_inputs(day, revision):
        tables = {
            "stock_info": pd.DataFrame({"ticker": ["000001.SZ"], "list_date": [first_day],
                                        "updated_at": [pd.Timestamp(day) + pd.Timedelta(hours=revision)]}),
            "trading_calendar": pd.DataFrame({"trade_date": [day], "is_open": [True]}),
            "adj_factor_changes": pd.DataFrame({"ticker": ["000001.SZ"], "trade_date": [day], "adj_factor": [1.]}),
            "suspend_d": pd.DataFrame(columns=["ticker", "trade_date", "suspend_type"]),
        }
        publications = {"daily_market_snapshot": publish_market(day, revision, 10. if day == first_day else 11.)}
        for name, source in tables.items():
            frame = _typed(name, source)
            full_table = name == "stock_info"
            publications[name] = core.publish(get_table(name), frame, partition=str(day),
                partition_column=None if full_table else "trade_date", idempotency_key=f"{name}-{day}-{revision}",
                lineage={"test_revision": revision}, quality="legal_empty" if frame.empty else "complete",
                expected_previous=core.current_publication(name, str(day), whole_table=full_table))
        manifest_path.write_text(json.dumps({name: {
            "domain": "core", "snapshot_id": result.snapshot_id, "publication_id": result.publication_id,
        } for name, result in publications.items()}), encoding="utf-8")

    def args(day, replay_from=None):
        return Namespace(domain="extension", date=str(day), dataset="system_b_state", plugins=str(enabled),
                         input_manifest=str(manifest_path), input_snapshot=None,
                         checkpoint=str(checkpoint_path) if replay_from else None,
                         parameters="{}", replay_from=str(replay_from) if replay_from else None, allow_observed=True)

    publish_inputs(first_day, 1)
    first = cli.run(args(first_day), settings)
    first_checkpoint = first["lineage"]["checkpoint"]
    publish_inputs(next_day, 2)
    second = cli.run(args(next_day), settings)
    checkpoint_path.write_text(json.dumps(second["lineage"]["checkpoint"]), encoding="utf-8")
    assert second["lineage"]["checkpoint"]["previous_checkpoint_id"] == first_checkpoint["checkpoint_id"]
    assert len(DataPortal(extension, second["snapshot_id"]).read("system_b_state")) == 2
    # The provider revises an older price; the next target's receipt pins this
    # newly revised catalog content without changing the plugin algorithm.
    publish_market(first_day, 3, 3.)
    publish_inputs(next_day, 3)
    with pytest.raises(PluginError, match="explicit replay"):
        cli.run(args(next_day), settings)
    replayed = cli.run(args(next_day, replay_from=first_day), settings)
    assert len(replayed["replayed_publications"]) == 2
    assert replayed["lineage"]["checkpoint"]["recovery_mode"] == "FULL_HISTORY_RECOMPUTE"
    old = DataPortal(extension, second["snapshot_id"]).read("system_b_state")
    revised = DataPortal(extension, replayed["snapshot_id"]).read("system_b_state")
    assert old.close.tolist() == [10., 11.]
    assert revised.close.tolist() == [3., 11.]
    repeated = cli.run(args(next_day, replay_from=first_day), settings)
    assert [item["publication_id"] for item in repeated["replayed_publications"]] == [item["publication_id"] for item in replayed["replayed_publications"]]
