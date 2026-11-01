"""Boundary regressions from the independent 86fb751 review."""
from dataclasses import replace
from datetime import date
from pathlib import Path
import json
import shutil

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.config.lake import LakeSettings
from qrp_atlas.contracts import get_table
from qrp_atlas.contracts.schema import TableSchema, ColumnSpec
from qrp_atlas.pipeline.lake import cli
from qrp_atlas.pipeline.lake.core import normalize_core, _input_scope, CoreQualityError
from qrp_atlas.pipeline.lake.migration import import_legacy
from qrp_atlas.pipeline.lake.plugins import BUILTIN_PLUGINS, PluginSpec, PluginError
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def environment(tmp_path):
    root = tmp_path / "lake"
    core_catalog, extension_catalog = str(tmp_path / "core.ducklake"), str(tmp_path / "extension.ducklake")
    core = DuckLakeStore(LakeLocation(core_catalog, root))
    extension = DuckLakeStore(LakeLocation(extension_catalog, root, "extension"))
    core.initialize()
    extension.initialize()
    settings = replace(AppSettings.load(environ={}, project_root=tmp_path),
        lake=LakeSettings(root=root, core_catalog=core_catalog, extension_catalog=extension_catalog,
                          core_read_catalog=core_catalog))
    return core, extension, settings


def typed(name, frame):
    with duckdb.connect() as con:
        con.execute(get_table(name).duckdb_create_sql())
        con.register("seed", frame)
        columns = ",".join(frame.columns)
        con.execute(f"INSERT INTO {name} ({columns}) SELECT {columns} FROM seed")
        return con.execute(f"SELECT * FROM {name}").fetchdf()


def test_manual_snapshot_identity_includes_revised_window(tmp_path):
    core, extension, settings = environment(tmp_path)
    settings = replace(settings, lake=replace(settings.lake, core_catalog=None))
    day1, day2 = date(2026, 9, 29), date(2026, 9, 30)
    def publish(day, close, key):
        return core.publish(get_table("daily_market_snapshot"), typed("daily_market_snapshot",
            pd.DataFrame({"trade_date": [day], "ticker": ["000001.SZ"], "close": [close]})),
            partition=str(day), idempotency_key=key, lineage={},
            expected_previous=core.current_publication("daily_market_snapshot", str(day)))
    publish(day1, 10., "first")
    target = publish(day2, 20., "target")
    enabled = tmp_path / "plugins.json"
    enabled.write_text(json.dumps({"rolling_mean": BUILTIN_PLUGINS["rolling_mean"]}))
    args = cli.parser().parse_args(["--env-file", "unused", "run", "--domain", "extension",
        "--dataset", "rolling_mean", "--date", str(day2), "--plugins", str(enabled),
        "--parameters", '{"window":2}', "--allow-observed", "--input-snapshot", str(target.snapshot_id)])
    first = cli.run(args, settings)
    revised = publish(day1, 30., "revision")
    args.input_snapshot = revised.snapshot_id
    second = cli.run(args, settings)
    assert first["publication_id"] != second["publication_id"]
    assert cli.run(args, settings)["publication_id"] == second["publication_id"]
    table = "rolling_mean"
    assert DataPortal(extension, first["snapshot_id"]).read(table).iloc[0]["mean_close"] == 15.
    assert DataPortal(extension, second["snapshot_id"]).read(table).iloc[0]["mean_close"] == 25.
    receipt = second["lineage"]["input_receipts"]["daily_market_snapshot"]
    assert receipt["publication_id"] == target.publication_id
    assert receipt["read_identity"]["snapshot_id"] == revised.snapshot_id


def test_live_invocation_lost_reply_reuses_raw_and_first_manifest(tmp_path, monkeypatch):
    core, _, settings = environment(tmp_path)
    target = date(2026, 9, 30)
    def calendar(key):
        return core.publish(get_table("trading_calendar"), typed("trading_calendar",
            pd.DataFrame({"trade_date": [target], "is_open": [True]})), partition=str(target),
            idempotency_key=key, lineage={"revision": key},
            expected_previous=core.current_publication("trading_calendar", str(target)))
    upstream = calendar("initial")
    manifest = tmp_path / "inputs.json"
    def write_manifest(receipt):
        manifest.write_text(json.dumps({"trading_calendar": {"publication_id": receipt.publication_id,
            "snapshot_id": receipt.snapshot_id, "domain": "core"}}))
    write_manifest(upstream)
    calls = []
    class Provider:
        def suspend_d(self, **kwargs):
            calls.append(kwargs)
            return pd.DataFrame()
    import qrp_atlas.config.tushare_client as client
    monkeypatch.setattr(client, "get_tushare_pro", lambda **kwargs: Provider())
    args = cli.parser().parse_args(["--env-file", "unused", "run", "--dataset", "suspend_d",
        "--date", str(target), "--input-manifest", str(manifest), "--invocation-id", "dagster-run-one"])
    original = DuckLakeStore.publish
    lost = []
    def drop_reply(self, schema, *a, **kw):
        result = original(self, schema, *a, **kw)
        if schema.name == "suspend_d" and not lost:
            lost.append(result)
            raise RuntimeError("lost receipt after actual commit")
        return result
    monkeypatch.setattr(DuckLakeStore, "publish", drop_reply)
    with pytest.raises(RuntimeError, match="lost receipt"):
        cli.run(args, settings)
    write_manifest(calendar("changed-upstream"))
    recovered = cli.run(args, settings)
    assert recovered["publication_id"] == lost[0].publication_id
    assert len(calls) == 1
    assert recovered["lineage"]["input_receipts"]["trading_calendar"]["publication_id"] == upstream.publication_id
    assert cli.run(args, settings)["publication_id"] == recovered["publication_id"]
    args.invocation_id = "intentional-new-run"
    assert cli.run(args, settings)["publication_id"] != recovered["publication_id"]
    assert len(calls) == 2


def test_stock_universe_distinguishes_unlisted_and_historical_delisted():
    target = date(2026, 9, 30)
    stocks = pd.DataFrame({"ticker": ["A", "G", "D"], "list_status": ["L", "G", "D"],
        "list_date": [date(2000, 1, 1), None, date(2000, 1, 1)],
        "delist_date": [None, None, date(2026, 10, 1)], "is_active": [True, False, False]})
    inputs = {"stock_info": stocks, "trading_calendar": pd.DataFrame({"trade_date": [target], "is_open": [True]}),
              "suspend_d": pd.DataFrame()}
    lineage = {"input_snapshots": dict.fromkeys(inputs, "pinned")}
    assert _input_scope("daily_market_snapshot", target, inputs, lineage) == (True, {"A", "D"})
    stocks.loc[0, "list_date"] = None
    with pytest.raises(CoreQualityError, match="STOCK_REFERENCE_INVALID"):
        _input_scope("daily_market_snapshot", target, inputs, lineage)


def test_legacy_file_growth_and_reexport_do_not_change_business_identity(tmp_path):
    core, _, _ = environment(tmp_path)
    source = tmp_path / "legacy.duckdb"
    target = date(2026, 9, 30)
    with duckdb.connect(str(source)) as con:
        con.execute(get_table("trading_calendar").duckdb_create_sql())
        con.execute("INSERT INTO trading_calendar VALUES ('2026-09-30',true,2026,9,3)")
    def load(path):
        return import_legacy(path, core, "trading_calendar", start=target, end=target, dry_run=False)[0]
    first = load(source)
    with duckdb.connect(str(source)) as con:
        con.execute("CREATE TABLE unrelated AS SELECT range AS id, repeat('x',1000) AS text FROM range(2000)")
    assert load(source)["publication_id"] == first["publication_id"]
    copy = tmp_path / "another-export.duckdb"
    shutil.copyfile(source, copy)
    assert load(copy)["publication_id"] == first["publication_id"]
    with duckdb.connect(str(source)) as con:
        con.execute("UPDATE trading_calendar SET is_open=false")
    with pytest.raises(ValueError, match="changed since computation"):
        load(source)


def test_partition_key_contract_rejected_before_writes(tmp_path):
    core, _, _ = environment(tmp_path)
    schema = TableSchema("bad_key", (ColumnSpec("trade_date", "DATE", False), ColumnSpec("ticker", "VARCHAR", False)), ("ticker",))
    frame = pd.DataFrame({"trade_date": [date(2026, 9, 30)], "ticker": ["A"]})
    with pytest.raises(ValueError, match="partition column must belong"):
        core.publish(schema, frame, partition="2026-09-30", idempotency_key="bad", lineage={})
    assert core.current_publication(schema.name, "2026-09-30") == ""
    with pytest.raises(PluginError, match="partition column must belong"):
        PluginSpec("bad", "1", "1", (), "bad_key", ("ticker",))


@pytest.mark.parametrize("hour", [None, "20"])
def test_real_code_location_has_stopped_schedule(tmp_path, monkeypatch, hour):
    import dagster as dg
    from qrp_atlas.pipeline.lake.orchestration import configured_definitions
    env = tmp_path / "core.env"
    env.write_text("QRP_LAKE_ROOT=" + tmp_path.as_posix() + "\n" + (f"QRP_LAKE_SCHEDULE_HOUR={hour}\n" if hour else ""))
    settings = AppSettings.load(env_file=env, environ={}, project_root=tmp_path)
    monkeypatch.setattr(AppSettings, "load", classmethod(lambda cls: settings))
    schedules = list(configured_definitions("core").schedules)
    assert len(schedules) == 1
    assert schedules[0].default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedules[0].execution_timezone == "Asia/Shanghai"
    assert schedules[0].cron_schedule == f"0 {hour or 18} * * *"


def test_deployment_instance_template_is_valid(tmp_path):
    from dagster._core.instance.config import dagster_instance_config
    source = Path(__file__).resolve().parents[3] / "deploy/lake/dagster.yaml.example"
    shutil.copyfile(source, tmp_path / "dagster.yaml")
    config, _ = dagster_instance_config(str(tmp_path))
    assert config["concurrency"]["runs"]["max_concurrent_runs"] == 2
    assert config["concurrency"]["pools"]["default_limit"] == 1
    assert config["run_monitoring"]["max_runtime_seconds"] == 7200
