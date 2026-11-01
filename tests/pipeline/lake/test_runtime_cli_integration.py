"""Actual Dagster -> CLI subprocess -> Raw replay -> DuckLake publications.

Embedded metadata is only a serial, isolated test fixture. PostgreSQL concurrent
catalog and Linux identity/cgroup acceptance are separate tests.
Provider frames here are synthetic fixtures, not live supplier evidence.
"""
from datetime import date, datetime, timezone
from pathlib import Path
import json
import os

import dagster as dg
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import get_table
from qrp_atlas.contracts.mappings import TUSHARE_STOCK_BASIC
from qrp_atlas.pipeline.lake.core import normalize_core
from qrp_atlas.pipeline.lake.orchestration import AssetTask, CliLauncher, build_definitions, core_tasks
from qrp_atlas.pipeline.lake.plugins import BUILTIN_PLUGINS
from qrp_atlas.pipeline.lake.raw import RawStore
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def _environment():
    env = {key: value for key, value in os.environ.items()
           if key.upper() in {"PATH", "SYSTEMROOT", "WINDIR", "PATHEXT", "HOME", "USERPROFILE"}}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    return env


def _daily(day, close):
    return pd.DataFrame([dict(ts_code="000001.SZ", trade_date=day.strftime("%Y%m%d"),
        open=close, high=close+1, low=close-1, close=close, pre_close=close-1,
        pct_chg=1., vol=100., amount=1000.)])


def _stock():
    row = dict.fromkeys(TUSHARE_STOCK_BASIC, None)
    row.update(ts_code="000001.SZ", symbol="000001", name="Test", list_status="L",
               exchange="SZSE", list_date="19910403", curr_type="CNY")
    return pd.DataFrame([row])


def test_real_cli_chain_replay_then_extension_requires_explicit_policy(tmp_path):
    root = tmp_path / "lake"
    core_catalog, ext_catalog = tmp_path / "core.ducklake", tmp_path / "extension.ducklake"
    core = DuckLakeStore(LakeLocation(str(core_catalog), root))
    ext = DuckLakeStore(LakeLocation(str(ext_catalog), root, "extension"))
    core.initialize()
    ext.initialize()
    raw = RawStore(root / "raw")
    envfile = tmp_path / "core.env"
    envfile.write_text(f"QRP_HOME={tmp_path.as_posix()}\nQRP_LAKE_ROOT={root.as_posix()}\n"
                       f"QRP_LAKE_CORE_CATALOG={core_catalog.as_posix()}\n", encoding="utf-8")
    # Prior history is a canonical fixture, distinct from the recorded Raw
    # batches exercised through the real CLI below.
    dates = [date(2026, 9, n) for n in (23, 24, 25, 28, 29, 30)]
    for index, day in enumerate(dates[:4]):
        frame = normalize_core("daily_market_snapshot", _daily(day, 10.+index),
            target_date=day, observed_at=datetime.combine(day, datetime.min.time(), timezone.utc), complete=True)
        core.publish(get_table("daily_market_snapshot"), frame, partition=day.isoformat(),
                     idempotency_key="fixture-"+day.isoformat(), lineage={}, time_model="observed")

    latest_receipt = None
    (tmp_path / "dagster").mkdir()
    with dg.DagsterInstance.local_temp(str(tmp_path / "dagster")) as instance:
        for index, day in enumerate(dates[4:], 4):
            stamp = day.strftime("%Y%m%d")
            frames = {
                "trading_calendar": pd.DataFrame([{"cal_date":stamp, "is_open":1}]),
                "stock_info": _stock(),
                "suspend_d": pd.DataFrame(),
                "daily_market_snapshot": _daily(day, 10.+index),
                "daily_basic": pd.DataFrame([dict(ts_code="000001.SZ",trade_date=stamp,close=10.+index)]),
                "adj_factor_changes": pd.DataFrame([dict(ts_code="000001.SZ",trade_date=stamp,adj_factor=1.)]),
            }
            batches = {name: raw.save_frame("tushare", name, frame, parameters={}, complete=True,
                observed_at=datetime.combine(day, datetime.min.time(), timezone.utc)).batch_id
                for name, frame in frames.items()}
            launcher = CliLauncher(envfile, root / "core-runtime", _environment(), recorded_batches=batches)
            defs = build_definitions(domain="core", tasks=core_tasks(), launcher=launcher)
            result = defs.resolve_job_def("qrp_lake_core").execute_in_process(
                instance=instance, partition_key=day.isoformat())
            assert result.success
            latest_receipt = core.current_publication("daily_market_snapshot", day.isoformat())
        # Replaying the exact same raw inputs through Dagster reconciles receipts.
        result = defs.resolve_job_def("qrp_lake_core").execute_in_process(
            instance=instance, partition_key=dates[-1].isoformat())
        assert result.success
        assert core.current_publication("daily_market_snapshot", dates[-1].isoformat()) == latest_receipt

        plugins = tmp_path / "plugins.json"
        plugins.write_text(json.dumps({"rolling_mean": BUILTIN_PLUGINS["rolling_mean"]}), encoding="utf-8")
        extenv = tmp_path / "extension.env"
        extenv.write_text(f"QRP_HOME={tmp_path.as_posix()}\nQRP_LAKE_ROOT={root.as_posix()}\n"
                          f"QRP_LAKE_CORE_READ_CATALOG={core_catalog.as_posix()}\n"
                          f"QRP_LAKE_EXTENSION_CATALOG={ext_catalog.as_posix()}\n", encoding="utf-8")
        task = AssetTask("rolling_mean", "extension", (("core","daily_market_snapshot"),), "qrp_extension")
        launcher = CliLauncher(extenv, root / "extension-runtime", _environment(), plugins_file=plugins)
        manifest = {"daily_market_snapshot": {"snapshot_id":core.recover(latest_receipt).snapshot_id,
                    "publication_id":latest_receipt, "domain":"core", "partition":dates[-1].isoformat()}}
        from qrp_atlas.pipeline.lake.worker import WorkerFailure
        with pytest.raises(WorkerFailure, match="WORKER_EXIT_FAILED"):
            launcher(task, dates[-1].isoformat(), "strict-policy-test", manifest)
        launcher.allow_observed = True
        extdefs = build_definitions(domain="extension", tasks=[task], launcher=launcher)
        assert extdefs.resolve_job_def("qrp_lake_extension").execute_in_process(
            instance=instance, partition_key=dates[-1].isoformat()).success
    publication = ext.recover(ext.current_publication("rolling_mean", dates[-1].isoformat()))
    output = DataPortal(ext, publication.snapshot_id).read("rolling_mean")
    assert output.mean_close.item() == pytest.approx(13.)
    assert publication.lineage["time_policy"] == "observed_not_historical_pit"


def test_plugin_configuration_is_explicit_and_defaults_to_strict(tmp_path):
    default = AppSettings.load(project_root=tmp_path, environ={})
    assert default.lake.allow_observed is False
    assert default.lake.plugins_file is None
    plugin_file = tmp_path / "enabled.json"
    cfg = AppSettings.load(project_root=tmp_path, environ={}, overrides={
        "QRP_LAKE_ALLOW_OBSERVED": "true", "QRP_LAKE_PLUGINS_FILE": str(plugin_file)})
    assert cfg.lake.allow_observed is True
    assert cfg.lake.plugins_file == plugin_file
    with pytest.raises(ValueError):
        AppSettings.load(project_root=tmp_path, environ={}, overrides={"QRP_LAKE_ALLOW_OBSERVED":"yes"})


def test_configured_extension_location_only_declares_enabled_plugins(tmp_path, monkeypatch):
    from qrp_atlas.pipeline.lake.orchestration import configured_definitions
    envfile = tmp_path / "extension.env"
    envfile.write_text("QRP_LAKE_ROOT=" + tmp_path.as_posix() + "\n", encoding="utf-8")
    settings = AppSettings.load(project_root=tmp_path, environ={}, env_file=envfile)
    monkeypatch.setattr(AppSettings, "load", classmethod(lambda cls: settings))
    empty = configured_definitions("extension")
    assert list(empty.assets) == []
    from dataclasses import replace
    plugins = tmp_path / "plugins.json"
    plugins.write_text(json.dumps({"rolling_mean":BUILTIN_PLUGINS["rolling_mean"]}), encoding="utf-8")
    settings = replace(settings, lake=replace(settings.lake, plugins_file=plugins))
    defs = configured_definitions("extension")
    keys = {key.to_user_string() for asset in defs.assets if isinstance(asset, dg.AssetsDefinition)
            for key in asset.keys}
    assert keys == {"extension/rolling_mean"}
    dg.Definitions.validate_loadable(defs)
