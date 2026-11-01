import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

dg = pytest.importorskip("dagster")

from qrp_atlas.pipeline.lake.orchestration import AssetTask, build_definitions, core_tasks


def test_core_graph_materializes_fixed_receipts_and_replays_partition(tmp_path):
    calls = []
    def launch(task, partition, run_id, inputs):
        calls.append((task.dataset, partition, inputs))
        return {"snapshot_id": 4 if task.dataset == "raw" else 5,
                "publication_id": task.dataset + "-" + partition}
    tasks = [AssetTask("raw"), AssetTask("fact", dependencies=(("core", "raw"),))]
    defs = build_definitions(domain="core", tasks=tasks, launcher=launch)
    dg.Definitions.validate_loadable(defs)
    with dg.DagsterInstance.local_temp(str(tmp_path)) as instance:
        for _ in range(2):
            result = defs.resolve_job_def("qrp_lake_core").execute_in_process(
                instance=instance, partition_key="2026-10-30")
            assert result.success
    assert len(calls) == 4
    assert calls[1][2]["raw"]["snapshot_id"] == 4
    assert calls[1][2]["raw"]["partition"] == "2026-10-30"
    assert calls[3][2]["raw"]["publication_id"] == "raw-2026-10-30"


def test_missing_same_date_upstream_fails_instead_of_using_prior_date(tmp_path):
    calls = []
    def launch(*args):
        calls.append(args)
        return {"snapshot_id": 1, "publication_id": "not-reached"}
    defs = build_definitions(domain="extension", tasks=[
        AssetTask("indicator", "extension", (("core", "fact"),), "qrp_extension")
    ], launcher=launch)
    with dg.DagsterInstance.local_temp(str(tmp_path)) as instance:
        instance.report_runless_asset_event(dg.AssetMaterialization(
            asset_key=dg.AssetKey(["core", "fact"]), partition="2026-10-29",
            metadata={"publication": dg.MetadataValue.json({"snapshot_id": 1, "publication_id": "old"})}))
        result = defs.resolve_job_def("qrp_lake_extension").execute_in_process(
            instance=instance, partition_key="2026-10-30", raise_on_error=False)
        assert not result.success
    assert calls == []


def test_core_cannot_depend_on_extension():
    with pytest.raises(ValueError, match="Core cannot depend"):
        build_definitions(domain="core", tasks=[AssetTask("fact", dependencies=(("extension", "bad"),))],
                          launcher=lambda *args: {})


def test_core_discovery_does_not_import_plugin(monkeypatch):
    import builtins
    original = builtins.__import__
    def guarded(name, *args, **kwargs):
        if "lake.extensions" in name or "lake.plugins" in name:
            raise AssertionError("Core imported extension")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)
    defs = build_definitions(domain="core", tasks=core_tasks(), launcher=lambda *args: {})
    dg.Definitions.validate_loadable(defs)


def test_schedule_targets_current_shanghai_date_and_is_disabled():
    defs = build_definitions(domain="core", tasks=[AssetTask("stock_info")], launcher=lambda *args: {},
                             schedule_hour=18)
    schedule = list(defs.schedules)[0]
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    context = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 11, 2, 18,
                                                                         tzinfo=ZoneInfo("Asia/Shanghai")))
    request = schedule(context)
    assert request.partition_key == "2026-11-02"


def test_reexecution_keeps_root_invocation_but_new_launch_can_refresh(tmp_path):
    from dagster._core.storage.tags import ROOT_RUN_ID_TAG, PARENT_RUN_ID_TAG
    calls = []
    def launch(task, partition, run_id, inputs):
        calls.append(run_id)
        return {"snapshot_id": 1, "publication_id": "bound-" + run_id}
    defs = build_definitions(domain="core", tasks=[AssetTask("trading_calendar")], launcher=launch)
    job = defs.resolve_job_def("qrp_lake_core")
    with dg.DagsterInstance.local_temp(str(tmp_path)) as instance:
        first = job.execute_in_process(instance=instance, partition_key="2026-10-30")
        retry = job.execute_in_process(instance=instance, partition_key="2026-10-30",
            tags={ROOT_RUN_ID_TAG: first.run_id, PARENT_RUN_ID_TAG: first.run_id})
        fresh = job.execute_in_process(instance=instance, partition_key="2026-10-30")
    assert first.success and retry.success and fresh.success
    assert first.run_id != retry.run_id
    assert calls == [first.run_id, first.run_id, fresh.run_id]
    event = retry.asset_materializations_for_node("core__trading_calendar")[0]
    assert event.metadata["publication"].value["dagster_run_id"] == retry.run_id
