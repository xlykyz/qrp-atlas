"""Optional Dagster adapter for published lake partitions.

Importing this module does not discover plugins or load the old Contract
catalog. Dagster owns run state/retries; publications own data visibility.
"""
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable, Mapping, Sequence
import json
import sys
import uuid

from .worker import WorkerBudget, run_worker


@dataclass(frozen=True)
class AssetTask:
    dataset: str
    domain: str = "core"
    dependencies: tuple[tuple[str, str], ...] = ()
    pool: str = "qrp_core"

    @property
    def key(self):
        return (self.domain, self.dataset)


@dataclass
class CliLauncher:
    """Separate credential files/environments must be supplied per code location."""
    env_file: Path
    work_root: Path
    environment: Mapping[str, str] = field(repr=False)
    budget: WorkerBudget = field(default_factory=WorkerBudget)
    python: str = sys.executable
    plugins_file: Path | None = None
    allow_observed: bool = False
    recorded_batches: Mapping[str, str] = field(default_factory=dict)

    def __call__(self, task: AssetTask, partition: str, run_id: str,
                 inputs: Mapping[str, Mapping]) -> Mapping:
        self.work_root.mkdir(parents=True, exist_ok=True)
        attempt = self.work_root / uuid.uuid4().hex
        manifest = self.work_root / (attempt.name + ".inputs.json")
        # Inputs contain only immutable publication IDs and dataset identities.
        with manifest.open("x", encoding="utf-8") as stream:
            json.dump(inputs, stream, sort_keys=True)
        argv = [self.python, "-m", "qrp_atlas.pipeline.lake.cli", "--env-file", str(self.env_file.resolve()),
                "run", "--domain", task.domain, "--dataset", task.dataset, "--date", partition]
        if inputs:
            argv.extend(["--input-manifest", str(manifest.resolve())])
        if task.dataset in self.recorded_batches:
            argv.extend(["--raw-batch", self.recorded_batches[task.dataset]])
        if task.domain == "extension":
            if self.plugins_file is not None:
                argv.extend(["--plugins", str(self.plugins_file.resolve())])
            if self.allow_observed:
                argv.append("--allow-observed")
        result = run_worker(argv, directory=attempt, budget=self.budget, environment=self.environment)
        try:
            payload = json.loads(result.stdout_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not {"snapshot_id", "publication_id"} <= payload.keys():
                raise ValueError
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("WORKER_PUBLICATION_RECEIPT_INVALID") from exc
        return {**payload, "worker_seconds": result.duration_seconds,
                "peak_rss_bytes": result.peak_rss_bytes, "dagster_run_id": run_id}


def build_definitions(*, domain: str, tasks: Sequence[AssetTask], launcher: Callable,
                      start_date: str = "2026-01-01", schedule_hour: int | None = None):
    """Build exactly one code location without importing the other domain.

    Dependencies are selected for the same date and bound to immutable receipts
    at launch. Different datasets may use different catalog snapshots. Missing
    materializations fail closed rather than substitute a previous date.
    """
    import dagster as dg

    if domain not in {"core", "extension"}:
        raise ValueError("Unknown lake domain")
    if any(task.domain != domain for task in tasks):
        raise ValueError("A code location cannot mix publication domains")
    if domain == "core" and any(dep[0] != "core" for task in tasks for dep in task.dependencies):
        raise ValueError("Core cannot depend on extensions")
    if len({t.key for t in tasks}) != len(tasks):
        raise ValueError("Duplicate asset tasks")
    if not tasks:
        return dg.Definitions(assets=[])
    # Current-day provider snapshots cannot be backdated to yesterday. Expose
    # today's partition and schedule it explicitly after the intended close.
    partitions = dg.DailyPartitionsDefinition(start_date=start_date, timezone="Asia/Shanghai", end_offset=1)
    local_keys = {task.key for task in tasks}
    external_keys = {dep for task in tasks for dep in task.dependencies if dep not in local_keys}
    assets = [dg.AssetSpec(key=dg.AssetKey(dep), partitions_def=partitions) for dep in sorted(external_keys)]

    def definition(task):
        @dg.asset(key=dg.AssetKey(task.key), deps=[dg.AssetKey(dep) for dep in task.dependencies],
                  partitions_def=partitions, pool=task.pool,
                  backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
                  retry_policy=dg.RetryPolicy(max_retries=2, delay=5))
        def compute(context: dg.AssetExecutionContext):
            inputs = {}
            for dep in task.dependencies:
                records = context.instance.fetch_materializations(
                    dg.AssetRecordsFilter(asset_key=dg.AssetKey(dep), asset_partitions=[context.partition_key]), limit=1
                ).records
                if not records:
                    raise dg.Failure(description="UPSTREAM_PARTITION_NOT_PUBLISHED: " + "/".join(dep))
                materialization = records[0].asset_materialization
                receipt_entry = materialization.metadata.get("publication") if materialization else None
                receipt = receipt_entry.value if receipt_entry is not None else None
                if not isinstance(receipt, dict) or not {"snapshot_id", "publication_id"} <= receipt.keys():
                    raise dg.Failure(description="UPSTREAM_PUBLICATION_RECEIPT_MISSING")
                if dep[1] in inputs:
                    raise dg.Failure(description="AMBIGUOUS_INPUT_DATASET")
                inputs[dep[1]] = {**receipt, "domain": dep[0], "partition": context.partition_key}
            receipt = dict(launcher(task, context.partition_key, context.run.run_id, inputs))
            if not {"snapshot_id", "publication_id"} <= receipt.keys():
                raise dg.Failure(description="PUBLICATION_RECEIPT_MISSING")
            return dg.MaterializeResult(metadata={"publication": dg.MetadataValue.json(receipt)})
        return compute

    assets.extend(definition(task) for task in tasks)
    job = dg.define_asset_job("qrp_lake_" + domain,
                              selection=dg.AssetSelection.assets(*(dg.AssetKey(t.key) for t in tasks)),
                              tags={"qrp/domain": domain},
                              executor_def=dg.multiprocess_executor.configured({"max_concurrent": 2}))
    schedules = []
    if schedule_hour is not None:
        if not 0 <= schedule_hour <= 23:
            raise ValueError("Invalid schedule hour")
        @dg.schedule(job=job, cron_schedule=f"0 {schedule_hour} * * *", execution_timezone="Asia/Shanghai",
                     default_status=dg.DefaultScheduleStatus.STOPPED)
        def current_day_schedule(context):
            target = context.scheduled_execution_time.date().isoformat()
            return dg.RunRequest(partition_key=target, run_key=target)
        schedules.append(current_day_schedule)
    return dg.Definitions(assets=assets, jobs=[job], schedules=schedules)


def core_tasks() -> tuple[AssetTask, ...]:
    return (
        AssetTask("trading_calendar"),
        AssetTask("stock_info"),
        AssetTask("daily_market_snapshot", dependencies=(("core", "trading_calendar"), ("core", "stock_info"), ("core", "suspend_d"))),
        AssetTask("daily_basic", dependencies=(("core", "trading_calendar"), ("core", "daily_market_snapshot"))),
        AssetTask("adj_factor_changes", dependencies=(("core", "trading_calendar"), ("core", "daily_market_snapshot"))),
        AssetTask("suspend_d", dependencies=(("core", "trading_calendar"),)),
    )


def extension_tasks() -> tuple[AssetTask, ...]:
    # This list is declarative. No System B or plugin module is imported here.
    dependencies = (("core", "daily_market_snapshot"), ("core", "adj_factor_changes"),
                    ("core", "stock_info"), ("core", "trading_calendar"), ("core", "suspend_d"))
    return (
        AssetTask("rolling_mean", "extension", (("core", "daily_market_snapshot"),), "qrp_extension"),
        AssetTask("system_b_state", "extension", dependencies, "qrp_extension"),
    )


def configured_definitions(domain: str):
    """Load one explicitly configured location via the unified AppSettings."""
    import os
    from qrp_atlas.config.settings import AppSettings
    settings = AppSettings.load()
    lake = settings.lake
    if lake.root is None or settings.runtime.env_file is None:
        raise ValueError("Lake code location requires QRP_LAKE_ROOT and an explicit configuration file")
    if domain == "extension" and lake.core_catalog:
        raise ValueError("Extension location must not receive core writer credentials")
    budget = WorkerBudget(timeout_seconds=lake.worker_timeout_seconds,
                          memory_bytes=lake.worker_memory_bytes,
                          temporary_bytes=lake.worker_temp_bytes, threads=lake.threads)
    # These are process-launch essentials, not QRP runtime configuration.
    inherited = {key: value for key, value in os.environ.items()
                 if key.upper() in {"PATH", "SYSTEMROOT", "WINDIR", "PATHEXT", "HOME", "USERPROFILE", "PYTHONPATH"}}
    launcher = CliLauncher(settings.runtime.env_file, lake.root / (domain + "-runtime"), inherited, budget,
                           plugins_file=lake.plugins_file, allow_observed=lake.allow_observed)
    if domain == "core":
        tasks = core_tasks()
    else:
        enabled = json.loads(lake.plugins_file.read_text(encoding="utf-8")) if lake.plugins_file else {}
        from .plugins import load_plugin
        tasks = tuple(AssetTask(plugin_id, "extension",
                               tuple(("core", name) for name in load_plugin(plugin_id, enabled=enabled).spec.inputs),
                               "qrp_extension") for plugin_id in enabled)
    return build_definitions(domain=domain, tasks=tasks,
                             launcher=launcher, start_date=lake.partition_start_date)
