"""Opt-in lake CLI. No implicit setup, production routing or legacy writes."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
import json
from pathlib import Path
import sys

from qrp_atlas.config.settings import AppSettings
from .store import DataPortal, DuckLakeStore, LakeLocation


def configured_store(settings: AppSettings, domain: str, *, read_only=False):
    cfg = settings.lake
    if cfg.root is None:
        raise ValueError("explicit QRP_LAKE_ROOT is required")
    catalog = (cfg.core_read_catalog or cfg.core_catalog) if domain == "core" and read_only else (cfg.core_catalog if domain == "core" else cfg.extension_catalog)
    if not catalog:
        raise ValueError("catalog is not configured for requested domain")
    return DuckLakeStore(LakeLocation(catalog, cfg.root, domain, cfg.threads, cfg.memory_limit, cfg.relocated),
                        read_only=read_only or settings.runtime.read_only)


def load_inputs(settings, names, manifest, snapshot=None, *, target_date=None):
    from .plugins import identity
    frames, versions, receipts = {}, {}, {}
    for dataset in names:
        entry = manifest.get(dataset)
        if entry is None and snapshot is None:
            raise ValueError("missing pinned input " + dataset)
        domain = entry.get("domain", "core") if entry else "core"
        source = configured_store(settings, domain, read_only=True)
        version = int(entry["snapshot_id"]) if entry else snapshot
        publication_id = entry["publication_id"] if entry else None
        if publication_id:
            receipt = source.recover(publication_id)
            if receipt is None or receipt.dataset != dataset or receipt.snapshot_id != version:
                raise ValueError("invalid input receipt")
            if target_date and receipt.partition != target_date.isoformat():
                raise ValueError("upstream partition is not ready for target date")
            receipts[dataset] = asdict(receipt)
        else:
            # A manual catalog snapshot must still contain an explicit target publication.
            with source.connect() as con:
                row = con.execute(f"SELECT publication_id FROM lake._qrp_publications AT (VERSION => {int(version)}) WHERE dataset=? AND partition_id=? ORDER BY published_at DESC LIMIT 1", [dataset, target_date.isoformat()]).fetchone()
            if not row:
                raise ValueError("target partition is absent in pinned snapshot")
            receipts[dataset] = asdict(source.recover(row[0]))
        frames[dataset] = DataPortal(source, version).read(dataset, publication_id=publication_id)
        read_identity = {"domain": domain, "dataset": dataset, "snapshot_id": version,
                         "publication_id": receipts[dataset]["publication_id"]}
        receipts[dataset]["read_identity"] = read_identity
        versions[dataset] = identity(read_identity)
    return frames, versions, receipts


def run(args, settings):
    if settings.runtime.read_only:
        raise PermissionError("read-only configuration cannot run production")
    if args.domain != "core":
        return _run(args, settings)
    from copy import copy
    import uuid
    from .invocation import bind_invocation
    if settings.lake.root is None:
        raise ValueError("explicit QRP_LAKE_ROOT is required")
    invocation_id = getattr(args, "invocation_id", None) or uuid.uuid4().hex
    scope = {"domain": args.domain, "dataset": args.dataset, "date": args.date}
    def request():
        return {"manifest": json.loads(Path(args.input_manifest).read_text(encoding="utf-8")) if args.input_manifest else {},
                "input_snapshot": args.input_snapshot, "raw_batch": args.raw_batch}
    with bind_invocation(settings.lake.root, invocation_id, scope, request) as binding:
        bound_args = copy(args)
        bound_args._binding = binding
        if binding.data.get("publication_id"):
            receipt = configured_store(settings, "core", read_only=True).recover(binding.data["publication_id"])
            if receipt is None:
                raise ValueError("bound invocation receipt is missing")
            return {**asdict(receipt), "domain": "core"}
        payload = _run(bound_args, settings)
        binding.data["publication_id"] = payload["publication_id"]
        binding.save()
        return payload


def _run(args, settings):
    target = date.fromisoformat(args.date)
    store = configured_store(settings, args.domain)
    binding = getattr(args, "_binding", None)
    manifest = binding.data["request"]["manifest"] if binding else (json.loads(Path(args.input_manifest).read_text(encoding="utf-8")) if args.input_manifest else {})
    if args.domain == "core":
        from .core import CORE_INPUTS, CoreRunner
        from qrp_atlas.contracts.lake_core import CORE_DATASETS
        from .raw import RawStore
        spec = CORE_DATASETS[args.dataset]
        previous = binding.value("expected_previous", lambda: store.current_publication(args.dataset, args.date, whole_table=not spec.date_field))
        frames, versions, receipts = load_inputs(settings, CORE_INPUTS.get(args.dataset, ()), manifest,
                                                binding.data["request"]["input_snapshot"], target_date=target)
        import pandas as pd
        for name in ("suspend_d", "daily_market_snapshot"):
            if name in frames:
                frames[name] = frames[name].loc[pd.to_datetime(frames[name].trade_date).dt.date == target].copy()
        raw = RawStore(settings.lake.root / "raw")
        runner = CoreRunner(raw, store)
        kwargs = dict(inputs=frames, lineage={"input_snapshots": versions, "input_receipts": receipts},
                      expected_previous=previous)
        if args.dataset == "stock_info":
            kwargs["expected_historical_previous"] = binding.value("expected_historical_previous",
                lambda: store.current_publication("stock_info_historical_identity", args.date, whole_table=True))
        batch = binding.data.get("raw_batch") or binding.data["request"]["raw_batch"]
        if batch:
            publication = runner.replay(args.dataset, batch, target_date=target, **kwargs)
        else:
            from qrp_atlas.config.tushare_client import get_tushare_pro
            from .provider import SDKProvider
            import tushare
            runner.provider = SDKProvider(get_tushare_pro(settings=settings), raw, sdk_version=tushare.__version__)
            def on_acquired(batch_id):
                binding.data["raw_batch"] = batch_id
                binding.save()
            publication = runner.acquire_and_publish(args.dataset, target, on_acquired=on_acquired, **kwargs)
    else:
        if settings.lake.core_catalog:
            raise PermissionError("extension environment must not contain core write catalog credentials")
        from .plugins import PluginContext, load_plugin, identity
        enabled = json.loads(Path(args.plugins).read_text(encoding="utf-8")) if args.plugins else {}
        plugin = load_plugin(args.dataset, enabled=enabled)
        frames, versions, receipts = load_inputs(settings, plugin.spec.inputs, manifest,
                                                args.input_snapshot, target_date=target)
        if not args.allow_observed:
            # Current core adapters do not fabricate historical knowledge evidence.
            raise ValueError("observed/legacy inputs require explicit --allow-observed; strict PIT production is unavailable for these schemas")
        replay_start = date.fromisoformat(args.replay_from) if args.replay_from else target
        if args.replay_from and not args.checkpoint:
            raise ValueError("replay requires an explicit frozen prior checkpoint file")
        import pandas as pd
        candidates = pd.date_range(replay_start, target, freq="D").date
        previous_by_date = {str(day): store.current_publication(plugin.spec.output_dataset, str(day)) for day in candidates}
        checkpoint = json.loads(Path(args.checkpoint).read_text(encoding="utf-8")) if args.checkpoint else None
        if checkpoint is None and plugin.spec.mode == "state":
            with store.connect() as con:
                old = con.execute("SELECT lineage,partition_id FROM lake._qrp_publications WHERE dataset=? AND partition_id < ? ORDER BY partition_id DESC,published_at DESC LIMIT 1", [plugin.spec.output_dataset,args.date]).fetchone()
            if old:
                checkpoint = json.loads(old[0]).get("checkpoint")
                if checkpoint is None or checkpoint.get("target_date") != old[1]:
                    raise ValueError("stored checkpoint does not match its publication date; explicit replay is required")
        params = json.loads(args.parameters)
        result = plugin.run(PluginContext(target, frames, versions, parameters=params, checkpoint=checkpoint,
                           replay_from=date.fromisoformat(args.replay_from) if args.replay_from else None,
                           strict_pit=False))
        outputs = []
        for day, frame in result.frame.groupby(plugin.spec.partition_column, sort=True):
            partition = pd.Timestamp(day).date().isoformat()
            if partition not in previous_by_date:
                raise ValueError("plugin emitted outside requested replay window")
            day_result = result
            if plugin.spec.mode == "state" and partition != args.date:
                # Recompute each recovery boundary with that day's bounded
                # history/adjustment anchor. Relabelling the terminal checkpoint
                # would leave its hashes, state and identity inconsistent.
                day_result = plugin.run(PluginContext(date.fromisoformat(partition), frames, versions,
                    parameters=params, checkpoint=checkpoint, replay_from=replay_start, strict_pit=False))
                frame = day_result.frame.loc[pd.to_datetime(day_result.frame[plugin.spec.partition_column]).dt.date == date.fromisoformat(partition)]
            if plugin.spec.mode == "state" and (not day_result.checkpoint or day_result.checkpoint.get("target_date") != partition):
                raise ValueError("plugin checkpoint does not match output date")
            lineage = dict(day_result.metadata, input_receipts=receipts, checkpoint=day_result.checkpoint,
                           time_policy="observed_not_historical_pit")
            publication = store.publish(plugin.schema, frame,
                partition=partition, idempotency_key=identity({"plugin":plugin.spec.code_version,
                    "inputs":versions,"parameters":params,"target":args.date,"partition":partition,
                    "checkpoint":checkpoint.get("checkpoint_id") if checkpoint else None,
                    "replay_from":args.replay_from}),
                lineage=lineage, time_model="observed", expected_previous=previous_by_date[partition])
            outputs.append(asdict(publication))
        if not outputs:
            raise ValueError("plugin returned no publishable partitions")
        return {**asdict(publication), "domain":args.domain,"replayed_publications":outputs}
    return {**asdict(publication), "domain":args.domain}


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env-file", required=True, help="explicit isolated configuration file")
    sub = p.add_subparsers(dest="command", required=True)
    setup = sub.add_parser("init")
    setup.add_argument("--domain", choices=["core","extension"], required=True)
    setup.add_argument("--install-extensions", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("--domain", choices=["core","extension"], default="core")
    r.add_argument("--dataset", required=True)
    r.add_argument("--date", required=True)
    r.add_argument("--raw-batch")
    r.add_argument("--invocation-id", help="stable caller identity for core retries; use a new identity for an intentional revision")
    r.add_argument("--input-manifest")
    r.add_argument("--input-snapshot", type=int)
    r.add_argument("--plugins", help="operator-owned explicit enabled plugin JSON")
    r.add_argument("--allow-observed", action="store_true")
    r.add_argument("--parameters", default="{}")
    r.add_argument("--checkpoint")
    r.add_argument("--replay-from")
    m = sub.add_parser("import-legacy")
    m.add_argument("--source", type=Path, required=True)
    m.add_argument("--dataset", required=True)
    m.add_argument("--start", required=True)
    m.add_argument("--end", required=True)
    m.add_argument("--apply", action="store_true", help="default is dry-run")
    rc = sub.add_parser("recover")
    rc.add_argument("--domain", choices=["core","extension"], default="core")
    rc.add_argument("--publication-id", required=True)
    for name in ("backup", "restore"):
        b = sub.add_parser(name)
        b.add_argument("--domain", choices=["core","extension"], required=True)
        b.add_argument("--path", type=Path, required=True)
        b.add_argument("--pg-bin", type=Path, help="directory containing PostgreSQL client utilities")
        if name == "backup":
            b.add_argument("--external-writers-quiesced", action="store_true")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        settings = AppSettings.load(env_file=args.env_file)
        if args.command == "init":
            if settings.runtime.read_only:
                raise PermissionError("read-only configuration")
            if args.install_extensions:
                import duckdb
                with duckdb.connect() as con:
                    con.execute("INSTALL ducklake")
                    con.execute("INSTALL postgres")
            configured_store(settings, args.domain).initialize()
            payload = {"initialized":args.domain}
        elif args.command == "run":
            payload = run(args, settings)
        elif args.command == "recover":
            recovered = configured_store(settings, args.domain, read_only=True).recover(args.publication_id)
            payload = asdict(recovered) if recovered else {"status":"not_committed"}
        elif args.command in {"backup","restore"}:
            from .backup import backup,restore
            import os
            store = configured_store(settings,args.domain)
            if settings.runtime.read_only:
                raise PermissionError("read-only configuration")
            executable = ("pg_dump" if args.command == "backup" else "pg_restore") + (".exe" if os.name == "nt" else "")
            binary = args.pg_bin/executable if args.pg_bin else executable
            if args.command == "backup":
                payload = backup(store,args.path,pg_dump=binary,external_writers_quiesced=args.external_writers_quiesced)
            else:
                restored = restore(args.path,store.location,pg_restore=binary)
                payload = {"restored":args.domain,"requires_QRP_LAKE_RELOCATED":True}
        else:
            from .migration import import_legacy
            payload = import_legacy(args.source, configured_store(settings,"core"), args.dataset,
                start=date.fromisoformat(args.start), end=date.fromisoformat(args.end), dry_run=not args.apply)
        print(json.dumps(payload, default=str, ensure_ascii=False))
        return 0
    except Exception as exc:
        # Provider, connection, SQL and filesystem exceptions may contain secrets.
        print(json.dumps({"status":"failed", "error_type":type(exc).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
