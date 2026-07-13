"""CLI entry: python -m qrp_atlas.pipeline.pit_backfill"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from qrp_atlas.pipeline.pit_backfill.batches import DEFAULT_INDEX_CODES
from qrp_atlas.pipeline.pit_backfill.rate_limit import DEFAULT_MIN_INTERVAL
from qrp_atlas.pipeline.pit_backfill.runner import RUN_TAG, BackfillConfig, PitBackfillRunner


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QRP PIT historical backfill orchestrator")
    p.add_argument(
        "--mode",
        choices=["full", "precheck", "plan-only"],
        default="full",
        help="full backfill / smoke precheck / plan only",
    )
    p.add_argument(
        "--datasets",
        default="fundamentals,industry,index",
        help="comma list: fundamentals,industry,index",
    )
    p.add_argument("--resume", action="store_true", help="resume from manifest")
    p.add_argument("--run-tag", default=RUN_TAG)
    p.add_argument("--db-path", default=None)
    p.add_argument("--raw-dir", default=None)
    p.add_argument("--state-dir", default=None)
    p.add_argument("--log-path", default=None)
    p.add_argument("--min-interval", type=float, default=DEFAULT_MIN_INTERVAL)
    p.add_argument("--max-batches", type=int, default=None)
    p.add_argument("--l1-codes", default=None, help="comma L1 codes; skip index_classify")
    p.add_argument("--index-codes", default=",".join(DEFAULT_INDEX_CODES))
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--no-backup", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--audit", action="store_true", help="run quality audit at end")
    p.add_argument("--json-out", default=None, help="write result summary json path")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    datasets = tuple(_parse_list(args.datasets) or [])
    cfg = BackfillConfig(
        run_tag=args.run_tag,
        mode=args.mode,
        datasets=datasets,
        resume=args.resume,
        db_path=args.db_path,
        raw_dir=args.raw_dir,
        state_dir=args.state_dir,
        log_path=args.log_path,
        min_interval=args.min_interval,
        create_backup=not args.no_backup,
        skip_preflight=args.skip_preflight,
        max_batches=args.max_batches,
        dry_run=args.dry_run,
        run_audit=args.audit,
        l1_codes=_parse_list(args.l1_codes),
        index_codes=tuple(_parse_list(args.index_codes) or DEFAULT_INDEX_CODES),
    )
    result = PitBackfillRunner(cfg).run()
    summary = {
        "ok": result.get("ok"),
        "mode": result.get("mode"),
        "summary": result.get("summary"),
        "counts": result.get("counts"),
        "totals": result.get("totals"),
        "request_count": result.get("request_count"),
        "paths": result.get("paths"),
        "preflight": result.get("preflight"),
    }
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
