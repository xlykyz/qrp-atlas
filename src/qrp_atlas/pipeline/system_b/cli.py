"""Command-line entry point for System B state production."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from qrp_atlas.config.settings import (
    ConfigError,
    get_settings,
    require_writable,
    reset_settings_cache,
)

from .repository import (
    MARKET_FACT_STATUS,
    UNRESOLVED_MISSING,
    SystemBProductionError,
    ensure_system_b_schema,
    execute_standard_input,
    open_database,
    validate_source_schema,
)
from .service import initialize_history, run_daily


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _assets(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    result: list[str] = []
    for value in values:
        result.extend(item.strip() for item in value.split(",") if item.strip())
    return tuple(sorted(set(result))) or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qrp-atlas-system-b")
    parser.add_argument("--env-file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate = subparsers.add_parser("migrate", help="create System B tables and latest view")
    migrate.add_argument("--output-database", type=Path)

    initialize = subparsers.add_parser("initialize", help="initialize historical System B states")
    initialize.add_argument("--start-date", type=_date)
    initialize.add_argument("--end-date", type=_date)
    initialize.add_argument("--asset", action="append", dest="assets")
    initialize.add_argument("--asset-batch-size", type=int, default=100)
    initialize.add_argument("--source-database", type=Path)
    initialize.add_argument("--output-database", type=Path)
    initialize.add_argument("--staging-dir", type=Path)
    initialize.add_argument("--dry-run", action="store_true")
    initialize.add_argument("--keep-staging", action="store_true")

    daily = subparsers.add_parser("run-daily", help="calculate one market-wide daily snapshot")
    daily.add_argument("--trade-date", type=_date, required=True)
    daily.add_argument("--source-database", type=Path)
    daily.add_argument("--output-database", type=Path)
    daily.add_argument("--staging-dir", type=Path)
    daily.add_argument("--dry-run", action="store_true")
    daily.add_argument("--keep-staging", action="store_true")

    readiness = subparsers.add_parser("readiness", help="validate daily source readiness")
    readiness.add_argument("--trade-date", type=_date, required=True)
    readiness.add_argument("--source-database", type=Path)
    return parser


def _latest_open_date(path: Path) -> date:
    connection = open_database(path, read_only=True)
    try:
        row = connection.execute(
            "SELECT max(trade_date) FROM trading_calendar WHERE is_open = TRUE"
        ).fetchone()
    finally:
        connection.close()
    if not row or row[0] is None:
        raise SystemBProductionError("EMPTY_TRADING_CALENDAR", "no open market dates exist")
    return row[0]


def _readiness(path: Path, trade_date: date) -> dict[str, object]:
    connection = open_database(path, read_only=True)
    try:
        validate_source_schema(connection)
        open_row = connection.execute(
            "SELECT is_open FROM trading_calendar WHERE trade_date = ?", [trade_date]
        ).fetchone()
        if not open_row or not bool(open_row[0]):
            raise SystemBProductionError("TARGET_NOT_MARKET_TRADING_DAY", str(trade_date))
        frame = execute_standard_input(
            connection,
            end_date=trade_date,
            target_date=trade_date,
        ).fetchdf()
        if frame.empty:
            raise SystemBProductionError("EMPTY_DAILY_UNIVERSE", str(trade_date))
        target = frame.loc[frame["trade_date"].dt.date == trade_date].copy()
        unresolved = target.loc[target[MARKET_FACT_STATUS] == UNRESOLVED_MISSING]
        missing_close = int((target["is_trading_day"] & target["close"].isna()).sum())
        missing_ma5 = int(
            (
                target["is_trading_day"]
                & (target["listing_trading_day_number"] >= 11)
                & target["ma5"].isna()
            ).sum()
        )
        if missing_close:
            raise SystemBProductionError("MISSING_FORWARD_ADJUSTED_CLOSE", str(missing_close))
        if missing_ma5:
            raise SystemBProductionError("MISSING_MA5_AFTER_WARMUP", str(missing_ma5))
        return {
            "status": "READY",
            "trade_date": trade_date.isoformat(),
            "asset_count": int(target["asset_id"].nunique()),
            "row_count": len(target),
            "trading_asset_count": int(target["is_trading_day"].sum()),
            "suspended_asset_count": int((target[MARKET_FACT_STATUS] == "EXPLICIT_NON_TRADING").sum()),
            "unresolved_asset_count": len(unresolved),
            "price_adjustment": "FORWARD_ADJUSTED",
        }
    finally:
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file:
        import os

        os.environ["QRP_ENV_FILE"] = args.env_file
    reset_settings_cache()
    try:
        settings = get_settings()
        if args.command == "migrate":
            path = args.output_database or settings.paths.duckdb_path
            if path.resolve(strict=False) == settings.paths.duckdb_path.resolve(strict=False):
                require_writable(settings, operation="migrating System B schema")
            connection = open_database(path, read_only=False)
            try:
                ensure_system_b_schema(connection)
            finally:
                connection.close()
            payload = {"status": "OK", "database": str(path)}
        elif args.command == "readiness":
            payload = _readiness(
                args.source_database or settings.paths.duckdb_path,
                args.trade_date,
            )
        elif args.command == "initialize":
            source = args.source_database or settings.paths.duckdb_path
            output = args.output_database or settings.paths.duckdb_path
            if (
                not args.dry_run
                and output.resolve(strict=False) == settings.paths.duckdb_path.resolve(strict=False)
            ):
                require_writable(settings, operation="initializing System B state history")
            end_date = args.end_date or _latest_open_date(source)
            report = initialize_history(
                source_database=source,
                output_database=output,
                staging_root=args.staging_dir or settings.paths.tmp_dir,
                start_date=args.start_date,
                end_date=end_date,
                asset_ids=_assets(args.assets),
                asset_batch_size=args.asset_batch_size,
                dry_run=args.dry_run,
                keep_staging=args.keep_staging,
            )
            payload = report.to_dict()
        else:
            output = args.output_database or settings.paths.duckdb_path
            if (
                not args.dry_run
                and output.resolve(strict=False) == settings.paths.duckdb_path.resolve(strict=False)
            ):
                require_writable(settings, operation="writing daily System B states")
            report = run_daily(
                source_database=args.source_database or settings.paths.duckdb_path,
                output_database=output,
                staging_root=args.staging_dir or settings.paths.tmp_dir,
                trade_date=args.trade_date,
                dry_run=args.dry_run,
                keep_staging=args.keep_staging,
            )
            payload = report.to_dict()
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return 0
    except (ConfigError, SystemBProductionError, RuntimeError, ValueError) as exc:
        code = exc.code if isinstance(exc, SystemBProductionError) else "SYSTEM_B_COMMAND_FAILED"
        print(json.dumps({"status": "FAILED", "error_code": code, "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_code": "SYSTEM_B_COMMAND_FAILED",
                    "detail": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
