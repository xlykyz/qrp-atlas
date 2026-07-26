"""High-throughput System B initialization and daily production services."""

from __future__ import annotations

import hashlib
import json
import resource
import shutil
import time
import uuid
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    DIAGNOSTICS,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    MA5,
    PARAMETER_SET_ID,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_PARAMETERS,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_CALCULATION_VERSION,
    SYSTEM_B_STATE_OUTPUT_COLUMNS,
    TRADE_DATE,
    TREND_STATE,
    PriceAdjustment,
    SystemBStateCheckpoint,
    SystemBStateMachineRequest,
    SystemBTrendState,
)
from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states

from .repository import (
    MARKET_FACT_STATUS,
    UNRESOLVED_MISSING,
    SystemBProductionError,
    create_run,
    ensure_system_b_schema,
    execute_standard_input,
    fail_run,
    find_succeeded_run,
    import_staging,
    iter_asset_batches,
    latest_success_trade_date,
    load_checkpoints,
    open_database,
    update_run_metrics,
    validate_source_schema,
)


@dataclass(frozen=True)
class SystemBRunReport:
    production_run_id: str | None
    run_type: str
    status: str
    input_snapshot_id: str
    asset_count: int
    market_start_date: str | None
    market_end_date: str | None
    input_row_count: int
    calculated_row_count: int
    output_row_count: int
    state_counts: dict[str, int]
    suspended_hold_count: int
    error_count: int
    sql_query_count: int
    batch_count: int
    asset_batch_size: int
    read_seconds: float
    calculation_seconds: float
    staging_write_seconds: float
    import_seconds: float
    total_seconds: float
    peak_memory_mb: float
    staging_directory: str
    idempotent_existing_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _request(
    frame: pd.DataFrame,
    *,
    checkpoints: tuple[SystemBStateCheckpoint, ...] = (),
) -> SystemBStateMachineRequest:
    return SystemBStateMachineRequest(
        observations=frame,
        parameters=SYSTEM_B_2_0_PARAMETERS,
        input_price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
        rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
        parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
        initial_states=checkpoints,
    )


def _update_hash(digest: hashlib._Hash, frame: pd.DataFrame) -> None:
    digest.update(frame.to_csv(index=False, header=False, na_rep="NULL").encode("utf-8"))


def _serialize_output(frame: pd.DataFrame) -> pd.DataFrame:
    staged = frame.loc[:, SYSTEM_B_STATE_OUTPUT_COLUMNS].copy()
    staged[SOURCE_RULE_IDS] = staged[SOURCE_RULE_IDS].map(
        lambda value: json.dumps(list(value), ensure_ascii=False, separators=(",", ":"))
    )
    staged[DIAGNOSTICS] = staged[DIAGNOSTICS].map(
        lambda value: json.dumps(list(value), ensure_ascii=False, separators=(",", ":"))
    )
    return staged


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.register("system_b_staging_frame", frame)
        escaped = str(path).replace("'", "''")
        connection.execute(
            f"COPY system_b_staging_frame TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.close()


def _unresolved_market_facts(frame: pd.DataFrame) -> pd.DataFrame:
    if MARKET_FACT_STATUS not in frame.columns:
        raise SystemBProductionError(
            "MISSING_MARKET_FACT_CLASSIFICATION",
            f"standard input is missing {MARKET_FACT_STATUS}",
        )
    return frame.loc[
        frame[MARKET_FACT_STATUS] == UNRESOLVED_MISSING,
        [ASSET_ID, TRADE_DATE, MARKET_FACT_STATUS],
    ].copy()


def _validate_batch_input(frame: pd.DataFrame, seen_assets: set[str]) -> None:
    for asset_id, group in frame.groupby(ASSET_ID, sort=False):
        if asset_id not in seen_assets:
            first = group.iloc[0]
            if not bool(first[IS_TRADING_DAY]) or int(first[LISTING_TRADING_DAY_NUMBER]) != 1:
                raise SystemBProductionError(
                    "INVALID_HISTORY_START",
                    f"{asset_id} does not start on actual listing trading day 1",
                )
            seen_assets.add(str(asset_id))


def _validate_staging(
    staging_glob: str,
    *,
    expected_rows: int,
    expected_assets: int,
) -> None:
    connection = duckdb.connect()
    try:
        escaped = staging_glob.replace("'", "''")
        relation = f"read_parquet('{escaped}')"
        row = connection.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(DISTINCT asset_id) AS assets,
                count(*) - count(DISTINCT (asset_id, trade_date, rule_version_set_id, parameter_set_id)) AS duplicates,
                count(*) FILTER (WHERE trend_state NOT IN ('NEW_LISTING_WARMUP','BASE','CANDIDATE','ACTIVE')) AS invalid_states,
                count(*) FILTER (WHERE listing_trading_day_number >= 11 AND is_trading_day AND ma5 IS NULL) AS missing_ma5,
                count(DISTINCT rule_version_set_id) AS rule_versions,
                count(DISTINCT parameter_set_id) AS parameter_versions
            FROM {relation}
            """
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise SystemBProductionError("EMPTY_STAGING", "staging validation returned no result")
    rows, assets, duplicates, invalid_states, missing_ma5, rules, parameters = row
    failures = {
        "rows": (rows, expected_rows),
        "assets": (assets, expected_assets),
        "duplicates": (duplicates, 0),
        "invalid_states": (invalid_states, 0),
        "missing_ma5": (missing_ma5, 0),
        "rule_versions": (rules, 1),
        "parameter_versions": (parameters, 1),
    }
    invalid = {key: value for key, value in failures.items() if value[0] != value[1]}
    if invalid:
        raise SystemBProductionError("STAGING_VALIDATION_FAILED", json.dumps(invalid, sort_keys=True))


def _peak_memory_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _make_staging_dir(root: Path, run_id: str) -> Path:
    path = root / f"system-b-{run_id}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def initialize_history(
    *,
    source_database: Path,
    output_database: Path,
    staging_root: Path,
    start_date: date | None = None,
    end_date: date,
    asset_ids: Sequence[str] | None = None,
    asset_batch_size: int = 100,
    dry_run: bool = False,
    keep_staging: bool = False,
) -> SystemBRunReport:
    started = time.perf_counter()
    run_id = uuid.uuid4().hex
    staging_dir = _make_staging_dir(staging_root, run_id)
    same_database = source_database.resolve() == output_database.resolve()
    if dry_run:
        source = open_database(source_database, read_only=True)
        output = None
    elif same_database:
        output = open_database(output_database, read_only=False)
        source = output
    else:
        source = open_database(source_database, read_only=True)
        output = open_database(output_database, read_only=False)
    created_run = False
    completed = False
    metrics: dict[str, Any] = {}
    try:
        validate_source_schema(source)
        if output is not None:
            ensure_system_b_schema(output)
        if not dry_run:
            create_run(
                output,
                production_run_id=run_id,
                run_type="INITIALIZE",
                target_start_date=start_date,
                target_end_date=end_date,
                rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
                parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
                calculation_version=SYSTEM_B_CALCULATION_VERSION,
            )
            created_run = True

        digest = hashlib.sha256()
        input_rows = calculated_rows = output_rows = batch_count = 0
        assets: set[str] = set()
        output_assets: set[str] = set()
        seen_assets: set[str] = set()
        state_counts: Counter[str] = Counter()
        suspended_count = 0
        market_start: date | None = None
        market_end: date | None = None
        read_seconds = calculation_seconds = staging_seconds = 0.0

        query_started = time.perf_counter()
        cursor = execute_standard_input(
            source,
            end_date=end_date,
            target_date=None,
            asset_ids=asset_ids,
        )
        read_seconds += time.perf_counter() - query_started
        batch_iterator = iter_asset_batches(cursor, asset_batch_size=asset_batch_size)
        unresolved_count = 0
        batch_count = 0
        while True:
            read_started = time.perf_counter()
            try:
                frame = next(batch_iterator)
            except StopIteration:
                read_seconds += time.perf_counter() - read_started
                break
            read_seconds += time.perf_counter() - read_started
            batch_count += 1
            input_rows += len(frame)
            assets.update(frame[ASSET_ID].astype(str).unique())
            _update_hash(digest, frame)
            frame_min = pd.Timestamp(frame[TRADE_DATE].min()).date()
            frame_max = pd.Timestamp(frame[TRADE_DATE].max()).date()
            market_start = frame_min if market_start is None else min(market_start, frame_min)
            market_end = frame_max if market_end is None else max(market_end, frame_max)

            unresolved = _unresolved_market_facts(frame)
            if not unresolved.empty:
                unresolved_count += len(unresolved)
                write_started = time.perf_counter()
                _write_parquet(
                    unresolved,
                    staging_dir / f"unresolved-market-facts-{batch_count:05d}.parquet",
                )
                staging_seconds += time.perf_counter() - write_started
                continue

            if unresolved_count:
                continue

            _validate_batch_input(frame, seen_assets)

            calculation_started = time.perf_counter()
            result = calculate_system_b_2_0_states(_request(frame))
            calculated_rows += len(result.frame)
            calculation_seconds += time.perf_counter() - calculation_started

            staged = result.frame
            if start_date is not None:
                staged = staged.loc[staged[TRADE_DATE].dt.date >= start_date]
            staged = staged.loc[staged[TRADE_DATE].dt.date <= end_date]
            serialized = _serialize_output(staged)
            output_rows += len(serialized)
            output_assets.update(serialized[ASSET_ID].astype(str).unique())
            state_counts.update(serialized[TREND_STATE].astype(str))
            suspended_count += int((~serialized[IS_TRADING_DAY].astype(bool)).sum())
            write_started = time.perf_counter()
            _write_parquet(serialized, staging_dir / f"part-{batch_count:05d}.parquet")
            staging_seconds += time.perf_counter() - write_started

        if unresolved_count:
            metrics = {
                "asset_count": len(assets),
                "input_row_count": input_rows,
                "output_row_count": output_rows,
                "unresolved_market_fact_count": unresolved_count,
                "sql_query_count": 1,
                "batch_count": batch_count,
                "asset_batch_size": asset_batch_size,
                "read_seconds": read_seconds,
                "calculation_seconds": calculation_seconds,
                "staging_write_seconds": staging_seconds,
                "peak_memory_mb": _peak_memory_mb(),
            }
            raise SystemBProductionError(
                "MISSING_DAILY_MARKET_FACT",
                f"{unresolved_count} market-domain rows lack daily or suspension facts; "
                f"see {staging_dir}/unresolved-market-facts-*.parquet",
            )

        if input_rows == 0 or output_rows == 0:
            raise SystemBProductionError("EMPTY_STANDARD_INPUT", "no System B observations were generated")
        input_snapshot_id = f"sha256:{digest.hexdigest()}"
        staging_glob = str(staging_dir / "part-*.parquet")
        _validate_staging(
            staging_glob,
            expected_rows=output_rows,
            expected_assets=len(output_assets),
        )
        metrics = {
            "asset_count": len(output_assets),
            "input_asset_count": len(assets),
            "market_start_date": market_start.isoformat() if market_start else None,
            "market_end_date": market_end.isoformat() if market_end else None,
            "input_row_count": input_rows,
            "calculated_row_count": calculated_rows,
            "output_row_count": output_rows,
            "state_counts": dict(state_counts),
            "suspended_hold_count": suspended_count,
            "error_count": 0,
            "sql_query_count": 1,
            "batch_count": batch_count,
            "asset_batch_size": asset_batch_size,
            "read_seconds": read_seconds,
            "calculation_seconds": calculation_seconds,
            "staging_write_seconds": staging_seconds,
            "peak_memory_mb": _peak_memory_mb(),
        }

        existing = None
        if output is not None:
            existing = find_succeeded_run(
                output,
                run_type="INITIALIZE",
                target_start_date=start_date,
                target_end_date=end_date,
                rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
                parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
                input_snapshot_id=input_snapshot_id,
            )
        import_seconds = 0.0
        status = "DRY_RUN" if dry_run else "SUCCEEDED"
        existing_id = str(existing["production_run_id"]) if existing else None
        if not dry_run and existing is None:
            import_started = time.perf_counter()
            import_staging(
                output,
                parquet_glob=staging_glob,
                production_run_id=run_id,
                input_snapshot_id=input_snapshot_id,
                calculation_version=SYSTEM_B_CALCULATION_VERSION,
                metrics={**metrics, "import_seconds": 0.0},
            )
            import_seconds = time.perf_counter() - import_started
        elif not dry_run and existing is not None:
            output.execute(
                "DELETE FROM system_b_production_run WHERE production_run_id = ?",
                [run_id],
            )
            status = "IDEMPOTENT_NOOP"

        total_seconds = time.perf_counter() - started
        if not dry_run and existing is None:
            update_run_metrics(
                output,
                production_run_id=run_id,
                metrics={
                    **metrics,
                    "import_seconds": import_seconds,
                    "total_seconds": total_seconds,
                    "peak_memory_mb": _peak_memory_mb(),
                },
            )
        report = SystemBRunReport(
            production_run_id=None if dry_run else (existing_id or run_id),
            run_type="INITIALIZE",
            status=status,
            input_snapshot_id=input_snapshot_id,
            asset_count=len(output_assets),
            market_start_date=metrics["market_start_date"],
            market_end_date=metrics["market_end_date"],
            input_row_count=input_rows,
            calculated_row_count=calculated_rows,
            output_row_count=output_rows,
            state_counts=dict(state_counts),
            suspended_hold_count=suspended_count,
            error_count=0,
            sql_query_count=1,
            batch_count=batch_count,
            asset_batch_size=asset_batch_size,
            read_seconds=read_seconds,
            calculation_seconds=calculation_seconds,
            staging_write_seconds=staging_seconds,
            import_seconds=import_seconds,
            total_seconds=total_seconds,
            peak_memory_mb=_peak_memory_mb(),
            staging_directory=str(staging_dir),
            idempotent_existing_run_id=existing_id,
        )
        (staging_dir / "manifest.json").write_text(
            json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        completed = True
        return report
    except Exception as exc:
        if created_run:
            code = exc.code if isinstance(exc, SystemBProductionError) else "SYSTEM_B_INITIALIZATION_FAILED"
            fail_run(
                output,
                production_run_id=run_id,
                error_code=code,
                error_detail=str(exc),
                metrics=metrics,
            )
        (staging_dir / "failure.json").write_text(
            json.dumps(
                {
                    "production_run_id": run_id if created_run else None,
                    "error_code": exc.code if isinstance(exc, SystemBProductionError) else "SYSTEM_B_INITIALIZATION_FAILED",
                    "detail": str(exc),
                    "metrics": metrics,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        raise
    finally:
        source.close()
        if output is not None and output is not source:
            output.close()
        if completed and not keep_staging and staging_dir.exists():
            shutil.rmtree(staging_dir)


def run_daily(
    *,
    source_database: Path,
    output_database: Path,
    staging_root: Path,
    trade_date: date,
    dry_run: bool = False,
    keep_staging: bool = False,
) -> SystemBRunReport:
    started = time.perf_counter()
    run_id = uuid.uuid4().hex
    staging_dir = _make_staging_dir(staging_root, run_id)
    same_database = source_database.resolve() == output_database.resolve()
    if same_database:
        output = open_database(output_database, read_only=dry_run)
        source = output
    else:
        source = open_database(source_database, read_only=True)
        output = open_database(output_database, read_only=dry_run)
    created_run = False
    completed = False
    metrics: dict[str, Any] = {}
    try:
        validate_source_schema(source)
        if dry_run:
            if not all(
                output.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                    [name],
                ).fetchone()[0]
                for name in ("system_b_state_observation", "system_b_production_run")
            ):
                raise SystemBProductionError(
                    "MISSING_SYSTEM_B_SCHEMA",
                    "daily dry-run requires existing System B history schema",
                )
        else:
            ensure_system_b_schema(output)
        is_open = source.execute(
            "SELECT is_open FROM trading_calendar WHERE trade_date = ?", [trade_date]
        ).fetchone()
        if not is_open or not bool(is_open[0]):
            raise SystemBProductionError("TARGET_NOT_MARKET_TRADING_DAY", str(trade_date))
        latest_success_date = latest_success_trade_date(
            output,
            rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
        )
        if not dry_run:
            create_run(
                output,
                production_run_id=run_id,
                run_type="DAILY",
                target_start_date=trade_date,
                target_end_date=trade_date,
                rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
                parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
                calculation_version=SYSTEM_B_CALCULATION_VERSION,
            )
            created_run = True
        if latest_success_date is not None and trade_date < latest_success_date:
            raise SystemBProductionError(
                "BACKDATED_RECOMPUTE_REQUIRED",
                f"target {trade_date} is earlier than latest successful state date "
                f"{latest_success_date}; use a future recompute-from workflow",
            )

        read_started = time.perf_counter()
        frame = execute_standard_input(
            source,
            end_date=trade_date,
            target_date=trade_date,
        ).fetchdf()
        read_seconds = time.perf_counter() - read_started
        if frame.empty:
            raise SystemBProductionError("EMPTY_DAILY_UNIVERSE", str(trade_date))
        unresolved = _unresolved_market_facts(frame)
        if not unresolved.empty:
            staging_started = time.perf_counter()
            _write_parquet(
                unresolved,
                staging_dir / "unresolved-market-facts-00001.parquet",
            )
            staging_seconds = time.perf_counter() - staging_started
            metrics = {
                "asset_count": int(frame[ASSET_ID].nunique()),
                "input_row_count": len(frame),
                "output_row_count": 0,
                "unresolved_market_fact_count": len(unresolved),
                "sql_query_count": 1,
                "batch_count": 1,
                "asset_batch_size": len(frame),
                "read_seconds": read_seconds,
                "calculation_seconds": 0.0,
                "staging_write_seconds": staging_seconds,
                "peak_memory_mb": _peak_memory_mb(),
            }
            raise SystemBProductionError(
                "MISSING_DAILY_MARKET_FACT",
                f"{len(unresolved)} target-date assets lack daily or suspension facts; "
                f"see {staging_dir}/unresolved-market-facts-00001.parquet",
            )
        digest = hashlib.sha256()
        _update_hash(digest, frame)
        input_snapshot_id = f"sha256:{digest.hexdigest()}"
        asset_ids = frame[ASSET_ID].astype(str).tolist()
        existing = None
        if not dry_run:
            existing = find_succeeded_run(
                output,
                run_type="DAILY",
                target_start_date=trade_date,
                target_end_date=trade_date,
                rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
                parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
                input_snapshot_id=input_snapshot_id,
            )
        if existing is not None:
            output.execute(
                "DELETE FROM system_b_production_run WHERE production_run_id = ?",
                [run_id],
            )
            existing_metrics = json.loads(str(existing.get("metrics") or "{}"))
            total_seconds = time.perf_counter() - started
            report = SystemBRunReport(
                production_run_id=str(existing["production_run_id"]),
                run_type="DAILY",
                status="IDEMPOTENT_NOOP",
                input_snapshot_id=input_snapshot_id,
                asset_count=int(existing_metrics.get("asset_count", len(asset_ids))),
                market_start_date=trade_date.isoformat(),
                market_end_date=trade_date.isoformat(),
                input_row_count=len(frame),
                calculated_row_count=0,
                output_row_count=int(existing_metrics.get("output_row_count", len(frame))),
                state_counts=dict(existing_metrics.get("state_counts", {})),
                suspended_hold_count=int(existing_metrics.get("suspended_hold_count", 0)),
                error_count=0,
                sql_query_count=1,
                batch_count=0,
                asset_batch_size=len(asset_ids),
                read_seconds=read_seconds,
                calculation_seconds=0.0,
                staging_write_seconds=0.0,
                import_seconds=0.0,
                total_seconds=total_seconds,
                peak_memory_mb=_peak_memory_mb(),
                staging_directory=str(staging_dir),
                idempotent_existing_run_id=str(existing["production_run_id"]),
            )
            (staging_dir / "manifest.json").write_text(
                json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            completed = True
            return report
        checkpoints = load_checkpoints(
            output,
            before_date=trade_date,
            rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
            asset_ids=asset_ids,
        )
        checkpoint_by_asset = {item.asset_id: item for item in checkpoints}
        missing_mask = (
            ~frame[ASSET_ID].astype(str).isin(checkpoint_by_asset)
            & (frame[LISTING_TRADING_DAY_NUMBER].astype(int) != 1)
        )
        missing = frame.loc[missing_mask, ASSET_ID].astype(str).tolist()
        if missing:
            raise SystemBProductionError(
                "MISSING_PREVIOUS_SUCCESS_STATE",
                f"{len(missing)} continuing assets lack checkpoints; sample={missing[:10]}",
            )

        calculation_started = time.perf_counter()
        result = calculate_system_b_2_0_states(_request(frame, checkpoints=checkpoints))
        calculation_seconds = time.perf_counter() - calculation_started
        serialized = _serialize_output(result.frame)
        staging_started = time.perf_counter()
        _write_parquet(serialized, staging_dir / "part-00001.parquet")
        staging_seconds = time.perf_counter() - staging_started
        _validate_staging(
            str(staging_dir / "part-*.parquet"),
            expected_rows=len(frame),
            expected_assets=len(asset_ids),
        )
        state_counts = Counter(serialized[TREND_STATE].astype(str))
        suspended_count = int((~serialized[IS_TRADING_DAY].astype(bool)).sum())
        metrics = {
            "asset_count": len(asset_ids),
            "market_start_date": trade_date.isoformat(),
            "market_end_date": trade_date.isoformat(),
            "input_row_count": len(frame),
            "calculated_row_count": len(result.frame),
            "output_row_count": len(serialized),
            "state_counts": dict(state_counts),
            "suspended_hold_count": suspended_count,
            "error_count": 0,
            "sql_query_count": 2,
            "batch_count": 1,
            "asset_batch_size": len(asset_ids),
            "read_seconds": read_seconds,
            "calculation_seconds": calculation_seconds,
            "staging_write_seconds": staging_seconds,
            "peak_memory_mb": _peak_memory_mb(),
        }
        import_seconds = 0.0
        status = "DRY_RUN" if dry_run else "SUCCEEDED"
        existing_id = None
        if not dry_run:
            import_started = time.perf_counter()
            import_staging(
                output,
                parquet_glob=str(staging_dir / "part-*.parquet"),
                production_run_id=run_id,
                input_snapshot_id=input_snapshot_id,
                calculation_version=SYSTEM_B_CALCULATION_VERSION,
                metrics={**metrics, "import_seconds": 0.0},
            )
            import_seconds = time.perf_counter() - import_started
        total_seconds = time.perf_counter() - started
        if not dry_run:
            update_run_metrics(
                output,
                production_run_id=run_id,
                metrics={
                    **metrics,
                    "import_seconds": import_seconds,
                    "total_seconds": total_seconds,
                    "peak_memory_mb": _peak_memory_mb(),
                },
            )
        report = SystemBRunReport(
            production_run_id=None if dry_run else (existing_id or run_id),
            run_type="DAILY",
            status=status,
            input_snapshot_id=input_snapshot_id,
            asset_count=len(asset_ids),
            market_start_date=trade_date.isoformat(),
            market_end_date=trade_date.isoformat(),
            input_row_count=len(frame),
            calculated_row_count=len(result.frame),
            output_row_count=len(serialized),
            state_counts=dict(state_counts),
            suspended_hold_count=suspended_count,
            error_count=0,
            sql_query_count=2,
            batch_count=1,
            asset_batch_size=len(asset_ids),
            read_seconds=read_seconds,
            calculation_seconds=calculation_seconds,
            staging_write_seconds=staging_seconds,
            import_seconds=import_seconds,
            total_seconds=total_seconds,
            peak_memory_mb=_peak_memory_mb(),
            staging_directory=str(staging_dir),
            idempotent_existing_run_id=existing_id,
        )
        (staging_dir / "manifest.json").write_text(
            json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        completed = True
        return report
    except Exception as exc:
        if created_run:
            code = exc.code if isinstance(exc, SystemBProductionError) else "SYSTEM_B_DAILY_FAILED"
            fail_run(
                output,
                production_run_id=run_id,
                error_code=code,
                error_detail=str(exc),
                metrics=metrics,
            )
        (staging_dir / "failure.json").write_text(
            json.dumps(
                {
                    "production_run_id": run_id if created_run else None,
                    "error_code": exc.code if isinstance(exc, SystemBProductionError) else "SYSTEM_B_DAILY_FAILED",
                    "detail": str(exc),
                    "metrics": metrics,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        raise
    finally:
        source.close()
        if output is not source:
            output.close()
        if completed and not keep_staging and staging_dir.exists():
            shutil.rmtree(staging_dir)
