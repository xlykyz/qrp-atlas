"""Production boundary for Task06-A asset-relative ranking.

This service owns point-in-time input resolution and one atomic quant.db write.
The ranking formulas themselves live in
``qrp_atlas.indicators.system_b.asset_ranking`` and are therefore directly
testable without a database or a scheduler.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    ASSET_RANK_CALCULATION_VERSION,
    CALCULATION_VERSION,
    CREATED_AT,
    EVIDENCE,
    INPUT_PROVENANCE,
    POPULARITY_AVAILABLE,
    POPULARITY_SOURCE_AVAILABILITY,
    POPULARITY_UNAVAILABLE,
    PRODUCTION_RUN_ID,
    SOURCE_PROVENANCE,
    SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT,
    SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE,
    SYSTEM_B_ASSET_RANK_SNAPSHOT,
    SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN_TABLE,
    TICKER,
    TRADE_DATE,
)
from qrp_atlas.indicators.system_b.asset_ranking import (
    AssetRankingError,
    AssetRankingResult,
    calculate_asset_ranking,
)
from qrp_atlas.pipeline.system_b.market_series import (
    CanonicalMarketSeriesError,
    load_canonical_market_series,
)


DC_HOT = "dc_hot"
THS_HOT = "ths_hot"
_HOT_TABLES = {DC_HOT: DC_HOT, THS_HOT: THS_HOT}
_A_SHARE_EXCHANGES = {"SH", "SZ", "BJ", "SSE", "SZSE", "BSE", "SHANGHAI", "SHENZHEN", "BEIJING"}
_NON_A_MARKERS = (
    "HK",
    "HONG KONG",
    "港",
    "美股",
    "US",
    "NASDAQ",
    "NYSE",
    "OTC",
    "基金",
    "BOND",
    "债",
    "ETF",
    "LOF",
)


class SystemBAssetRankProductionError(RuntimeError):
    """Stable production-boundary error with a machine-readable code."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _tables(connection: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }


def _columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
            [table_name],
        ).fetchall()
    }


def _require_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    required: set[str],
    code: str,
) -> None:
    tables = _tables(connection)
    if table_name not in tables:
        raise SystemBAssetRankProductionError(code, table_name)
    missing = sorted(required - _columns(connection, table_name))
    if missing:
        raise SystemBAssetRankProductionError(code, f"{table_name}: {','.join(missing)}")


def ensure_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create Task06-A output and popularity availability tables if absent."""

    connection.execute(SYSTEM_B_ASSET_RANK_SNAPSHOT.duckdb_create_sql())
    connection.execute(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.duckdb_create_sql())
    connection.execute(POPULARITY_SOURCE_AVAILABILITY.duckdb_create_sql())

    for schema, code in (
        (SYSTEM_B_ASSET_RANK_SNAPSHOT, "ASSET_RANK_SCHEMA_RECREATION_REQUIRED"),
        (SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT, "ASSET_RANK_SCHEMA_RECREATION_REQUIRED"),
        (POPULARITY_SOURCE_AVAILABILITY, "POPULARITY_AVAILABILITY_SCHEMA_RECREATION_REQUIRED"),
    ):
        actual = _columns(connection, schema.name)
        expected = set(schema.column_names())
        if not expected <= actual:
            raise SystemBAssetRankProductionError(code, f"{schema.name}: missing {sorted(expected - actual)}")


def _normalise_date(value: Any) -> date:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise SystemBAssetRankProductionError("ASSET_RANK_TARGET_DATE_INVALID", str(value))
    return parsed.date()


def _looks_like_a_share(ticker: str, exchange: Any, market: Any) -> bool:
    values = " ".join(
        str(value).strip().upper()
        for value in (exchange, market)
        if value is not None and not pd.isna(value)
    )
    if any(marker in values for marker in _NON_A_MARKERS):
        return False
    if any(value in _A_SHARE_EXCHANGES for value in values.replace("/", " ").split()):
        return True
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ|SS|SZSE|BSE)", ticker.upper()):
        return True
    if re.fullmatch(r"\d{6}", ticker):
        return True
    # stock_info is the project's canonical stock table.  If a fixture or an
    # older load has no exchange/market value, retaining the row is safer than
    # silently replacing the formal A-share domain with the market table.
    return not values


def build_canonical_a_share_universe(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date | str,
) -> pd.DataFrame:
    """Resolve the target-date canonical A-share domain from stock_info.

    The domain is based on listing and delisting dates, never on whether a
    daily market row happens to exist.  Thus a suspended or missing-market-row
    stock still receives a materialized null Task06 result.
    """

    target = _normalise_date(trade_date)
    _require_columns(
        connection,
        "stock_info",
        {TICKER, "list_date", "delist_date"},
        "ASSET_RANK_UNIVERSE_SCHEMA_MISSING",
    )
    columns = _columns(connection, "stock_info")
    select = [TICKER, "list_date", "delist_date"]
    for optional in ("exchange", "market", "list_status"):
        if optional in columns:
            select.append(optional)
    frame = connection.execute(f"SELECT {', '.join(select)} FROM stock_info").fetchdf()
    if frame.empty:
        raise SystemBAssetRankProductionError("EMPTY_CANONICAL_A_SHARE_UNIVERSE", target.isoformat())
    frame[TICKER] = frame[TICKER].astype(str).str.strip()
    if frame[TICKER].eq("").any() or frame[TICKER].duplicated().any():
        raise SystemBAssetRankProductionError("ASSET_RANK_UNIVERSE_IDENTITY_INVALID")
    parsed_list_dates = pd.to_datetime(frame["list_date"], errors="coerce")
    parsed_delist_dates = pd.to_datetime(frame["delist_date"], errors="coerce")
    if parsed_list_dates.isna().any():
        raise SystemBAssetRankProductionError("ASSET_RANK_UNIVERSE_LIST_DATE_INVALID")
    if frame["delist_date"].notna().to_numpy().any() and (
        frame["delist_date"].notna() & parsed_delist_dates.isna()
    ).any():
        raise SystemBAssetRankProductionError("ASSET_RANK_UNIVERSE_DELIST_DATE_INVALID")
    frame["list_date"] = [value.date() for value in parsed_list_dates]
    frame["delist_date"] = [None if pd.isna(value) else value.date() for value in parsed_delist_dates]
    selected = frame.loc[
        frame["list_date"].le(target)
        & (frame["delist_date"].isna() | frame["delist_date"].ge(target))
    ].copy()
    exchange = selected["exchange"] if "exchange" in selected else pd.Series(None, index=selected.index)
    market = selected["market"] if "market" in selected else pd.Series(None, index=selected.index)
    selected = selected.loc[
        [
            _looks_like_a_share(ticker, exchange.loc[index], market.loc[index])
            for index, ticker in selected[TICKER].items()
        ]
    ]
    if selected.empty:
        raise SystemBAssetRankProductionError("EMPTY_CANONICAL_A_SHARE_UNIVERSE", target.isoformat())
    return pd.DataFrame({TRADE_DATE: target, TICKER: sorted(selected[TICKER].tolist())})


# The longer name is useful for callers that use the design-book terminology.
resolve_canonical_a_share_universe = build_canonical_a_share_universe


def _read_pool_inputs(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> tuple[pd.DataFrame, dict[str, str]]:
    _require_columns(
        connection,
        SYSTEM_B_POOL_RUN_TABLE,
        {TRADE_DATE, "pool_type", "status", "completed_run_id"},
        "ASSET_RANK_POOL_RUN_SCHEMA_MISSING",
    )
    _require_columns(
        connection,
        SYSTEM_B_POOL_MEMBERSHIP_TABLE,
        {TRADE_DATE, ASSET_ID, "pool_type", "membership_state"},
        "ASSET_RANK_POOL_MEMBERSHIP_SCHEMA_MISSING",
    )
    run_rows = connection.execute(
        f"""SELECT trade_date, pool_type, status, completed_run_id
            FROM {SYSTEM_B_POOL_RUN_TABLE}
            WHERE trade_date=? AND pool_type IN ('CAPACITY','HEIGHT','RECOGNITION')""",
        [trade_date],
    ).fetchdf()
    if run_rows.empty:
        raise SystemBAssetRankProductionError("ASSET_RANK_POOL_COMPLETION_MISSING", trade_date.isoformat())
    run_rows["pool_type"] = run_rows["pool_type"].astype(str).str.upper()
    if run_rows.duplicated([TRADE_DATE, "pool_type"]).any():
        raise SystemBAssetRankProductionError("ASSET_RANK_POOL_COMPLETION_DUPLICATE")
    expected = {"CAPACITY", "HEIGHT", "RECOGNITION"}
    actual = set(run_rows["pool_type"])
    if actual != expected or not run_rows["status"].astype(str).str.upper().eq("COMPLETED").all():
        raise SystemBAssetRankProductionError("ASSET_RANK_POOL_COMPLETION_MISSING", repr(run_rows.to_dict(orient="records")))
    run_ids = dict(zip(run_rows["pool_type"], run_rows["completed_run_id"].astype(str), strict=True))
    columns = _columns(connection, SYSTEM_B_POOL_MEMBERSHIP_TABLE)
    select = [
        TRADE_DATE,
        ASSET_ID,
        "pool_type",
        "membership_state",
        "metrics_json" if "metrics_json" in columns else "NULL AS metrics_json",
    ]
    for optional in ("entry_date", "exit_date", "entry_reason", "exit_reason", "episode_id"):
        if optional in columns:
            select.append(optional)
    frame = connection.execute(
        f"SELECT {', '.join(select)} FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE} WHERE trade_date=?",
        [trade_date],
    ).fetchdf()
    if not frame.empty and frame.duplicated([TRADE_DATE, ASSET_ID, "pool_type"]).any():
        raise SystemBAssetRankProductionError("ASSET_RANK_POOL_MEMBERSHIP_DUPLICATE")
    return frame, run_ids


def _read_episode_inputs(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> pd.DataFrame:
    _require_columns(
        connection,
        SYSTEM_B_EPISODE_OBSERVATION_TABLE,
        {TRADE_DATE, ASSET_ID, "episode_return"},
        "ASSET_RANK_EPISODE_SCHEMA_MISSING",
    )
    columns = _columns(connection, SYSTEM_B_EPISODE_OBSERVATION_TABLE)
    episode_id = "episode_id" if "episode_id" in columns else "NULL AS episode_id"
    frame = connection.execute(
        f"SELECT trade_date, asset_id, {episode_id}, episode_return FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE trade_date <= ?",
        [trade_date],
    ).fetchdf()
    if not frame.empty and frame.duplicated([TRADE_DATE, ASSET_ID]).any():
        raise SystemBAssetRankProductionError("ASSET_RANK_EPISODE_DUPLICATE_KEY")
    return frame


def _availability_rows(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> pd.DataFrame:
    _require_columns(
        connection,
        POPULARITY_SOURCE_AVAILABILITY.name,
        {TRADE_DATE, "source", "source_status", "valid_snapshot_count", "snapshot_seqs", "input_version", "source_provenance"},
        "POPULARITY_AVAILABILITY_SCHEMA_MISSING",
    )
    frame = connection.execute(
        f"SELECT * FROM {POPULARITY_SOURCE_AVAILABILITY.name} WHERE trade_date=?",
        [trade_date],
    ).fetchdf()
    if frame.empty:
        raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_MISSING", trade_date.isoformat())
    frame["source_key"] = frame["source"].map(lambda value: str(value).strip().lower())
    canonical = frame["source_key"].replace({"eastmoney": DC_HOT, "dc": DC_HOT, "ths": THS_HOT})
    frame["source_key"] = canonical
    if frame.duplicated("source_key").any() or set(frame["source_key"]) != {DC_HOT, THS_HOT}:
        raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_MISSING", repr(frame.to_dict(orient="records")))
    for row in frame.itertuples(index=False):
        status = str(row.source_status).upper()
        if status not in {POPULARITY_AVAILABLE, POPULARITY_UNAVAILABLE}:
            raise SystemBAssetRankProductionError("POPULARITY_SOURCE_STATUS_INVALID", f"{row.source}:{status}")
        try:
            count = int(row.valid_snapshot_count)
        except (TypeError, ValueError) as exc:
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source)) from exc
        if count < 0 or not str(row.input_version).strip() or row.snapshot_seqs is None or row.source_provenance is None:
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source))
        try:
            snapshot_seqs = json.loads(str(row.snapshot_seqs))
            source_provenance = json.loads(str(row.source_provenance))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source)) from exc
        if not isinstance(snapshot_seqs, list) or not all(isinstance(value, int) and value > 0 for value in snapshot_seqs):
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source))
        if snapshot_seqs != list(range(1, len(snapshot_seqs) + 1)) or len(snapshot_seqs) != count:
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source))
        if not isinstance(source_provenance, Mapping):
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source))
        if status == POPULARITY_UNAVAILABLE and count != 0:
            raise SystemBAssetRankProductionError("POPULARITY_AVAILABILITY_INVALID", str(row.source))
    return frame


def _read_popularity_inputs(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, dict[str, Mapping[str, Any]]]:
    availability = _availability_rows(connection, trade_date)
    frames: dict[str, pd.DataFrame] = {}
    metadata: dict[str, Mapping[str, Any]] = {}
    tables = _tables(connection)
    for row in availability.to_dict(orient="records"):
        source = row["source_key"]
        status = str(row["source_status"]).upper()
        metadata[source] = {
            "source_status": status,
            "valid_snapshot_count": int(row["valid_snapshot_count"]),
            "snapshot_seqs": json.loads(str(row["snapshot_seqs"])),
            "input_version": str(row["input_version"]),
            "source_provenance": json.loads(str(row["source_provenance"])),
        }
        table = _HOT_TABLES[source]
        if status == POPULARITY_UNAVAILABLE:
            frames[source] = pd.DataFrame()
            continue
        if table not in tables:
            raise SystemBAssetRankProductionError("POPULARITY_SOURCE_TABLE_MISSING", table)
        frames[source] = connection.execute(
            f"SELECT * FROM {table} WHERE trade_date=?",
            [trade_date],
        ).fetchdf()
    return frames, availability, metadata


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(v) for v in value]
    if isinstance(value, (date, pd.Timestamp)):
        return value.isoformat()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return number if math.isfinite(number) else None


def _json(value: Any) -> str:
    return json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest_frame(frame: pd.DataFrame | None, label: str) -> str:
    if frame is None:
        return f"{label}:NULL"
    if frame.empty:
        return f"{label}:EMPTY"
    data = frame.copy()
    data = data.reindex(sorted(data.columns), axis=1)
    for column in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[column]):
            data[column] = data[column].astype(str)
        elif data[column].dtype == object:
            data[column] = data[column].map(_json_value)
    records = [_json_value(row) for row in data.to_dict(orient="records")]
    records.sort(key=lambda row: _json(row))
    return f"{label}:{hashlib.sha256(_json(records).encode('utf-8')).hexdigest()}"


def _input_provenance(
    *,
    target: date,
    universe: pd.DataFrame,
    market: pd.DataFrame,
    episode: pd.DataFrame,
    memberships: pd.DataFrame,
    pool_run_ids: Mapping[str, str],
    availability: pd.DataFrame,
    popularity: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    sources: dict[str, Any] = {}
    for row in availability.to_dict(orient="records"):
        source = str(row["source_key"])
        sources[source] = {
            "source_status": str(row["source_status"]).upper(),
            "valid_snapshot_count": int(row["valid_snapshot_count"]),
            "snapshot_seqs": json.loads(str(row["snapshot_seqs"])),
            "input_version": str(row["input_version"]),
            "source_provenance": json.loads(str(row["source_provenance"])),
            "consumed_rows": len(popularity.get(source, pd.DataFrame())),
        }
    pieces = [
        f"target:{target.isoformat()}",
        _digest_frame(universe, "universe"),
        _digest_frame(market, "market"),
        _digest_frame(episode, "episode"),
        _digest_frame(memberships, "memberships"),
        _digest_frame(availability, "availability"),
    ] + [_digest_frame(popularity.get(source), source) for source in (DC_HOT, THS_HOT)]
    return {
        "target_date": target.isoformat(),
        "universe": {"source": "stock_info.list_date_delist_date", "asset_count": len(universe)},
        "pool_run_ids": dict(sorted(pool_run_ids.items())),
        "episode": {"table": SYSTEM_B_EPISODE_OBSERVATION_TABLE, "cutoff": target.isoformat()},
        "market_series": {
            "table": "daily_market_snapshot",
            "cutoff": target.isoformat(),
            "price_adjustment": "FORWARD_ADJUSTED",
            "actual_trading_only": True,
        },
        "popularity": sources,
        "input_snapshot_id": "SNAP:" + hashlib.sha256("|".join(pieces).encode("utf-8")).hexdigest()[:32].upper(),
    }


def _prepare_persisted_frames(
    result: AssetRankingResult,
    *,
    target: date,
    run_id: str,
    provenance: Mapping[str, Any],
    created_at: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshot = result.snapshot.copy()
    snapshot[TRADE_DATE] = target
    snapshot[INPUT_PROVENANCE] = _json(provenance)
    snapshot["diagnostics"] = _json(list(result.diagnostics))
    if EVIDENCE not in snapshot:
        snapshot[EVIDENCE] = _json({})
    snapshot[PRODUCTION_RUN_ID] = run_id
    snapshot[CALCULATION_VERSION] = ASSET_RANK_CALCULATION_VERSION
    snapshot[CREATED_AT] = created_at
    # Rebuild the structured evidence after production-boundary fields are
    # attached; the pure layer's placeholder provenance must not survive into
    # the persisted audit payload.
    snapshot[EVIDENCE] = snapshot.apply(
        lambda row: _json(
            {
                key: value
                for key, value in row.items()
                if key not in {EVIDENCE, PRODUCTION_RUN_ID, CREATED_AT}
            }
        ),
        axis=1,
    )
    snapshot = snapshot.loc[:, list(SYSTEM_B_ASSET_RANK_SNAPSHOT.column_names())]

    audit = result.component_audit.copy()
    audit[TRADE_DATE] = target
    audit[CALCULATION_VERSION] = ASSET_RANK_CALCULATION_VERSION
    audit[PRODUCTION_RUN_ID] = run_id
    audit[CREATED_AT] = created_at
    enriched_sources: list[str] = []
    for raw_source in audit[SOURCE_PROVENANCE].tolist():
        try:
            source = json.loads(raw_source) if isinstance(raw_source, str) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {"raw_source_provenance": raw_source}
        source["input_snapshot_id"] = provenance.get("input_snapshot_id")
        enriched_sources.append(_json(source))
    audit[SOURCE_PROVENANCE] = enriched_sources
    audit = audit.loc[:, list(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.column_names())]
    if snapshot[TICKER].duplicated().any() or audit.duplicated([TRADE_DATE, TICKER, "dimension", "component"]).any():
        raise SystemBAssetRankProductionError("ASSET_RANK_OUTPUT_DUPLICATE_KEY")
    return snapshot, audit


def _persist(
    connection: duckdb.DuckDBPyConnection,
    snapshot: pd.DataFrame,
    audit: pd.DataFrame,
    target: date,
) -> None:
    snapshot_columns = ", ".join(SYSTEM_B_ASSET_RANK_SNAPSHOT.column_names())
    audit_columns = ", ".join(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.column_names())
    registered_snapshot = False
    registered_audit = False
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            f"DELETE FROM {SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE} WHERE trade_date=?",
            [target],
        )
        connection.execute(
            f"DELETE FROM {SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?",
            [target],
        )
        connection.register("_asset_rank_snapshot_rows", snapshot)
        registered_snapshot = True
        connection.execute(
            f"INSERT INTO {SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE} ({snapshot_columns}) SELECT {snapshot_columns} FROM _asset_rank_snapshot_rows"
        )
        connection.register("_asset_rank_audit_rows", audit)
        registered_audit = True
        connection.execute(
            f"INSERT INTO {SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE} ({audit_columns}) SELECT {audit_columns} FROM _asset_rank_audit_rows"
        )
        connection.execute(
            f"""SELECT CASE WHEN count(*)=? THEN 1 ELSE 0 END
                FROM {SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE} WHERE trade_date=?""",
            [len(snapshot), target],
        )
        check = connection.fetchone()
        if not check or int(check[0]) != 1:
            raise SystemBAssetRankProductionError("ASSET_RANK_COMPLETION_CHECK_FAILED")
        audit_count = int(
            connection.execute(
                f"SELECT count(*) FROM {SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0]
        )
        if audit_count != len(audit):
            raise SystemBAssetRankProductionError("ASSET_RANK_AUDIT_COMPLETION_CHECK_FAILED")
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if registered_snapshot:
            try:
                connection.unregister("_asset_rank_snapshot_rows")
            except Exception:
                pass
        if registered_audit:
            try:
                connection.unregister("_asset_rank_audit_rows")
            except Exception:
                pass


def run_asset_rank_daily(
    *,
    quant_database: Path,
    pool_database: Path,
    episode_database: Path | None,
    trade_date: date | str,
    production_run_id: str | None = None,
    execution_control: Any | None = None,
) -> dict[str, Any]:
    """Calculate and atomically publish one target-day Asset Rank snapshot."""

    target = _normalise_date(trade_date)
    if not Path(quant_database).is_absolute() or not Path(pool_database).is_absolute():
        raise SystemBAssetRankProductionError("ASSET_RANK_DATABASE_PATH_MUST_BE_ABSOLUTE")
    if episode_database is not None and not Path(episode_database).is_absolute():
        raise SystemBAssetRankProductionError("ASSET_RANK_DATABASE_PATH_MUST_BE_ABSOLUTE")
    run_id = production_run_id or f"system_b_asset_rank_{uuid4().hex}"
    control = execution_control
    quant = duckdb.connect(str(quant_database))
    pool = duckdb.connect(str(pool_database), read_only=True)
    episode: duckdb.DuckDBPyConnection | None = None
    try:
        if control is not None:
            control.check()
        ensure_schema(quant)
        calendar_tables = _tables(quant)
        if "trading_calendar" in calendar_tables:
            row = quant.execute("SELECT is_open FROM trading_calendar WHERE trade_date=?", [target]).fetchone()
            if not row:
                raise SystemBAssetRankProductionError("ASSET_RANK_CALENDAR_DATE_MISSING", target.isoformat())
            if not bool(row[0]):
                return {"status": "NOOP", "reason": "non_trading_day", "trade_date": target.isoformat()}
        universe = build_canonical_a_share_universe(quant, target)
        memberships, pool_run_ids = _read_pool_inputs(pool, target)
        episode_source = episode_database if episode_database is not None else quant_database
        if Path(episode_source).resolve() == Path(quant_database).resolve():
            episode = quant
        else:
            episode = duckdb.connect(str(episode_source), read_only=True)
        episode_frame = _read_episode_inputs(episode, target)
        market_frame = load_canonical_market_series(quant, target)
        popularity_frames, availability, _metadata = _read_popularity_inputs(quant, target)
        if control is not None:
            control.check()
        provenance = _input_provenance(
            target=target,
            universe=universe,
            market=market_frame,
            episode=episode_frame,
            memberships=memberships,
            pool_run_ids=pool_run_ids,
            availability=availability,
            popularity=popularity_frames,
        )
        result = calculate_asset_ranking(
            universe,
            trade_date=target,
            market_series=market_frame,
            episode_observations=episode_frame,
            memberships=memberships,
            popularity=popularity_frames,
            popularity_availability={
                row["source_key"]: {
                    "source_status": row["source_status"],
                    "valid_snapshot_count": row["valid_snapshot_count"],
                    "snapshot_seqs": row["snapshot_seqs"],
                    "input_version": row["input_version"],
                    "source_provenance": row["source_provenance"],
                }
                for row in availability.to_dict(orient="records")
            },
            input_provenance=provenance,
        )
        if control is not None:
            control.check()
        created_at = datetime.now(UTC).replace(tzinfo=None)
        snapshot, audit = _prepare_persisted_frames(
            result,
            target=target,
            run_id=run_id,
            provenance=provenance,
            created_at=created_at,
        )
        _persist(quant, snapshot, audit, target)
        return {
            "status": "COMPLETED",
            "trade_date": target.isoformat(),
            "production_run_id": run_id,
            "rows_written": len(snapshot) + len(audit),
            "snapshot_rows": len(snapshot),
            "component_audit_rows": len(audit),
            "asset_count": len(universe),
            "diagnostics": list(result.diagnostics),
            "input_provenance": provenance,
        }
    except (SystemBAssetRankProductionError, AssetRankingError, CanonicalMarketSeriesError) as exc:
        if isinstance(exc, SystemBAssetRankProductionError):
            raise
        raise SystemBAssetRankProductionError(getattr(exc, "code", "ASSET_RANK_CALCULATION_FAILED"), str(exc)) from exc
    finally:
        if episode is not None and episode is not quant:
            episode.close()
        pool.close()
        quant.close()


def get_asset_rank_snapshot(database: Path, trade_date: date | str) -> pd.DataFrame:
    target = _normalise_date(trade_date)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        return connection.execute(
            f"SELECT * FROM {SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE} WHERE trade_date=? ORDER BY ticker",
            [target],
        ).fetchdf()
    finally:
        connection.close()


def get_asset_rank_component_audit(database: Path, trade_date: date | str) -> pd.DataFrame:
    target = _normalise_date(trade_date)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        return connection.execute(
            f"SELECT * FROM {SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=? ORDER BY ticker, dimension, component",
            [target],
        ).fetchdf()
    finally:
        connection.close()


__all__ = [
    "SystemBAssetRankProductionError",
    "build_canonical_a_share_universe",
    "resolve_canonical_a_share_universe",
    "ensure_schema",
    "run_asset_rank_daily",
    "get_asset_rank_snapshot",
    "get_asset_rank_component_audit",
]
