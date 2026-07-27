"""Production storage and query service for System B stock pools."""
from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import shutil
import uuid

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CREATED_AT,
    DAILY_BASIC,
    DAILY_MARKET_SNAPSHOT,
    EPISODE_END_DATE,
    EPISODE_ID,
    EPISODE_RETURN,
    RULE_VERSION,
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN,
    SYSTEM_B_POOL_RUN_TABLE,
    SYSTEM_B_POOL_RULE_VERSION,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
    TRADE_DATE,
    ZT_POOL,
)
from qrp_atlas.indicators.system_b import calculate_stock_pools
from qrp_atlas.indicators.system_b.pools import EXITED, HEIGHT, CAPACITY, RECOGNITION, IN_POOL


class SystemBPoolProductionError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _absolute(path: Path, label: str) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        raise SystemBPoolProductionError(f"{label}_MUST_BE_ABSOLUTE", str(path))
    return path.resolve(strict=False)


def _open_readonly(path: Path, label: str) -> tuple[Path, duckdb.DuckDBPyConnection]:
    resolved = _absolute(path, label)
    if not resolved.exists():
        raise SystemBPoolProductionError(f"{label}_NOT_FOUND", str(resolved))
    if not resolved.is_file():
        raise SystemBPoolProductionError(f"{label}_NOT_FILE", str(resolved))
    try:
        con = duckdb.connect(str(resolved), read_only=True)
    except Exception as exc:
        raise SystemBPoolProductionError(f"{label}_NOT_READABLE", str(resolved)) from exc
    return resolved, con


def open_input_database(path: Path) -> tuple[Path, duckdb.DuckDBPyConnection]:
    resolved, con = _open_readonly(path, "POOL_INPUT_DATABASE")
    try:
        tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        missing = {SYSTEM_B_STATE_OBSERVATION_TABLE, DAILY_MARKET_SNAPSHOT.name} - tables
        if missing:
            raise SystemBPoolProductionError("MISSING_POOL_INPUT_TABLE", ",".join(sorted(missing)))
        return resolved, con
    except Exception:
        con.close()
        raise


def open_episode_database(path: Path) -> tuple[Path, duckdb.DuckDBPyConnection]:
    resolved, con = _open_readonly(path, "POOL_EPISODE_DATABASE")
    try:
        tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        missing = {SYSTEM_B_EPISODE_TABLE, SYSTEM_B_EPISODE_OBSERVATION_TABLE} - tables
        if missing:
            raise SystemBPoolProductionError("MISSING_POOL_EPISODE_TABLE", ",".join(sorted(missing)))
        return resolved, con
    except Exception:
        con.close()
        raise


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(SYSTEM_B_POOL_MEMBERSHIP.duckdb_create_sql())
    con.execute(SYSTEM_B_POOL_RUN.duckdb_create_sql())


def _load_market_panel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Read the canonical facts and form one in-memory pool feature panel."""
    tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    market = DAILY_MARKET_SNAPSHOT.name
    basic = DAILY_BASIC.name
    zt = ZT_POOL.name
    basic_join = (
        f"LEFT JOIN {basic} b ON b.trade_date=m.trade_date AND b.ticker=m.ticker"
        if basic in tables else ""
    )
    basic_amount = "COALESCE(m.float_cap, b.circ_mv * 10000)" if basic in tables else "m.float_cap"
    limit_join = (
        f"LEFT JOIN {zt} z ON z.trade_date=m.trade_date AND z.ticker=m.ticker"
        if zt in tables else ""
    )
    if zt in tables:
        limit_flag = "COALESCE(m.is_limit_up, z.ticker IS NOT NULL, FALSE)"
    else:
        limit_flag = "COALESCE(m.is_limit_up, FALSE)"
    sql = f"""
        SELECT
            s.asset_id,
            s.trade_date,
            m.open,
            m.high,
            m.low,
            m.close,
            m.amount,
            {basic_amount} AS float_cap,
            {limit_flag} AS is_limit_up,
            s.trend_state,
            s.previous_trend_state,
            s.is_trading_day,
            s.market_fact_status,
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} s
        JOIN {market} m ON m.trade_date=s.trade_date AND m.ticker=s.asset_id
        {basic_join}
        {limit_join}
        ORDER BY s.asset_id, s.trade_date
    """
    data = con.execute(sql).fetchdf()
    required = ["open", "high", "low", "close", "amount", "float_cap", "is_limit_up"]
    if data.empty:
        raise SystemBPoolProductionError("EMPTY_POOL_MARKET_PANEL")
    missing = [column for column in required if data[column].isna().all()]
    if missing:
        raise SystemBPoolProductionError("MISSING_POOL_MARKET_FACT", ",".join(missing))
    return data


def _load_episode_panel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    required = {SYSTEM_B_EPISODE_TABLE, SYSTEM_B_EPISODE_OBSERVATION_TABLE}
    missing = required - tables
    if missing:
        raise SystemBPoolProductionError("MISSING_POOL_EPISODE_TABLE", ",".join(sorted(missing)))
    return con.execute(f"""
        SELECT o.trade_date, o.asset_id, o.episode_id, e.episode_end_date,
               o.episode_return
        FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o
        JOIN {SYSTEM_B_EPISODE_TABLE} e ON e.episode_id=o.episode_id
        ORDER BY o.asset_id, o.trade_date
    """).fetchdf()


def _normalise_membership(result: pd.DataFrame, run_id: str) -> pd.DataFrame:
    columns = list(SYSTEM_B_POOL_MEMBERSHIP.column_names())
    if result.empty:
        return pd.DataFrame(columns=columns)
    data = result.copy()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE]).dt.date
    data["entry_date"] = pd.to_datetime(data["entry_date"]).dt.date
    data["exit_date"] = pd.to_datetime(data["exit_date"], errors="coerce").dt.date
    data["completed_run_id"] = run_id
    data[RULE_VERSION] = SYSTEM_B_POOL_RULE_VERSION
    data[CREATED_AT] = datetime.now(timezone.utc).replace(tzinfo=None)
    for column in columns:
        if column not in data.columns:
            data[column] = None
    return data.loc[:, columns]


def _validate_membership(data: pd.DataFrame) -> None:
    key = [TRADE_DATE, ASSET_ID, "pool_type"]
    if data.duplicated(key).any():
        raise SystemBPoolProductionError("DUPLICATE_POOL_MEMBERSHIP_KEY")
    invalid = data.loc[(data["membership_state"] == IN_POOL) & data["exit_date"].notna()]
    if not invalid.empty:
        raise SystemBPoolProductionError("INVALID_OPEN_POOL_ROW")
    invalid = data.loc[data["membership_state"] == EXITED].query("exit_date != trade_date")
    if not invalid.empty:
        raise SystemBPoolProductionError("INVALID_EXIT_POOL_ROW")


def build_stock_pools(
    input_database: Path,
    output_database: Path,
    *,
    start_date: date,
    end_date: date,
    episode_database: Path | None = None,
) -> dict[str, object]:
    """Rebuild all pools in chronological order using one shared feature pass."""
    input_path, source = open_input_database(input_database)
    output_path = _absolute(output_database, "POOL_OUTPUT_DATABASE")
    if input_path == output_path:
        source.close()
        raise SystemBPoolProductionError("POOL_OUTPUT_MUST_DIFFER_FROM_INPUT")
    episode_source = source
    episode_path = input_path
    separate_episode = episode_database is not None
    if separate_episode:
        episode_path, episode_source = open_episode_database(episode_database)
    try:
        panel = _load_market_panel(source)
        episode_tables = {row[0] for row in episode_source.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        if {SYSTEM_B_EPISODE_TABLE, SYSTEM_B_EPISODE_OBSERVATION_TABLE} - episode_tables:
            raise SystemBPoolProductionError("MISSING_POOL_EPISODE_TABLE")
        episode_panel = _load_episode_panel(episode_source)
        panel = panel.merge(episode_panel, on=[TRADE_DATE, ASSET_ID], how="left", validate="one_to_one")
    finally:
        if separate_episode:
            episode_source.close()
        source.close()
    panel[TRADE_DATE] = pd.to_datetime(panel[TRADE_DATE]).dt.date
    context = panel.loc[panel[TRADE_DATE] <= end_date].copy()
    if context.empty or context[TRADE_DATE].max() < end_date:
        raise SystemBPoolProductionError("POOL_INPUT_DATE_RANGE_INSUFFICIENT")
    results = calculate_stock_pools(context)
    run_id = f"system_b_pool_{uuid.uuid4().hex}"
    membership = pd.concat(
        [_normalise_membership(result.membership, run_id) for result in results.values()],
        ignore_index=True,
    )
    membership = membership.loc[
        membership[TRADE_DATE].between(start_date, end_date)
    ].reset_index(drop=True)
    _validate_membership(membership)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    staging = output_path.with_name(f".{output_path.name}.{run_id}.staging")
    if staging.exists():
        staging.unlink()
    if output_path.exists():
        if not output_path.is_file():
            raise SystemBPoolProductionError("POOL_OUTPUT_DATABASE_NOT_FILE", str(output_path))
        shutil.copy2(output_path, staging)
    con = duckdb.connect(str(staging))
    try:
        ensure_schema(con)
        con.begin()
        con.execute(
            f"DELETE FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE} WHERE trade_date BETWEEN ? AND ?",
            [start_date, end_date],
        )
        con.execute(
            f"DELETE FROM {SYSTEM_B_POOL_RUN_TABLE} WHERE trade_date BETWEEN ? AND ?",
            [start_date, end_date],
        )
        if not membership.empty:
            con.register("pool_membership_frame", membership)
            cols = ",".join(SYSTEM_B_POOL_MEMBERSHIP.column_names())
            con.execute(f"INSERT INTO {SYSTEM_B_POOL_MEMBERSHIP_TABLE} ({cols}) SELECT {cols} FROM pool_membership_frame")
        metrics = {"pool_types": {pool: int((membership["pool_type"] == pool).sum()) for pool in (HEIGHT, CAPACITY, RECOGNITION)}}
        run_dates = sorted(set(context.loc[context[TRADE_DATE].between(start_date, end_date), TRADE_DATE]))
        run = pd.DataFrame([{
            TRADE_DATE: run_date,
            "status": "COMPLETED",
            "completed_run_id": run_id,
            "input_snapshot_id": json.dumps({"market": str(input_path), "episode": str(episode_path)}, sort_keys=True),
            "asset_count": int(context.loc[context[TRADE_DATE] == run_date, ASSET_ID].nunique()),
            "membership_row_count": int((membership[TRADE_DATE] == run_date).sum()),
            "metrics": json.dumps(metrics, sort_keys=True),
            CREATED_AT: datetime.now(timezone.utc).replace(tzinfo=None),
            "pool_completed_at": datetime.now(timezone.utc).replace(tzinfo=None),
        } for run_date in run_dates])
        con.register("pool_run_frame", run)
        con.execute(f"INSERT INTO {SYSTEM_B_POOL_RUN_TABLE} SELECT * FROM pool_run_frame")
        check = con.execute(
            f"SELECT count(*) FROM {SYSTEM_B_POOL_RUN_TABLE} WHERE status='COMPLETED' AND trade_date BETWEEN ? AND ?",
            [start_date, end_date],
        ).fetchone()[0]
        if check != len(run_dates):
            raise SystemBPoolProductionError("POOL_COMPLETION_CHECK_FAILED")
        duplicate_count = con.execute(f"""
            SELECT count(*) FROM (
                SELECT trade_date, asset_id, pool_type, count(*) AS row_count
                FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE}
                GROUP BY 1,2,3 HAVING count(*) > 1
            )
        """).fetchone()[0]
        if duplicate_count:
            raise SystemBPoolProductionError("DUPLICATE_POOL_MEMBERSHIP_KEY")
        con.commit()
        con.close()
        staging.replace(output_path)
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        con.close()
        if staging.exists():
            staging.unlink()
        raise
    return {
        "status": "COMPLETED",
        "input_database": str(input_path),
        "output_database": str(output_path),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "run_id": run_id,
        "membership_rows": int(len(membership)),
        "asset_count": int(context[ASSET_ID].nunique()),
    }


def _read_completed(con: duckdb.DuckDBPyConnection) -> date:
    row = con.execute(
        f"SELECT trade_date FROM {SYSTEM_B_POOL_RUN_TABLE} WHERE status='COMPLETED' ORDER BY trade_date DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise SystemBPoolProductionError("NO_COMPLETED_POOL_RUN")
    return row[0]


def get_pool_members(output_database: Path, trade_date: date, pool_type: str) -> pd.DataFrame:
    path = _absolute(output_database, "POOL_OUTPUT_DATABASE")
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(
            f"SELECT * FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE} WHERE trade_date=? AND pool_type=? AND membership_state='IN_POOL' ORDER BY asset_id",
            [trade_date, pool_type],
        ).fetchdf()
    finally:
        con.close()


def get_daily_pool_snapshot(output_database: Path, trade_date: date) -> dict[str, pd.DataFrame]:
    return {pool: get_pool_members(output_database, trade_date, pool) for pool in (HEIGHT, CAPACITY, RECOGNITION)}


def get_stock_pool_history(output_database: Path, asset_id: str, pool_type: str | None = None) -> pd.DataFrame:
    path = _absolute(output_database, "POOL_OUTPUT_DATABASE")
    con = duckdb.connect(str(path), read_only=True)
    try:
        sql = f"SELECT * FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE} WHERE asset_id=?"
        params: list[object] = [asset_id]
        if pool_type is not None:
            sql += " AND pool_type=?"
            params.append(pool_type)
        return con.execute(sql + " ORDER BY trade_date, pool_type", params).fetchdf()
    finally:
        con.close()


def get_stock_pool_memberships(output_database: Path, asset_id: str, trade_date: date) -> pd.DataFrame:
    path = _absolute(output_database, "POOL_OUTPUT_DATABASE")
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(
            f"SELECT * FROM {SYSTEM_B_POOL_MEMBERSHIP_TABLE} WHERE asset_id=? AND trade_date=? AND membership_state='IN_POOL' ORDER BY pool_type",
            [asset_id, trade_date],
        ).fetchdf()
    finally:
        con.close()


def get_latest_completed_pool_snapshot(output_database: Path) -> tuple[date, dict[str, pd.DataFrame]]:
    path = _absolute(output_database, "POOL_OUTPUT_DATABASE")
    con = duckdb.connect(str(path), read_only=True)
    try:
        latest = _read_completed(con)
    finally:
        con.close()
    return latest, get_daily_pool_snapshot(path, latest)
