"""M5 Theme popularity fact calculation and atomic daily materialization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import time
import uuid
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CREATED_AT,
    DC_HOT,
    INPUT_SNAPSHOT_ID,
    PRODUCTION_RUN_ID,
    SOURCE,
    THS_HOT,
    THEME_M5_OBSERVATION,
    THEME_M5_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_VERSION,
    THEME_ID,
    COLLECTION_ID,
    TRADE_DATE,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError
from qrp_atlas.stock_collections.resolver import StockCollectionResolver

from qrp_atlas.indicators.m5.observations import M5ObservationError, calculate_m5_raw_observations


DC_HOT_SOURCE = "EASTMONEY"
DC_HOT_LIST = "POPULARITY"
THS_HOT_SOURCE = "THS"
THS_HOT_LIST = "HOT_STOCK"


class ThemeM5PipelineError(ValueError):
    """Stable, fail-closed error for M5 input, calculation, or write failures."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ThemeM5CalculatedFacts:
    """Read-only M5 facts and their complete input lineage."""

    trade_date: date
    theme_count: int
    total_member_rows: int
    total_popularity_rows: int
    input_snapshot_id: str
    observations: pd.DataFrame
    memberships: pd.DataFrame
    dc_hot: pd.DataFrame
    ths_hot: pd.DataFrame
    execution_seconds: float

    @property
    def m5_observations(self) -> pd.DataFrame:
        """Compatibility alias for callers that name the output explicitly."""
        return self.observations


@dataclass(frozen=True)
class ThemeM5ProductionReport:
    production_run_id: str
    input_snapshot_id: str
    theme_count: int
    trade_date_count: int
    start_date: date
    end_date: date
    total_observation_rows: int
    total_member_rows: int
    total_popularity_rows: int
    execution_seconds: float

    @property
    def total_membership_rows(self) -> int:
        return self.total_member_rows


def _table_columns(table: Any) -> tuple[str, ...]:
    return tuple(table.column_names())


def _source_error(code: str, table_name: str, detail: str) -> ThemeM5PipelineError:
    return ThemeM5PipelineError(code, f"{table_name}: {detail}")


def _normalise_date_frame(frame: pd.DataFrame, table_name: str) -> pd.DataFrame:
    result = frame.copy(deep=True)
    if TRADE_DATE not in result.columns:
        raise _source_error("THEME_M5_INPUTS_INCOMPLETE", table_name, "missing trade_date")
    try:
        result[TRADE_DATE] = pd.to_datetime(result[TRADE_DATE], errors="raise").dt.date
    except (TypeError, ValueError, OverflowError) as exc:
        raise _source_error("THEME_M5_INPUTS_INCOMPLETE", table_name, "invalid trade_date") from exc
    if result[TRADE_DATE].isna().any():
        raise _source_error("THEME_M5_INPUTS_INCOMPLETE", table_name, "NULL trade_date")
    return result


def validate_complete_popularity_frame(
    frame: pd.DataFrame,
    *,
    table_name: str,
    expected_source: str,
    expected_list_name: str,
    trade_date: date,
    error_code: str = "THEME_M5_INPUTS_INCOMPLETE",
) -> pd.DataFrame:
    """Validate one B1 canonical source for one date without changing its rows."""
    if not isinstance(frame, pd.DataFrame):
        raise _source_error(error_code, table_name, "source is not a DataFrame")
    required = {
        "trade_date",
        SOURCE,
        "list_name",
        "ticker",
        "rank_position",
        "source_rank_time",
        "snapshot_seq",
        "snapshot_started_at",
        "snapshot_completed_at",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise _source_error(error_code, table_name, f"missing columns: {missing}")
    if frame.empty:
        raise _source_error(error_code, table_name, f"no complete snapshot for {trade_date.isoformat()}")

    result = _normalise_date_frame(frame, table_name)
    if set(result[TRADE_DATE].unique()) != {trade_date}:
        raise _source_error(error_code, table_name, f"contains a date other than {trade_date.isoformat()}")
    if result[SOURCE].isna().any() or result[SOURCE].astype(str).str.strip().eq("").any():
        raise _source_error(error_code, table_name, "source contains NULL or empty values")
    if result["list_name"].isna().any() or result["list_name"].astype(str).str.strip().eq("").any():
        raise _source_error(error_code, table_name, "list_name contains NULL or empty values")
    if set(result[SOURCE].astype(str)) != {expected_source}:
        raise _source_error(error_code, table_name, f"source must be exactly {expected_source}")
    if set(result["list_name"].astype(str)) != {expected_list_name}:
        raise _source_error(error_code, table_name, f"list_name must be exactly {expected_list_name}")
    for column in ("ticker", "source_rank_time", "snapshot_started_at", "snapshot_completed_at"):
        if result[column].isna().any() or result[column].astype(str).str.strip().eq("").any():
            raise _source_error(error_code, table_name, f"{column} contains NULL or empty values")

    try:
        snapshot_seq_values = pd.to_numeric(result["snapshot_seq"], errors="raise")
        rank_values = pd.to_numeric(result["rank_position"], errors="raise")
        if (snapshot_seq_values % 1 != 0).any() or (rank_values % 1 != 0).any():
            raise ValueError("snapshot_seq/rank_position must be integral")
        result["snapshot_seq"] = snapshot_seq_values.astype(int)
        result["rank_position"] = rank_values.astype(int)
    except (TypeError, ValueError, OverflowError) as exc:
        raise _source_error(error_code, table_name, "snapshot_seq/rank_position are not integers") from exc
    if (result["snapshot_seq"] < 1).any() or (result["rank_position"] < 1).any():
        raise _source_error(error_code, table_name, "snapshot_seq/rank_position must be positive")

    duplicate_key = result.duplicated(subset=[TRADE_DATE, "snapshot_seq", "rank_position"])
    if duplicate_key.any():
        raise _source_error(error_code, table_name, "duplicate canonical snapshot rank")

    seqs = sorted(result["snapshot_seq"].unique().tolist())
    expected_seqs = list(range(1, len(seqs) + 1))
    if seqs != expected_seqs:
        raise _source_error(error_code, table_name, f"snapshot sequence is not contiguous: {seqs}")

    snapshot_times: dict[int, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for seq in seqs:
        snapshot = result[result["snapshot_seq"] == seq]
        if len(snapshot) != 100:
            raise _source_error(error_code, table_name, f"snapshot {seq} has {len(snapshot)} rows, expected 100")
        if snapshot["rank_position"].nunique() != 100 or set(snapshot["rank_position"]) != set(range(1, 101)):
            raise _source_error(error_code, table_name, f"snapshot {seq} does not cover ranks 1..100 exactly")
        if snapshot["ticker"].nunique() != 100:
            raise _source_error(error_code, table_name, f"snapshot {seq} does not contain 100 distinct tickers")
        started = pd.to_datetime(snapshot["snapshot_started_at"], errors="coerce")
        completed = pd.to_datetime(snapshot["snapshot_completed_at"], errors="coerce")
        if started.isna().any() or completed.isna().any():
            raise _source_error(error_code, table_name, f"snapshot {seq} has invalid snapshot timing")
        start_time = started.min()
        end_time = completed.max()
        if started.nunique() != 1 or completed.nunique() != 1:
            raise _source_error(error_code, table_name, f"snapshot {seq} has inconsistent snapshot timing")
        if end_time < start_time:
            raise _source_error(error_code, table_name, f"snapshot {seq} completes before it starts")
        snapshot_times[seq] = (start_time, end_time)

    for previous, following in zip(seqs, seqs[1:]):
        if snapshot_times[previous][1] >= snapshot_times[following][0]:
            raise _source_error(
                error_code,
                table_name,
                f"snapshot {previous} overlaps snapshot {following}",
            )
    return result.sort_values([TRADE_DATE, "snapshot_seq", "rank_position"]).reset_index(drop=True)


def read_complete_popularity_source(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    expected_source: str,
    expected_list_name: str,
    trade_date: date,
    error_code: str = "THEME_M5_INPUTS_INCOMPLETE",
) -> pd.DataFrame:
    """Read and validate all D-day canonical records for one formal source."""
    table_by_name = {DC_HOT.name: DC_HOT, THS_HOT.name: THS_HOT}
    table = table_by_name.get(table_name)
    if table is None:
        raise _source_error(error_code, table_name, "table is not an admitted M5 source")
    columns = ", ".join(column for column in _table_columns(table) if column != CREATED_AT)
    try:
        frame = con.execute(
            f"SELECT {columns} FROM {table_name} WHERE trade_date = ? "
            "ORDER BY snapshot_seq ASC, rank_position ASC",
            [trade_date],
        ).df()
    except Exception as exc:
        raise _source_error(error_code, table_name, f"table could not be read: {type(exc).__name__}") from exc
    return validate_complete_popularity_frame(
        frame,
        table_name=table_name,
        expected_source=expected_source,
        expected_list_name=expected_list_name,
        trade_date=trade_date,
        error_code=error_code,
    )


def read_m5_popularity_inputs(
    con: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read both formal B1 sources; either source being incomplete fails closed."""
    dc_hot = read_complete_popularity_source(
        con,
        table_name=DC_HOT.name,
        expected_source=DC_HOT_SOURCE,
        expected_list_name=DC_HOT_LIST,
        trade_date=trade_date,
    )
    ths_hot = read_complete_popularity_source(
        con,
        table_name=THS_HOT.name,
        expected_source=THS_HOT_SOURCE,
        expected_list_name=THS_HOT_LIST,
        trade_date=trade_date,
    )
    return dc_hot, ths_hot


def _digest_frame(hasher: hashlib._Hash, label: str, frame: pd.DataFrame) -> None:
    hasher.update(f"{label}:columns={','.join(sorted(frame.columns))}\n".encode("utf-8"))
    if frame.empty:
        hasher.update(f"{label}:empty\n".encode("utf-8"))
        return
    columns = sorted(frame.columns)
    values = frame[columns].copy(deep=True).astype("string").fillna("<NULL>")
    values = values.sort_values(columns, kind="stable").reset_index(drop=True)
    hasher.update(values.to_csv(index=False, lineterminator="\n").encode("utf-8"))


def compute_m5_input_snapshot_id(
    themes: pd.DataFrame,
    memberships: pd.DataFrame,
    dc_hot: pd.DataFrame,
    ths_hot: pd.DataFrame,
) -> str:
    """Compute a deterministic digest over all M5 logical input facts."""
    hasher = hashlib.sha256()
    hasher.update(f"calculation_version={THEME_M5_OBSERVATION_VERSION}\n".encode("utf-8"))
    _digest_frame(hasher, "themes", themes)
    _digest_frame(hasher, "memberships", memberships)
    _digest_frame(hasher, DC_HOT.name, dc_hot)
    _digest_frame(hasher, THS_HOT.name, ths_hot)
    return f"SNAP:{hasher.hexdigest()[:24].upper()}"


def _empty_memberships() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            COLLECTION_ID,
            "asset_id",
            TRADE_DATE,
            "membership_id",
            "revision_id",
            "effective_from",
            "effective_to",
            "available_trade_date",
            THEME_ID,
        ]
    )


class ThemeM5PipelineService:
    """Production service for PIT Theme mapping and M5 observation facts."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.resolver = StockCollectionResolver(con)

    def _checkpoint(self, execution_control: Any | None) -> None:
        if execution_control is not None:
            execution_control.check()

    def calculate_m5_facts(
        self,
        trade_date: date,
        *,
        execution_control: Any | None = None,
    ) -> ThemeM5CalculatedFacts:
        """Calculate M5 facts read-only for exactly one D-day."""
        started = time.monotonic()
        self._checkpoint(execution_control)

        try:
            themes = self.resolver.resolve_active_themes(
                trade_date,
                allowed_scopes=("CANONICAL",),
                enforce_admission_cutoff=True,
            )
        except Exception as exc:
            raise ThemeM5PipelineError(
                "THEME_M5_THEME_INPUTS_UNAVAILABLE",
                f"failed to resolve the PIT Theme universe: {type(exc).__name__}",
            ) from exc
        if themes.empty:
            raise ThemeM5PipelineError("NO_ACTIVE_THEMES", f"No active canonical themes as of {trade_date}")
        themes = themes[[THEME_ID, COLLECTION_ID]].copy(deep=True)

        # The two source reads happen before any output transaction.  A missing
        # source therefore cannot be represented as a zero source count.
        dc_hot, ths_hot = read_m5_popularity_inputs(self.con, trade_date)
        self._checkpoint(execution_control)

        theme_map = dict(zip(themes[COLLECTION_ID], themes[THEME_ID]))
        collection_ids = themes[COLLECTION_ID].tolist()
        try:
            raw_memberships = self.resolver.batch_resolve_members(
                collection_ids,
                [trade_date],
                trade_date,
                enforce_admission_cutoff=True,
            )
        except Exception as exc:
            raise ThemeM5PipelineError(
                "M5_MEMBERSHIP_RESOLUTION_FAILED",
                f"failed to resolve D-day PIT memberships: {type(exc).__name__}",
            ) from exc
        if not isinstance(raw_memberships, pd.DataFrame):
            raise ThemeM5PipelineError("M5_MEMBERSHIP_RESOLUTION_FAILED", "resolver did not return a DataFrame")
        memberships = raw_memberships.copy(deep=True)
        if memberships.empty:
            memberships = _empty_memberships()
        else:
            if COLLECTION_ID not in memberships.columns or "asset_id" not in memberships.columns:
                raise ThemeM5PipelineError(
                    "M5_MEMBERSHIP_RESOLUTION_FAILED",
                    "resolver output must contain collection_id and asset_id",
                )
            memberships[THEME_ID] = memberships[COLLECTION_ID].map(theme_map)
            if memberships[THEME_ID].isna().any():
                raise ThemeM5PipelineError(
                    "M5_MEMBERSHIP_RESOLUTION_FAILED",
                    "resolver returned a collection outside the PIT Theme universe",
                )

        universe = themes.copy(deep=True)
        popularity = pd.concat([dc_hot, ths_hot], ignore_index=True, sort=False)
        try:
            observations = calculate_m5_raw_observations(
                memberships,
                popularity,
                theme_universe=universe,
                trade_date=trade_date,
            )
        except M5ObservationError as exc:
            raise ThemeM5PipelineError(exc.code, exc.detail) from exc
        snapshot_id = compute_m5_input_snapshot_id(themes, memberships, dc_hot, ths_hot)
        self._checkpoint(execution_control)
        return ThemeM5CalculatedFacts(
            trade_date=trade_date,
            theme_count=len(themes),
            total_member_rows=len(memberships),
            total_popularity_rows=len(popularity),
            input_snapshot_id=snapshot_id,
            observations=observations,
            memberships=memberships,
            dc_hot=dc_hot,
            ths_hot=ths_hot,
            execution_seconds=time.monotonic() - started,
        )

    def run_m5_daily(
        self,
        trade_date: date,
        *,
        production_run_id: str | None = None,
        execution_control: Any | None = None,
    ) -> ThemeM5ProductionReport:
        """Calculate first, then atomically replace only the target date."""
        started = time.monotonic()
        facts = self.calculate_m5_facts(trade_date, execution_control=execution_control)
        self._checkpoint(execution_control)
        run_id = production_run_id or (
            f"RUN:THEME_M5:{trade_date.strftime('%Y%m%d')}:{uuid.uuid4().hex[:12].upper()}"
        )
        created_at = datetime.now(UTC).replace(tzinfo=None)
        persisted = facts.observations.copy(deep=True)
        persisted[CALCULATION_VERSION] = THEME_M5_OBSERVATION_VERSION
        persisted[PRODUCTION_RUN_ID] = run_id
        persisted[INPUT_SNAPSHOT_ID] = facts.input_snapshot_id
        persisted[CREATED_AT] = created_at
        output_columns = list(THEME_M5_OBSERVATION.column_names())

        transaction_open = False
        registered = False
        try:
            self.con.execute(THEME_M5_OBSERVATION.duckdb_create_sql())
            self.con.execute("BEGIN TRANSACTION")
            transaction_open = True
            self._checkpoint(execution_control)
            self.con.execute(
                f"DELETE FROM {THEME_M5_OBSERVATION_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            self._checkpoint(execution_control)
            self.con.register("_theme_m5_observation_rows", persisted[output_columns])
            registered = True
            self.con.execute(
                f"INSERT INTO {THEME_M5_OBSERVATION_TABLE} ({', '.join(output_columns)}) "
                f"SELECT {', '.join(output_columns)} FROM _theme_m5_observation_rows"
            )
            self._checkpoint(execution_control)
            self.con.unregister("_theme_m5_observation_rows")
            registered = False
            self.con.execute("COMMIT")
            transaction_open = False
        except ExecutionControlError:
            if transaction_open:
                try:
                    self.con.execute("ROLLBACK")
                except Exception:
                    pass
            raise
        except Exception as exc:
            if transaction_open:
                try:
                    self.con.execute("ROLLBACK")
                except Exception:
                    pass
            raise ThemeM5PipelineError(
                "THEME_M5_TRANSACTION_FAILED",
                f"failed to replace {trade_date}: {type(exc).__name__}: {exc}",
            ) from exc
        finally:
            if registered:
                try:
                    self.con.unregister("_theme_m5_observation_rows")
                except Exception:
                    pass

        return ThemeM5ProductionReport(
            production_run_id=run_id,
            input_snapshot_id=facts.input_snapshot_id,
            theme_count=facts.theme_count,
            trade_date_count=1,
            start_date=trade_date,
            end_date=trade_date,
            total_observation_rows=len(persisted),
            total_member_rows=facts.total_member_rows,
            total_popularity_rows=facts.total_popularity_rows,
            execution_seconds=time.monotonic() - started,
        )


__all__ = [
    "DC_HOT_LIST",
    "DC_HOT_SOURCE",
    "THS_HOT_LIST",
    "THS_HOT_SOURCE",
    "ThemeM5CalculatedFacts",
    "ThemeM5PipelineError",
    "ThemeM5PipelineService",
    "ThemeM5ProductionReport",
    "compute_m5_input_snapshot_id",
    "read_complete_popularity_source",
    "read_m5_popularity_inputs",
    "validate_complete_popularity_frame",
]
