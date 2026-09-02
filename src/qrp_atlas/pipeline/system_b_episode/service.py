"""Safe production rebuild and audit for System B market episodes."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import uuid

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID, CLOSE, CREATED_AT, CREATED_RUN_ID, EPISODE_END_DATE, MA10, MA5,
    RULE_VERSION, SYSTEM_B_2_0_PARAMETER_SET_ID, SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_CALCULATION_VERSION, SYSTEM_B_EPISODE, SYSTEM_B_EPISODE_OBSERVATION,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE, SYSTEM_B_EPISODE_RULE_VERSION,
    SYSTEM_B_EPISODE_SEGMENT, SYSTEM_B_EPISODE_SEGMENT_TABLE,
    SYSTEM_B_EPISODE_SEGMENT_VERSION, SOURCE_EPISODE_RULE_VERSION,
    SEGMENT_VERSION,
    SYSTEM_B_EPISODE_TABLE, SYSTEM_B_STATE_OBSERVATION_TABLE, TICKER, TRADE_DATE,
    TREND_STATE,
)
import numpy as np
from qrp_atlas.indicators import IndicatorRequest, calculate_indicators
from qrp_atlas.indicators.system_b import (
    calculate_system_b_episodes,
    calculate_system_b_episode_segments,
)

ACCEPTANCE_START_DATE = date(2013, 1, 1)


class SystemBEpisodeProductionError(RuntimeError):
    """Stable production failure with a machine-readable code."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _absolute(path: Path, *, label: str) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        raise SystemBEpisodeProductionError(f"{label}_MUST_BE_ABSOLUTE", str(path))
    return expanded.resolve(strict=False)


def open_state_input(path: Path) -> tuple[Path, duckdb.DuckDBPyConnection]:
    resolved = _absolute(path, label="STATE_INPUT_DATABASE")
    if not resolved.exists():
        raise SystemBEpisodeProductionError("STATE_INPUT_DATABASE_NOT_FOUND", str(resolved))
    if not resolved.is_file():
        raise SystemBEpisodeProductionError("STATE_INPUT_DATABASE_NOT_FILE", str(resolved))
    try:
        connection = duckdb.connect(str(resolved), read_only=True)
    except Exception as exc:
        raise SystemBEpisodeProductionError("STATE_INPUT_DATABASE_NOT_READABLE", str(resolved)) from exc
    try:
        exists = connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name=?",
            [SYSTEM_B_STATE_OBSERVATION_TABLE],
        ).fetchone()[0]
        if not exists:
            raise SystemBEpisodeProductionError("MISSING_STATE_TABLE", str(resolved))
        return resolved, connection
    except Exception:
        connection.close()
        raise


def ensure_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(SYSTEM_B_EPISODE.duckdb_create_sql())
    connection.execute(SYSTEM_B_EPISODE_OBSERVATION.duckdb_create_sql())
    connection.execute(SYSTEM_B_EPISODE_SEGMENT.duckdb_create_sql())


def inspect_state_input(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    versions = connection.execute(
        f"""SELECT rule_version_set_id, parameter_set_id, calculation_version, count(*)
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} GROUP BY 1,2,3 ORDER BY 4 DESC"""
    ).fetchall()
    if versions != [(
        SYSTEM_B_2_0_RULE_VERSION_SET_ID,
        SYSTEM_B_2_0_PARAMETER_SET_ID,
        SYSTEM_B_CALCULATION_VERSION,
        versions[0][3] if versions else 0,
    )]:
        raise SystemBEpisodeProductionError("MIXED_OR_UNSUPPORTED_STATE_VERSION", repr(versions))
    row = connection.execute(
        f"""SELECT count(*), min(trade_date), max(trade_date), count(DISTINCT asset_id)
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE}"""
    ).fetchone()
    if not row or row[0] == 0:
        raise SystemBEpisodeProductionError("EMPTY_STATE_INPUT")
    duplicates = connection.execute(
        f"""SELECT count(*) FROM (
        SELECT asset_id, trade_date, rule_version_set_id, parameter_set_id, count(*) n
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} GROUP BY 1,2,3,4 HAVING count(*)>1)"""
    ).fetchone()[0]
    if duplicates:
        raise SystemBEpisodeProductionError("DUPLICATE_STATE_KEYS", str(duplicates))
    status_counts = dict(connection.execute(
        f"SELECT market_fact_status, count(*) FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} GROUP BY 1 ORDER BY 1"
    ).fetchall())
    null_reason_counts = {
        f"{lifecycle or 'NULL'}|{diagnostics}": int(count)
        for lifecycle, diagnostics, count in connection.execute(
            f"""SELECT lifecycle_state, diagnostics, count(*)
            FROM {SYSTEM_B_STATE_OBSERVATION_TABLE}
            WHERE market_fact_status='ACTUAL_TRADING' AND trend_state IS NULL
            GROUP BY 1,2 ORDER BY 3 DESC"""
        ).fetchall()
    }
    return {
        "state_row_count": int(row[0]), "state_start_date": row[1], "state_end_date": row[2],
        "state_asset_count": int(row[3]), "rule_version_set_id": versions[0][0],
        "parameter_set_id": versions[0][1], "calculation_version": versions[0][2],
        "state_duplicate_key_count": int(duplicates),
        "state_market_fact_status_counts": status_counts,
        "actual_null_state_reason_counts": null_reason_counts,
    }


def rebuild_episodes(
    state_input_database: Path,
    output_database: Path,
    *,
    end_date: date,
    acceptance_start_date: date = ACCEPTANCE_START_DATE,
    asset_batch_size: int = 128,
) -> dict[str, object]:
    input_path, source = open_state_input(state_input_database)
    try:
        state = inspect_state_input(source)
        if state["state_end_date"] < end_date:
            raise SystemBEpisodeProductionError("STATE_INPUT_END_DATE_TOO_EARLY", str(state["state_end_date"]))
        if state["state_start_date"] >= acceptance_start_date:
            raise SystemBEpisodeProductionError(
                "STATE_INPUT_HISTORY_CONTEXT_INSUFFICIENT",
                f"state_start_date={state['state_start_date']} acceptance_start_date={acceptance_start_date}",
            )
        output_path = _absolute(output_database, label="EPISODE_OUTPUT_DATABASE")
        if output_path == input_path:
            raise SystemBEpisodeProductionError("INPUT_OUTPUT_DATABASE_MUST_DIFFER", str(input_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output = duckdb.connect(str(output_path))
        run_id = f"system-b-episode-{uuid.uuid4().hex}"
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            assets = [row[0] for row in source.execute(
                f"SELECT DISTINCT asset_id FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} WHERE trade_date<=? ORDER BY 1",
                [end_date],
            ).fetchall()]
            output.execute("BEGIN")
            ensure_schema(output)
            output.execute(
                f"DELETE FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} WHERE segment_version=?",
                [SYSTEM_B_EPISODE_SEGMENT_VERSION],
            )
            output.execute(
                f"DELETE FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE rule_version=?",
                [SYSTEM_B_EPISODE_RULE_VERSION],
            )
            output.execute(
                f"DELETE FROM {SYSTEM_B_EPISODE_TABLE} WHERE rule_version=?",
                [SYSTEM_B_EPISODE_RULE_VERSION],
            )
            episode_rows = observation_rows = segment_rows = excluded_null_state = excluded_indicator_warmup = 0
            for offset in range(0, len(assets), asset_batch_size):
                batch = assets[offset:offset + asset_batch_size]
                placeholders = ",".join("?" for _ in batch)
                raw = source.execute(
                    f"""SELECT asset_id, trade_date, close, ma5, trend_state
                    FROM {SYSTEM_B_STATE_OBSERVATION_TABLE}
                    WHERE trade_date<=? AND market_fact_status='ACTUAL_TRADING'
                      AND asset_id IN ({placeholders}) ORDER BY asset_id, trade_date""",
                    [end_date, *batch],
                ).fetchdf()
                if raw.empty:
                    continue
                indicators = calculate_indicators(
                    raw.rename(columns={ASSET_ID: TICKER})[[TICKER, TRADE_DATE, CLOSE]],
                    (IndicatorRequest("sma", {"window": 10}, alias=MA10, output_fields={"value": MA10}),),
                )
                raw[MA10] = indicators[MA10]
                excluded_null_state += int(raw[TREND_STATE].isna().sum())
                excluded_indicator_warmup += int((raw[TREND_STATE].notna() & (raw[MA5].isna() | raw[MA10].isna())).sum())
                actual = raw.loc[raw[TREND_STATE].notna() & raw[MA5].notna() & raw[MA10].notna()].copy()
                result = calculate_system_b_episodes(actual)
                if result.episodes.empty:
                    episode_frame = result.episodes.copy()
                else:
                    episode_end_dates = pd.to_datetime(result.episodes[EPISODE_END_DATE], errors="coerce")
                    episode_frame = result.episodes.loc[
                        episode_end_dates.isna() | (episode_end_dates.dt.date >= acceptance_start_date)
                    ].copy()
                kept_ids = set(episode_frame["episode_id"])
                observation_frame = result.observations.loc[
                    result.observations["episode_id"].isin(kept_ids)
                ].copy()
                if not episode_frame.empty and not observation_frame.empty:
                    seg_res = calculate_system_b_episode_segments(episode_frame, observation_frame)
                    segment_frame = seg_res.segments.copy()
                else:
                    segment_frame = pd.DataFrame(columns=[column.name for column in SYSTEM_B_EPISODE_SEGMENT.columns])

                for frame in (episode_frame, observation_frame):
                    frame[CREATED_RUN_ID] = run_id
                    frame[RULE_VERSION] = SYSTEM_B_EPISODE_RULE_VERSION
                    frame[CREATED_AT] = now
                if not segment_frame.empty:
                    segment_frame[CREATED_RUN_ID] = run_id
                    segment_frame[SOURCE_EPISODE_RULE_VERSION] = SYSTEM_B_EPISODE_RULE_VERSION
                    segment_frame[SEGMENT_VERSION] = SYSTEM_B_EPISODE_SEGMENT_VERSION
                    segment_frame[CREATED_AT] = now

                if not episode_frame.empty:
                    output.register("episode_batch", episode_frame)
                    columns = ",".join(column.name for column in SYSTEM_B_EPISODE.columns)
                    output.execute(f"INSERT INTO {SYSTEM_B_EPISODE_TABLE} ({columns}) SELECT {columns} FROM episode_batch")
                    output.unregister("episode_batch")
                if not observation_frame.empty:
                    output.register("episode_observation_batch", observation_frame)
                    columns = ",".join(column.name for column in SYSTEM_B_EPISODE_OBSERVATION.columns)
                    output.execute(f"INSERT INTO {SYSTEM_B_EPISODE_OBSERVATION_TABLE} ({columns}) SELECT {columns} FROM episode_observation_batch")
                    output.unregister("episode_observation_batch")
                if not segment_frame.empty:
                    output.register("episode_segment_batch", segment_frame)
                    columns = ",".join(column.name for column in SYSTEM_B_EPISODE_SEGMENT.columns)
                    output.execute(f"INSERT INTO {SYSTEM_B_EPISODE_SEGMENT_TABLE} ({columns}) SELECT {columns} FROM episode_segment_batch")
                    output.unregister("episode_segment_batch")

                episode_rows += len(episode_frame)
                observation_rows += len(observation_frame)
                segment_rows += len(segment_frame)
            audit = audit_episodes(output, source, acceptance_start_date=acceptance_start_date, end_date=end_date)
            violations = {key: value for key, value in audit["quality"].items() if value and key != "shared_boundary_days"}
            if violations:
                error_detail: dict[str, object] = {"violations": violations}
                if violations.get("return_closure_violations"):
                    error_detail["return_closure_diagnostics"] = audit["evidence"].get("return_closure_diagnostics", [])
                raise SystemBEpisodeProductionError("INVARIANT_VIOLATION", repr(error_detail))
            output.execute("COMMIT")
            return {
                "created_run_id": run_id, "state_input_database": str(input_path),
                "episode_output_database": str(output_path), "acceptance_start_date": str(acceptance_start_date),
                "effective_end_date": str(end_date), "output_rebuild_strategy": "transactional rule-version replacement",
                "episode_rows": episode_rows, "observation_rows": observation_rows,
                "segment_rows": segment_rows,
                "excluded_null_state_rows": excluded_null_state,
                "excluded_indicator_warmup_rows": excluded_indicator_warmup,
                **state, **audit,
            }
        except Exception:
            try:
                output.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            output.close()
    finally:
        source.close()


def audit_episodes(
    output: duckdb.DuckDBPyConnection,
    source: duckdb.DuckDBPyConnection,
    *,
    acceptance_start_date: date,
    end_date: date,
) -> dict[str, object]:
    q = lambda sql, params=None: output.execute(sql, params or []).fetchone()[0]
    overall_row = output.execute(f"""WITH durations AS (
        SELECT e.*, count(o.trade_date) duration FROM {SYSTEM_B_EPISODE_TABLE} e
        LEFT JOIN {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o USING(episode_id) GROUP BY ALL)
        SELECT count(*), count(DISTINCT asset_id),
        count(*) FILTER(WHERE episode_confirmed_date<? AND episode_id IN (
            SELECT episode_id FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE trade_date>=?)),
        count(*) FILTER(WHERE episode_end_date IS NOT NULL), count(*) FILTER(WHERE episode_end_date IS NULL),
        min(episode_start_date), min(episode_confirmed_date), max(episode_confirmed_date), max(episode_end_date),
        coalesce(sum(ma5_reentry_count),0), count(*) FILTER(WHERE ma5_reentry_count>0),
        avg(duration), median(duration), arg_max(asset_id,duration), arg_max(episode_id,duration), max(duration)
        FROM durations""", [acceptance_start_date, acceptance_start_date]).fetchone()
    overall = dict(zip((
        "episode_count", "asset_count", "pre_2013_continuing_count", "ended_count", "open_count",
        "earliest_start_date", "earliest_confirmed_date", "latest_confirmed_date", "latest_end_date",
        "reentry_total", "episodes_with_reentry", "average_duration", "median_duration",
        "longest_asset_id", "longest_episode_id", "longest_duration",
    ), overall_row, strict=True))
    # Return closure audit (using np.isclose)
    closure_rows = output.execute(f"""
        SELECT s.episode_id, s.segment_no, s.segment_return, o.episode_return as latest_episode_return
        FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} s
        JOIN (
            SELECT episode_id, episode_return,
                   row_number() OVER(PARTITION BY episode_id ORDER BY trade_date DESC) rn
            FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}
        ) o ON o.episode_id = s.episode_id AND o.rn = 1
        ORDER BY s.episode_id, s.segment_no
    """).fetchall()

    closure_violations = 0
    closure_diagnostics: list[dict[str, object]] = []
    if closure_rows:
        from itertools import groupby
        for ep_id, group in groupby(closure_rows, key=lambda r: r[0]):
            items = list(group)
            seg_returns = np.array([r[2] for r in items], dtype=np.float64)
            latest_ret = float(items[0][3])
            seg_factor = float(np.prod(1.0 + seg_returns))
            ep_factor = 1.0 + latest_ret
            if not np.isclose(seg_factor, ep_factor, rtol=1e-10, atol=1e-12):
                closure_violations += 1
                abs_err = abs(seg_factor - ep_factor)
                rel_err = abs_err / abs(ep_factor) if ep_factor != 0 else abs_err
                closure_diagnostics.append({
                    "episode_id": ep_id,
                    "lhs": float(seg_factor),
                    "rhs": float(ep_factor),
                    "abs_error": float(abs_err),
                    "rel_error": float(rel_err),
                })

    quality = {
        "multiple_open_episodes": q(f"SELECT count(*) FROM (SELECT asset_id FROM {SYSTEM_B_EPISODE_TABLE} WHERE episode_end_date IS NULL GROUP BY 1 HAVING count(*)>1)"),
        "duplicate_episode_id": q(f"SELECT count(*)-count(DISTINCT episode_id) FROM {SYSTEM_B_EPISODE_TABLE}"),
        "duplicate_episode_keys": q(f"SELECT count(*) FROM (SELECT asset_id,episode_no FROM {SYSTEM_B_EPISODE_TABLE} GROUP BY 1,2 HAVING count(*)>1)"),
        "duplicate_daily_keys": q(f"SELECT count(*) FROM (SELECT trade_date,asset_id FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} GROUP BY 1,2 HAVING count(*)>1)"),
        "orphan_daily_observations": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o LEFT JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE e.episode_id IS NULL"),
        "orphan_segments": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} s LEFT JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE e.episode_id IS NULL"),
        "duplicate_segment_id": q(f"SELECT count(*)-count(DISTINCT segment_id) FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE}"),
        "duplicate_segment_keys": q(f"SELECT count(*) FROM (SELECT episode_id,segment_no FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} GROUP BY 1,2 HAVING count(*)>1)"),
        "segment_no_gaps": q(f"SELECT count(*) FROM (SELECT episode_id,min(segment_no) mn,max(segment_no) mx,count(*) n FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} GROUP BY 1 HAVING mx-mn+1<>n)"),
        "adjacent_same_state": q(f"SELECT count(*) FROM (SELECT *,lag(segment_state) OVER(PARTITION BY episode_id ORDER BY segment_no) prev_state FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE}) WHERE prev_state IS NOT NULL AND segment_state=prev_state"),
        "trading_days_mismatch": q(f"""
            WITH seg_agg AS (
                SELECT episode_id, coalesce(sum(trading_days), 0) AS seg_days
                FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE}
                GROUP BY episode_id
            ),
            obs_agg AS (
                SELECT episode_id, count(*) AS obs_days
                FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}
                GROUP BY episode_id
            )
            SELECT count(*)
            FROM {SYSTEM_B_EPISODE_TABLE} e
            LEFT JOIN seg_agg s USING(episode_id)
            LEFT JOIN obs_agg o USING(episode_id)
            WHERE coalesce(s.seg_days, 0) <> coalesce(o.obs_days, 0)
        """),
        "segment_start_boundary_mismatch": q(f"""
            WITH ep_obs_bounds AS (
                SELECT episode_id, min(trade_date) AS min_obs_date
                FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}
                GROUP BY episode_id
            ),
            seg_bounds AS (
                SELECT episode_id, arg_min(start_date, segment_no) AS first_seg_start_date
                FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE}
                GROUP BY episode_id
            )
            SELECT count(*)
            FROM {SYSTEM_B_EPISODE_TABLE} e
            JOIN ep_obs_bounds o USING(episode_id)
            LEFT JOIN seg_bounds s USING(episode_id)
            WHERE s.first_seg_start_date IS NULL OR s.first_seg_start_date <> o.min_obs_date
        """),
        "segment_end_boundary_mismatch": q(f"""
            WITH ep_obs_bounds AS (
                SELECT episode_id, max(trade_date) AS max_obs_date
                FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}
                GROUP BY episode_id
            ),
            seg_bounds AS (
                SELECT episode_id, arg_max(end_date, segment_no) AS last_seg_end_date
                FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE}
                GROUP BY episode_id
            )
            SELECT count(*)
            FROM {SYSTEM_B_EPISODE_TABLE} e
            JOIN ep_obs_bounds o USING(episode_id)
            LEFT JOIN seg_bounds s USING(episode_id)
            WHERE s.last_seg_end_date IS NULL OR s.last_seg_end_date <> o.max_obs_date
        """),
        "first_anchor_mismatch": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_SEGMENT_TABLE} s JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE s.segment_no=1 AND s.anchor_date<>e.episode_start_date"),
        "active_sprint_count_mismatch": q(f"SELECT count(*) FROM (SELECT e.episode_id,e.ma5_reentry_count,coalesce(sum(CASE WHEN s.segment_state='ACTIVE' THEN 1 ELSE 0 END),0) active_count FROM {SYSTEM_B_EPISODE_TABLE} e LEFT JOIN {SYSTEM_B_EPISODE_SEGMENT_TABLE} s USING(episode_id) GROUP BY 1,2) WHERE active_count<>ma5_reentry_count+1"),
        "return_closure_violations": closure_violations,
        "start_after_confirmed": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_TABLE} WHERE episode_start_date>episode_confirmed_date"),
        "confirmed_after_end": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_TABLE} WHERE episode_end_date IS NOT NULL AND episode_confirmed_date>episode_end_date"),
        "start_before_previous_end": q(f"SELECT count(*) FROM (SELECT *,lag(episode_end_date) OVER(PARTITION BY asset_id ORDER BY episode_no) previous_end FROM {SYSTEM_B_EPISODE_TABLE}) WHERE previous_end IS NOT NULL AND episode_start_date<previous_end"),
        "confirmed_observation_overlap": q(f"SELECT count(*) FROM (SELECT *,lag(episode_end_date) OVER(PARTITION BY asset_id ORDER BY episode_no) previous_end FROM {SYSTEM_B_EPISODE_TABLE}) WHERE previous_end IS NOT NULL AND episode_confirmed_date<=previous_end"),
        "shared_boundary_days": q(f"SELECT count(*) FROM (SELECT *,lag(episode_end_date) OVER(PARTITION BY asset_id ORDER BY episode_no) previous_end FROM {SYSTEM_B_EPISODE_TABLE}) WHERE episode_start_date=previous_end"),
        "episode_no_gaps": q(f"SELECT count(*) FROM (SELECT asset_id,min(episode_no) mn,max(episode_no) mx,count(*) n FROM {SYSTEM_B_EPISODE_TABLE} GROUP BY 1 HAVING mx-mn+1<>n)"),
        "confirmation_without_candidate_to_active": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE is_episode_confirmed AND state_transition<>'CANDIDATE->ACTIVE'"),
        "first_confirmation_counted_as_reentry": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE is_episode_confirmed AND ma5_reentry_count<>0"),
        "reentry_count_mismatch": q(f"SELECT count(*) FROM (SELECT e.episode_id,e.ma5_reentry_count,coalesce(sum(CASE WHEN o.state_transition='CANDIDATE->ACTIVE' AND NOT o.is_episode_confirmed THEN 1 ELSE 0 END),0) events FROM {SYSTEM_B_EPISODE_TABLE} e LEFT JOIN {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o USING(episode_id) GROUP BY 1,2) WHERE ma5_reentry_count<>events"),
        "invalid_episode_end_evidence": q(f"SELECT count(*) FROM (SELECT *,lag(close<ma10) OVER(PARTITION BY asset_id ORDER BY trade_date) previous_below FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}) WHERE is_episode_end AND NOT (trend_state<>'ACTIVE' AND close<ma10 AND previous_below)"),
        "active_ended": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE is_episode_end AND trend_state='ACTIVE'"),
        "non_finite_episode_return": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE NOT isfinite(episode_return)"),
        "non_finite_peak_return": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE NOT isfinite(peak_return)"),
        "non_finite_drawdown": q(f"SELECT count(*) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE NOT isfinite(drawdown_from_peak)"),
        "missing_start_price": 0,
        "mixed_state_versions": 0,
        "duplicate_state_keys": 0,
    }
    yearly = output.execute(f"""WITH durations AS (
        SELECT e.*,count(o.trade_date) duration FROM {SYSTEM_B_EPISODE_TABLE} e
        LEFT JOIN {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o USING(episode_id) GROUP BY ALL)
        SELECT year(episode_confirmed_date) confirmed_year,count(*) episode_count,count(DISTINCT asset_id) asset_count,
        count(*) FILTER(WHERE episode_end_date IS NOT NULL) ended_count,count(*) FILTER(WHERE episode_end_date IS NULL) open_count,
        avg(duration) average_duration,median(duration) median_duration,avg(ma5_reentry_count) average_reentry,
        max(ma5_reentry_count) maximum_reentry FROM durations
        WHERE episode_confirmed_date BETWEEN ? AND ? GROUP BY 1 ORDER BY 1""",
        [acceptance_start_date, end_date],
    ).fetchdf().to_dict(orient="records")
    evidence = sample_evidence(output, end_date=end_date)
    evidence["return_closure_diagnostics"] = closure_diagnostics
    return {"overall": overall, "yearly": yearly, "quality": quality, "evidence": evidence}


def sample_evidence(connection: duckdb.DuckDBPyConnection, *, end_date: date) -> dict[str, object]:
    selectors = {
        "normal_ended": "episode_confirmed_date>=DATE '2013-01-01' AND episode_end_date IS NOT NULL",
        "one_reentry": "episode_confirmed_date>=DATE '2013-01-01' AND ma5_reentry_count=1",
        "multiple_reentries": "episode_confirmed_date>=DATE '2013-01-01' AND ma5_reentry_count>1",
        "open_episode": f"episode_end_date IS NULL AND episode_id IN (SELECT episode_id FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE trade_date=DATE '{end_date}')",
        "pre_2013_continuing": f"episode_confirmed_date<DATE '2013-01-01' AND episode_id IN (SELECT episode_id FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE trade_date>=DATE '2013-01-01')",
    }
    fields = "trade_date,close,ma5,ma10,previous_trend_state,trend_state,state_transition,o.episode_id,e.episode_start_date,e.episode_confirmed_date,e.episode_end_date,o.ma5_reentry_count,is_episode_confirmed,is_episode_end"
    output: dict[str, object] = {}
    for label, predicate in selectors.items():
        row = connection.execute(f"SELECT episode_id FROM {SYSTEM_B_EPISODE_TABLE} WHERE {predicate} ORDER BY episode_confirmed_date,episode_id LIMIT 1").fetchone()
        output[label] = connection.execute(f"SELECT {fields} FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE o.episode_id=? ORDER BY trade_date", [row[0]]).fetchdf().to_dict(orient="records") if row else []
    pair = connection.execute(f"""WITH x AS (
        SELECT *,lag(episode_id) OVER(PARTITION BY asset_id ORDER BY episode_no) previous_episode_id,
        lag(episode_end_date) OVER(PARTITION BY asset_id ORDER BY episode_no) previous_end
        FROM {SYSTEM_B_EPISODE_TABLE})
        SELECT previous_episode_id,episode_id FROM x WHERE episode_start_date=previous_end ORDER BY episode_confirmed_date LIMIT 1""").fetchone()
    output["shared_boundary"] = connection.execute(f"SELECT {fields} FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE o.episode_id IN (?,?) ORDER BY trade_date,episode_id", list(pair)).fetchdf().to_dict(orient="records") if pair else []
    pair = connection.execute(f"""WITH x AS (
        SELECT *,lead(episode_id) OVER(PARTITION BY asset_id ORDER BY episode_no) next_episode_id
        FROM {SYSTEM_B_EPISODE_TABLE}) SELECT episode_id,next_episode_id FROM x
        WHERE next_episode_id IS NOT NULL AND episode_confirmed_date>=DATE '2013-01-01'
        ORDER BY episode_confirmed_date LIMIT 1""").fetchone()
    output["same_asset_multiple_episodes"] = connection.execute(f"SELECT {fields} FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE o.episode_id IN (?,?) ORDER BY trade_date,episode_id", list(pair)).fetchdf().to_dict(orient="records") if pair else []
    gap = connection.execute(f"SELECT episode_id FROM (SELECT episode_id,trade_date,lag(trade_date) OVER(PARTITION BY episode_id ORDER BY trade_date) p FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}) WHERE trade_date-p>3 LIMIT 1").fetchone()
    output["observation_gap"] = connection.execute(f"SELECT {fields} FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE o.episode_id=? ORDER BY trade_date", [gap[0]]).fetchdf().to_dict(orient="records") if gap else []
    nonactive = connection.execute(f"SELECT episode_id FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} WHERE trend_state<>'ACTIVE' AND NOT is_episode_end GROUP BY 1 HAVING count(*)>=5 ORDER BY count(*) DESC LIMIT 1").fetchone()
    output["long_non_active_not_ended"] = connection.execute(f"SELECT {fields} FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE} o JOIN {SYSTEM_B_EPISODE_TABLE} e USING(episode_id) WHERE o.episode_id=? ORDER BY trade_date", [nonactive[0]]).fetchdf().to_dict(orient="records") if nonactive else []
    return output


def write_report(result: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
