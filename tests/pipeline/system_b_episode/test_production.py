from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace
import sys
import duckdb
import pandas as pd
from qrp_atlas.contracts import SYSTEM_B_PRODUCTION_RUN, SYSTEM_B_STATE_OBSERVATION
import pytest
from qrp_atlas.pipeline.system_b_episode import rebuild_episodes
from qrp_atlas.pipeline.system_b_episode.service import (
    SystemBEpisodeProductionError, audit_episodes, ensure_schema,
)

ACCEPTANCE_START=pd.Timestamp("2026-01-06").date()


def _state_database(tmp_path: Path) -> Path:
    database=tmp_path/"state.duckdb"; con=duckdb.connect(str(database))
    con.execute(SYSTEM_B_STATE_OBSERVATION.duckdb_create_sql()); con.execute(SYSTEM_B_PRODUCTION_RUN.duckdb_create_sql())
    dates=pd.bdate_range("2026-01-05",periods=15); states=["BASE"]*10+["CANDIDATE","ACTIVE","BASE","BASE","BASE"]
    rows=[]; columns=[column.name for column in SYSTEM_B_STATE_OBSERVATION.columns]
    for i,(day,state) in enumerate(zip(dates,states,strict=True)):
        close=float(10+i) if i<12 else float(10-i/2)
        values={column:None for column in columns}
        values.update(asset_id="A",trade_date=day.date(),lifecycle_state="NORMAL",trend_state=state,
            previous_trend_state=states[i-1] if i else None,state_changed=True,market_fact_status="ACTUAL_TRADING",
            is_trading_day=True,listing_trading_day_number=20,confirmed_listing_trading_day_count=20,
            listing_trading_day_number_is_exact=True,close=close,ma5=close,ma5_window_complete=True,
            is_above_or_equal_ma5=True,latest_actual_trade_date=day.date(),latest_actual_close=close,
            latest_actual_ma5=close,latest_actual_ma5_window_complete=True,latest_actual_is_above_or_equal_ma5=True,
            previous_actual_trade_date=dates[i-1].date() if i else None,
            previous_actual_is_above_or_equal_ma5=True if i else None,
            previous_actual_ma5_window_complete=bool(i),state_basis_sequence_intact=True,actual_pair_contiguous=True,
            price_adjustment="FORWARD_ADJUSTED",rule_version_set_id="system_b_2_0_fact_derived_ma5_complete_1__user_20260726",
            parameter_set_id="system_b_2_0_fact_derived_ma5_complete_1_params_1",source_rule_ids="[]",diagnostics="[]",
            production_run_id="r",input_snapshot_id="s",calculation_version="system_b_fact_derived_state@2.1.0",created_at=pd.Timestamp("2026-01-01"))
        rows.append(tuple(values[column] for column in columns))
    con.executemany("INSERT INTO system_b_state_observation VALUES ("+",".join("?" for _ in columns)+")",rows); con.close()
    return database


def test_rebuild_uses_state_table_and_official_sma(tmp_path: Path):
    state=_state_database(tmp_path); output=tmp_path/"episode.duckdb"
    first=rebuild_episodes(state.resolve(),output.resolve(),end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    second=rebuild_episodes(state.resolve(),output.resolve(),end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    assert first["episode_rows"]==second["episode_rows"]==1
    assert first["state_input_database"]==str(state.resolve())
    con=duckdb.connect(str(output),read_only=True)
    assert con.execute("select count(*) from system_b_episode").fetchone()[0]==1
    assert con.execute("select count(*) from system_b_episode_observation").fetchone()[0]>0
    con.close()


def test_missing_input_does_not_create_or_modify_output(tmp_path: Path):
    output=tmp_path/"output.duckdb"
    with pytest.raises(SystemBEpisodeProductionError,match="STATE_INPUT_DATABASE_NOT_FOUND"):
        rebuild_episodes((tmp_path/"missing.duckdb").resolve(),output.resolve(),end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    assert not output.exists()


@pytest.mark.parametrize("create_empty",[False,True])
def test_database_without_state_table_fails_without_output_side_effect(tmp_path: Path,create_empty: bool):
    source=tmp_path/"source.duckdb"
    con=duckdb.connect(str(source))
    if not create_empty: con.execute("create table unrelated(id integer)")
    con.close()
    output=tmp_path/"output.duckdb"
    with pytest.raises(SystemBEpisodeProductionError,match="MISSING_STATE_TABLE"):
        rebuild_episodes(source.resolve(),output.resolve(),end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    assert not output.exists()


def test_invalid_input_preserves_existing_output_byte_for_byte(tmp_path: Path):
    output=tmp_path/"output.duckdb"; output.write_bytes(b"existing-success")
    before=output.read_bytes()
    with pytest.raises(SystemBEpisodeProductionError):
        rebuild_episodes((tmp_path/"missing.duckdb").resolve(),output.resolve(),end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    assert output.read_bytes()==before


def test_relative_input_is_rejected_independently_of_current_directory(tmp_path: Path,monkeypatch):
    output=(tmp_path/"output.duckdb").resolve()
    for cwd in (tmp_path,tmp_path.parent):
        monkeypatch.chdir(cwd)
        with pytest.raises(SystemBEpisodeProductionError,match="STATE_INPUT_DATABASE_MUST_BE_ABSOLUTE"):
            rebuild_episodes(Path("state.duckdb"),output,end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)


def test_failed_rebuild_rolls_back_previous_success(tmp_path: Path,monkeypatch):
    state=_state_database(tmp_path); output=(tmp_path/"episode.duckdb").resolve()
    rebuild_episodes(state.resolve(),output,end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    before=duckdb.connect(str(output),read_only=True)
    expected=before.execute("select * from system_b_episode order by episode_id").fetchall(); before.close()
    import qrp_atlas.pipeline.system_b_episode.service as service
    monkeypatch.setattr(service,"calculate_system_b_episodes",lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError,match="boom"):
        rebuild_episodes(state.resolve(),output,end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=ACCEPTANCE_START)
    after=duckdb.connect(str(output),read_only=True)
    assert after.execute("select * from system_b_episode order by episode_id").fetchall()==expected
    after.close()


def test_acceptance_requires_pre_start_history_context(tmp_path: Path):
    state=_state_database(tmp_path); output=(tmp_path/"output.duckdb").resolve()
    with pytest.raises(SystemBEpisodeProductionError,match="STATE_INPUT_HISTORY_CONTEXT_INSUFFICIENT"):
        rebuild_episodes(state.resolve(),output,end_date=pd.Timestamp("2026-01-23").date(),acceptance_start_date=pd.Timestamp("2026-01-05").date())
    assert not output.exists()


def test_audit_rejects_true_crossing_and_confirmed_overlap(tmp_path: Path):
    output=duckdb.connect(str(tmp_path/"audit.duckdb")); ensure_schema(output)
    output.execute("""insert into system_b_episode values
        ('A_EP_0001','A',1,date '2026-01-01',date '2026-01-02',date '2026-01-10',0,'r','system_b_episode@1.0.0__user_20260727',timestamp '2026-01-01'),
        ('A_EP_0002','A',2,date '2026-01-09',date '2026-01-10',date '2026-01-12',0,'r','system_b_episode@1.0.0__user_20260727',timestamp '2026-01-01')""")
    source=duckdb.connect()
    audit=audit_episodes(output,source,acceptance_start_date=pd.Timestamp("2013-01-01").date(),end_date=pd.Timestamp("2026-07-24").date())
    assert audit["quality"]["start_before_previous_end"]==1
    assert audit["quality"]["confirmed_observation_overlap"]==1
    source.close(); output.close()


def test_cli_config_path_is_independent_of_current_directory(tmp_path: Path,monkeypatch,capsys):
    import qrp_atlas.pipeline.system_b_episode.cli as cli
    configured=(tmp_path/"configured-state.duckdb").resolve()
    output=(tmp_path/"output.duckdb").resolve()
    monkeypatch.setattr(cli,"get_settings",lambda: SimpleNamespace(paths=SimpleNamespace(duckdb_path=configured)))
    calls=[]
    monkeypatch.setattr(cli,"rebuild_episodes",lambda state,out,**kwargs: calls.append((state,out,kwargs)) or {"status":"ok"})
    for cwd in (tmp_path,tmp_path.parent):
        monkeypatch.chdir(cwd)
        monkeypatch.setattr(sys,"argv",["qrp-atlas-system-b-episode","--output-database",str(output),"--end-date","2026-07-24"])
        assert cli.main()==0
        capsys.readouterr()
    assert calls[0][0]==calls[1][0]==configured
    assert calls[0][1]==calls[1][1]==output
