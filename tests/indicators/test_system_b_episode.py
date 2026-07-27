from __future__ import annotations
import pandas as pd
import pytest
from qrp_atlas.indicators.system_b import SystemBEpisodeError, calculate_system_b_episodes


def frame(states, closes=None, ma10=None):
    n=len(states); closes=closes or [10+i for i in range(n)]; ma10=ma10 or [8]*n
    return pd.DataFrame({"asset_id":["000001.SZ"]*n,"trade_date":pd.bdate_range("2026-01-05",periods=n),"close":closes,"ma5":[9]*n,"ma10":ma10,"trend_state":states})


def test_create_episode_dates_and_first_confirmation_not_reentry():
    result=calculate_system_b_episodes(frame(["BASE","CANDIDATE","ACTIVE"]))
    episode=result.episodes.iloc[0]; observation=result.observations.iloc[0]
    assert episode.episode_id=="000001.SZ_EP_0001"
    assert episode.episode_start_date==pd.Timestamp("2026-01-06")
    assert episode.episode_confirmed_date==pd.Timestamp("2026-01-07")
    assert episode.ma5_reentry_count==0
    assert observation.days_since_start==1 and observation.days_since_confirmed==0
    assert bool(observation.is_episode_confirmed)


def test_reentries_increment_immediately_and_do_not_create_more_episodes():
    states=["BASE","CANDIDATE","ACTIVE","CANDIDATE","ACTIVE","BASE","CANDIDATE","ACTIVE"]
    result=calculate_system_b_episodes(frame(states))
    assert len(result.episodes)==1
    assert result.episodes.iloc[0].ma5_reentry_count==2
    assert result.observations.loc[result.observations.state_transition=="CANDIDATE->ACTIVE","ma5_reentry_count"].tolist()==[0,1,2]


def test_end_requires_two_actual_below_ma10_and_current_non_active():
    states=["BASE","CANDIDATE","ACTIVE","ACTIVE","BASE","BASE"]
    result=calculate_system_b_episodes(frame(states, closes=[8,9,12,9,9,8], ma10=[10]*6))
    assert result.episodes.iloc[0].episode_end_date==pd.Timestamp("2026-01-09")
    assert result.observations.is_episode_end.tolist()==[False,False,True]


def test_active_does_not_end_and_recovery_above_ma10_resets_pair():
    states=["BASE","CANDIDATE","ACTIVE","ACTIVE","BASE","BASE","BASE"]
    result=calculate_system_b_episodes(frame(states, closes=[8,9,12,9,11,9,11], ma10=[10]*7))
    assert pd.isna(result.episodes.iloc[0].episode_end_date)
    assert not result.observations.is_episode_end.any()


def test_missing_calendar_dates_do_not_progress_or_break_actual_sequence():
    data=frame(["BASE","CANDIDATE","ACTIVE","BASE","BASE"], closes=[8,9,12,9,8], ma10=[10]*5)
    data.loc[4,"trade_date"] += pd.Timedelta(days=8)
    result=calculate_system_b_episodes(data)
    assert result.episodes.iloc[0].episode_end_date==data.loc[4,"trade_date"]


def test_returns_drawdown_and_prefix_are_point_in_time_stable():
    data=frame(["BASE","CANDIDATE","ACTIVE","ACTIVE","BASE","BASE"], closes=[8,10,12,15,9,8], ma10=[7,7,7,7,10,10])
    full=calculate_system_b_episodes(data)
    assert full.observations.episode_return.iloc[0]==pytest.approx(.2)
    assert full.observations.peak_return.iloc[1]==pytest.approx(.5)
    assert full.observations.drawdown_from_peak.iloc[2]==pytest.approx(-.4)
    prefix=calculate_system_b_episodes(data.iloc[:5])
    pd.testing.assert_frame_equal(prefix.observations,full.observations.iloc[:3].reset_index(drop=True))


def test_shuffle_is_deterministic_and_duplicate_input_fails_closed():
    data=frame(["BASE","CANDIDATE","ACTIVE","BASE"])
    left=calculate_system_b_episodes(data)
    right=calculate_system_b_episodes(data.sample(frac=1,random_state=7))
    pd.testing.assert_frame_equal(left.episodes,right.episodes)
    pd.testing.assert_frame_equal(left.observations,right.observations)
    with pytest.raises(SystemBEpisodeError):
        calculate_system_b_episodes(pd.concat([data,data.iloc[[0]]],ignore_index=True))


def test_previous_end_date_can_equal_next_episode_start_date():
    data=frame(["BASE","CANDIDATE","ACTIVE","BASE","CANDIDATE","ACTIVE"], closes=[8,9,12,9,9,12], ma10=[10]*6)
    result=calculate_system_b_episodes(data)
    assert len(result.episodes)==2
    assert result.episodes.iloc[0].episode_end_date==result.episodes.iloc[1].episode_start_date
    assert not result.observations.duplicated(["asset_id","trade_date"]).any()


def test_separate_episodes_are_numbered_stably_without_overlap():
    data=frame(["BASE","CANDIDATE","ACTIVE","BASE","BASE","BASE","CANDIDATE","ACTIVE"], closes=[8,9,12,9,8,11,12,13], ma10=[10]*8)
    result=calculate_system_b_episodes(data)
    assert result.episodes.episode_id.tolist()==["000001.SZ_EP_0001","000001.SZ_EP_0002"]
    assert result.episodes.iloc[0].episode_end_date < result.episodes.iloc[1].episode_start_date


def test_two_below_ma10_while_active_do_not_end():
    data=frame(["BASE","CANDIDATE","ACTIVE","ACTIVE","ACTIVE"], closes=[8,9,12,9,8], ma10=[10]*5)
    result=calculate_system_b_episodes(data)
    assert pd.isna(result.episodes.iloc[0].episode_end_date)


def test_multi_asset_episode_boundaries_are_isolated():
    first=frame(["BASE","CANDIDATE","ACTIVE","BASE","BASE"],closes=[8,9,12,9,8],ma10=[10]*5)
    second=frame(["BASE","CANDIDATE","ACTIVE"],closes=[8,9,12],ma10=[10]*3)
    second["asset_id"]="000002.SZ"
    second["trade_date"]=pd.bdate_range("2025-01-06",periods=3)
    result=calculate_system_b_episodes(pd.concat([first,second],ignore_index=True))
    assert result.episodes.episode_id.tolist()==["000001.SZ_EP_0001","000002.SZ_EP_0001"]
