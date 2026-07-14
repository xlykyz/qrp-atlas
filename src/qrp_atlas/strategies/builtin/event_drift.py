"""Basic earnings-forecast event drift strategy (long-only)."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.indicators.events.earnings_forecast import (
    DIRECTION_SCORE,
    PROFIT_CHANGE_MIDPOINT,
    attach_earnings_forecast_indicators,
)

from ..models import (
    ParameterSpec,
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyInput,
    StrategyRunResult,
    StrategyType,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
)


_ACTION_ORDER = {
    StrategyAction.EXIT: 0,
    StrategyAction.ENTER: 1,
    StrategyAction.HOLD: 2,
    StrategyAction.NO_ACTION: 3,
}


def _number(default: float, minimum: float = -1e6, maximum: float = 1e6) -> ParameterSpec:
    return ParameterSpec(
        "number",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _integer(default: int, minimum: int = 1, maximum: int = 10000) -> ParameterSpec:
    return ParameterSpec(
        "integer",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _normalize_date(value: Any) -> str:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise StrategyValidationError(f"invalid date: {value!r}")
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize().strftime("%Y-%m-%d")


def _finite(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _resolve_max_positions(runtime_context: Mapping[str, Any] | None) -> int | None:
    """Optional capacity from portfolio config via runtime_context.

    When provided, the strategy only emits ENTER for events that fit remaining
    capacity after same-day exits. This prevents delayed entry and early
    displacement that would otherwise occur if the adapter re-ranked a backlog
    of ENTER decisions later.
    """
    if not runtime_context:
        return None
    raw = runtime_context.get("max_positions")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise StrategyValidationError(f"max_positions must be a positive integer: {raw!r}") from exc
    if value <= 0:
        raise StrategyValidationError("max_positions must be positive")
    return value


class EventDriftBasicStrategy:
    """Long-only drift on positive earnings-forecast events.

    Time semantics (do not shift available_trade_date again):
    - announcement_date: event evidence date
    - available_trade_date: actual entry trade date from 05-A
    - entry price: open on available_trade_date
    - hold_days: number of legal trading days held including the entry day as day 1;
      exit is the next open after the hold window, i.e.
      ``exit_index = entry_index + hold_days``

    Capacity / weights:
    - Selection and hold windows are owned by this strategy.
    - When ``runtime_context["max_positions"]`` is provided (public runner injects
      ``PortfolioBacktestConfig.max_positions``), capacity is enforced at entry
      after same-day exits. Rejected events are diagnostics only and never enter
      the active book (no delayed entry, no early displacement of unexpired holds).
    - Concurrent equal-weight and ``max_weight_per_asset`` remain owned by
      ``strategy_decisions_to_target_weights`` / portfolio config.
    - ENTER weights are left unset so the portfolio adapter can equal-weight the
      full concurrent book.
    """

    definition = StrategyDefinition(
        code="event_drift_basic",
        name="Earnings Forecast Event Drift Basic",
        version="1.0.2",
        description=(
            "Long-only positive earnings-forecast drift. Enters on available_trade_date "
            "open, holds hold_days trading days, then exits on the next open. Enforces "
            "optional max_positions at entry (from portfolio config runtime_context); "
            "adapter owns equal-weight and max_weight_per_asset."
        ),
        strategy_type=StrategyType.BUILTIN,
        required_fields=(
            "ticker",
            "announcement_date",
            "available_trade_date",
            "forecast_type",
            "profit_change_min",
            "profit_change_max",
            "event_series_id",
            "source_record_id",
        ),
        # Event metrics are derived from EventFrame columns inside run(); they are
        # not price-panel calculation-registry indicators consumed by
        # prepare_strategy_data().
        required_indicators=(),
        parameter_schema={
            "hold_days": _integer(5, minimum=1, maximum=120),
            "min_profit_change_midpoint": _number(0.0, minimum=-1000.0, maximum=1000.0),
        },
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        hold_days = int(parameters["hold_days"])
        min_mid = float(parameters["min_profit_change_midpoint"])
        max_positions = _resolve_max_positions(strategy_input.runtime_context)

        prepared = strategy_input.prepared_data
        if prepared is None or not isinstance(prepared, pd.DataFrame):
            raise StrategyValidationError("prepared_data must be a DataFrame of events")
        events = prepared.copy()
        missing = [c for c in self.definition.required_fields if c not in events.columns]
        if missing:
            raise StrategyValidationError(f"prepared_data missing fields: {missing}")

        enriched, diagnostics = attach_earnings_forecast_indicators(events, copy=False)
        enriched[PROFIT_CHANGE_MIDPOINT] = pd.to_numeric(
            enriched[PROFIT_CHANGE_MIDPOINT], errors="coerce"
        )
        candidates = enriched[enriched[DIRECTION_SCORE] > 0].copy()
        candidates = candidates[
            candidates[PROFIT_CHANGE_MIDPOINT].map(
                lambda x: math.isfinite(float(x)) if pd.notna(x) else False
            )
        ]
        candidates = candidates[candidates[PROFIT_CHANGE_MIDPOINT] >= min_mid].copy()

        if candidates.empty:
            return StrategyRunResult(
                definition=self.definition,
                parameters=parameters,
                decisions=(),
                diagnostics=tuple(diagnostics + ["no_positive_events"]),
            )

        candidates["__entry_date"] = candidates["available_trade_date"].map(_normalize_date)
        candidates["__ann_date"] = candidates["announcement_date"].map(_normalize_date)
        candidates = candidates.sort_values(
            ["__entry_date", "ticker", "__ann_date", "source_record_id"],
            kind="mergesort",
        )
        candidates = candidates.drop_duplicates(subset=["__entry_date", "ticker"], keep="last")

        open_dates = self._resolve_open_dates(strategy_input, candidates)
        entry_dates = sorted(candidates["__entry_date"].unique())
        timeline = sorted(set(entry_dates) | set(open_dates))
        by_entry = {
            day: part.sort_values(
                [PROFIT_CHANGE_MIDPOINT, "ticker"],
                ascending=[False, True],
                kind="mergesort",
            )
            for day, part in candidates.groupby("__entry_date", sort=True)
        }

        decisions: list[StrategyDecision] = []
        active: dict[str, dict[str, Any]] = {}

        for day in timeline:
            # exits first — free capacity before same-day entries
            to_exit = [
                asset
                for asset, state in active.items()
                if state["exit_date"] is not None and state["exit_date"] == day
            ]
            for asset in sorted(to_exit):
                state = active.pop(asset)
                decisions.append(
                    StrategyDecision(
                        trade_date=day,
                        asset_id=asset,
                        action=StrategyAction.EXIT,
                        direction="long",
                        strategy_code=self.definition.code,
                        strategy_version=self.definition.version,
                        reason_code="HOLD_DAYS_REACHED",
                        score=state.get("score"),
                        weight=0.0,
                        evidence={
                            "entry_date": state["entry_date"],
                            "announcement_date": state["announcement_date"],
                            "available_trade_date": state["entry_date"],
                            "hold_days": hold_days,
                            "exit_rule": "next_open_after_hold_window",
                            "event_series_id": state.get("event_series_id"),
                            "source_record_id": state.get("source_record_id"),
                        },
                    )
                )

            day_entries = by_entry.get(day)
            if day_entries is not None and not day_entries.empty:
                remaining: int | None
                if max_positions is None:
                    remaining = None
                else:
                    remaining = max(0, max_positions - len(active))

                for _, row in day_entries.iterrows():
                    asset = str(row["ticker"])
                    score = _finite(row[PROFIT_CHANGE_MIDPOINT])
                    ann = row["__ann_date"]
                    if day <= ann:
                        diagnostics.append(
                            f"rejected_entry_not_after_announcement:{asset}:{day}"
                        )
                        continue
                    if asset in active:
                        # Already held from an earlier entry; keep existing hold window.
                        continue
                    if remaining is not None and remaining <= 0:
                        diagnostics.append(
                            f"rejected_entry_max_positions:{asset}:{day}:score={score}"
                        )
                        continue

                    exit_date = self._exit_date(day, hold_days, open_dates)
                    active[asset] = {
                        "entry_date": day,
                        "exit_date": exit_date,
                        "score": score,
                        "announcement_date": ann,
                        "event_series_id": row.get("event_series_id"),
                        "source_record_id": row.get("source_record_id"),
                    }
                    if remaining is not None:
                        remaining -= 1
                    decisions.append(
                        StrategyDecision(
                            trade_date=day,
                            asset_id=asset,
                            action=StrategyAction.ENTER,
                            direction="long",
                            strategy_code=self.definition.code,
                            strategy_version=self.definition.version,
                            reason_code="POSITIVE_FORECAST_DRIFT",
                            score=score,
                            # Portfolio adapter owns concurrent equal-weight.
                            weight=None,
                            evidence={
                                "announcement_date": ann,
                                "available_trade_date": day,
                                "entry_price_field": "open",
                                "forecast_type": row.get("forecast_type"),
                                "direction_score": int(row[DIRECTION_SCORE]),
                                "profit_change_midpoint": score,
                                "hold_days": hold_days,
                                "exit_date": exit_date,
                                "event_series_id": row.get("event_series_id"),
                                "source_record_id": row.get("source_record_id"),
                            },
                        )
                    )

            for asset in sorted(active):
                state = active[asset]
                if state["entry_date"] == day:
                    continue
                decisions.append(
                    StrategyDecision(
                        trade_date=day,
                        asset_id=asset,
                        action=StrategyAction.HOLD,
                        direction="long",
                        strategy_code=self.definition.code,
                        strategy_version=self.definition.version,
                        reason_code="IN_HOLD_WINDOW",
                        score=state.get("score"),
                        weight=None,
                        evidence={
                            "entry_date": state["entry_date"],
                            "exit_date": state.get("exit_date"),
                            "announcement_date": state["announcement_date"],
                            "available_trade_date": state["entry_date"],
                        },
                    )
                )

        # Explicit action priority so same-day EXIT free capacity before ENTER
        # for the same asset (enum value order is ENTER < HOLD < EXIT).
        decisions.sort(
            key=lambda d: (
                d.trade_date,
                d.asset_id,
                _ACTION_ORDER.get(d.action, 9),
            )
        )
        selected_entries = sum(1 for d in decisions if d.action is StrategyAction.ENTER)
        diagnostics.append(f"selected_entries={selected_entries}")
        if max_positions is not None:
            diagnostics.append(f"max_positions={max_positions}")
        diagnostics.append(
            "time_semantics:announcement_date=evidence;"
            "available_trade_date=entry;"
            "exit=next_open_after_hold_days;"
            "no_extra_next_open_on_entry;"
            "capacity_enforced_at_entry_when_runtime_max_positions;"
            "equal_weight_owned_by_portfolio_adapter"
        )
        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=tuple(decisions),
            diagnostics=tuple(diagnostics),
        )

    def _resolve_open_dates(
        self,
        strategy_input: StrategyInput,
        candidates: pd.DataFrame,
    ) -> list[str]:
        ctx = strategy_input.runtime_context or {}
        raw = ctx.get("open_dates") or ctx.get("trading_days")
        if raw is not None:
            days = sorted({_normalize_date(v) for v in raw})
            if days:
                return days
        return sorted({_normalize_date(v) for v in candidates["__entry_date"].tolist()})

    def _exit_date(self, entry_date: str, hold_days: int, open_dates: list[str]) -> str | None:
        """Exit on the next open after holding ``hold_days`` trading days.

        Entry day is day 1. Example:
        - hold_days=1, entry=D0 -> exit=D1 open
        - hold_days=5, entry=D0 -> exit=D5 open
        """
        if not open_dates:
            return None
        if entry_date not in open_dates:
            later = [d for d in open_dates if d >= entry_date]
            if not later:
                return None
            entry_date = later[0]
        idx = open_dates.index(entry_date)
        exit_idx = idx + hold_days
        if exit_idx >= len(open_dates):
            return None
        return open_dates[exit_idx]


def build_event_drift_prepared_data(
    events: pd.DataFrame,
    *,
    copy: bool = True,
) -> pd.DataFrame:
    """Helper: validate/copy EventFrame for strategy input without side effects."""
    if events is None or not isinstance(events, pd.DataFrame):
        raise StrategyValidationError("events must be a DataFrame")
    return events.copy() if copy else events
