"""Basic earnings-forecast event drift strategy (long-only)."""

from __future__ import annotations

import math
from collections.abc import Mapping
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


class EventDriftBasicStrategy:
    """Long-only drift on positive earnings-forecast events.

    Time semantics (do not shift again):
    - announcement_date: evidence date
    - available_trade_date: actual entry trade date (already next open day from 05-A)
    - entry price: open on available_trade_date (portfolio engine price_field)
    - hold_days: number of legal trading days held, including entry day as day 1
      when converting targets to exits at rebalance/exit generation time.
    """

    definition = StrategyDefinition(
        code="event_drift_basic",
        name="Earnings Forecast Event Drift Basic",
        version="1.0.0",
        description=(
            "Long-only positive earnings-forecast drift. Enters on available_trade_date "
            "open, holds hold_days trading days, equal-weights concurrent names."
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
        required_indicators=(DIRECTION_SCORE, PROFIT_CHANGE_MIDPOINT),
        parameter_schema={
            "hold_days": _integer(5, minimum=1, maximum=120),
            "min_profit_change_midpoint": _number(0.0, minimum=-1000.0, maximum=1000.0),
            "max_positions": _integer(50, minimum=1, maximum=5000),
            "max_weight_per_asset": _number(1.0, minimum=0.0001, maximum=1.0),
        },
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        hold_days = int(parameters["hold_days"])
        min_mid = float(parameters["min_profit_change_midpoint"])
        max_positions = int(parameters["max_positions"])

        prepared = strategy_input.prepared_data
        if prepared is None or not isinstance(prepared, pd.DataFrame):
            raise StrategyValidationError("prepared_data must be a DataFrame of events")
        # Do not mutate caller frame.
        events = prepared.copy()
        missing = [c for c in self.definition.required_fields if c not in events.columns]
        if missing:
            raise StrategyValidationError(f"prepared_data missing fields: {missing}")

        enriched, diagnostics = attach_earnings_forecast_indicators(events, copy=False)

        # Keep only positive directional forecasts.
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

        # Same ticker same trade_date: keep deterministic latest formal disclosure.
        candidates["__entry_date"] = candidates["available_trade_date"].map(_normalize_date)
        candidates["__ann_date"] = candidates["announcement_date"].map(_normalize_date)
        candidates = candidates.sort_values(
            ["__entry_date", "ticker", "__ann_date", "source_record_id"],
            kind="mergesort",
        )
        candidates = candidates.drop_duplicates(subset=["__entry_date", "ticker"], keep="last")

        # Capacity: equal-weight among same-day entries, capped by max_positions.
        decisions: list[StrategyDecision] = []
        active: dict[str, dict[str, Any]] = {}
        open_dates = self._resolve_open_dates(strategy_input, candidates)

        # Process chronological entry dates and manage exits by hold_days.
        entry_dates = sorted(candidates["__entry_date"].unique())
        # Include open_dates after last entry for exits.
        timeline = sorted(set(entry_dates) | set(open_dates))
        # Map for next-day iteration
        if not timeline:
            timeline = entry_dates

        by_entry = {
            day: part.sort_values(["ticker"], kind="mergesort")
            for day, part in candidates.groupby("__entry_date", sort=True)
        }

        date_index = {day: i for i, day in enumerate(timeline)}
        for day in timeline:
            # exits first (same-day rebalance friendly)
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
                            "event_series_id": state.get("event_series_id"),
                            "source_record_id": state.get("source_record_id"),
                        },
                    )
                )

            # entries
            day_entries = by_entry.get(day)
            if day_entries is not None and not day_entries.empty:
                # equal weight among today's selected entries that fit capacity
                remaining = max(0, max_positions - len(active))
                selected = day_entries.head(remaining) if remaining else day_entries.iloc[0:0]
                if remaining == 0:
                    diagnostics.append(f"max_positions_reached_on_{day}")
                n_sel = int(len(selected))
                weight = (1.0 / n_sel) if n_sel else None
                for _, row in selected.iterrows():
                    asset = str(row["ticker"])
                    score = _finite(row[PROFIT_CHANGE_MIDPOINT])
                    ann = row["__ann_date"]
                    # Hard safety: entry must be after announcement evidence date.
                    if day <= ann:
                        diagnostics.append(f"rejected_entry_not_after_announcement:{asset}:{day}")
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
                            weight=weight,
                            evidence={
                                "announcement_date": ann,
                                "available_trade_date": day,
                                "entry_price_field": "open",
                                "forecast_type": row.get("forecast_type"),
                                "direction_score": int(row[DIRECTION_SCORE]),
                                "profit_change_midpoint": score,
                                "hold_days": hold_days,
                                "event_series_id": row.get("event_series_id"),
                                "source_record_id": row.get("source_record_id"),
                            },
                        )
                    )

            # holds
            for asset in sorted(active):
                state = active[asset]
                if state["entry_date"] == day:
                    continue  # entered today
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

        decisions.sort(key=lambda d: (d.trade_date, d.asset_id, d.action.value))
        selected_entries = sum(1 for d in decisions if d.action is StrategyAction.ENTER)
        diagnostics.append(f"selected_entries={selected_entries}")
        diagnostics.append("time_semantics:announcement_date=evidence;available_trade_date=entry;no_extra_next_open")
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
        # Fallback: use entry dates only (exits may become None and rely on later data).
        return sorted({_normalize_date(v) for v in candidates["__entry_date"].tolist()})

    def _exit_date(self, entry_date: str, hold_days: int, open_dates: list[str]) -> str | None:
        if not open_dates:
            return None
        if entry_date not in open_dates:
            # insert logically: find first open_date >= entry
            later = [d for d in open_dates if d >= entry_date]
            if not later:
                return None
            entry_date = later[0]
        idx = open_dates.index(entry_date)
        exit_idx = idx + (hold_days - 1)
        if exit_idx >= len(open_dates):
            return None
        # Exit decision emitted on the exit day; holding window includes entry as day 1.
        # For portfolio targets, EXIT weight 0 is applied on that day.
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
