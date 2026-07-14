"""Deterministic cross-sectional long-only selection strategies."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_trade_date,
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
from ..selection import (
    EligibilityError,
    REBALANCE_FREQUENCIES,
    RebalanceScheduleError,
    SelectionError,
    WeightConstructionError,
    build_rebalance_schedule,
    equal_weight_targets,
    select_top_n,
)
from ..selection.eligibility import (
    ELIGIBILITY_REASON_COLUMN,
    ELIGIBLE_COLUMN,
    REASON_INELIGIBLE,
    REASON_INVALID_SCORE,
    REASON_MISSING_ELIGIBILITY,
)
from ..selection.selection import (
    RANK_COLUMN,
    RESERVED_SCORE_COLUMNS,
    SCORE_COLUMN,
    SELECTED_COLUMN,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
)


def _integer(default: int, minimum: int = 1, maximum: int = 10000) -> ParameterSpec:
    return ParameterSpec(
        "integer",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _number(default: float, minimum: float = 0.0, maximum: float = 1.0) -> ParameterSpec:
    return ParameterSpec(
        "number",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _string(default: str) -> ParameterSpec:
    return ParameterSpec("string", default=default, has_default=True)


def _boolean(default: bool) -> ParameterSpec:
    return ParameterSpec("boolean", default=default, has_default=True)


def compute_composite_score(
    frame: pd.DataFrame,
    *,
    factor_columns: Sequence[str],
    factor_weights: Sequence[float] | Mapping[str, float],
) -> pd.Series:
    """Deterministic complete-case linear composite score.

    composite = sum(factor_i * weight_i) / sum(abs(weight_i))

    Any non-finite required factor makes the composite NaN for that row.
    Factor direction must already be "larger is better" before calling.
    """
    if not factor_columns:
        raise StrategyValidationError("factor_columns must be non-empty")
    missing = [column for column in factor_columns if column not in frame.columns]
    if missing:
        raise StrategyValidationError(f"prepared_data missing factor columns: {missing}")

    weights = _normalize_factor_weights(factor_columns, factor_weights)
    denom = sum(abs(weight) for weight in weights)
    if denom <= 0:
        raise StrategyValidationError("at least one factor weight must be non-zero")

    values: list[float] = []
    for _, row in frame.iterrows():
        pieces: list[float] = []
        valid = True
        for column, weight in zip(factor_columns, weights, strict=True):
            raw = row[column]
            try:
                number = float(raw)
            except (TypeError, ValueError):
                valid = False
                break
            if not math.isfinite(number):
                valid = False
                break
            pieces.append(number * weight)
        values.append((sum(pieces) / denom) if valid else math.nan)
    return pd.Series(values, index=frame.index, dtype=float)


def _normalize_factor_weights(
    factor_columns: Sequence[str],
    factor_weights: Sequence[float] | Mapping[str, float],
) -> list[float]:
    if isinstance(factor_weights, Mapping):
        weights = []
        for column in factor_columns:
            if column not in factor_weights:
                raise StrategyValidationError(
                    f"missing factor weight for column: {column!r}"
                )
            weights.append(factor_weights[column])
    else:
        weights = list(factor_weights)
        if len(weights) != len(factor_columns):
            raise StrategyValidationError(
                "factor_weights length must match factor_columns"
            )
    normalized: list[float] = []
    for weight in weights:
        try:
            number = float(weight)
        except (TypeError, ValueError) as exc:
            raise StrategyValidationError("factor weights must be finite numbers") from exc
        if not math.isfinite(number):
            raise StrategyValidationError("factor weights must be finite numbers")
        normalized.append(number)
    if not any(abs(weight) > 0 for weight in normalized):
        raise StrategyValidationError("at least one factor weight must be non-zero")
    return normalized


def _parse_jsonish(value: Any, *, name: str) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, tuple, Mapping)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise StrategyValidationError(f"{name} must be valid JSON") from exc
    raise StrategyValidationError(f"{name} must be a list/mapping/JSON string")




def _exit_eligibility_reason(row: Any | None) -> str:
    """Map a same-day selection row into a precise EXIT audit reason."""
    if row is None:
        return "MISSING_SIGNAL_ROW"
    reason = getattr(row, ELIGIBILITY_REASON_COLUMN, None)
    eligible = bool(getattr(row, ELIGIBLE_COLUMN, False))
    selected = bool(getattr(row, SELECTED_COLUMN, False))
    rank = getattr(row, RANK_COLUMN, None)
    if reason == REASON_INVALID_SCORE:
        return "INVALID_SCORE"
    if reason == REASON_MISSING_ELIGIBILITY:
        return "MISSING_ELIGIBILITY"
    if reason == REASON_INELIGIBLE or (reason not in (None, "ELIGIBLE", "OK") and not eligible):
        # Preserve explicit non-eligible panel reasons under INELIGIBLE bucket only
        # when the panel marked the asset ineligible.
        if not eligible:
            return "INELIGIBLE"
    if not eligible:
        return "INELIGIBLE"
    if rank is not None and pd.notna(rank) and not selected:
        return "NOT_TOP_N"
    if not selected:
        return "NOT_TOP_N"
    return "NOT_TOP_N"


class _CrossSectionalLongOnlyBase:
    """Shared Top-N long-only machinery for cross-sectional strategies."""

    definition: StrategyDefinition
    default_score_column: str

    def __init__(self) -> None:
        validate_definition(self.definition)

    def _prepare_score_frame(
        self,
        prepared: pd.DataFrame,
        parameters: Mapping[str, Any],
    ) -> tuple[pd.DataFrame, str, dict[str, Any]]:
        """Return score frame, working score column, and evidence extras."""
        raise NotImplementedError

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        prepared = self._validate_prepared(strategy_input.prepared_data)
        score_frame, score_column, evidence_extras = self._prepare_score_frame(
            prepared, parameters
        )

        top_n = int(parameters["top_n"])
        max_positions = int(parameters["max_positions"])
        max_weight_per_asset = float(parameters["max_weight_per_asset"])
        cash_buffer = float(parameters["cash_buffer"])
        ascending = bool(parameters["ascending"])
        frequency = str(parameters["rebalance_frequency"])
        if frequency not in REBALANCE_FREQUENCIES:
            raise StrategyValidationError(
                f"unsupported rebalance_frequency: {frequency!r}"
            )
        if not 0.0 <= cash_buffer < 1.0:
            raise StrategyValidationError("cash_buffer must be in [0, 1)")
        if not 0.0 < max_weight_per_asset <= 1.0:
            raise StrategyValidationError("max_weight_per_asset must be in (0, 1]")
        if max_positions < 1:
            raise StrategyValidationError("max_positions must be >= 1")
        if top_n < 1:
            raise StrategyValidationError("top_n must be >= 1")

        trading_days = self._resolve_trading_days(strategy_input, score_frame)
        explicit_dates = self._resolve_explicit_dates(parameters, strategy_input)
        try:
            schedule = build_rebalance_schedule(
                trading_days,
                frequency=frequency,  # type: ignore[arg-type]
                explicit_dates=explicit_dates,
            )
        except RebalanceScheduleError as exc:
            raise StrategyValidationError(str(exc)) from exc

        eligibility = self._resolve_eligibility(strategy_input)
        decisions = self._build_decisions(
            score_frame=score_frame,
            score_column=score_column,
            schedule=schedule,
            eligibility=eligibility,
            top_n=top_n,
            max_positions=max_positions,
            max_weight_per_asset=max_weight_per_asset,
            cash_buffer=cash_buffer,
            ascending=ascending,
            frequency=frequency,
            initial_positions=strategy_input.initial_positions,
            evidence_extras=evidence_extras,
        )
        diagnostics: list[str] = []
        if schedule.empty:
            diagnostics.append("empty_rebalance_schedule")
        return StrategyRunResult(
            self.definition,
            parameters,
            tuple(decisions),
            tuple(diagnostics),
        )

    def _validate_prepared(self, prepared: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(prepared, pd.DataFrame):
            raise StrategyValidationError("prepared_data must be a pandas DataFrame")
        frame = prepared.copy()
        if ASSET_ID not in frame.columns and "ticker" in frame.columns:
            frame[ASSET_ID] = frame["ticker"]
        if "ticker" not in frame.columns and ASSET_ID in frame.columns:
            frame["ticker"] = frame[ASSET_ID]
        if TRADE_DATE not in frame.columns:
            raise StrategyValidationError("prepared_data must include trade_date")
        if ASSET_ID not in frame.columns:
            raise StrategyValidationError(
                "prepared_data must include asset_id or ticker"
            )
        try:
            return ensure_cross_section_frame(frame, copy=True, enforce_primary_key=True)
        except CrossSectionFrameError as exc:
            raise StrategyValidationError(str(exc)) from exc

    def _resolve_trading_days(
        self,
        strategy_input: StrategyInput,
        score_frame: pd.DataFrame,
    ) -> list[Any]:
        context = strategy_input.runtime_context or {}
        if "trading_days" in context and context["trading_days"] is not None:
            return list(context["trading_days"])
        return sorted(
            {normalize_trade_date(value) for value in score_frame[TRADE_DATE].tolist()}
        )

    def _resolve_explicit_dates(
        self,
        parameters: Mapping[str, Any],
        strategy_input: StrategyInput,
    ) -> list[Any] | None:
        context = strategy_input.runtime_context or {}
        if "explicit_dates" in context and context["explicit_dates"] is not None:
            return list(context["explicit_dates"])
        raw = parameters.get("explicit_dates_json")
        parsed = _parse_jsonish(raw, name="explicit_dates_json")
        if parsed is None:
            return None
        if not isinstance(parsed, (list, tuple)):
            raise StrategyValidationError("explicit_dates_json must be a JSON list")
        return list(parsed)

    def _resolve_eligibility(
        self,
        strategy_input: StrategyInput,
    ) -> pd.DataFrame | None:
        context = strategy_input.runtime_context or {}
        eligibility = context.get("eligibility")
        if eligibility is None:
            return None
        if not isinstance(eligibility, pd.DataFrame):
            raise StrategyValidationError(
                "runtime_context['eligibility'] must be a pandas DataFrame"
            )
        return eligibility

    def _build_decisions(
        self,
        *,
        score_frame: pd.DataFrame,
        score_column: str,
        schedule: pd.DataFrame,
        eligibility: pd.DataFrame | None,
        top_n: int,
        max_positions: int,
        max_weight_per_asset: float,
        cash_buffer: float,
        ascending: bool,
        frequency: str,
        initial_positions: Mapping[str, bool],
        evidence_extras: Mapping[str, Any],
    ) -> list[StrategyDecision]:
        if schedule.empty:
            return []

        signal_dates = {
            normalize_trade_date(value) for value in schedule["signal_date"].tolist()
        }
        available = score_frame[
            score_frame[TRADE_DATE].map(normalize_trade_date).isin(signal_dates)
        ].copy()

        try:
            selection = select_top_n(
                available,
                n=top_n,
                score_column=score_column,
                ascending=ascending,
                eligibility=eligibility,
            )
        except (SelectionError, EligibilityError) as exc:
            raise StrategyValidationError(str(exc)) from exc

        previous_selected: set[str] = {
            str(asset_id)
            for asset_id, held in (initial_positions or {}).items()
            if held
        }
        decisions: list[StrategyDecision] = []
        display_score_column = str(
            evidence_extras.get("score_column", score_column)
        )

        for row in schedule.itertuples(index=False):
            signal_date = normalize_trade_date(row.signal_date)
            trade_date = normalize_trade_date(row.trade_date)
            day_selection = selection[
                selection[TRADE_DATE].map(normalize_trade_date) == signal_date
            ]
            selected = day_selection[day_selection[SELECTED_COLUMN].astype(bool)].copy()
            if not selected.empty and len(selected) > max_positions:
                selected = selected.sort_values(
                    [RANK_COLUMN, ASSET_ID],
                    kind="mergesort",
                ).head(max_positions)

            selected_ids = selected[ASSET_ID].astype(str).tolist()
            scores = {
                str(asset_id): float(score)
                for asset_id, score in zip(
                    selected[ASSET_ID].tolist(),
                    selected[SCORE_COLUMN].tolist(),
                    strict=True,
                )
                if score is not None and pd.notna(score)
            }
            ranks = {
                str(asset_id): int(rank)
                for asset_id, rank in zip(
                    selected[ASSET_ID].tolist(),
                    selected[RANK_COLUMN].tolist(),
                    strict=True,
                )
                if rank is not None and pd.notna(rank)
            }
            try:
                weights = equal_weight_targets(
                    selected_ids,
                    trade_date=trade_date,
                    scores=scores,
                    ranks=ranks,
                    top_n=top_n,
                    max_positions=max_positions,
                    max_weight_per_asset=max_weight_per_asset,
                    cash_buffer=cash_buffer,
                )
            except WeightConstructionError as exc:
                raise StrategyValidationError(str(exc)) from exc
            weight_map = {
                str(item["asset_id"]): float(item["target_weight"])
                for item in weights.to_dict("records")
            }

            current_selected = set(selected_ids)
            selected_rows = selected.sort_values(
                [RANK_COLUMN, ASSET_ID], kind="mergesort"
            )
            for item in selected_rows.itertuples(index=False):
                asset_id = str(getattr(item, ASSET_ID))
                score = getattr(item, SCORE_COLUMN)
                rank = getattr(item, RANK_COLUMN)
                eligible = bool(getattr(item, ELIGIBLE_COLUMN))
                reason = getattr(item, ELIGIBILITY_REASON_COLUMN)
                action = (
                    StrategyAction.HOLD
                    if asset_id in previous_selected
                    else StrategyAction.ENTER
                )
                reason_code = (
                    "CROSS_SECTION_HOLD"
                    if action is StrategyAction.HOLD
                    else "CROSS_SECTION_ENTER"
                )
                rank_value = None if rank is None or pd.isna(rank) else int(rank)
                evidence: dict[str, Any] = {
                    "signal_date": signal_date.strftime("%Y-%m-%d"),
                    "execution_trade_date": trade_date.strftime("%Y-%m-%d"),
                    "score_column": display_score_column,
                    "score": None if score is None or pd.isna(score) else float(score),
                    "rank": rank_value,
                    "priority": None if rank_value is None else float(-rank_value),
                    "top_n": top_n,
                    "max_positions": max_positions,
                    "cash_buffer": cash_buffer,
                    "max_weight_per_asset": max_weight_per_asset,
                    "rebalance_frequency": frequency,
                    "eligible": eligible,
                    "eligibility_reason": reason,
                    "ascending": ascending,
                }
                evidence.update(dict(evidence_extras))
                decisions.append(
                    StrategyDecision(
                        trade_date=trade_date.strftime("%Y-%m-%d"),
                        asset_id=asset_id,
                        action=action,
                        direction="long",
                        strategy_code=self.definition.code,
                        strategy_version=self.definition.version,
                        reason_code=reason_code,
                        score=None if score is None or pd.isna(score) else float(score),
                        weight=weight_map.get(asset_id),
                        evidence=evidence,
                    )
                )

            day_lookup = {
                str(getattr(item, ASSET_ID)): item
                for item in day_selection.itertuples(index=False)
            }
            for asset_id in sorted(previous_selected - current_selected):
                source = day_lookup.get(asset_id)
                exit_reason = _exit_eligibility_reason(source)
                source_score = None
                source_rank = None
                source_eligible = False
                if source is not None:
                    raw_score = getattr(source, SCORE_COLUMN, None)
                    if raw_score is not None and pd.notna(raw_score):
                        source_score = float(raw_score)
                    raw_rank = getattr(source, RANK_COLUMN, None)
                    if raw_rank is not None and pd.notna(raw_rank):
                        source_rank = int(raw_rank)
                    source_eligible = bool(getattr(source, ELIGIBLE_COLUMN, False))
                evidence = {
                    "signal_date": signal_date.strftime("%Y-%m-%d"),
                    "execution_trade_date": trade_date.strftime("%Y-%m-%d"),
                    "score_column": display_score_column,
                    "score": source_score,
                    "rank": source_rank,
                    "top_n": top_n,
                    "max_positions": max_positions,
                    "cash_buffer": cash_buffer,
                    "max_weight_per_asset": max_weight_per_asset,
                    "rebalance_frequency": frequency,
                    "eligible": source_eligible,
                    "eligibility_reason": exit_reason,
                    "ascending": ascending,
                }
                evidence.update(dict(evidence_extras))
                decisions.append(
                    StrategyDecision(
                        trade_date=trade_date.strftime("%Y-%m-%d"),
                        asset_id=asset_id,
                        action=StrategyAction.EXIT,
                        direction="long",
                        strategy_code=self.definition.code,
                        strategy_version=self.definition.version,
                        reason_code="CROSS_SECTION_EXIT",
                        score=source_score,
                        weight=0.0,
                        evidence=evidence,
                    )
                )
            previous_selected = current_selected

        action_order = {
            StrategyAction.EXIT: 0,
            StrategyAction.ENTER: 1,
            StrategyAction.HOLD: 2,
            StrategyAction.NO_ACTION: 3,
        }
        decisions.sort(
            key=lambda item: (
                item.trade_date,
                item.asset_id,
                action_order.get(item.action, 9),
            )
        )
        return decisions


class CrossSectionalMomentumLongOnlyStrategy(_CrossSectionalLongOnlyBase):
    """Long-only Top-N strategy on a prepared cross-sectional momentum score."""

    default_score_column = "momentum"
    definition = StrategyDefinition(
        code="cross_sectional_momentum_long_only",
        name="Cross-Sectional Momentum Long Only",
        version="1.0.0",
        description=(
            "Select Top-N assets by a prepared cross-sectional momentum score "
            "on rebalance signal dates and execute long-only equal weights on "
            "the next trading day."
        ),
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TRADE_DATE,),
        required_indicators=(),
        parameter_schema={
            "top_n": _integer(10, 1),
            "max_positions": _integer(10, 1),
            "max_weight_per_asset": _number(0.1, 0.000001, 1.0),
            "cash_buffer": _number(0.0, 0.0, 0.999999),
            "momentum_lookback": _integer(20, 1, 500),
            "score_column": _string("momentum"),
            "ascending": _boolean(False),
            "rebalance_frequency": _string("weekly"),
            "explicit_dates_json": _string(""),
        },
        indicator_requests=(),
    )

    def _prepare_score_frame(
        self,
        prepared: pd.DataFrame,
        parameters: Mapping[str, Any],
    ) -> tuple[pd.DataFrame, str, dict[str, Any]]:
        score_column = str(parameters.get("score_column") or self.default_score_column)
        if score_column in RESERVED_SCORE_COLUMNS:
            raise StrategyValidationError(
                f"score_column {score_column!r} conflicts with reserved selection fields"
            )
        if score_column not in prepared.columns:
            raise StrategyValidationError(
                f"prepared_data missing score column: {score_column!r}"
            )
        frame = prepared.copy()
        frame[SCORE_COLUMN] = pd.to_numeric(frame[score_column], errors="coerce")
        return frame, SCORE_COLUMN, {"score_column": score_column}


class MultifactorLongOnlyStrategy(_CrossSectionalLongOnlyBase):
    """Long-only Top-N strategy on a deterministic multifactor composite score."""

    default_score_column = "composite_score"
    definition = StrategyDefinition(
        code="multifactor_long_only",
        name="Multifactor Long Only",
        version="1.0.0",
        description=(
            "Combine prepared cross-sectional factor scores with explicit "
            "complete-case linear weights, select Top-N, and execute long-only "
            "equal weights on the next trading day."
        ),
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TRADE_DATE,),
        required_indicators=(),
        parameter_schema={
            "top_n": _integer(10, 1),
            "max_positions": _integer(10, 1),
            "max_weight_per_asset": _number(0.1, 0.000001, 1.0),
            "cash_buffer": _number(0.0, 0.0, 0.999999),
            "factor_columns_json": _string('["momentum"]'),
            "factor_weights_json": _string("[1.0]"),
            "ascending": _boolean(False),
            "rebalance_frequency": _string("weekly"),
            "explicit_dates_json": _string(""),
        },
        indicator_requests=(),
    )

    def _prepare_score_frame(
        self,
        prepared: pd.DataFrame,
        parameters: Mapping[str, Any],
    ) -> tuple[pd.DataFrame, str, dict[str, Any]]:
        factor_columns_raw = _parse_jsonish(
            parameters.get("factor_columns_json"),
            name="factor_columns_json",
        )
        factor_weights_raw = _parse_jsonish(
            parameters.get("factor_weights_json"),
            name="factor_weights_json",
        )
        if not factor_columns_raw:
            raise StrategyValidationError("factor_columns_json must be non-empty")
        if not isinstance(factor_columns_raw, (list, tuple)):
            raise StrategyValidationError("factor_columns_json must be a JSON list")
        factor_columns = [str(column) for column in factor_columns_raw]

        if factor_weights_raw is None:
            raise StrategyValidationError("factor_weights_json must be non-empty")
        if isinstance(factor_weights_raw, Mapping):
            factor_weights: Sequence[float] | Mapping[str, float] = {
                str(key): float(value) for key, value in factor_weights_raw.items()
            }
        elif isinstance(factor_weights_raw, (list, tuple)):
            factor_weights = [float(value) for value in factor_weights_raw]
        else:
            raise StrategyValidationError(
                "factor_weights_json must be a JSON list or object"
            )

        frame = prepared.copy()
        frame[SCORE_COLUMN] = compute_composite_score(
            frame,
            factor_columns=factor_columns,
            factor_weights=factor_weights,
        )
        extras = {
            "score_column": SCORE_COLUMN,
            "factor_columns": factor_columns,
            "factor_weights": (
                dict(factor_weights)
                if isinstance(factor_weights, Mapping)
                else list(factor_weights)
            ),
            "composite_method": "complete_case_linear",
        }
        return frame, SCORE_COLUMN, extras
