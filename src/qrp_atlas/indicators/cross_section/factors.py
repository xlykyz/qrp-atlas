"""Formal cross-sectional factor definitions and generation (task 04-B).

This module produces raw factor values for historical stock pools. It does not
implement rank / winsorize / z-score (reuse 04-A), neutralization, Top-N,
weights, or backtest strategies.

Architecture boundary:

```text
contracts -> indicators -> backtest
```

Indicators consume already-prepared panels only. Point-in-time financial
version selection and DuckDB access live in ``qrp_atlas.backtest.factor_data``
(``prepare_financial_factor_panel``). This module never imports backtest and
never opens a database.

Factor frame contract (aligned with 04-A):

- columns include ``trade_date``, ``asset_id``, and named factor columns;
- ``trade_date`` is timezone-naive midnight;
- ``(trade_date, asset_id)`` is unique and non-null;
- outputs are stably sorted by ``(trade_date, asset_id)``;
- caller inputs are never mutated;
- empty universes return empty frames with stable columns;
- illegal / missing values are NaN (never silently filled with zero).

Layers of data:

- raw market / financial fields (inputs / prepared panels);
- raw factor values (this module);
- cross-sectionally standardized scores (04-A operators).
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    BPS,
    CIRC_MV,
    CLOSE,
    FLOAT_CAP,
    MARKET_CAP,
    ROE,
    TICKER,
    TOTAL_MV,
    TRADE_DATE,
)
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    empty_cross_section_frame,
    ensure_cross_section_frame,
    normalize_asset_id,
    normalize_trade_date,
    normalize_trade_dates,
    sort_cross_section_frame,
)
from qrp_atlas.indicators.cross_section.universe import build_historical_universe

_SIZE_FIELDS: tuple[str, ...] = (MARKET_CAP, FLOAT_CAP, TOTAL_MV, CIRC_MV)
_PRICE_ID_CANDIDATES: tuple[str, ...] = (ASSET_ID, TICKER)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_OUTPUT_COLUMNS: frozenset[str] = frozenset({TRADE_DATE, ASSET_ID})


class FactorError(CrossSectionFrameError):
    """Base error for factor definition / generation failures."""


class UnknownFactorError(FactorError):
    """Raised when a factor code is not registered."""


class FactorRequestError(FactorError):
    """Raised when a factor request or input panel is invalid."""


@dataclass(frozen=True)
class FactorParameterSpec:
    """Validation rules for one factor parameter."""

    type: str
    default: Any = None
    has_default: bool = False
    minimum: float | None = None
    maximum: float | None = None
    choices: tuple[Any, ...] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "default": self.default,
            "has_default": self.has_default,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "choices": list(self.choices) if self.choices is not None else None,
        }


@dataclass(frozen=True)
class FactorRequest:
    """Stable request for one parameterized factor instance."""

    code: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    alias: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "parameters": dict(sorted(self.parameters.items())),
            "alias": self.alias,
        }


@dataclass(frozen=True)
class FactorDefinition:
    """Public metadata for one raw factor family."""

    code: str
    name: str
    family: Literal["momentum", "size", "fundamental"]
    description: str
    formula: str
    direction: str
    time_semantics: str
    inputs: tuple[str, ...]
    parameter_schema: Mapping[str, FactorParameterSpec]
    default_output: str
    nan_semantics: str


@dataclass(frozen=True)
class ResolvedFactorRequest:
    request: FactorRequest
    definition: FactorDefinition
    parameters: Mapping[str, Any]
    output_column: str


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise FactorRequestError(f"{label} must be a stable identifier: {value!r}")


def _validate_output_column(value: str) -> None:
    _validate_identifier(value, "factor output column")
    if value in _RESERVED_OUTPUT_COLUMNS:
        raise FactorRequestError(
            f"factor output column {value!r} is reserved; "
            f"cannot use {sorted(_RESERVED_OUTPUT_COLUMNS)}"
        )


def _stable_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return format(value, ".12g").replace("-", "neg_").replace(".", "_")
    return str(value).replace("-", "neg_").replace(".", "_")


def _default_alias(
    code: str,
    parameters: Mapping[str, Any],
    schema: Mapping[str, FactorParameterSpec],
) -> str:
    """Build a stable output name; omit params that equal schema defaults."""
    parts: list[str] = []
    for key in sorted(parameters):
        value = parameters[key]
        spec = schema.get(key)
        if spec is not None and spec.has_default and value == spec.default:
            continue
        parts.append(f"{key}_{_stable_value(value)}")
    return f"{code}_{'_'.join(parts)}" if parts else code


def _validate_parameter(code: str, name: str, value: Any, spec: FactorParameterSpec) -> None:
    if spec.type == "integer":
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif spec.type == "number":
        valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif spec.type == "string":
        valid = isinstance(value, str)
    elif spec.type == "boolean":
        valid = isinstance(value, bool)
    else:
        raise FactorRequestError(f"factor {code!r} has invalid parameter schema for {name!r}")
    if not valid:
        raise FactorRequestError(f"factor {code!r} parameter {name!r} must be {spec.type}")
    if spec.choices is not None and value not in spec.choices:
        raise FactorRequestError(
            f"factor {code!r} parameter {name!r} must be one of {list(spec.choices)}; got {value!r}"
        )
    if spec.type in {"integer", "number"}:
        numeric = float(value)
        if not math.isfinite(numeric):
            raise FactorRequestError(f"factor {code!r} parameter {name!r} must be finite")
        if spec.minimum is not None and numeric < spec.minimum:
            raise FactorRequestError(
                f"factor {code!r} parameter {name!r} is below minimum {spec.minimum}"
            )
        if spec.maximum is not None and numeric > spec.maximum:
            raise FactorRequestError(
                f"factor {code!r} parameter {name!r} is above maximum {spec.maximum}"
            )


def _as_finite_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def _finite(x: Any) -> bool:
        return bool(pd.notna(x) and math.isfinite(float(x)))

    return values.where(values.map(_finite))


def _resolve_id_column(df: pd.DataFrame, *, label: str) -> str:
    for candidate in _PRICE_ID_CANDIDATES:
        if candidate in df.columns:
            return candidate
    raise FactorRequestError(
        f"{label} requires an asset identifier column ({ASSET_ID} or {TICKER})"
    )


def _reject_duplicate_keys(df: pd.DataFrame, *, label: str) -> None:
    """Raise when (trade_date, asset_id) is duplicated in an input panel."""
    if df is None or df.empty:
        return
    if TRADE_DATE not in df.columns or ASSET_ID not in df.columns:
        return
    duplicated = df.duplicated(subset=[TRADE_DATE, ASSET_ID], keep=False)
    if bool(duplicated.any()):
        sample = (
            df.loc[duplicated, [TRADE_DATE, ASSET_ID]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise FactorRequestError(
            f"{label} contains duplicate (trade_date, asset_id) keys: {sample}"
        )


def _normalize_panel_keys(
    df: pd.DataFrame,
    *,
    label: str,
    required_value_columns: Sequence[str] = (),
) -> pd.DataFrame:
    """Normalize a date/asset panel and reject duplicate keys."""
    if df is None:
        raise FactorRequestError(f"{label} is required")
    if not isinstance(df, pd.DataFrame):
        raise FactorRequestError(f"{label} must be a pandas DataFrame")
    if df.empty:
        out = df.copy()
        if ASSET_ID not in out.columns and TICKER in out.columns:
            out[ASSET_ID] = out[TICKER].astype(str)
        if TRADE_DATE in out.columns:
            out[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
        return out

    missing = [c for c in required_value_columns if c not in df.columns]
    if missing:
        raise FactorRequestError(f"{label} missing required columns: {missing}")
    if TRADE_DATE not in df.columns:
        raise FactorRequestError(f"{label} missing required column: {TRADE_DATE!r}")

    id_col = _resolve_id_column(df, label=label)
    out = df.copy()
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    _reject_duplicate_keys(out, label=label)
    return out


def _universe_from_inputs(
    *,
    trade_dates: Sequence[Any] | Any | None,
    asset_ids: Sequence[str] | None,
    universe: pd.DataFrame | None,
) -> pd.DataFrame:
    if universe is not None:
        return ensure_cross_section_frame(universe, enforce_primary_key=True)
    dates = normalize_trade_dates(trade_dates) if trade_dates is not None else []
    return build_historical_universe(dates, asset_ids=asset_ids, source="explicit")


def _attach_nan_column(universe: pd.DataFrame, column: str) -> pd.DataFrame:
    out = universe[[TRADE_DATE, ASSET_ID]].copy()
    out[column] = math.nan
    return sort_cross_section_frame(out)


def compute_momentum_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 20,
    output_column: str = "momentum",
    price_column: str = CLOSE,
) -> pd.DataFrame:
    """Trailing N-bar return including the target close.

    Formula:
        momentum = close[T] / close[T - lookback] - 1

    Implementation is a standard per-asset N-bar row shift. The window requires
    ``lookback + 1`` bars in the asset's ordered history. Intermediate missing
    closes still occupy bar positions; only the two endpoints must be finite and
    strictly positive for a non-NaN result.

    Time semantics:
        Uses only bars with ``trade_date <= T`` for each target date T. The
        target bar's close is included. Values are therefore known only after
        the close of T and are intended for execution on T+1 or later.
    """
    if not isinstance(lookback, int) or isinstance(lookback, bool) or lookback < 1:
        raise FactorRequestError(
            f"momentum lookback must be a positive integer; got {lookback!r}"
        )

    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return empty_cross_section_frame([output_column])

    panel = _normalize_panel_keys(
        prices, label="prices", required_value_columns=[price_column]
    )
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    max_date = uni[TRADE_DATE].max()
    panel = panel.loc[panel[TRADE_DATE] <= max_date].copy()
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    needed_assets = set(uni[ASSET_ID].tolist())
    panel = panel.loc[panel[ASSET_ID].isin(needed_assets)].copy()
    # Keep bar positions even when closes are invalid; only endpoints matter.
    panel[price_column] = pd.to_numeric(panel[price_column], errors="coerce")
    panel.loc[panel[price_column] <= 0, price_column] = math.nan
    panel = panel.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort")

    def _one_asset(group: pd.DataFrame) -> pd.DataFrame:
        closes = group[price_column]
        lagged = closes.shift(lookback)
        values = closes.div(lagged).sub(1.0)
        values = values.replace([math.inf, -math.inf], math.nan)
        return pd.DataFrame(
            {
                TRADE_DATE: group[TRADE_DATE].to_numpy(),
                ASSET_ID: group[ASSET_ID].to_numpy(),
                output_column: values.to_numpy(),
            }
        )

    pieces = [_one_asset(g) for _, g in panel.groupby(ASSET_ID, sort=False)]
    if not pieces:
        return _attach_nan_column(uni, output_column)
    computed = pd.concat(pieces, ignore_index=True)
    computed = ensure_cross_section_frame(
        computed,
        feature_columns=[output_column],
        enforce_primary_key=True,
    )
    out = uni[[TRADE_DATE, ASSET_ID]].merge(
        computed[[TRADE_DATE, ASSET_ID, output_column]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    out[output_column] = _as_finite_series(out[output_column])
    return sort_cross_section_frame(out)


def compute_log_market_cap_factor(
    size_panel: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    field: str = MARKET_CAP,
    output_column: str = "log_market_cap",
) -> pd.DataFrame:
    """Natural log of a same-day available market-cap field.

    Formula:
        log_market_cap = ln(field[T])

    Only strictly positive finite values are valid. Zero, negative, NaN and
    infinite inputs become NaN. Uses only the market-cap record for the same
    ``trade_date`` (no forward fill from future days).
    """
    if field not in _SIZE_FIELDS:
        raise FactorRequestError(
            f"log_market_cap field must be one of {list(_SIZE_FIELDS)}; got {field!r}"
        )

    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return empty_cross_section_frame([output_column])

    panel = _normalize_panel_keys(
        size_panel, label="size_panel", required_value_columns=[field]
    )
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    max_date = uni[TRADE_DATE].max()
    panel = panel.loc[panel[TRADE_DATE] <= max_date].copy()
    needed_assets = set(uni[ASSET_ID].tolist())
    panel = panel.loc[panel[ASSET_ID].isin(needed_assets)].copy()
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    panel = panel.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort")
    raw = _as_finite_series(panel[field])
    log_values = raw.where(raw > 0).map(
        lambda x: math.log(float(x)) if pd.notna(x) else math.nan
    )
    computed = pd.DataFrame(
        {
            TRADE_DATE: panel[TRADE_DATE].to_numpy(),
            ASSET_ID: panel[ASSET_ID].to_numpy(),
            output_column: log_values.to_numpy(),
        }
    )
    computed = ensure_cross_section_frame(
        computed,
        feature_columns=[output_column],
        enforce_primary_key=True,
    )
    out = uni[[TRADE_DATE, ASSET_ID]].merge(
        computed[[TRADE_DATE, ASSET_ID, output_column]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    out[output_column] = _as_finite_series(out[output_column])
    return sort_cross_section_frame(out)


def _normalize_financial_panel(financial_panel: pd.DataFrame) -> pd.DataFrame:
    """Validate a prepared financial factor panel.

    Expected columns:
        trade_date, asset_id, and one or both of roe / bps.

    This is not a versioned statement table. PIT selection must already have
    been performed by backtest data preparation.
    """
    if financial_panel is None:
        raise FactorRequestError("financial_panel is required")
    if not isinstance(financial_panel, pd.DataFrame):
        raise FactorRequestError("financial_panel must be a pandas DataFrame")
    if financial_panel.empty:
        out = financial_panel.copy()
        if ASSET_ID not in out.columns and TICKER in out.columns:
            out[ASSET_ID] = out[TICKER].astype(str)
        if TRADE_DATE in out.columns:
            out[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
        return out

    if TRADE_DATE not in financial_panel.columns:
        raise FactorRequestError("financial_panel missing required column: 'trade_date'")
    id_col = _resolve_id_column(financial_panel, label="financial_panel")
    out = financial_panel.copy()
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    _reject_duplicate_keys(out, label="financial_panel")
    return out


def compute_roe_factor(
    *,
    universe: pd.DataFrame,
    financial_panel: pd.DataFrame,
    output_column: str = "roe",
) -> pd.DataFrame:
    """ROE from a prepared same-day financial panel.

    Formula:
        roe = financial_panel.roe[trade_date=T, asset_id]

    Direction: higher is better (quality). Non-finite ROE values become NaN.
    ``financial_panel`` must already be point-in-time aligned for each target
    ``trade_date`` (see ``prepare_financial_factor_panel`` in backtest).
    """
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return empty_cross_section_frame([output_column])

    panel = _normalize_financial_panel(financial_panel)
    if panel.empty or ROE not in panel.columns:
        return _attach_nan_column(uni, output_column)

    values = panel[[TRADE_DATE, ASSET_ID, ROE]].copy()
    values[ROE] = _as_finite_series(values[ROE])
    out = uni[[TRADE_DATE, ASSET_ID]].merge(
        values.rename(columns={ROE: output_column}),
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    out[output_column] = _as_finite_series(out[output_column])
    return sort_cross_section_frame(out)


def compute_book_to_price_factor(
    *,
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    financial_panel: pd.DataFrame,
    output_column: str = "book_to_price",
    price_column: str = CLOSE,
) -> pd.DataFrame:
    """Book-to-price = prepared bps / close[T].

    Formula:
        book_to_price = financial_panel.bps[T] / close[T]

    Direction: higher is better (value). Requires strictly positive finite BPS
    and close; otherwise NaN. ``financial_panel`` must already be point-in-time
    aligned for each target ``trade_date``.
    """
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return empty_cross_section_frame([output_column])

    price_panel = _normalize_panel_keys(
        prices, label="prices", required_value_columns=[price_column]
    )
    if not price_panel.empty:
        max_date = uni[TRADE_DATE].max()
        price_panel = price_panel.loc[price_panel[TRADE_DATE] <= max_date].copy()
        price_panel = price_panel.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort")
        price_panel[price_column] = _as_finite_series(price_panel[price_column])
        price_panel.loc[price_panel[price_column] <= 0, price_column] = math.nan

    fina = _normalize_financial_panel(financial_panel)
    out = uni[[TRADE_DATE, ASSET_ID]].copy()
    if price_panel.empty or fina.empty or BPS not in fina.columns:
        out[output_column] = math.nan
        return sort_cross_section_frame(out)

    out = out.merge(
        price_panel[[TRADE_DATE, ASSET_ID, price_column]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    out = out.merge(
        fina[[TRADE_DATE, ASSET_ID, BPS]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    bps = _as_finite_series(out[BPS])
    px = _as_finite_series(out[price_column])
    valid = (bps > 0) & (px > 0)
    ratio = bps.div(px).where(valid).replace([math.inf, -math.inf], math.nan)
    out[output_column] = ratio
    out = out.drop(columns=[c for c in (price_column, BPS) if c in out.columns])
    out[output_column] = _as_finite_series(out[output_column])
    return sort_cross_section_frame(out)


_LOOKBACK = {
    "lookback": FactorParameterSpec("integer", 20, True, 1, 10000),
}
_SIZE_FIELD = {
    "field": FactorParameterSpec(
        "string",
        MARKET_CAP,
        True,
        choices=_SIZE_FIELDS,
    )
}


FACTOR_DEFINITIONS: dict[str, FactorDefinition] = {
    "momentum": FactorDefinition(
        code="momentum",
        name="Trailing return / momentum",
        family="momentum",
        description="Parameterized trailing N-bar return including the target close.",
        formula="close[T] / close[T-lookback] - 1",
        direction="higher = stronger recent winners",
        time_semantics=(
            "Uses only prices with trade_date <= T; includes T close; "
            "intended for T+1 or later execution. Standard N-bar row shift: "
            "intermediate missing bars still occupy positions."
        ),
        inputs=(CLOSE,),
        parameter_schema=_LOOKBACK,
        default_output="momentum",
        nan_semantics=(
            "NaN when fewer than lookback+1 bars, or either endpoint is "
            "non-positive/non-finite; intermediate NaNs keep their bar slots; "
            "never zero-filled."
        ),
    ),
    "log_market_cap": FactorDefinition(
        code="log_market_cap",
        name="Log market capitalization",
        family="size",
        description="Natural log of a same-day market-cap field.",
        formula="ln(market_cap_field[T])",
        direction="higher = larger company size",
        time_semantics="Uses only the market-cap record for the same trade_date T.",
        inputs=_SIZE_FIELDS,
        parameter_schema=_SIZE_FIELD,
        default_output="log_market_cap",
        nan_semantics=(
            "NaN for missing, non-finite, zero or negative market cap; never zero-filled."
        ),
    ),
    "roe": FactorDefinition(
        code="roe",
        name="Return on equity (prepared PIT panel)",
        family="fundamental",
        description="ROE from a prepared point-in-time financial panel.",
        formula="financial_panel.roe[trade_date=T]",
        direction="higher = better quality",
        time_semantics=(
            "Consumes a prepared financial_panel already aligned so that each "
            "row is valid for the target trade_date. PIT selection is performed "
            "by backtest.prepare_financial_factor_panel, not by this module."
        ),
        inputs=(ROE,),
        parameter_schema={},
        default_output="roe",
        nan_semantics=(
            "NaN when panel lacks the asset/date or ROE is non-finite; never fabricated."
        ),
    ),
    "book_to_price": FactorDefinition(
        code="book_to_price",
        name="Book-to-price (prepared PIT panel)",
        family="fundamental",
        description="Prepared BPS divided by same-day close.",
        formula="financial_panel.bps[T] / close[T]",
        direction="higher = cheaper value",
        time_semantics=(
            "BPS comes from a prepared PIT financial_panel; close is the "
            "same-day price on T (post-close / T+1 oriented)."
        ),
        inputs=(BPS, CLOSE),
        parameter_schema={},
        default_output="book_to_price",
        nan_semantics="NaN when BPS or close missing/non-finite/non-positive.",
    ),
}


def list_factors() -> tuple[FactorDefinition, ...]:
    """Return registered factor definitions in stable code order."""
    return tuple(FACTOR_DEFINITIONS[code] for code in sorted(FACTOR_DEFINITIONS))


def get_factor_definition(code: str) -> FactorDefinition:
    try:
        return FACTOR_DEFINITIONS[code]
    except KeyError as exc:
        raise UnknownFactorError(f"unknown factor code: {code}") from exc


def _coerce_factor_request(value: FactorRequest | str | Mapping[str, Any]) -> FactorRequest:
    if isinstance(value, FactorRequest):
        return value
    if isinstance(value, str):
        return FactorRequest(code=value)
    if isinstance(value, Mapping):
        code = value.get("code")
        if not code:
            raise FactorRequestError("factor mapping requires a 'code' field")
        parameters = value.get("parameters") or {}
        if not isinstance(parameters, Mapping):
            raise FactorRequestError("factor parameters must be a mapping")
        alias = value.get("alias")
        return FactorRequest(code=str(code), parameters=dict(parameters), alias=alias)
    raise FactorRequestError(f"unsupported factor request type: {type(value)!r}")


def resolve_factor_requests(
    factors: Sequence[FactorRequest | str | Mapping[str, Any]],
) -> tuple[ResolvedFactorRequest, ...]:
    """Validate factor requests and assign stable output column names."""
    if factors is None:
        raise FactorRequestError("factors must be provided")
    if isinstance(factors, (str, bytes)) or not isinstance(factors, Sequence):
        raise FactorRequestError("factors must be a sequence of factor requests")
    if len(factors) == 0:
        raise FactorRequestError("at least one factor is required")

    resolved: list[ResolvedFactorRequest] = []
    outputs: set[str] = set()
    for raw in factors:
        request = _coerce_factor_request(raw)
        definition = get_factor_definition(request.code)
        unknown = sorted(set(request.parameters) - set(definition.parameter_schema))
        if unknown:
            raise FactorRequestError(
                f"factor {request.code!r} has unknown parameters: {unknown}"
            )
        parameters: dict[str, Any] = {}
        for name, spec in definition.parameter_schema.items():
            if name in request.parameters:
                value = request.parameters[name]
            elif spec.has_default:
                value = spec.default
            else:
                raise FactorRequestError(
                    f"factor {request.code!r} missing required parameter {name!r}"
                )
            _validate_parameter(request.code, name, value, spec)
            parameters[name] = value

        if request.alias is not None:
            output_column = request.alias
        else:
            non_default = {
                k: v
                for k, v in parameters.items()
                if not (
                    definition.parameter_schema[k].has_default
                    and v == definition.parameter_schema[k].default
                )
            }
            if not non_default:
                output_column = definition.default_output
            else:
                output_column = _default_alias(
                    request.code, parameters, definition.parameter_schema
                )

        _validate_output_column(output_column)
        if output_column in outputs:
            raise FactorRequestError(f"duplicate factor output column: {output_column}")
        outputs.add(output_column)
        resolved.append(
            ResolvedFactorRequest(
                request=request,
                definition=definition,
                parameters=parameters,
                output_column=output_column,
            )
        )
    return tuple(resolved)


def _compute_one_factor(
    resolved: ResolvedFactorRequest,
    *,
    universe: pd.DataFrame,
    prices: pd.DataFrame | None,
    size_panel: pd.DataFrame | None,
    financial_panel: pd.DataFrame | None,
) -> pd.DataFrame:
    code = resolved.definition.code
    output = resolved.output_column
    params = resolved.parameters

    if code == "momentum":
        if prices is None:
            raise FactorRequestError("momentum requires a prices panel")
        return compute_momentum_factor(
            prices,
            universe=universe,
            lookback=int(params["lookback"]),
            output_column=output,
        )
    if code == "log_market_cap":
        panel = size_panel if size_panel is not None else prices
        if panel is None:
            raise FactorRequestError(
                "log_market_cap requires a size_panel or prices panel with market-cap fields"
            )
        return compute_log_market_cap_factor(
            panel,
            universe=universe,
            field=str(params["field"]),
            output_column=output,
        )
    if code == "roe":
        if financial_panel is None:
            raise FactorRequestError(
                "roe requires a prepared financial_panel "
                "(use backtest.prepare_financial_factor_panel)"
            )
        return compute_roe_factor(
            universe=universe,
            financial_panel=financial_panel,
            output_column=output,
        )
    if code == "book_to_price":
        if prices is None:
            raise FactorRequestError("book_to_price requires a prices panel")
        if financial_panel is None:
            raise FactorRequestError(
                "book_to_price requires a prepared financial_panel "
                "(use backtest.prepare_financial_factor_panel)"
            )
        return compute_book_to_price_factor(
            universe=universe,
            prices=prices,
            financial_panel=financial_panel,
            output_column=output,
        )
    raise UnknownFactorError(f"unknown factor code: {code}")


def generate_factor_frame(
    factors: Sequence[FactorRequest | str | Mapping[str, Any]],
    *,
    trade_dates: Sequence[Any] | Any | None = None,
    asset_ids: Sequence[str] | None = None,
    universe: pd.DataFrame | None = None,
    prices: pd.DataFrame | None = None,
    size_panel: pd.DataFrame | None = None,
    financial_panel: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Generate a unique, stable raw-factor frame for a historical universe.

    Args:
        factors: one or more factor codes / :class:`FactorRequest` objects.
        trade_dates / asset_ids: explicit universe specification.
        universe: optional pre-built cross-section universe frame.
        prices: OHLCV (or at least close) panel keyed by trade_date + asset.
            Duplicate ``(trade_date, asset_id)`` keys raise explicitly.
        size_panel: optional market-cap panel; defaults to ``prices`` for size.
        financial_panel: prepared same-day financial fields with columns
            ``trade_date``, ``asset_id``, and ``roe`` / ``bps`` as needed.
            Build it with ``qrp_atlas.backtest.prepare_financial_factor_panel``
            so that ROE and book-to-price share one PIT snapshot. This entry
            point never queries DuckDB.

    Returns:
        DataFrame with ``trade_date``, ``asset_id`` and factor columns in the
        resolved request order. Ready for :func:`process_cross_section`.
    """
    resolved = resolve_factor_requests(factors)
    output_columns = [item.output_column for item in resolved]
    uni = _universe_from_inputs(
        trade_dates=trade_dates,
        asset_ids=asset_ids,
        universe=universe,
    )
    if uni.empty:
        return empty_cross_section_frame(output_columns)

    frames: list[pd.DataFrame] = [uni[[TRADE_DATE, ASSET_ID]].copy()]
    for item in resolved:
        factor_frame = _compute_one_factor(
            item,
            universe=uni,
            prices=prices,
            size_panel=size_panel,
            financial_panel=financial_panel,
        )
        frames.append(factor_frame[[TRADE_DATE, ASSET_ID, item.output_column]])

    out = frames[0]
    for piece in frames[1:]:
        out = out.merge(piece, on=[TRADE_DATE, ASSET_ID], how="left")

    ordered_cols = [TRADE_DATE, ASSET_ID] + output_columns
    out = out[ordered_cols]
    out = ensure_cross_section_frame(
        out,
        feature_columns=output_columns,
        enforce_primary_key=True,
    )
    return sort_cross_section_frame(out)


__all__ = [
    "FACTOR_DEFINITIONS",
    "FactorDefinition",
    "FactorError",
    "FactorParameterSpec",
    "FactorRequest",
    "FactorRequestError",
    "ResolvedFactorRequest",
    "UnknownFactorError",
    "compute_book_to_price_factor",
    "compute_log_market_cap_factor",
    "compute_momentum_factor",
    "compute_roe_factor",
    "generate_factor_frame",
    "get_factor_definition",
    "list_factors",
    "resolve_factor_requests",
]
