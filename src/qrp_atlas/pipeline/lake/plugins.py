"""Explicit, lazy extension discovery and versioned calculation boundaries.

This module contains no extension imports and grants no storage permissions.
The host owns process/resource isolation and snapshot publication.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

import pandas as pd
from qrp_atlas.contracts.schema import TableSchema


class PluginError(ValueError):
    """A rejected extension request; callers must not publish its output."""


@dataclass(frozen=True)
class PluginSpec:
    plugin_id: str
    code_version: str
    schema_version: str
    inputs: tuple[str, ...]
    output_dataset: str
    primary_key: tuple[str, ...]
    partition_column: str = "trade_date"
    output_domain: str = "extension"
    mode: str = "stateless"
    lookback: int | None = None
    pit_requirement: str = "verified_inputs"
    resource_class: str = "extension_compute"

    def __post_init__(self) -> None:
        if self.output_domain != "extension":
            raise PluginError("extension plugins cannot publish into the core domain")
        if self.mode not in {"stateless", "window", "state"}:
            raise PluginError("unsupported calculation mode")
        if not self.primary_key or not self.plugin_id or not self.code_version:
            raise PluginError("plugin identity and output primary key are required")
        if self.partition_column not in self.primary_key:
            raise PluginError("partition column must belong to primary key")


@dataclass(frozen=True)
class PluginContext:
    target_date: date
    inputs: Mapping[str, pd.DataFrame]
    input_versions: Mapping[str, str]
    parameters: Mapping[str, Any] = field(default_factory=dict)
    checkpoint: Mapping[str, Any] | None = None
    replay_from: date | None = None
    strict_pit: bool = True
    pit_evidence: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PluginResult:
    frame: pd.DataFrame
    checkpoint: Mapping[str, Any] | None
    metadata: Mapping[str, Any]


class Plugin(Protocol):
    spec: PluginSpec
    schema: TableSchema

    def run(self, context: PluginContext) -> PluginResult: ...


BUILTIN_PLUGINS = {
    "rolling_mean": "qrp_atlas.pipeline.lake.extensions.rolling:RollingMeanPlugin",
    "system_b_state": "qrp_atlas.pipeline.lake.extensions.system_b:SystemBStatePlugin",
}


def load_plugin(plugin_id: str, *, enabled: Mapping[str, str]) -> Plugin:
    """Load exactly one explicitly enabled entry point, without scanning packages.

``enabled`` is an operator-owned allowlist, never a provider or API payload.
Installing a package or adding it to BUILTIN_PLUGINS does not enable it.
"""
    if plugin_id not in enabled:
        raise PluginError(f"plugin is not enabled: {plugin_id}")
    entrypoint = enabled[plugin_id]
    module_name, separator, attribute = entrypoint.partition(":")
    if not separator or not module_name or not attribute or "." in attribute:
        raise PluginError("entry point must be module:factory")
    factory = getattr(importlib.import_module(module_name), attribute)
    plugin = factory()
    if not isinstance(plugin.spec, PluginSpec) or plugin.spec.plugin_id != plugin_id:
        raise PluginError("plugin declaration does not match its enabled identity")
    schema = getattr(plugin, "schema", None)
    if not isinstance(schema, TableSchema):
        raise PluginError("plugin must expose its output TableSchema as schema")
    if schema.name != plugin.spec.output_dataset or schema.primary_key != plugin.spec.primary_key:
        raise PluginError("plugin output schema identity or primary key disagrees with declaration")
    names = schema.column_names()
    if len(names) != len(set(names)) or not set(schema.primary_key) <= set(names):
        raise PluginError("plugin output schema has invalid columns or primary key")
    partition = next((column for column in schema.columns if column.name == plugin.spec.partition_column), None)
    if partition is None or partition.dtype.upper() != "DATE":
        raise PluginError("plugin output requires its declared DATE partition column")
    if not callable(getattr(plugin, "run", None)):
        raise PluginError("plugin must provide a run method")
    return plugin


def _scalar(value: Any) -> Any:
    if isinstance(value, (tuple, list)):
        return [_scalar(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _scalar(item) for key, item in value.items()}
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def identity(value: Any) -> str:
    encoded = json.dumps(_scalar(value), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def frame_identity(frame: pd.DataFrame) -> str:
    """Content identity independent of row/column ordering, preserving float values."""
    columns = sorted(frame.columns)
    rows = [[_scalar(value) for value in row] for row in frame[columns].itertuples(index=False, name=None)]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True, allow_nan=False))
    return identity({"columns": columns, "rows": rows})


def bounded_inputs(context: PluginContext, spec: PluginSpec, end_date: date | None = None) -> dict[str, pd.DataFrame]:
    end = end_date or context.target_date
    result = {}
    for name in spec.inputs:
        if name not in context.inputs or not context.input_versions.get(name):
            raise PluginError(f"fixed input and version required: {name}")
        if context.strict_pit and context.pit_evidence.get(name) != "verified":
            raise PluginError(f"strict PIT evidence missing: {name}")
        frame = context.inputs[name].copy(deep=True)
        if "trade_date" in frame:
            dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
            if dates.isna().any():
                raise PluginError(f"invalid trade_date in {name}")
            frame = frame.loc[dates <= end].copy()
        elif name == "stock_info" and "list_date" in frame:
            dates = pd.to_datetime(frame["list_date"], errors="raise").dt.date
            frame = frame.loc[dates.notna() & (dates <= end)].copy()
        result[name] = frame
    return result


def validate_output(frame: pd.DataFrame, spec: PluginSpec) -> None:
    if not set(spec.primary_key) <= set(frame):
        raise PluginError("plugin output is missing its declared key")
    if frame[list(spec.primary_key)].isna().any().any() or frame.duplicated(list(spec.primary_key)).any():
        raise PluginError("plugin output key is null or duplicated")
