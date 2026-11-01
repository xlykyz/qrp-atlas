"""System B adapter retaining the existing SQL preparation and frozen algorithm.

Checkpoints are immutable lineage/recovery boundaries. This first adapter
recomputes complete supplied history; it does not invent an incremental state
algorithm. A changed prefix requires an explicit replay revision.
"""

from __future__ import annotations

from datetime import date
import json

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    SYSTEM_B_2_0_PARAMETERS, SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID, SYSTEM_B_CALCULATION_VERSION,
    PriceAdjustment, SystemBStateMachineRequest, get_table,
)
from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states
from qrp_atlas.contracts.lake_plugins import LAKE_SYSTEM_B_STATE
from qrp_atlas.pipeline.system_b.repository import execute_standard_input
from qrp_atlas.pipeline.lake.plugins import (
    PluginContext, PluginError, PluginResult, PluginSpec, bounded_inputs,
    frame_identity, identity, validate_output,
)


def _semantic_input_identity(frame: pd.DataFrame) -> str:
    """Ignore acquisition bookkeeping, never the facts used by the algorithm.

The full publication identities remain in input_versions. These hashes answer
only whether an already-computed business-history prefix changed. In
particular, list/delist dates, prices, factors and suspension facts remain
hashed; a fresh observation timestamp alone is not a historical revision.
"""
    return frame_identity(frame.drop(columns=["created_at", "updated_at", "captured_at"], errors="ignore"))


class SystemBStatePlugin:
    schema = LAKE_SYSTEM_B_STATE
    spec = PluginSpec(
        "system_b_state", SYSTEM_B_CALCULATION_VERSION + ":lake-2", "1",
        ("stock_info", "trading_calendar", "daily_market_snapshot", "adj_factor_changes", "suspend_d"),
        "system_b_state", LAKE_SYSTEM_B_STATE.primary_key, mode="state",
    )

    def run(self, context: PluginContext) -> PluginResult:
        if context.parameters:
            raise PluginError("System B uses its frozen parameter set; overrides are not allowed")
        inputs = bounded_inputs(context, self.spec)
        calendar = inputs["trading_calendar"]
        dates = pd.to_datetime(calendar["trade_date"], errors="raise").dt.date
        open_dates = sorted(set(dates.loc[calendar["is_open"].eq(True)]))
        if context.target_date not in open_dates:
            raise PluginError("System B target must be a known open date")
        parameter_hash = identity(SYSTEM_B_2_0_PARAMETERS.to_dict())
        previous = context.checkpoint
        if previous is not None:
            required = {"checkpoint_id", "plugin_id", "code_version", "schema_version", "parameters_hash", "target_date", "history_start", "history_hashes"}
            if not required <= set(previous):
                raise PluginError("checkpoint is incomplete")
            if previous["checkpoint_id"] != identity({key: value for key, value in previous.items() if key != "checkpoint_id"}):
                raise PluginError("checkpoint content identity mismatch")
            if (previous["plugin_id"], previous["code_version"], previous["schema_version"], previous["parameters_hash"]) != (
                self.spec.plugin_id, self.spec.code_version, self.spec.schema_version, parameter_hash,
            ):
                raise PluginError("checkpoint algorithm, schema or parameters do not match")
            previous_date = date.fromisoformat(previous["target_date"])
            if context.replay_from is None:
                next_dates = [item for item in open_dates if item > previous_date]
                if not next_dates or next_dates[0] != context.target_date:
                    raise PluginError("state advancement must use the next open date")
                prefix = bounded_inputs(context, self.spec, previous_date)
                if {name: _semantic_input_identity(frame) for name, frame in prefix.items()} != previous["history_hashes"]:
                    raise PluginError("historical input revision requires explicit replay")
            elif context.replay_from > min(date.fromisoformat(previous["history_start"]), context.target_date):
                # Prefix hashes establish that a revision happened but do not
                # locate its first affected row. Until finer checkpoints are
                # supported, require replay of the entire prior history.
                raise PluginError("this adapter requires replay from the checkpoint history start")
        elif context.replay_from is not None:
            raise PluginError("replay requires the prior checkpoint identity")

        with duckdb.connect(":memory:") as connection:
            connection.execute("SET threads=2")
            connection.execute("SET memory_limit='512MB'")
            for name, frame in inputs.items():
                schema = get_table(name)
                unknown = set(frame) - set(schema.column_names())
                if unknown:
                    raise PluginError(f"unexpected columns in {name}: {sorted(unknown)}")
                connection.execute(schema.duckdb_create_sql())
                if not frame.empty:
                    connection.register("fixed_input", frame)
                    columns = ", ".join('"' + str(column).replace('"', '""') + '"' for column in frame.columns)
                    connection.execute(f'INSERT INTO "{name}" ({columns}) SELECT {columns} FROM fixed_input')
                    connection.unregister("fixed_input")
            prepared = execute_standard_input(connection, end_date=context.target_date).fetchdf()
        if prepared.empty:
            raise PluginError("System B has no eligible observations")
        if prepared["market_fact_status"].eq("UNRESOLVED_MISSING").any():
            raise PluginError("System B history has unresolved missing market facts")
        result = calculate_system_b_2_0_states(SystemBStateMachineRequest(
            observations=prepared, parameters=SYSTEM_B_2_0_PARAMETERS,
            input_price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
            rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
        ))
        output = result.frame.copy(deep=True)
        output["trade_date"] = pd.to_datetime(output["trade_date"]).dt.date
        # Full-history replay produces a replacement candidate only in the
        # requested affected range; all earlier observations remain historical.
        start = context.replay_from or context.target_date
        output = output.loc[output["trade_date"].between(start, context.target_date)].reset_index(drop=True)
        if output.empty:
            raise PluginError("System B target output is empty")
        validate_output(output, self.spec)
        for name in ("source_rule_ids", "diagnostics"):
            output[name] = output[name].map(lambda value: json.dumps(list(value), ensure_ascii=False, separators=(",", ":")))
        output = output.loc[:, LAKE_SYSTEM_B_STATE.column_names()]
        checkpoint = {
            "plugin_id": self.spec.plugin_id,
            "code_version": self.spec.code_version,
            "schema_version": self.spec.schema_version,
            "parameters_hash": parameter_hash,
            "target_date": context.target_date.isoformat(),
            "history_start": pd.to_datetime(prepared["trade_date"]).min().date().isoformat(),
            "input_versions": dict(context.input_versions),
            "history_hashes": {name: _semantic_input_identity(frame) for name, frame in inputs.items()},
            "output_hash": frame_identity(output.loc[output.trade_date.eq(context.target_date)].reset_index(drop=True)),
            "previous_checkpoint_id": previous["checkpoint_id"] if previous else None,
            "replay_from": context.replay_from.isoformat() if context.replay_from else None,
            "recovery_mode": "FULL_HISTORY_RECOMPUTE",
        }
        checkpoint["checkpoint_id"] = identity(checkpoint)
        return PluginResult(output, checkpoint, {
            "plugin_id": self.spec.plugin_id,
            "input_versions": dict(context.input_versions),
            "parameters_hash": parameter_hash,
            "price_adjustment": "FORWARD_ADJUSTED",
            "adjustment_anchor": context.target_date.isoformat(),
            "recovery_mode": "FULL_HISTORY_RECOMPUTE",
            "strict_pit": context.strict_pit,
        })
