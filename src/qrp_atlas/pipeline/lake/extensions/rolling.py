"""Fixed-window example using the existing indicator calculation registry."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qrp_atlas.indicators.parameterized import IndicatorRequest, calculate_indicators
from qrp_atlas.contracts.lake_plugins import LAKE_ROLLING_MEAN
from qrp_atlas.pipeline.lake.plugins import (
    PluginContext, PluginError, PluginResult, PluginSpec, bounded_inputs,
    frame_identity, identity, validate_output,
)


class RollingMeanPlugin:
    schema = LAKE_ROLLING_MEAN
    spec = PluginSpec(
        "rolling_mean", "1", "1", ("daily_market_snapshot",), "rolling_mean",
        ("ticker", "trade_date"), mode="window", lookback=5,
    )

    def run(self, context: PluginContext) -> PluginResult:
        if set(context.parameters) - {"window"}:
            raise PluginError("unknown rolling parameter")
        window = context.parameters.get("window", 5)
        if isinstance(window, bool) or not isinstance(window, int) or window < 1:
            raise PluginError("window must be a positive integer")
        frame = bounded_inputs(context, self.spec)["daily_market_snapshot"]
        if frame.empty:
            raise PluginError("rolling input is empty")
        validate_output(frame, self.spec)
        close = pd.to_numeric(frame["close"], errors="raise")
        if close.isna().any() or not np.isfinite(close).all():
            raise PluginError("rolling input close must be finite")
        frame["close"] = close
        calculated = calculate_indicators(frame, [IndicatorRequest("rolling_mean", {"window": window}, "mean_close")])
        output = calculated.loc[
            calculated["trade_date"].dt.date == context.target_date,
            ["ticker", "trade_date", "mean_close"],
        ].copy()
        if output.empty:
            raise PluginError("target date is not present")
        output["trade_date"] = output["trade_date"].dt.date
        validate_output(output, self.spec)
        return PluginResult(output, None, {
            "plugin_id": self.spec.plugin_id,
            "code_version": self.spec.code_version,
            "schema_version": self.spec.schema_version,
            "input_versions": dict(context.input_versions),
            "input_content": frame_identity(frame),
            "parameters_hash": identity({"window": window}),
            "window": window,
            "price_adjustment": "UNADJUSTED",
            "availability": "AFTER_TARGET_CLOSE",
            "strict_pit": context.strict_pit,
        })
