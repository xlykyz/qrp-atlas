"""Explicit catalog of production-admitted Pipeline contract modules.

Only modules whose real Pipelines pass the complete formal contract and public
acceptance suite belong here. Examples must never be added here.
"""

from __future__ import annotations


CONTRACT_MODULES: tuple[str, ...] = (
    "qrp_atlas.pipeline.market_data_contracts",
    "qrp_atlas.pipeline.etf_daily_contracts",
    "qrp_atlas.pipeline.etf_adj_factor_contracts",
    "qrp_atlas.pipeline.index_basic_contracts",
    "qrp_atlas.pipeline.stock_basic_contracts",
    "qrp_atlas.pipeline.limit_step_contracts",
    "qrp_atlas.pipeline.ths_daily_contracts",
    "qrp_atlas.pipeline.stk_high_shock_contracts",
    "qrp_atlas.pipeline.cninfo_contracts",
    "qrp_atlas.pipeline.irm_qa_contracts",
    "qrp_atlas.pipeline.membership_contracts",
    "qrp_atlas.pipeline.pit_fundamentals_contracts",
    "qrp_atlas.pipeline.system_b_contracts",
    "qrp_atlas.pipeline.theme_contracts",
    "qrp_atlas.pipeline.research_report_contracts",
    "qrp_atlas.pipeline.research_industry_contracts",
    "qrp_atlas.pipeline.dc_hot_contracts",
    "qrp_atlas.pipeline.ths_hot_contracts",
    "qrp_atlas.pipeline.theme_m5_contracts",
    "qrp_atlas.pipeline.market_m6_contracts",
)
