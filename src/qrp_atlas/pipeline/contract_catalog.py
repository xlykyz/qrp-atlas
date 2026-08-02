"""Explicit catalog of production-admitted Pipeline contract modules.

Only modules whose real Pipelines pass the complete formal contract and public
acceptance suite belong here. Examples must never be added here.
"""

from __future__ import annotations


CONTRACT_MODULES: tuple[str, ...] = (
    "qrp_atlas.pipeline.market_data_contracts",
    "qrp_atlas.pipeline.cninfo_contracts",
    "qrp_atlas.pipeline.irm_qa_contracts",
)
