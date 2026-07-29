"""Explicit catalog of formal Pipeline contract modules.

Existing pipelines intentionally do not appear here until they pass the complete
formal contract and public acceptance suite.  The example proves the interface
without being selected for deployment.
"""

from __future__ import annotations


CONTRACT_MODULES: tuple[str, ...] = (
    "qrp_atlas.pipeline.contract_template",
)
