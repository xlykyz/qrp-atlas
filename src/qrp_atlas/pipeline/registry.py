"""Source registration for formal Pipeline contracts."""

from __future__ import annotations

from collections.abc import Iterable
from importlib import import_module

from .contracts import PipelineContract


class ContractRegistrationError(ValueError):
    """Raised when source code attempts an ambiguous Pipeline registration."""


class PipelineRegistry:
    """Small explicit registry; it is not a scheduler or a second runtime."""

    def __init__(self) -> None:
        self._contracts: dict[str, PipelineContract] = {}

    def register(self, contract: PipelineContract) -> PipelineContract:
        existing = self._contracts.get(contract.pipeline_id)
        if existing is not None and existing is not contract:
            raise ContractRegistrationError(f"duplicate pipeline_id registration: {contract.pipeline_id}")
        self._contracts[contract.pipeline_id] = contract
        return contract

    def get(self, pipeline_id: str) -> PipelineContract:
        try:
            return self._contracts[pipeline_id]
        except KeyError as exc:
            raise KeyError(f"unknown formal pipeline: {pipeline_id}") from exc

    def all(self) -> tuple[PipelineContract, ...]:
        return tuple(self._contracts[pipeline_id] for pipeline_id in sorted(self._contracts))

    def __contains__(self, pipeline_id: object) -> bool:
        return pipeline_id in self._contracts

    def register_all(self, contracts: Iterable[PipelineContract]) -> None:
        for contract in contracts:
            self.register(contract)


_DEFAULT_REGISTRY = PipelineRegistry()
_DEFAULT_CATALOG_LOADED = False


def register_pipeline(contract: PipelineContract) -> PipelineContract:
    """Register a formal contract at module import time."""

    return _DEFAULT_REGISTRY.register(contract)


def default_registry() -> PipelineRegistry:
    """Load only the explicit source catalog, then return the singleton registry."""

    global _DEFAULT_CATALOG_LOADED
    if not _DEFAULT_CATALOG_LOADED:
        from .contract_catalog import CONTRACT_MODULES

        for module_name in CONTRACT_MODULES:
            import_module(module_name)
        _DEFAULT_CATALOG_LOADED = True
    return _DEFAULT_REGISTRY
