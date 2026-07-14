"""Safe, JSON-shaped declarative strategy support."""

from .evaluator import DeclarativeStrategy
from .models import (
    Comparison,
    DeclarativeStrategySpec,
    InputReference,
    SourceType,
    condition_to_dict,
    parse_condition,
)

__all__ = [
    "Comparison",
    "DeclarativeStrategy",
    "DeclarativeStrategySpec",
    "InputReference",
    "SourceType",
    "condition_to_dict",
    "parse_condition",
]

from .store import DeclarativeStrategyStore, get_declarative_store, reset_declarative_store_for_tests
