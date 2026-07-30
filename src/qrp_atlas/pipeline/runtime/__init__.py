"""Persistent, non-production Pipeline scheduling and execution foundation."""

from .definitions import DefinitionValidationError, load_definitions
from .models import PipelineDefinition, PipelineRun, PipelineStatus, StageRun
from .scheduler import PipelineScheduler
from .store import PipelineRuntimeStore

__all__ = [
    "DefinitionValidationError",
    "PipelineDefinition",
    "PipelineRun",
    "PipelineRunner",
    "PipelineRuntimeStore",
    "PipelineScheduler",
    "PipelineStatus",
    "StageRun",
    "load_definitions",
]


def __getattr__(name: str):
    """Load the runner lazily to keep source contracts importable.

    ``contracts`` owns the public execution-control types and imports the
    lightweight runtime models.  Eagerly importing the runner here would make
    package initialisation recurse back into ``contracts``.
    """

    if name == "PipelineRunner":
        from .runner import PipelineRunner

        return PipelineRunner
    raise AttributeError(name)
