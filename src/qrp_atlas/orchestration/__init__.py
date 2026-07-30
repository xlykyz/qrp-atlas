"""Business-neutral Job orchestration and durable execution foundation."""

from .definitions import DefinitionValidationError, load_definitions
from .execution_control import ExecutionControl, ExecutionControlError
from .models import JobDefinition, JobExecutionResult, JobRun, JobStatus, JobStageRun
from .scheduler import JobScheduler
from .store import JobRuntimeStore

__all__ = [
    "DefinitionValidationError",
    "JobDefinition",
    "JobRun",
    "JobExecutionResult",
    "JobRunner",
    "JobRuntimeStore",
    "JobScheduler",
    "JobStatus",
    "JobStageRun",
    "load_definitions",
    "ExecutionControl",
    "ExecutionControlError",
]


def __getattr__(name: str):
    """Load the runner lazily so lightweight model imports stay cheap."""

    if name == "JobRunner":
        from .runner import JobRunner

        return JobRunner
    raise AttributeError(name)
