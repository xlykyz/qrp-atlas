"""Reusable assertions and a small harness for formal Pipeline contract tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from collections.abc import Mapping
from uuid import uuid4

from .contract_validation import validate_contracts
from .contracts import PipelineContract, PipelineInvocation, PipelineResult
from .execution import execute_pipeline_contract


@dataclass(frozen=True, slots=True)
class ContractTestHarness:
    """Runs one contract against caller-supplied test settings only."""

    contract: PipelineContract
    settings: object
    scheduled_for: datetime = datetime(2026, 1, 2, tzinfo=UTC)

    def run(
        self,
        *,
        trade_date: date | None = None,
        parameter_overrides: Mapping[str, object] | None = None,
        attempt: int = 1,
    ) -> PipelineResult:
        validate_contracts((self.contract,))
        return execute_pipeline_contract(
            self.contract,
            PipelineInvocation(
                run_id=str(uuid4()),
                pipeline_id=self.contract.pipeline_id,
                scheduled_for=self.scheduled_for,
                attempt=attempt,
                settings=self.settings,  # type: ignore[arg-type]
                parameter_overrides=parameter_overrides or {},
                trade_date_override=trade_date,
                audit_context={"test": "true"},
            ),
        )


def assert_contract_result_matches_context(result: PipelineResult, contract: PipelineContract) -> None:
    """Common identity assertions for every success, failure, or NOOP test."""

    assert result.pipeline_id == contract.pipeline_id
    assert result.run_id
    assert result.attempt >= 1
    assert result.completed_at >= result.started_at
    assert result.duration_seconds >= 0
