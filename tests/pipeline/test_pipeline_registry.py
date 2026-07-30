"""Offline consistency checks for the Git-versioned Pipeline registry."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

from qrp_atlas.orchestration.definitions import load_definitions


ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "deploy" / "pipeline" / "pipeline-registry.json"
SHADOW_PATH = ROOT / "deploy" / "pipeline" / "pipeline-definitions.shadow.json"

VALID_STATUSES = {
    "LEGACY_SCHEDULED",
    "READY_UNSCHEDULED",
    "SHADOW",
    "PRODUCTION",
    "PLANNED",
    "DEFERRED",
}
VALID_TASK_TYPES = {
    "DETERMINISTIC_CLI",
    "AGENT_ORCHESTRATED",
    "REPORT",
    "HEALTH_CHECK",
    "MANUAL_OPERATION",
    "PLANNED",
}
DATABASE_LOCKS = {
    "quant.db": "quant_db_writer",
    "system_b_episode.duckdb": "system_b_episode_writer",
    "system_b_pools.duckdb": "system_b_pools_writer",
}


def _registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _cycle(graph: dict[str, list[str]]) -> list[str] | None:
    visited: set[str] = set()
    visiting: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> list[str] | None:
        if node in visiting:
            return path[path.index(node) :] + [node]
        if node in visited:
            return None
        visiting.add(node)
        path.append(node)
        for dependency in graph[node]:
            found = visit(dependency)
            if found:
                return found
        path.pop()
        visiting.remove(node)
        visited.add(node)
        return None

    return next((found for node in graph if (found := visit(node))), None)


def test_registry_identifiers_statuses_and_dependencies_are_valid() -> None:
    registry = _registry()
    pipelines = registry["pipelines"]
    pipeline_ids = [pipeline["pipeline_id"] for pipeline in pipelines]
    assert len(pipeline_ids) == len(set(pipeline_ids))
    assert {pipeline["status"] for pipeline in pipelines} <= VALID_STATUSES
    assert {pipeline["task_type"] for pipeline in pipelines} <= VALID_TASK_TYPES

    external_ids = {item["dependency_id"] for item in registry["external_dependencies"]}
    graph: dict[str, list[str]] = {}
    for pipeline in pipelines:
        dependencies: list[str] = []
        for dependency in pipeline["orchestration"]["dependencies"]:
            if dependency["kind"] == "PIPELINE":
                dependency_id = dependency["pipeline_id"]
                assert dependency_id in pipeline_ids
                dependencies.append(dependency_id)
            else:
                assert dependency["kind"] == "EXTERNAL"
                assert dependency["dependency_id"] in external_ids
        graph[pipeline["pipeline_id"]] = dependencies
    assert _cycle(graph) is None


def test_shadow_definitions_are_registered_disabled_and_runtime_valid() -> None:
    registry_ids = {pipeline["pipeline_id"] for pipeline in _registry()["pipelines"]}
    raw_shadow = json.loads(SHADOW_PATH.read_text(encoding="utf-8"))
    assert raw_shadow["definitions"]
    assert all(entry["enabled"] is False for entry in raw_shadow["definitions"])
    assert {entry["pipeline_id"] for entry in raw_shadow["definitions"]} <= registry_ids
    assert len(load_definitions(SHADOW_PATH)) == len(raw_shadow["definitions"])


def test_registry_contains_only_environment_variable_names_not_credential_values() -> None:
    registry = _registry()
    for pipeline in registry["pipelines"]:
        variable_names = pipeline["execution"]["environment_variable_names"]
        assert isinstance(variable_names, list)
        assert all(re.fullmatch(r"[A-Z][A-Z0-9_]*", name) for name in variable_names)

    def walk(value: object) -> None:
        if isinstance(value, dict):
            assert not ({"password", "secret", "credential", "api_key_value", "token_value"} & set(value))
            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)

    walk(registry)


def test_same_duckdb_write_target_uses_one_consistent_lock_name() -> None:
    writers: dict[str, set[str]] = defaultdict(set)
    for pipeline in _registry()["pipelines"]:
        locks = set(pipeline["orchestration"]["resource_locks"])
        for output in pipeline["execution"]["outputs"]:
            for database, expected_lock in DATABASE_LOCKS.items():
                if output.startswith(database):
                    writers[database].update(locks)
                    assert expected_lock in locks, pipeline["pipeline_id"]
    assert writers == {database: {lock} for database, lock in DATABASE_LOCKS.items()}


def test_system_b_readiness_requires_adjustment_factors_and_all_real_input_tables() -> None:
    pipelines = {pipeline["pipeline_id"]: pipeline for pipeline in _registry()["pipelines"]}
    adjustment_factors = pipelines["adj_factor_daily"]
    readiness = pipelines["system_b_state_readiness"]

    assert adjustment_factors["execution"]["outputs"] == ["quant.db.adj_factor_changes"]
    assert "system_b_state_readiness" in adjustment_factors["orchestration"]["downstream"]
    assert {
        dependency["pipeline_id"]
        for dependency in readiness["orchestration"]["dependencies"]
        if dependency["kind"] == "PIPELINE"
    } == {"market_daily_update", "adj_factor_daily"}
    assert readiness["execution"]["inputs"] == [
        "quant.db.stock_info",
        "quant.db.trading_calendar",
        "quant.db.daily_market_snapshot",
        "quant.db.adj_factor_changes",
        "quant.db.suspend_d",
    ]
    assert readiness["orchestration"]["freshness_checks"] == [
        {
            "input": "quant.db.suspend_d",
            "status": "UNRESOLVED",
            "requirement": (
                "No formal daily suspend_d schedule exists; before enabling System B, "
                "add an update pipeline or a reliable freshness check."
            ),
        }
    ]
