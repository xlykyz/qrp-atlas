"""Static dependency checks for the business-neutral Job layer."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATION = ROOT / "src" / "qrp_atlas" / "orchestration"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_orchestration_has_no_business_reverse_dependency() -> None:
    imports = {name for path in ORCHESTRATION.glob("*.py") for name in _imports(path)}
    assert not any(name.startswith("qrp_atlas.pipeline") for name in imports)
    assert not any(name.startswith("qrp_atlas.indicators") for name in imports)
    assert not any(name.startswith("qrp_atlas.strategies") for name in imports)
    assert not any(name.startswith("qrp_atlas.backtest") for name in imports)
    assert not any(name.startswith("qrp_atlas.api") for name in imports)


def test_old_runtime_package_and_entrypoint_are_removed() -> None:
    assert not (ROOT / "src" / "qrp_atlas" / "pipeline" / "runtime").exists()
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "qrp-atlas-pipeline" not in pyproject
    assert "qrp-atlas-jobs = \"qrp_atlas.jobs_cli:main\"" in pyproject


def test_formal_source_contains_no_old_runtime_imports() -> None:
    source_files = tuple((ROOT / "src").rglob("*.py"))
    assert all("qrp_atlas.pipeline.runtime" not in path.read_text(encoding="utf-8") for path in source_files)


def test_job_runtime_schema_and_audit_names_are_job_scoped() -> None:
    store_source = (ORCHESTRATION / "store.py").read_text(encoding="utf-8")
    result_log_source = (ORCHESTRATION / "result_log.py").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS job_run" in store_source
    assert "CREATE TABLE IF NOT EXISTS job_result" in store_source
    assert "CREATE TABLE IF NOT EXISTS job_service_lease" in store_source
    assert "job-results-" in result_log_source
    assert '"job_id"' in result_log_source
    assert '"job_run_id"' in result_log_source
