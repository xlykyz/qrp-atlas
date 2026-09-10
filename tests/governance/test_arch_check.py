"""架构检查器（``tools/arch_check.py``）自身的测试。

覆盖：真阳性、真阴性、相对导入解析、baseline 机制、config 读取规则、
路径字面量规则、结构规则，以及对当前仓库的集成断言。

这些测试防止"检查器本身写错"导致误报 / 漏报。
"""

from __future__ import annotations

import json
from pathlib import Path

from tools.arch_check import (
    DEPENDENCY_RULES,
    BASELINE_PATH,
    collect_violations,
    configured_rules,
    load_baseline,
    partition_by_baseline,
    scan_config_access,
    scan_dependencies,
    scan_local_paths,
    scan_module_structure,
    supported_env_vars,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _write(repo: Path, relative: str, content: str) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# 依赖规则：真阳性
# ---------------------------------------------------------------------------


def test_forbidden_from_import_is_detected(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.backtest import models\n")

    violations = scan_dependencies(tmp_path)

    assert [violation.rule_id for violation in violations] == ["ARCH-DEP-INDICATORS"]
    assert violations[0].symbol == "qrp_atlas.backtest"
    assert violations[0].lineno == 1
    assert violations[0].path == "src/qrp_atlas/indicators/probe.py"


def test_forbidden_plain_import_is_detected(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "import qrp_atlas.api.routes\n")

    violations = scan_dependencies(tmp_path)

    assert [violation.symbol for violation in violations] == ["qrp_atlas.api.routes"]


def test_cross_layer_relative_import_is_detected(tmp_path: Path) -> None:
    # indicators/deep/probe.py 中 `from ...api import routes` 越过 indicators 指向 api
    _write(tmp_path, "src/qrp_atlas/indicators/deep/probe.py", "from ...api import routes\n")

    violations = scan_dependencies(tmp_path)

    assert [violation.rule_id for violation in violations] == ["ARCH-DEP-INDICATORS"]
    assert violations[0].symbol == "...api"


def test_conditional_and_deferred_imports_are_detected(tmp_path: Path) -> None:
    # 函数体内延迟导入与 TYPE_CHECKING 分支同样是依赖声明
    source = (
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from qrp_atlas.strategies import registry\n"
        "\n"
        "def run():\n"
        "    from qrp_atlas.backtest import engine\n"
        "    return engine\n"
    )
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", source)

    violations = scan_dependencies(tmp_path)

    assert {violation.symbol for violation in violations} == {
        "qrp_atlas.strategies",
        "qrp_atlas.backtest",
    }


def test_orchestration_business_reverse_dependency_is_detected(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/orchestration/probe.py", "from qrp_atlas.pipeline import contracts\n")

    violations = scan_dependencies(tmp_path)

    assert [violation.rule_id for violation in violations] == ["ARCH-DEP-ORCHESTRATION"]


def test_users_and_auth_constraints_are_enforced(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/users/probe.py", "from qrp_atlas.auth import service\n")
    _write(tmp_path, "src/qrp_atlas/auth/probe.py", "from qrp_atlas.contracts import schema\n")

    violations = scan_dependencies(tmp_path)

    assert {violation.rule_id for violation in violations} == {
        "ARCH-DEP-USERS",
        "ARCH-DEP-AUTH",
    }


# ---------------------------------------------------------------------------
# 依赖规则：真阴性
# ---------------------------------------------------------------------------


def test_allowed_direction_is_not_reported(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/strategies/probe.py", "from qrp_atlas.indicators import core\n")
    _write(tmp_path, "src/qrp_atlas/pipeline/probe.py", "from qrp_atlas.contracts import schema\n")
    _write(tmp_path, "src/qrp_atlas/api/probe.py", "from qrp_atlas.auth import service\n")

    assert scan_dependencies(tmp_path) == []


def test_same_layer_relative_import_is_not_reported(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/deep/probe.py", "from ..core import helper\n")

    assert scan_dependencies(tmp_path) == []


def test_third_party_and_stdlib_imports_are_ignored(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/qrp_atlas/contracts/probe.py",
        "import json\nimport pandas as pd\nfrom pathlib import Path\n",
    )

    assert scan_dependencies(tmp_path) == []


def test_top_level_files_are_out_of_layer_scope(tmp_path: Path) -> None:
    # src/qrp_atlas/jobs_cli.py 不属于任何架构层，不参与层规则
    _write(tmp_path, "src/qrp_atlas/jobs_cli.py", "from qrp_atlas.api import cli\n")

    assert scan_dependencies(tmp_path) == []


# ---------------------------------------------------------------------------
# baseline 机制
# ---------------------------------------------------------------------------


def test_baseline_partitions_known_and_new_violations(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.backtest import models\n")
    _write(tmp_path, "src/qrp_atlas/indicators/other.py", "from qrp_atlas.api import routes\n")

    violations = scan_dependencies(tmp_path)
    probe = next(violation for violation in violations if violation.path.endswith("probe.py"))
    known, new, stale = partition_by_baseline(violations, {probe.key(): 1})

    assert [violation.path for violation in known] == ["src/qrp_atlas/indicators/probe.py"]
    assert [violation.path for violation in new] == ["src/qrp_atlas/indicators/other.py"]
    assert stale == []


def test_baseline_matches_independent_of_line_numbers(tmp_path: Path) -> None:
    """行号不参与匹配：同一违规前移/后移行不影响 baseline 命中。"""

    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", '"""doc."""\n\n\nfrom qrp_atlas.backtest import models\n')

    violations = scan_dependencies(tmp_path)
    known, new, stale = partition_by_baseline(
        violations,
        {("src/qrp_atlas/indicators/probe.py", "ARCH-DEP-INDICATORS", "qrp_atlas.backtest"): 1},
    )

    assert known and not new and not stale


def test_baseline_allowance_is_counted_per_symbol(tmp_path: Path) -> None:
    """同一文件对同一目标模块重复 import：超出允许条数的部分视为新债。"""

    _write(
        tmp_path,
        "src/qrp_atlas/indicators/probe.py",
        "from qrp_atlas.backtest.pit_queries import alpha\nfrom qrp_atlas.backtest.pit_queries import beta\n",
    )

    violations = scan_dependencies(tmp_path)
    assert len(violations) == 2

    known, new, stale = partition_by_baseline(violations, {violations[0].key(): 1})

    assert len(known) == 1
    assert len(new) == 1
    assert new[0].symbol == "qrp_atlas.backtest.pit_queries"
    assert stale == []


def test_baseline_reports_stale_entries(tmp_path: Path) -> None:
    """baseline 声明但不再命中的条目必须被报告（防永久免疫）。"""

    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.contracts import schema\n")

    key = ("src/qrp_atlas/indicators/probe.py", "ARCH-DEP-INDICATORS", "qrp_atlas.backtest")
    known, new, stale = partition_by_baseline(scan_dependencies(tmp_path), {key: 1})

    assert known == []
    assert new == []
    assert stale == [key]


def test_current_baseline_entries_are_well_formed() -> None:
    payload = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    rule_ids = {rule_id for rule_id, _, _ in configured_rules()}

    for entry in payload["entries"]:
        assert entry["rule_id"] in rule_ids
        assert entry["reason"].strip()
        assert int(entry.get("occurrences", 1)) >= 1


# ---------------------------------------------------------------------------
# config 读取规则
# ---------------------------------------------------------------------------


def test_config_rule_detects_supported_variable_reads(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/qrp_atlas/api/probe.py",
        'import os\n\nA = os.getenv("QRP_DATA_DIR")\nB = os.environ.get("QRP_DUCKDB_PATH")\nC = os.environ["QRP_LOG_LEVEL"]\n',
    )

    violations = scan_config_access(tmp_path, variables=frozenset({"QRP_DATA_DIR", "QRP_DUCKDB_PATH", "QRP_LOG_LEVEL"}))

    assert {violation.symbol for violation in violations} == {
        "QRP_DATA_DIR",
        "QRP_DUCKDB_PATH",
        "QRP_LOG_LEVEL",
    }
    assert {violation.rule_id for violation in violations} == {"ARCH-CONFIG-NO-DIRECT-ENV"}


def test_config_rule_ignores_unknown_or_non_configuration_variables(tmp_path: Path) -> None:
    # QRP_JOB_RUN_ID 等 Job 运行上下文不是 settings 管辖的运行配置
    _write(tmp_path, "src/qrp_atlas/pipeline/probe.py", 'import os\n\nA = os.getenv("QRP_JOB_RUN_ID")\n')

    assert scan_config_access(tmp_path, variables=frozenset({"QRP_DATA_DIR"})) == []


def test_config_rule_ignores_writes_and_config_layer(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/api/probe.py", 'import os\n\nos.environ["QRP_ENV_FILE"] = "x"\n')
    _write(tmp_path, "src/qrp_atlas/config/probe.py", 'import os\n\nA = os.getenv("QRP_DATA_DIR")\n')

    assert scan_config_access(tmp_path, variables=frozenset({"QRP_ENV_FILE", "QRP_DATA_DIR"})) == []


def test_supported_env_vars_ast_extraction_matches_runtime_module() -> None:
    from qrp_atlas.config.settings import SUPPORTED_ENV_VARS as runtime_variables

    assert supported_env_vars() == frozenset(runtime_variables)


# ---------------------------------------------------------------------------
# 路径字面量规则
# ---------------------------------------------------------------------------


def test_local_path_literals_are_detected(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/qrp_atlas/api/probe.py",
        'A = "/home/example-user/data"\nB = "D:\\\\projects\\\\qrp"\nC = "192.168.1.10"\n',
    )

    violations = scan_local_paths(tmp_path)

    assert {violation.rule_id for violation in violations} == {"ARCH-NO-LOCAL-PATHS"}
    assert len(violations) == 3


def test_loopback_hosts_and_urls_are_not_flagged(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/qrp_atlas/api/probe.py",
        'A = "127.0.0.1"\nB = "0.0.0.0"\nC = "https://example.com/api/v1"\n',
    )

    assert scan_local_paths(tmp_path) == []


# ---------------------------------------------------------------------------
# 结构规则
# ---------------------------------------------------------------------------


def test_factors_parallel_module_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/factors/__init__.py", "")

    violations = scan_module_structure(tmp_path)

    assert [violation.rule_id for violation in violations] == ["ARCH-NO-FACTORS-MODULE"]


# ---------------------------------------------------------------------------
# 规则表与集成
# ---------------------------------------------------------------------------


def test_rule_ids_are_unique_and_sourced() -> None:
    rules = configured_rules()
    rule_ids = [rule_id for rule_id, _, _ in rules]

    assert len(rule_ids) == len(set(rule_ids))
    assert all(rule_id.startswith("ARCH-") for rule_id in rule_ids)
    assert all(source.strip() for _, _, source in rules)
    assert all(rule.rule_id.startswith("ARCH-DEP-") and rule.source for rule in DEPENDENCY_RULES)


def test_current_repository_has_no_new_architecture_violations() -> None:
    violations = collect_violations(REPO_ROOT)
    known, new, stale = partition_by_baseline(violations, load_baseline(BASELINE_PATH))

    assert new == [], "\n".join(violation.format() for violation in new)
    assert known, "baseline 不应为空：历史债务已登记（若已修复请同步清理 baseline）"
    assert stale == [], "baseline 存在不再命中的条目，请同步清理"
