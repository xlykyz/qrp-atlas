"""QRP Atlas architecture checks（机械化架构门禁）。

规则来源（唯一）：``src/qrp_atlas/AGENTS.md`` 与模块级 ``AGENTS.md``。
本检查器**不发明**新的架构原则；每条规则都带 ``source`` 字段指向其出处，
可机械化的子集在此实现，不可机械化项在 ``--list-rules`` 中显式登记。

设计原则
--------
- 依赖判定基于 Python AST（import / from-import，含相对导入），不做纯字符串 grep；
- 已存在历史债务使用 baseline 隔离（``arch_check_baseline.json``）：
  *existing debt may remain, new debt must not grow*；
- 失败输出必须给出：文件、行号、违反的规则、规则出处。

用法::

    python -m tools.arch_check                 # 全量检查（baseline 外违规 => 退出码 1）
    python -m tools.arch_check --list-rules    # 规则清单与覆盖状态
    python -m tools.arch_check --json          # 机器可读输出
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import tokenize
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPO_ROOT / "src" / "qrp_atlas"
BASELINE_PATH = Path(__file__).resolve().parent / "arch_check_baseline.json"
SETTINGS_PATH = PACKAGE_ROOT / "config" / "settings.py"

#: 架构层 = ``src/qrp_atlas/`` 的顶层业务包（AGENTS.md 各章节的管辖单位）
LAYERS: tuple[str, ...] = (
    "api",
    "auth",
    "backtest",
    "config",
    "contracts",
    "database",
    "indicators",
    "orchestration",
    "pipeline",
    "stock_collections",
    "strategies",
    "users",
)


@dataclass(frozen=True, slots=True)
class DependencyRule:
    """一条"某层不得 import 某组层"的禁止依赖规则。"""

    rule_id: str
    layer: str
    forbidden: tuple[str, ...]
    source: str

    @property
    def detail(self) -> str:
        return (
            f"{self.layer} 不得依赖 {' / '.join(self.forbidden)}"
            f"（规则来源：{self.source}）"
        )


#: 依赖方向规则。逐条对应 ``src/qrp_atlas/AGENTS.md`` 的「禁止的典型依赖」
#: 与各层章节的显式约束；不得在此添加 AGENTS.md 未声明的限制。
DEPENDENCY_RULES: tuple[DependencyRule, ...] = (
    DependencyRule(
        rule_id="ARCH-DEP-CONTRACTS",
        layer="contracts",
        forbidden=("pipeline", "stock_collections", "indicators", "strategies", "backtest", "api"),
        source="AGENTS.md「禁止的典型依赖」第 1 条；「contracts/」章节（不依赖任何业务上层模块）",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-STOCK-COLLECTIONS",
        layer="stock_collections",
        forbidden=("pipeline", "indicators", "strategies", "backtest", "api"),
        source="AGENTS.md「禁止的典型依赖」第 2 条；「stock_collections/」章节",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-INDICATORS",
        layer="indicators",
        forbidden=("stock_collections", "strategies", "backtest", "api"),
        source="AGENTS.md「禁止的典型依赖」第 3 条；「indicators/」章节",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-STRATEGIES",
        layer="strategies",
        forbidden=("pipeline", "stock_collections", "backtest", "api"),
        source="AGENTS.md「禁止的典型依赖」第 4 条；「strategies/」章节",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-ORCHESTRATION",
        layer="orchestration",
        forbidden=("pipeline", "stock_collections", "indicators", "strategies", "backtest", "api"),
        source="AGENTS.md「orchestration/ 是业务无关的顶级 Job Runtime」段",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-USERS",
        layer="users",
        forbidden=("auth", "pipeline", "stock_collections", "indicators", "strategies", "backtest"),
        source="AGENTS.md「禁止的典型依赖」第 7 条；「users/」章节（不依赖 auth 或量化核心）",
    ),
    DependencyRule(
        rule_id="ARCH-DEP-AUTH",
        layer="auth",
        forbidden=("contracts", "stock_collections", "indicators", "strategies", "backtest"),
        source="AGENTS.md「禁止的典型依赖」第 8 条；「auth/」章节（不依赖 contracts 与量化核心）",
    ),
)

#: 显式登记"AGENTS.md 已声明、但当前无法可靠机械化"的规则。
#: 用于让读者明确知道机械检查的边界，而不是误以为已全覆盖。
UNMECHANIZED_RULES: tuple[tuple[str, str], ...] = (
    ("backtest engine → 具体指标 / 题材 / 具体策略", "语义约束：禁止含义取决于「具体」的判定，需人工审查"),
    ("backtest research → 修改历史策略决策", "语义约束：涉及运行期行为，非静态 import 结构"),
    ("frontend → DuckDB / 文件结果目录 / Python 策略对象", "前端为主（web/），需前端构建工具链检查，非 Python AST"),
    ("strategies / api → frontend", "Python 侧不存在 frontend 包，无静态 import 可查"),
    ("下层模块不得反向依赖上层模块（完整依赖图）", "已由 ARCH-DEP-* 覆盖 AGENTS.md 列举的具体方向；完整全序图尚未定义"),
    ("PIT：历史能力不得未来数据泄漏", "数据语义约束，由各模块测试与审查覆盖，非 import 结构"),
    ("数据定义与数据生产分离 / 客观事实与交易决策分离 等核心原则", "语义分层原则，非静态依赖方向"),
    ("owner 隔离（用户业务数据）", "运行期授权行为，由 API/存储边界测试覆盖"),
    (
        "动态导入 / 反射式依赖（importlib、__import__）",
        "import 目标运行时才确定，静态 AST 无法判定；现有使用（如 pipeline 的 Contract 模块注册）不构成跨层反向依赖，但新增动态导入需人工审查",
    ),
)

class ArchCheckError(RuntimeError):
    """检查器无法完成评估（配置缺失等）。

    必须显式失败：静默跳过会让验证入口产生假绿。
    """


_DRIVE_PATH = re.compile(r"[A-Za-z]:\\")
_LINUX_HOME_PATH = re.compile(r"/home/[A-Za-z0-9._-]+/")
_PRIVATE_NETWORK_IP = re.compile(
    r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    r"|192\.168\.\d{1,3}\.\d{1,3}"
    r"|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"
)
_FACTORS_MODULE = PACKAGE_ROOT / "factors"


@dataclass(frozen=True, slots=True)
class Violation:
    """一条违规记录。``key()`` 是 baseline 的匹配键（行号刻意不参与匹配）。"""

    rule_id: str
    path: str
    lineno: int
    symbol: str
    detail: str

    def key(self) -> tuple[str, str, str]:
        return (self.path, self.rule_id, self.symbol)

    def format(self) -> str:
        return (
            f"[{self.rule_id}] {self.path}:{self.lineno}\n"
            f"    {self.detail}\n"
            f"    symbol: {self.symbol}"
        )


# ---------------------------------------------------------------------------
# import 依赖扫描
# ---------------------------------------------------------------------------


def _iter_python_files(root: Path) -> Iterator[Path]:
    yield from sorted(root.rglob("*.py"))


def _read_source(path: Path) -> str:
    """按解释器的加载语义读取源码（正确处理 BOM 与编码声明）。"""

    with tokenize.open(path) as handle:
        return handle.read()


def _layer_of_file(path: Path, package_root: Path) -> str | None:
    """返回文件所属架构层（顶层包名），顶层单文件返回 None。"""

    parts = path.relative_to(package_root).parts
    if len(parts) > 1 and parts[0] in LAYERS:
        return parts[0]
    return None


def _resolve_import_layer(
    path: Path, node: ast.Import | ast.ImportFrom, package_root: Path
) -> list[tuple[int, str, str | None]]:
    """把一条 import 语句解析为 ``(lineno, 原始目标, 命中的层)`` 列表。"""

    results: list[tuple[int, str, str | None]] = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            results.append((node.lineno, alias.name, _layer_of_module(alias.name)))
        return results

    module = node.module or ""
    if node.level and node.level > 0:
        package_parts = path.relative_to(package_root).parts[:-1]
        trim = node.level - 1
        base = package_parts[: len(package_parts) - trim] if trim else package_parts
        target_parts = base + tuple(part for part in module.split(".") if part)
        target = "." * node.level + module
        layer = target_parts[0] if target_parts and target_parts[0] in LAYERS else None
        results.append((node.lineno, target, layer))
        return results

    results.append((node.lineno, module, _layer_of_module(module)))
    return results


def _layer_of_module(module: str) -> str | None:
    parts = module.split(".")
    if len(parts) > 1 and parts[0] == "qrp_atlas" and parts[1] in LAYERS:
        return parts[1]
    return None


def scan_dependencies(repo_root: Path = REPO_ROOT) -> list[Violation]:
    """按 ``DEPENDENCY_RULES`` 扫描 ``src/qrp_atlas/**`` 的 import 依赖。"""

    package_root = repo_root / "src" / "qrp_atlas"
    rules_by_layer = {rule.layer: rule for rule in DEPENDENCY_RULES}
    violations: list[Violation] = []
    for path in _iter_python_files(package_root):
        layer = _layer_of_file(path, package_root)
        if layer is None:
            continue
        rule = rules_by_layer.get(layer)
        if rule is None:
            continue
        tree = ast.parse(_read_source(path), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            for lineno, target, imported_layer in _resolve_import_layer(path, node, package_root):
                if imported_layer and imported_layer in rule.forbidden:
                    violations.append(
                        Violation(
                            rule_id=rule.rule_id,
                            path=path.relative_to(repo_root).as_posix(),
                            lineno=lineno,
                            symbol=target,
                            detail=rule.detail,
                        )
                    )
    return violations


# ---------------------------------------------------------------------------
# config 读取规则
# ---------------------------------------------------------------------------


def supported_env_vars(settings_path: Path = SETTINGS_PATH) -> frozenset[str]:
    """从 ``settings.py`` 源码提取 ``SUPPORTED_ENV_VARS``（AST 字面量，无 import）。"""

    if not settings_path.exists():
        raise ArchCheckError(
            f"找不到 {settings_path}，无法提取 SUPPORTED_ENV_VARS；"
            "config 读取规则无法评估（显式失败，避免假绿）"
        )
    tree = ast.parse(_read_source(settings_path), filename=str(settings_path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "SUPPORTED_ENV_VARS" for t in node.targets):
            continue
        value = node.value
        args = value.args if isinstance(value, ast.Call) else []
        for arg in args:
            if isinstance(arg, ast.Set):
                return frozenset(
                    element.value
                    for element in arg.elts
                    if isinstance(element, ast.Constant) and isinstance(element.value, str)
                )
    raise ArchCheckError(f"SUPPORTED_ENV_VARS literal not found in {settings_path}")


def _env_read_variable(node: ast.AST) -> str | None:
    """识别 ``os.getenv("X")`` / ``os.environ.get("X")`` / 读取 ``os.environ["X"]``。"""

    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "getenv":
            if isinstance(func.value, ast.Name) and func.value.id == "os":
                return _first_str(node.args)
        if isinstance(func, ast.Attribute) and func.attr == "get" and node.args:
            owner = func.value
            if (
                isinstance(owner, ast.Attribute)
                and owner.attr == "environ"
                and isinstance(owner.value, ast.Name)
                and owner.value.id == "os"
            ):
                return _first_str(node.args)
        return None
    if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
        owner = node.value
        if (
            isinstance(owner, ast.Attribute)
            and owner.attr == "environ"
            and isinstance(owner.value, ast.Name)
            and owner.value.id == "os"
        ):
            slice_node = node.slice
            if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, str):
                return slice_node.value
    return None


def _first_str(args: Sequence[ast.expr]) -> str | None:
    if args and isinstance(args[0], ast.Constant) and isinstance(args[0].value, str):
        return args[0].value
    return None


def scan_config_access(
    repo_root: Path = REPO_ROOT,
    *,
    variables: frozenset[str] | None = None,
) -> list[Violation]:
    """业务层不得直接读取属于 ``SUPPORTED_ENV_VARS`` 的运行配置变量。

    来源：AGENTS.md「config/」章节 —— "普通业务模块禁止直接使用 ``os.getenv``、
    ``os.environ.get`` …… 应依赖 ``AppSettings``/``get_settings``"。
    仅检测**读取**；向后代进程传递环境、写入 ``QRP_ENV_FILE`` 等不在此列。
    """

    effective = variables if variables is not None else supported_env_vars(repo_root / "src" / "qrp_atlas" / "config" / "settings.py")
    package_root = repo_root / "src" / "qrp_atlas"
    violations: list[Violation] = []
    for path in _iter_python_files(package_root):
        layer = _layer_of_file(path, package_root)
        if layer is None or layer == "config":
            continue
        tree = ast.parse(_read_source(path), filename=str(path))
        for node in ast.walk(tree):
            variable = _env_read_variable(node)
            if variable and variable in effective:
                violations.append(
                    Violation(
                        rule_id="ARCH-CONFIG-NO-DIRECT-ENV",
                        path=path.relative_to(repo_root).as_posix(),
                        lineno=node.lineno,
                        symbol=variable,
                        detail=(
                            f"{layer} 不得直接读取运行配置变量 {variable}，"
                            "应经由 AppSettings/get_settings（规则来源：AGENTS.md「config/」章节）"
                        ),
                    )
                )
    return violations


# ---------------------------------------------------------------------------
# 字面量 / 结构规则
# ---------------------------------------------------------------------------


def scan_local_paths(repo_root: Path = REPO_ROOT) -> list[Violation]:
    """源码字符串字面量不得包含开发机绝对路径或内网 IP。

    来源：AGENTS.md「config/」章节 —— "不在源码、脚本、unit 或示例中写开发机
    绝对路径、内网 IP、真实用户名或秘密"（通用部署路径与业务 URL 除外）。
    """

    package_root = repo_root / "src" / "qrp_atlas"
    violations: list[Violation] = []
    for path in _iter_python_files(package_root):
        tree = ast.parse(_read_source(path), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            value = node.value
            for pattern, label in (
                (_LINUX_HOME_PATH, "Linux 用户家目录绝对路径"),
                (_DRIVE_PATH, "Windows 盘符绝对路径"),
                (_PRIVATE_NETWORK_IP, "内网 IP"),
            ):
                match = pattern.search(value)
                if match:
                    violations.append(
                        Violation(
                            rule_id="ARCH-NO-LOCAL-PATHS",
                            path=path.relative_to(repo_root).as_posix(),
                            lineno=node.lineno,
                            symbol=match.group(0),
                            detail=(
                                f"源码字面量包含{label}，不得写入仓库"
                                "（规则来源：AGENTS.md「config/」章节）"
                            ),
                        )
                    )
    return violations


def scan_module_structure(repo_root: Path = REPO_ROOT) -> list[Violation]:
    """禁止新增与 ``indicators`` 平行的 ``qrp_atlas.factors`` 模块。

    来源：AGENTS.md「indicators/」章节 —— "因子不是独立顶级架构层，不得新增与
    ``indicators`` 平行的 ``qrp_atlas.factors`` 模块"。
    """

    factors = repo_root / "src" / "qrp_atlas" / "factors"
    if factors.exists():
        return [
            Violation(
                rule_id="ARCH-NO-FACTORS-MODULE",
                path=factors.relative_to(repo_root).as_posix(),
                lineno=0,
                symbol="qrp_atlas.factors",
                detail=(
                    "不得新增与 indicators 平行的 factors 顶层模块"
                    "（规则来源：AGENTS.md「indicators/」章节）"
                ),
            )
        ]
    return []


# ---------------------------------------------------------------------------
# 汇总 / baseline
# ---------------------------------------------------------------------------


def configured_rules() -> list[tuple[str, str, str]]:
    """返回 ``(rule_id, 覆盖范围, 规则来源)`` 清单（供 ``--list-rules``）。"""

    rules: list[tuple[str, str, str]] = []
    for rule in DEPENDENCY_RULES:
        rules.append((rule.rule_id, f"{rule.layer} → {' / '.join(rule.forbidden)}", rule.source))
    rules.append(
        (
            "ARCH-CONFIG-NO-DIRECT-ENV",
            "业务层读取 SUPPORTED_ENV_VARS 中的变量（os.getenv / os.environ.get / os.environ[...]）",
            "AGENTS.md「config/」章节",
        )
    )
    rules.append(
        (
            "ARCH-NO-LOCAL-PATHS",
            "src/ 字符串字面量中的家目录绝对路径 / 盘符路径 / 内网 IP",
            "AGENTS.md「config/」章节",
        )
    )
    rules.append(
        (
            "ARCH-NO-FACTORS-MODULE",
            "禁止 qrp_atlas.factors 平行模块",
            "AGENTS.md「indicators/」章节",
        )
    )
    return rules


def collect_violations(repo_root: Path = REPO_ROOT) -> list[Violation]:
    violations = [
        *scan_dependencies(repo_root),
        *scan_config_access(repo_root),
        *scan_local_paths(repo_root),
        *scan_module_structure(repo_root),
    ]
    return sorted(violations, key=lambda violation: (violation.path, violation.lineno, violation.rule_id))


def load_baseline(path: Path = BASELINE_PATH) -> dict[tuple[str, str, str], int]:
    """读取 baseline：``(path, rule_id, symbol) -> 允许出现的条数``。"""

    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        (entry["path"], entry["rule_id"], entry["symbol"]): int(entry.get("occurrences", 1))
        for entry in payload.get("entries", ())
    }


def partition_by_baseline(
    violations: Iterable[Violation], baseline: dict[tuple[str, str, str], int]
) -> tuple[list[Violation], list[Violation], list[tuple[str, str, str]]]:
    """按 baseline 分组：``(known, new, stale)``。

    - ``known``：在 baseline 允许条数内的违规；
    - ``new``：同一 key 超出允许条数的部分 —— **新债**。计数语义保证
      “同一文件对同一目标模块新增 import”不会被既有条目静默吸收；
    - ``stale``：baseline 声明了允许条数但实际违规更少（含已完全修复）的条目，
      提示同步清理 baseline。
    """

    grouped: dict[tuple[str, str, str], list[Violation]] = {}
    for violation in violations:
        grouped.setdefault(violation.key(), []).append(violation)

    known: list[Violation] = []
    new: list[Violation] = []
    for key, items in grouped.items():
        allowance = baseline.get(key, 0)
        known.extend(items[:allowance])
        new.extend(items[allowance:])

    stale = [
        key for key, allowance in baseline.items() if len(grouped.get(key, ())) < allowance
    ]
    return known, new, stale


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QRP Atlas architecture checks")
    parser.add_argument("--list-rules", action="store_true", help="打印规则清单与覆盖状态后退出")
    parser.add_argument("--json", action="store_true", help="输出机器可读 JSON")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT, help="仓库根（默认自动定位）")
    parser.add_argument("--baseline", type=Path, default=BASELINE_PATH, help="baseline 文件路径")
    args = parser.parse_args(argv)

    if args.list_rules:
        print("# 已机械化规则")
        for rule_id, scope, source in configured_rules():
            print(f"  {rule_id}\n      scope : {scope}\n      source: {source}")
        print("\n# 已声明但未机械化（人工审查范围）")
        for description, reason in UNMECHANIZED_RULES:
            print(f"  - {description}\n      原因: {reason}")
        return 0

    violations = collect_violations(args.repo_root)
    baseline = load_baseline(args.baseline)
    known, new, stale = partition_by_baseline(violations, baseline)

    if args.json:
        print(
            json.dumps(
                {
                    "rule_count": len(configured_rules()),
                    "total_violations": len(violations),
                    "baseline_violations": len(known),
                    "stale_baseline_entries": [list(key) for key in stale],
                    "new_violations": [
                        {
                            "rule_id": v.rule_id,
                            "path": v.path,
                            "lineno": v.lineno,
                            "symbol": v.symbol,
                        }
                        for v in new
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1 if new else 0

    if known:
        print(f"baseline 内历史债务: {len(known)} 条（允许保留，不得增长）")
        for violation in known:
            print(f"  · [{violation.rule_id}] {violation.path} symbol={violation.symbol}")
        print()

    if stale:
        print(f"提示：{len(stale)} 条 baseline 条目已不再命中（可能已修复，请同步清理）：")
        for stale_path, stale_rule_id, stale_symbol in stale:
            print(f"  · [{stale_rule_id}] {stale_path} symbol={stale_symbol}")
        print()

    if new:
        print(f"发现 {len(new)} 条新的架构违规：\n")
        for violation in new:
            print(violation.format())
            print()
        print("处理方式：修正依赖；确属历史债务时须在 tools/arch_check_baseline.json 中显式登记并说明理由。")
        return 1

    print(f"架构检查通过：{len(configured_rules())} 条机械化规则，无 baseline 外违规。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
