"""QRP Atlas 统一验证入口（正式交付验证）。

这是**唯一推荐的正式验证命令**：无论人工还是 Agent 完成开发，交付前运行
本入口即可覆盖完整验证；不需要临时判断"应该跑哪些检查"。

阶段（按顺序执行，任一失败 => 总入口返回非零）：

1. ``syntax``        对 ``src`` / ``scripts`` / ``tools`` / ``tests`` 下全部
                     ``.py`` 做语法编译校验（``compile()``，无副作用）；
2. ``git-diff-check`` ``git diff --check HEAD``：空白错误、冲突标记等；
3. ``arch``           ``tools/arch_check``：机械化架构规则 + baseline 新债检查；
4. ``pytest``         全量 pytest（仅 full 模式；fast 模式跳过并明确提示）。

用法::

    python -m tools.verify           # full：完整正式交付验证
    python -m tools.verify --fast    # fast：仅静态检查（开发中快速自检）

跨环境说明：请使用**已安装项目依赖（含 pytest）的解释器**运行本入口
（例如各工作区的项目虚拟环境），以保证第 4 阶段可执行。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tokenize
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from tools.arch_check import (
    BASELINE_PATH,
    ArchCheckError,
    collect_violations,
    configured_rules,
    load_baseline,
    partition_by_baseline,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 语法校验覆盖的源码根
SYNTAX_ROOTS: tuple[str, ...] = ("src", "scripts", "tools", "tests")


@dataclass
class StageResult:
    name: str
    ok: bool
    summary: str
    details: list[str] = field(default_factory=list)


def check_syntax(repo_root: Path) -> StageResult:
    """对全部 Python 源文件做语法层编译校验（不写任何字节码文件）。"""

    errors: list[str] = []
    checked = 0
    for root_name in SYNTAX_ROOTS:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            checked += 1
            try:
                # 按解释器加载语义读取（容忍 UTF-8 BOM 与 PEP 263 编码声明）
                with tokenize.open(path) as handle:
                    source = handle.read()
                compile(source, str(path), "exec")
            except SyntaxError as exc:
                errors.append(
                    f"{path.relative_to(repo_root)}:{exc.lineno}: {exc.msg}"
                )
    return StageResult(
        name="syntax",
        ok=not errors,
        summary=f"{checked} 个文件通过语法校验" if not errors else f"{len(errors)} 个文件存在语法错误",
        details=errors,
    )


def _trailing_whitespace_issues(repo_root: Path, relative: str) -> list[str]:
    """未跟踪文本文件的逐行行尾空白检查。

    ``git diff --check`` 不覆盖未跟踪文件；新增文件如果不查，验证入口就存在
    假绿通道。
    """

    path = repo_root / relative
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    issues: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if line != line.rstrip():
            issues.append(f"{relative}:{lineno}: 行尾存在空白（未跟踪文件）")
    return issues


def check_git_diff(repo_root: Path) -> StageResult:
    """空白/冲突检查：已跟踪文件用 ``git diff --check HEAD``，未跟踪文件自查行尾空白。"""

    try:
        completed = subprocess.run(
            ["git", "diff", "--check", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
        )
        untracked = subprocess.run(
            ["git", "ls-files", "-z", "--others", "--exclude-standard"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
        )
    except FileNotFoundError:
        return StageResult(
            name="git-diff-check",
            ok=False,
            summary="git 可执行文件不可用",
            details=["统一验证入口需要 git（用于 diff --check 阶段）"],
        )
    output = (completed.stdout + completed.stderr).strip()
    details = [line for line in output.splitlines() if line.strip()]
    if untracked.returncode == 0:
        # -z 输出（NUL 分隔）避免 git 对非 ASCII 文件名做 quotepath 转义
        for name in untracked.stdout.split("\0"):
            if name.strip():
                details.extend(_trailing_whitespace_issues(repo_root, name))
    if completed.returncode != 0 and not details:
        return StageResult(
            name="git-diff-check",
            ok=False,
            summary=f"git diff --check 失败（退出码 {completed.returncode}）",
            details=[],
        )
    return StageResult(
        name="git-diff-check",
        ok=not details,
        summary="无空白/冲突问题（未跟踪文件仅检查行尾空白）" if not details else f"{len(details)} 处问题",
        details=details,
    )


def check_architecture(repo_root: Path) -> StageResult:
    """机械化架构检查（baseline 外违规视为失败）。"""

    try:
        violations = collect_violations(repo_root)
    except ArchCheckError as exc:
        return StageResult(
            name="architecture",
            ok=False,
            summary="架构检查无法完成（显式失败，不视为通过）",
            details=[str(exc)],
        )
    baseline = load_baseline(BASELINE_PATH)
    known, new, stale = partition_by_baseline(violations, baseline)
    rules = len(configured_rules())
    if new:
        return StageResult(
            name="architecture",
            ok=False,
            summary=f"{rules} 条规则；发现 {len(new)} 条新违规（baseline {len(known)} 条）",
            details=[violation.format() for violation in new],
        )
    summary = f"{rules} 条规则通过；baseline 历史债务 {len(known)} 条（不得增长）"
    if stale:
        summary += f"；{len(stale)} 条 baseline 条目已不再命中（建议同步清理）"
    return StageResult(name="architecture", ok=True, summary=summary, details=[])


def run_pytest(repo_root: Path) -> StageResult:
    """全量 pytest（完整交付验证必须覆盖）。"""

    completed = subprocess.run([sys.executable, "-m", "pytest"], cwd=repo_root)
    if completed.returncode == 0:
        return StageResult(name="pytest", ok=True, summary="全量测试通过", details=[])
    return StageResult(
        name="pytest",
        ok=False,
        summary=(
            f"pytest 返回非零退出码 {completed.returncode}"
            f"（确认当前解释器已安装 pytest：{sys.executable}）"
        ),
        details=[],
    )


def run_verification(mode: str, repo_root: Path = REPO_ROOT) -> int:
    """执行完整验证流程，返回进程退出码（0 = 全部通过）。"""

    if mode not in ("fast", "full"):
        raise ValueError(f"未知验证模式: {mode!r}（支持 fast / full）")

    stages: list[StageResult] = [
        check_syntax(repo_root),
        check_git_diff(repo_root),
        check_architecture(repo_root),
    ]
    skipped: list[str] = []
    if mode == "full":
        stages.append(run_pytest(repo_root))
    else:
        skipped.append("pytest（fast 模式跳过；正式交付请运行 full 模式）")

    width = max(len(stage.name) for stage in stages)
    print(f"=== QRP Atlas verification (mode={mode}) ===")
    for index, stage in enumerate(stages, start=1):
        status = "PASS" if stage.ok else "FAIL"
        print(f"[{index}/{len(stages) + len(skipped)}] {stage.name.ljust(width)}  {status}  {stage.summary}")
        for detail in stage.details:
            for line in detail.splitlines():
                print(f"      {line}")
    for note in skipped:
        print(f"[跳过] {note}")

    failed = [stage.name for stage in stages if not stage.ok]
    if failed:
        print(f"=== RESULT: FAIL （失败阶段：{', '.join(failed)}）===")
        return 1
    print("=== RESULT: PASS ===")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QRP Atlas 统一验证入口")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="快速模式：语法 + diff-check + 架构检查（跳过 pytest）",
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT, help="仓库根（默认自动定位）")
    args = parser.parse_args(argv)
    return run_verification("fast" if args.fast else "full", args.repo_root)


if __name__ == "__main__":
    sys.exit(main())
