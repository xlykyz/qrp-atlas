"""统一验证入口（``tools/verify.py``）的测试。

覆盖：各阶段的真阳性 / 真阴性、退出码语义（任一步失败 => 非零）、
以及对当前仓库的 fast 模式端到端断言。
"""

from __future__ import annotations

from pathlib import Path

from tools.verify import (
    REPO_ROOT,
    check_architecture,
    check_git_diff,
    check_syntax,
    run_verification,
)


def _write(repo: Path, relative: str, content: str) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _seed_settings_stub(repo: Path) -> None:
    """最小 settings stub：让 config 读取规则可评估（否则架构阶段显式失败）。"""

    _write(
        repo,
        "src/qrp_atlas/config/settings.py",
        'SUPPORTED_ENV_VARS = frozenset({"QRP_DATA_DIR"})\n',
    )


# ---------------------------------------------------------------------------
# syntax 阶段
# ---------------------------------------------------------------------------


def test_syntax_stage_passes_on_valid_sources(tmp_path: Path) -> None:
    _write(tmp_path, "src/pkg/module.py", "VALUE = 1\n")
    _write(tmp_path, "tools/tool.py", "def main() -> int:\n    return 0\n")

    result = check_syntax(tmp_path)

    assert result.ok
    assert "2 个文件" in result.summary


def test_syntax_stage_reports_broken_source(tmp_path: Path) -> None:
    _write(tmp_path, "src/pkg/broken.py", "def broken(:\n")

    result = check_syntax(tmp_path)

    assert not result.ok
    assert any("broken.py" in detail for detail in result.details)


def test_syntax_stage_tolerates_bom_and_encoding_declaration(tmp_path: Path) -> None:
    # 与解释器加载语义一致：UTF-8 BOM 与 PEP 263 编码声明不视为语法错误
    bom_path = tmp_path / "src" / "pkg" / "with_bom.py"
    bom_path.parent.mkdir(parents=True, exist_ok=True)
    bom_path.write_bytes("\ufeffVALUE = 1\n".encode("utf-8"))
    _write(tmp_path, "src/pkg/declared.py", "# -*- coding: utf-8 -*-\nVALUE = 2\n")

    result = check_syntax(tmp_path)

    assert result.ok, result.details


# ---------------------------------------------------------------------------
# architecture 阶段
# ---------------------------------------------------------------------------


def test_architecture_stage_fails_on_new_violation(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.backtest import models\n")
    _seed_settings_stub(tmp_path)

    result = check_architecture(tmp_path)

    assert not result.ok
    assert any("ARCH-DEP-INDICATORS" in detail for detail in result.details)


def test_architecture_stage_fails_when_settings_missing(tmp_path: Path) -> None:
    """配置规则无法评估时必须显式失败，不得静默通过。"""

    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.contracts import schema\n")

    result = check_architecture(tmp_path)

    assert not result.ok
    assert "无法完成" in result.summary


def test_architecture_stage_passes_on_clean_tree(tmp_path: Path) -> None:
    _write(tmp_path, "src/qrp_atlas/indicators/probe.py", "from qrp_atlas.contracts import schema\n")
    _seed_settings_stub(tmp_path)

    result = check_architecture(tmp_path)

    assert result.ok


# ---------------------------------------------------------------------------
# git-diff-check 阶段
# ---------------------------------------------------------------------------


def test_git_diff_stage_detects_trailing_whitespace_in_worktree(tmp_path: Path) -> None:
    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "verify@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "verify-test"], cwd=tmp_path, check=True)
    _write(tmp_path, "note.txt", "clean\n")
    subprocess.run(["git", "add", "note.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True)
    _write(tmp_path, "note.txt", "trailing spaces   \n")

    result = check_git_diff(tmp_path)

    assert not result.ok
    assert result.details


def test_git_diff_stage_passes_on_clean_worktree(tmp_path: Path) -> None:
    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "verify@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "verify-test"], cwd=tmp_path, check=True)
    _write(tmp_path, "note.txt", "clean\n")
    subprocess.run(["git", "add", "note.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True)

    result = check_git_diff(tmp_path)

    assert result.ok


def test_git_diff_stage_detects_untracked_file_whitespace(tmp_path: Path) -> None:
    """未跟踪文件（``git diff --check`` 不覆盖）也必须检查，避免假绿通道。"""

    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "verify@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "verify-test"], cwd=tmp_path, check=True)
    _write(tmp_path, "tracked.txt", "clean\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True)
    _write(tmp_path, "untracked.txt", "trailing spaces   \n")

    result = check_git_diff(tmp_path)

    assert not result.ok
    assert any("untracked.txt" in detail for detail in result.details)


def test_git_diff_stage_checks_non_ascii_untracked_filenames(tmp_path: Path) -> None:
    """中文文件名不得因 git quotepath 转义而被跳过。"""

    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "verify@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "verify-test"], cwd=tmp_path, check=True)
    _write(tmp_path, "tracked.txt", "clean\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True)
    _write(tmp_path, "治理台账.md", "trailing spaces   \n")

    result = check_git_diff(tmp_path)

    assert not result.ok
    assert any("治理台账.md" in detail for detail in result.details)


# ---------------------------------------------------------------------------
# 退出码与端到端
# ---------------------------------------------------------------------------


def test_run_verification_returns_nonzero_when_a_stage_fails(tmp_path: Path) -> None:
    _write(tmp_path, "src/pkg/broken.py", "def broken(:\n")

    exit_code = run_verification("fast", tmp_path)

    assert exit_code != 0


def test_run_verification_returns_zero_for_clean_fast_run(tmp_path: Path) -> None:
    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "verify@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "verify-test"], cwd=tmp_path, check=True)
    _write(tmp_path, "src/pkg/module.py", "VALUE = 1\n")
    _seed_settings_stub(tmp_path)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True)

    exit_code = run_verification("fast", tmp_path)

    assert exit_code == 0


def test_current_repository_fast_verification_passes() -> None:
    """端到端：当前仓库在 fast 模式下必须通过（不触发 pytest 阶段）。"""

    exit_code = run_verification("fast", REPO_ROOT)

    assert exit_code == 0


def test_run_verification_rejects_unknown_mode() -> None:
    import pytest

    with pytest.raises(ValueError):
        run_verification("bogus", REPO_ROOT)
