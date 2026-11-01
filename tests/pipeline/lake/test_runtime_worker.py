from pathlib import Path
import os
import sys
import time
import subprocess

import pytest
import psutil

from qrp_atlas.pipeline.lake.worker import WorkerBudget, WorkerFailure, child_environment, run_worker


def _environment():
    return {key: value for key, value in os.environ.items()
            if key.upper() in {"SYSTEMROOT", "WINDIR", "PATH", "PATHEXT"}}


def _script(tmp_path: Path, body: str) -> list[str]:
    script = tmp_path / "child.py"
    script.write_text(body, encoding="utf-8")
    return [sys.executable, str(script)]


def test_child_environment_does_not_inherit_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "should-not-be-inherited")
    env = child_environment({"PATH": "test"}, WorkerBudget(threads=2), tmp_path)
    assert "TUSHARE_TOKEN" not in env
    assert env["OMP_NUM_THREADS"] == "2"


def test_success_and_nonzero_exit_are_distinct(tmp_path):
    argv = _script(tmp_path, "print('complete')\n")
    result = run_worker(argv, directory=tmp_path / "ok", budget=WorkerBudget(), environment=_environment())
    assert result.returncode == 0
    assert result.stdout_path.read_text().strip() == "complete"
    argv = _script(tmp_path, "raise SystemExit(7)\n")
    with pytest.raises(WorkerFailure, match="WORKER_EXIT_FAILED") as error:
        run_worker(argv, directory=tmp_path / "bad", budget=WorkerBudget(), environment=_environment())
    assert error.value.result.returncode == 7


@pytest.mark.parametrize("case", ["timeout", "cancel", "disk", "memory"])
def test_worker_limits_terminate_process(tmp_path, case):
    script = "import time\ntime.sleep(20)\n"
    budget = WorkerBudget(timeout_seconds=0.5)
    if case == "disk":
        script = "import pathlib,time\npathlib.Path('blob').write_bytes(b'x' * 10000)\ntime.sleep(20)\n"
        budget = WorkerBudget(temporary_bytes=1000, timeout_seconds=5)
    if case == "memory":
        script = "import time\nblob = bytearray(50_000_000)\ntime.sleep(20)\n"
        budget = WorkerBudget(memory_bytes=20_000_000, timeout_seconds=5)
    started = time.monotonic()
    expected = {"timeout": "WORKER_TIMEOUT", "cancel": "WORKER_CANCELLED",
                "disk": "WORKER_DISK_LIMIT", "memory": "WORKER_MEMORY_LIMIT"}[case]
    with pytest.raises(WorkerFailure, match=expected):
        run_worker(_script(tmp_path, script), directory=tmp_path / "attempt", budget=budget,
                   environment=_environment(),
                   cancel_requested=lambda: case == "cancel" and time.monotonic() - started > 0.15)
    assert time.monotonic() - started < 10


def test_worker_does_not_reuse_attempt_directory(tmp_path):
    with pytest.raises(FileExistsError):
        run_worker([sys.executable], directory=tmp_path, budget=WorkerBudget(), environment=_environment())


def test_timeout_stops_descendant_process(tmp_path):
    child = tmp_path / "grandchild.py"
    child.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
    pidfile = tmp_path / "grandchild.pid"
    body = ("import pathlib,subprocess,sys,time\n"
            f"p = subprocess.Popen([sys.executable, {str(child)!r}])\n"
            f"pathlib.Path({str(pidfile)!r}).write_text(str(p.pid))\n"
            "time.sleep(30)\n")
    with pytest.raises(WorkerFailure, match="WORKER_TIMEOUT"):
        run_worker(_script(tmp_path, body), directory=tmp_path / "attempt", environment=_environment(),
                   budget=WorkerBudget(timeout_seconds=1))
    assert pidfile.exists()
    assert not psutil.pid_exists(int(pidfile.read_text()))


def test_failed_extension_does_not_poison_independent_core_worker(tmp_path):
    slow = _script(tmp_path, "import time\ntime.sleep(30)\n")
    with pytest.raises(WorkerFailure):
        run_worker(slow, directory=tmp_path / "extension", environment=_environment(),
                   budget=WorkerBudget(timeout_seconds=0.2))
    core = _script(tmp_path, "print('core can publish')\n")
    result = run_worker(core, directory=tmp_path / "core", environment=_environment(), budget=WorkerBudget())
    assert result.returncode == 0


def test_guard_cleans_child_after_supervisor_is_killed(tmp_path):
    child = tmp_path / "long_child.py"
    pidfile = tmp_path / "child.pid"
    child.write_text("import os,pathlib,time\n"
                     f"pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid()))\n"
                     "time.sleep(30)\n", encoding="utf-8")
    source = Path(__file__).resolve().parents[3] / "src"
    supervisor = tmp_path / "supervisor.py"
    supervisor.write_text(
        "import sys\n" + f"sys.path.insert(0, {str(source)!r})\n"
        "from pathlib import Path\n"
        "from qrp_atlas.pipeline.lake.worker import WorkerBudget,run_worker\n"
        f"run_worker([sys.executable, {str(child)!r}], directory=Path({str(tmp_path / 'attempt')!r}), "
        f"budget=WorkerBudget(), environment={_environment()!r})\n", encoding="utf-8")
    process = subprocess.Popen([sys.executable, str(supervisor)], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    try:
        deadline = time.monotonic() + 10
        while not pidfile.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert pidfile.exists()
        pid = int(pidfile.read_text())
        process.kill()
        process.wait(timeout=5)
        deadline = time.monotonic() + 5
        while psutil.pid_exists(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not psutil.pid_exists(pid)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
