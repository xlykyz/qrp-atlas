"""Opt-in Linux mount-namespace/cgroup acceptance using transient user units.

Run with the isolated venv and --root <dedicated review directory>. No system
users, production services or databases are changed. This validates user-unit
namespaces; distinct system Unix identities remain a deployment acceptance.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    if sys.platform != "linux" or not root.is_relative_to(Path.home() / "qrp-lake-tests"):
        raise ValueError("requires a dedicated Linux review directory")
    source = Path(__file__).resolve().parents[3]
    sandbox = root / ("namespace-" + uuid.uuid4().hex)
    sandbox.mkdir()
    for name in ("raw", "core", "core-runtime", "extension", "extension-runtime", "dagster"):
        (sandbox / name).mkdir()
    probe = sandbox / "raw_probe.py"
    probe.write_text("from pathlib import Path\nimport pandas as pd\nfrom qrp_atlas.pipeline.lake.raw import RawStore\n"
                     "import sys\nb = RawStore(Path(sys.argv[1])).save_frame('fixture','daily',pd.DataFrame({'x':[1]}),parameters={},complete=True)\n"
                     "print(b.batch_id)\n", encoding="utf-8")

    def writable(unit):
        text = (source / "deploy/lake" / unit).read_text()
        value = next(line.split("=", 1)[1] for line in text.splitlines() if line.startswith("ReadWritePaths="))
        return value.replace("/var/lib/qrp-lake", str(sandbox))

    def run(label, paths, command, *, oom=False):
        unit = "qrp-lake-review-" + label + "-" + uuid.uuid4().hex[:8]
        args = ["systemd-run", "--user", "--unit=" + unit, "--wait", "--pipe",
                "-p", "PrivateUsers=yes", "-p", "ProtectSystem=strict", "-p", "ProtectHome=read-only",
                "-p", "ReadWritePaths=" + paths, "-p", "NoNewPrivileges=yes", "-p", "KillMode=control-group",
                "-p", "MemoryMax=" + ("128M" if oom else "1G"), "-p", "MemorySwapMax=0",
                "-p", "CPUQuota=100%", "-p", "TasksMax=64", "-p", "RuntimeMaxSec=45",
                "-p", "OOMPolicy=kill", "--setenv=PYTHONDONTWRITEBYTECODE=1", *command]
        result = subprocess.run(args, text=True, capture_output=True, timeout=60)
        status = subprocess.run(["systemctl", "--user", "show", unit, "-p", "Result", "-p", "MainPID",
                                 "-p", "MemoryPeak"], text=True, capture_output=True, check=True).stdout
        (root / (label + ".log")).write_text(result.stdout + result.stderr + status)
        subprocess.run(["systemctl", "--user", "reset-failed", unit], capture_output=True)
        return result, status

    core_paths = writable("qrp-lake-core.service.example")
    extension_paths = writable("qrp-lake-extension.service.example")
    command = [sys.executable, str(probe), str(sandbox / "raw")]
    old_paths = " ".join(path for path in core_paths.split() if path != str(sandbox / "raw"))
    old, _ = run("raw-before-fix", old_paths, command)
    namespace_enforced = old.returncode != 0 and "Read-only file system" in old.stderr
    if namespace_enforced:
        patched, _ = run("raw-core-fixed", core_paths, command)
        assert patched.returncode == 0, patched.stderr
        extension, _ = run("raw-extension-denied", extension_paths, command)
        assert extension.returncode != 0 and "Read-only file system" in extension.stderr, extension.stderr
        assert len(list((sandbox / "raw").glob("*/batch.json"))) == 1
    oom, status = run("kernel-memory-limit", str(sandbox / "extension"),
                      [sys.executable, "-c", "allocation = bytearray(512 * 1024 * 1024)"], oom=True)
    assert oom.returncode != 0 and "Result=oom-kill" in status and "MainPID=0" in status, status + oom.stderr
    report = {"raw_old_template_denied": namespace_enforced, "raw_fixed_template_saved": namespace_enforced,
              "extension_raw_write_denied": namespace_enforced, "kernel_memory_limit_killed_process": True,
              "scope": "transient systemd user units; PrivateUsers + ProtectSystem=strict",
              "limitation": "ProtectHome=read-only permits isolated code under home; distinct system Unix users not tested"}
    (root / "linux-isolation-results.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
    return 0 if namespace_enforced else 2


if __name__ == "__main__":
    raise SystemExit(main())
