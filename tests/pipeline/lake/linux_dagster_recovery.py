"""Opt-in real daemon/code-location restart and concurrency-slot recovery.

Uses a new database in the dedicated loopback test PostgreSQL and transient
user services. Schedules are never started; only two synthetic runs are sent.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

import psycopg
from psycopg import sql
import yaml


PROBE = '''from pathlib import Path
import os
import sys
import dagster as dg
from qrp_atlas.pipeline.lake.worker import run_worker, WorkerBudget

@dg.asset(pool="review_pool")
def review_probe(context):
    root = Path(os.environ["QRP_REVIEW_ROOT"])
    run_worker([sys.executable, str(root / "child.py"), str(root), context.run_id],
        directory=root / "attempts" / context.run_id,
        budget=WorkerBudget(timeout_seconds=180, memory_bytes=512*1024**2),
        environment={"PATH":os.environ["PATH"]})
    return dg.MaterializeResult(metadata={"isolated":True})

defs = dg.Definitions(assets=[review_probe], jobs=[dg.define_asset_job("review_job")])
'''

CHILD = '''from pathlib import Path
import os
import sys
import time
root = Path(sys.argv[1])
(root / (sys.argv[2]+".pid")).write_text(str(os.getpid()))
if (root / "mode").read_text() == "block":
    time.sleep(150)
'''


def wait_for(predicate, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(1)
    raise TimeoutError("isolated Dagster acceptance condition did not complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    if sys.platform != "linux" or not root.is_relative_to(Path.home() / "qrp-lake-tests"):
        raise ValueError("requires dedicated Linux review directory")
    dsn = os.environ["QRP_TEST_POSTGRES_DSN"]
    suffix = uuid.uuid4().hex[:10]
    database = "qrp_review_dagster_" + suffix
    home = root / ("dagster-" + suffix)
    home.mkdir()
    port = 14261
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", port))  # fail rather than displace another service
    with psycopg.connect(dsn, autocommit=True) as admin:
        if admin.info.host != "127.0.0.1" or admin.info.port != 55439:
            raise ValueError("requires the isolated test PostgreSQL port")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        admin.execute(sql.SQL("ALTER DATABASE {} SET timezone TO 'UTC'").format(sql.Identifier(database)))
    source = Path(__file__).resolve().parents[3]
    config = yaml.safe_load((source / "deploy/lake/dagster.yaml.example").read_text())
    config["run_monitoring"].update(poll_interval_seconds=1, free_slots_after_run_end_seconds=2,
                                     max_runtime_seconds=30)
    config["run_retries"] = {"enabled": False}
    (home / "dagster.yaml").write_text(yaml.safe_dump(config))
    (home / "definitions.py").write_text(PROBE)
    (home / "child.py").write_text(CHILD)
    (home / "mode").write_text("block")
    workspace = home / "workspace.yaml"
    workspace.write_text(yaml.safe_dump({"load_from": [{"grpc_server": {
        "host": "127.0.0.1", "port": port, "location_name": "isolated_review"}}]}))
    os.environ.update(DAGSTER_HOME=str(home), QRP_REVIEW_ROOT=str(home),
        DAGSTER_POSTGRES_URL=f"postgresql+psycopg2://qrp_review@127.0.0.1:55439/{database}")
    executable = Path(sys.executable).parent
    units = []
    def start(label, command):
        unit = f"qrp-lake-review-{label}-{suffix}-{len(units)}"
        args = ["systemd-run", "--user", "--unit=" + unit, "-p", "MemoryMax=3G", "-p", "CPUQuota=150%",
                "-p", "TasksMax=256", "-p", "KillMode=control-group", "-p", "TimeoutStopSec=10"]
        for name in ("DAGSTER_HOME", "DAGSTER_POSTGRES_URL", "QRP_REVIEW_ROOT", "PATH", "LD_LIBRARY_PATH"):
            if name in os.environ:
                args.append("--setenv=" + name + "=" + os.environ[name])
        subprocess.run([*args, *command], check=True, capture_output=True, text=True)
        units.append(unit)
        return unit
    grpc_command = [str(executable / "dagster"), "api", "grpc", "-h", "127.0.0.1", "-p", str(port),
                    "-f", str(home / "definitions.py"), "-a", "defs"]
    daemon_command = [str(executable / "dagster-daemon"), "run", "-w", str(workspace)]
    def launch(run_id):
        completed = subprocess.run([str(executable / "dagster"), "job", "launch", "-w", str(workspace),
            "-l", "isolated_review", "-j", "review_job", "--run-id", run_id], capture_output=True, text=True, timeout=60)
        (home / (run_id + ".launch.log")).write_text(completed.stdout + completed.stderr)
        assert completed.returncode == 0, "launch failed; see isolated launch log"
    def ready():
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            return False
    try:
        from dagster import DagsterInstance, DagsterRunStatus
        with DagsterInstance.get() as instance:
            instance.event_log_storage.set_concurrency_slots("review_pool", 1)
            grpc = start("grpc", grpc_command)
            wait_for(ready, 45)
            daemon = start("daemon", daemon_command)
            first_id = str(uuid.uuid4())
            launch(first_id)
            wait_for(lambda: (home / (first_id + ".pid")).exists())
            assert instance.event_log_storage.get_concurrency_info("review_pool").active_slot_count == 1
            child_pid = int((home / (first_id + ".pid")).read_text())
            subprocess.run(["systemctl", "--user", "stop", daemon], check=True)
            assert instance.get_run_by_id(first_id).status == DagsterRunStatus.STARTED
            start("daemon-restarted", daemon_command)
            subprocess.run(["systemctl", "--user", "kill", "--signal=KILL", "--kill-whom=all", grpc], check=True)
            wait_for(lambda: not Path(f"/proc/{child_pid}").exists(), 30)
            start("grpc-restarted", grpc_command)
            wait_for(ready, 45)
            wait_for(lambda: instance.get_run_by_id(first_id).is_finished)
            assert instance.get_run_by_id(first_id).status == DagsterRunStatus.FAILURE
            wait_for(lambda: instance.event_log_storage.get_concurrency_info("review_pool").active_slot_count == 0, 60)
            (home / "mode").write_text("fast")
            second_id = str(uuid.uuid4())
            launch(second_id)
            wait_for(lambda: instance.get_run_by_id(second_id).is_finished)
            assert instance.get_run_by_id(second_id).status == DagsterRunStatus.SUCCESS
            report = {"daemon_restart_preserved_active_run": True, "code_location_kill_removed_child": True,
                "interrupted_run": "FAILURE", "slot_released": True, "next_run": "SUCCESS",
                "recovery_policy": "Dagster run deadline; 30s in this test, 7200s template default; not instant worker health",
                "first_run_id": first_id, "second_run_id": second_id, "metadata_database": database}
            (root / "linux-dagster-recovery-results.json").write_text(json.dumps(report, indent=2))
            print(json.dumps(report))
    finally:
        for unit in reversed(units):
            log = subprocess.run(["journalctl", "--user", "-u", unit, "--no-pager", "-n", "150"], capture_output=True, text=True)
            (home / (unit + ".log")).write_text(log.stdout + log.stderr)
            subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True)
            subprocess.run(["systemctl", "--user", "reset-failed", unit], capture_output=True)


if __name__ == "__main__":
    main()
