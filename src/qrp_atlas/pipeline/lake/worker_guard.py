"""Small process supervisor that outlives an abruptly killed Dagster worker.

Invoked by file path so it can start before the project's package is imported.
No QRP configuration or credentials are parsed here.
"""
import os
import signal
import subprocess
import sys
import time

import psutil


def main():
    parent_pid, parent_created = int(sys.argv[1]), float(sys.argv[2])
    known = {}
    process = subprocess.Popen(sys.argv[3:], stdin=subprocess.DEVNULL)
    try:
        while process.poll() is None:
            for child in psutil.Process().children(recursive=True):
                known[child.pid] = child
            try:
                parent = psutil.Process(parent_pid)
                alive = parent.create_time() == parent_created and parent.is_running()
            except psutil.NoSuchProcess:
                alive = False
            if not alive:
                if os.name != "nt":
                    os.killpg(os.getpgrp(), signal.SIGKILL)
                return 125
            time.sleep(0.1)
        return process.returncode
    finally:
        for child in reversed(list(known.values())):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
        psutil.wait_procs(list(known.values()), timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
