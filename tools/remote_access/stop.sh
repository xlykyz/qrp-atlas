#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"

is_expected_process() {
    local pid="$1"
    local required_fragment="$2"
    [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -Fq -- "$required_fragment"
}

stop_component() {
    local component="$1"
    local required_fragment="$2"
    local pid_file="$RUNTIME_DIR/${component}.pid"
    [[ -f "$pid_file" ]] || return 0
    local pid
    pid="$(<"$pid_file")"
    if is_expected_process "$pid" "$required_fragment"; then
        kill -TERM "$pid"
        for _ in $(seq 1 10); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid"
        fi
        echo "Stopped $component process $pid."
    else
        echo "Skipping stale or non-matching $component PID file."
    fi
    rm -f "$pid_file"
}

stop_component tunnel "cloudflared tunnel"
stop_component api "remote_access"
rm -rf "$RUNTIME_DIR"
echo "Temporary remote access state cleared."
