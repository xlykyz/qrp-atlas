#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"
PORT="${QRP_REMOTE_ACCESS_PORT:-8765}"

show_component() {
    local component="$1"
    local pid_file="$RUNTIME_DIR/${component}.pid"
    if [[ ! -f "$pid_file" ]]; then
        echo "$component: stopped"
        return
    fi
    local pid
    pid="$(<"$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        echo "$component: running (PID $pid)"
    else
        echo "$component: stale PID file"
    fi
}

show_component api
if [[ -f "$RUNTIME_DIR/api.pid" ]] && ! kill -0 "$(<"$RUNTIME_DIR/api.pid")" 2>/dev/null \
    && curl --fail --silent --max-time 2 "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    echo "api_http: reachable (PID visibility unavailable)"
fi
show_component tunnel
if [[ -f "$RUNTIME_DIR/public_url" ]]; then
    echo "public_url: $(<"$RUNTIME_DIR/public_url")"
    if curl --fail --silent --max-time 10 "$(<"$RUNTIME_DIR/public_url")/health" >/dev/null 2>&1; then
        echo "tunnel_https: reachable"
    else
        echo "tunnel_https: unreachable"
    fi
fi
if [[ -f "$RUNTIME_DIR/token" ]]; then
    echo "token: present (redacted)"
fi
