#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOL_DIR="$ROOT_DIR/tools/remote_access"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"
PORT="${QRP_REMOTE_ACCESS_PORT:-8765}"
PYTHON_BIN="${QRP_REMOTE_ACCESS_PYTHON:-$ROOT_DIR/.venv/bin/python}"

mkdir -p "$RUNTIME_DIR/bin"
chmod 700 "$RUNTIME_DIR" "$RUNTIME_DIR/bin"

is_our_process() {
    local pid="$1"
    local required_fragment="$2"
    [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -Fq -- "$required_fragment"
}

for component in api tunnel; do
    pid_file="$RUNTIME_DIR/${component}.pid"
    if [[ -f "$pid_file" ]]; then
        pid="$(<"$pid_file")"
        fragment="remote_access"
        [[ "$component" == "tunnel" ]] && fragment="cloudflared tunnel"
        if is_our_process "$pid" "$fragment"; then
            echo "remote_access is already running ($component PID $pid). Use tools/remote_access/status.sh."
            exit 1
        fi
        rm -f "$pid_file"
    fi
done

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Python runtime not found at $PYTHON_BIN. Run 'UV_CACHE_DIR=/tmp/qrp-uv-cache uv sync --extra test' first." >&2
    exit 1
fi

if command -v cloudflared >/dev/null 2>&1; then
    CLOUDFLARED_BIN="$(command -v cloudflared)"
else
    CLOUDFLARED_BIN="$RUNTIME_DIR/bin/cloudflared"
    if [[ ! -x "$CLOUDFLARED_BIN" ]]; then
        case "$(uname -m)" in
            x86_64) asset="cloudflared-linux-amd64" ;;
            aarch64|arm64) asset="cloudflared-linux-arm64" ;;
            *) echo "Unsupported architecture for cloudflared: $(uname -m)" >&2; exit 1 ;;
        esac
        echo "cloudflared is not installed; downloading the official Cloudflare release binary locally."
        curl --fail --location --silent --show-error \
            "https://github.com/cloudflare/cloudflared/releases/latest/download/${asset}" \
            --output "$CLOUDFLARED_BIN"
        chmod 700 "$CLOUDFLARED_BIN"
    fi
fi

if ! "$CLOUDFLARED_BIN" --version >/dev/null 2>&1; then
    echo "cloudflared is unavailable or failed its version check." >&2
    exit 1
fi

umask 077
TOKEN_FILE="$RUNTIME_DIR/token"
"$PYTHON_BIN" - <<'PY' > "$TOKEN_FILE"
import secrets
print(secrets.token_urlsafe(32))
PY
chmod 600 "$TOKEN_FILE"
rm -f "$RUNTIME_DIR/public_url" "$RUNTIME_DIR/api.log" "$RUNTIME_DIR/tunnel.log"

cleanup_on_error() {
    "$TOOL_DIR/stop.sh" >/dev/null 2>&1 || true
}
trap cleanup_on_error ERR

QRP_REMOTE_ACCESS_TOKEN_FILE="$TOKEN_FILE" \
    nohup "$PYTHON_BIN" -m uvicorn --app-dir "$TOOL_DIR" --factory app:create_app \
    --host 127.0.0.1 --port "$PORT" --no-access-log \
    </dev/null >"$RUNTIME_DIR/api.log" 2>&1 &
API_PID=$!
echo "$API_PID" > "$RUNTIME_DIR/api.pid"

for _ in $(seq 1 30); do
    if curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:${PORT}/health" > "$RUNTIME_DIR/health.json"; then
        break
    fi
    sleep 1
done
if ! grep -Fq '"status":"ok"' "$RUNTIME_DIR/health.json" 2>/dev/null; then
    echo "Temporary API failed its local health check; see $RUNTIME_DIR/api.log" >&2
    exit 1
fi

nohup "$CLOUDFLARED_BIN" tunnel --no-autoupdate --url "http://127.0.0.1:${PORT}" --loglevel info \
    </dev/null >"$RUNTIME_DIR/tunnel.log" 2>&1 &
TUNNEL_PID=$!
echo "$TUNNEL_PID" > "$RUNTIME_DIR/tunnel.pid"

PUBLIC_URL=""
for _ in $(seq 1 45); do
    PUBLIC_URL="$(grep -Eo 'https://[-a-z0-9]+\.trycloudflare\.com' "$RUNTIME_DIR/tunnel.log" | head -n 1 || true)"
    [[ -n "$PUBLIC_URL" ]] && break
    sleep 1
done
if [[ -z "$PUBLIC_URL" ]]; then
    echo "Quick Tunnel did not provide a public URL; see $RUNTIME_DIR/tunnel.log" >&2
    exit 1
fi
printf '%s\n' "$PUBLIC_URL" > "$RUNTIME_DIR/public_url"

PUBLIC_READY=false
for _ in $(seq 1 12); do
    if curl --fail --silent --show-error --max-time 20 "$PUBLIC_URL/health" > /dev/null; then
        PUBLIC_READY=true
        break
    fi
    sleep 5
done
if [[ "$PUBLIC_READY" != true ]]; then
    echo "Quick Tunnel public health verification failed." >&2
    exit 1
fi

trap - ERR
TOKEN="$(<"$TOKEN_FILE")"
echo "Temporary QRP read-only gateway is running."
echo "Local API: http://127.0.0.1:${PORT}"
echo "Public API: ${PUBLIC_URL}"
echo "Token: ${TOKEN}"
echo "Header: Authorization: Bearer ${TOKEN}"
echo "Stop with: $TOOL_DIR/stop.sh"
