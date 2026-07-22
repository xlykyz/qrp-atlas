#!/usr/bin/env bash
# Fallback launcher when systemd is unavailable.
set -euo pipefail
ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
MODE="${2:-full}"
PY="${QRP_ATLAS_PYTHON:-$ROOT/.venv/bin/python}"
cd "$ROOT"
readarray -t QRP_PATHS < <("$PY" - <<'PY'
from qrp_atlas.config.settings import AppSettings
settings = AppSettings.load()
if settings.runtime.read_only:
    raise SystemExit("QRP_READ_ONLY=true forbids the PIT backfill writer")
print(settings.paths.log_dir)
print(settings.paths.state_dir)
PY
)
LOG_DIR="${QRP_PATHS[0]}"
STATE_ROOT="${QRP_PATHS[1]}"
STATE_DIR="$STATE_ROOT/pit_backfill_${TAG}"
mkdir -p "$LOG_DIR" "$STATE_DIR"
LOCK="$STATE_DIR/runner.flock"
PID_FILE="$STATE_DIR/runner.pid"
OUT_LOG="$LOG_DIR/pit_backfill_${TAG}.service.out"

nohup flock -n "$LOCK" \
  "$PY" -m qrp_atlas.pipeline.pit_backfill \
    --mode "$MODE" --resume --run-tag "$TAG" --audit \
  >>"$OUT_LOG" 2>&1 &
echo $! >"$PID_FILE"
echo "started pid=$(cat "$PID_FILE") log=$OUT_LOG lock=$LOCK"
