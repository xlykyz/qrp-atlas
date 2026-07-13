#!/usr/bin/env bash
# Fallback launcher when user systemd is unavailable.
set -euo pipefail
ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
MODE="${2:-full}"
LOG_DIR="$ROOT/data/logs"
STATE_DIR="$ROOT/data/state/pit_backfill_${TAG}"
mkdir -p "$LOG_DIR" "$STATE_DIR"
LOCK="$STATE_DIR/runner.flock"
PID_FILE="$STATE_DIR/runner.pid"
OUT_LOG="$LOG_DIR/pit_backfill_${TAG}.service.out"

cd "$ROOT"
# shellcheck disable=SC1091
set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a

nohup flock -n "$LOCK" \
  "$ROOT/.venv/bin/python" -m qrp_atlas.pipeline.pit_backfill \
    --mode "$MODE" --resume --run-tag "$TAG" --audit \
  >>"$OUT_LOG" 2>&1 &
echo $! >"$PID_FILE"
echo "started pid=$(cat "$PID_FILE") log=$OUT_LOG lock=$LOCK"
