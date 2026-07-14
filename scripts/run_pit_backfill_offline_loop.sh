#!/usr/bin/env bash
set -euo pipefail
ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
PY="${QRP_ATLAS_PYTHON:-$ROOT/.venv/bin/python}"
LOG="$ROOT/data/logs/pit_backfill_${TAG}.offline.log"
APP_LOG="$ROOT/data/logs/pit_backfill_${TAG}.offline.app.log"
mkdir -p "$ROOT/data/logs" "$ROOT/data/state/pit_backfill_${TAG}"
cd "$ROOT"
set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a

idle_rounds=0
while true; do
  echo "[$(date '+%F %T')] offline clean,load pass" | tee -a "$LOG"
  set +e
  "$PY" -m qrp_atlas.pipeline.pit_backfill \
    --mode full --resume --stages clean,load --offline-only \
    --run-tag "$TAG" --skip-preflight \
    --log-path "$APP_LOG" \
    >>"$LOG" 2>&1
  rc=$?
  set -e
  echo "[$(date '+%F %T')] offline pass rc=$rc" | tee -a "$LOG"

  # Prefer process + SubState over naive is-active (oneshot RemainAfterExit)
  fetch_active=$(systemctl --user show "qrp-pit-backfill-${TAG}-fetch.service" -p ActiveState --value 2>/dev/null || echo unknown)
  fetch_sub=$(systemctl --user show "qrp-pit-backfill-${TAG}-fetch.service" -p SubState --value 2>/dev/null || echo unknown)
  if pgrep -f "python -m qrp_atlas.pipeline.pit_backfill --mode full --resume --stages fetch --run-tag ${TAG}" >/dev/null 2>&1; then
    fetch_running=1
  else
    fetch_running=0
  fi
  pending=$("$PY" - <<PY
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore, TERMINAL_OK
s = ManifestStore("data/state/pit_backfill_${TAG}/manifest.jsonl")
n = 0
for r in s.iter_records():
    if r.fetch_status in ("success", "empty") and (
        r.clean_status not in TERMINAL_OK or r.load_status not in TERMINAL_OK
    ):
        n += 1
print(n)
PY
)
  echo "[$(date '+%F %T')] pending_clean_load=$pending fetch_active=$fetch_active/$fetch_sub process=$fetch_running" | tee -a "$LOG"

  fetch_busy=0
  if [[ "$fetch_running" == "1" ]]; then
    fetch_busy=1
  elif [[ "$fetch_active" == "activating" ]]; then
    fetch_busy=1
  elif [[ "$fetch_active" == "active" && "$fetch_sub" != "exited" && "$fetch_sub" != "dead" ]]; then
    fetch_busy=1
  fi

  if [[ "$pending" == "0" ]]; then
    if [[ "$fetch_busy" == "0" ]]; then
      echo "[$(date '+%F %T')] offline drain complete" | tee -a "$LOG"
      exit 0
    fi
    idle_rounds=$((idle_rounds + 1))
    sleep 20
  else
    idle_rounds=0
    sleep 3
  fi

  if [[ "$fetch_busy" == "0" && $idle_rounds -ge 3 ]]; then
    echo "[$(date '+%F %T')] fetch stopped and idle; exit" | tee -a "$LOG"
    exit 0
  fi
done
