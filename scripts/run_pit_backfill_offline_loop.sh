#!/usr/bin/env bash
set -euo pipefail
ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
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
RUN_STATE_DIR="$STATE_ROOT/pit_backfill_${TAG}"
LOG="$LOG_DIR/pit_backfill_${TAG}.offline.log"
APP_LOG="$LOG_DIR/pit_backfill_${TAG}.offline.app.log"
MANIFEST_PATH="$RUN_STATE_DIR/manifest.jsonl"
mkdir -p "$LOG_DIR" "$RUN_STATE_DIR"

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

  fetch_active=$(systemctl show "qrp-pit-backfill-${TAG}-fetch.service" -p ActiveState --value 2>/dev/null || echo unknown)
  fetch_sub=$(systemctl show "qrp-pit-backfill-${TAG}-fetch.service" -p SubState --value 2>/dev/null || echo unknown)
  if pgrep -f "python -m qrp_atlas.pipeline.pit_backfill --mode full --resume --stages fetch --run-tag ${TAG}" >/dev/null 2>&1; then
    fetch_running=1
  else
    fetch_running=0
  fi
  pending=$("$PY" - "$MANIFEST_PATH" <<'PY'
import sys
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore, TERMINAL_OK
store = ManifestStore(sys.argv[1])
count = 0
for record in store.iter_records():
    if record.fetch_status in ("success", "empty") and (
        record.clean_status not in TERMINAL_OK or record.load_status not in TERMINAL_OK
    ):
        count += 1
print(count)
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
