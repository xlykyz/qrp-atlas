#!/usr/bin/env bash
# Wait for fetch completion, raw-gate + backup + offline clean/load + audit.
set -euo pipefail

ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
PY="${QRP_ATLAS_PYTHON:-$ROOT/.venv/bin/python}"
FETCH_UNIT="${QRP_PIT_FETCH_UNIT:-qrp-pit-backfill-${TAG}-fetch.service}"
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
MANIFEST_PATH="$RUN_STATE_DIR/manifest.jsonl"
LOG="$LOG_DIR/pit_backfill_${TAG}.finish.log"
mkdir -p "$LOG_DIR" "$RUN_STATE_DIR"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

fetch_process_running() {
  pgrep -f "python -m qrp_atlas.pipeline.pit_backfill --mode full --resume --stages fetch --run-tag ${TAG}" >/dev/null 2>&1
}

fetch_unit_done() {
  local active sub result mainstatus
  active=$(systemctl show "$FETCH_UNIT" -p ActiveState --value 2>/dev/null || echo unknown)
  sub=$(systemctl show "$FETCH_UNIT" -p SubState --value 2>/dev/null || echo unknown)
  result=$(systemctl show "$FETCH_UNIT" -p Result --value 2>/dev/null || echo unknown)
  mainstatus=$(systemctl show "$FETCH_UNIT" -p ExecMainStatus --value 2>/dev/null || echo unknown)

  if [[ "$active" == "inactive" || "$active" == "failed" ]]; then
    return 0
  fi
  if [[ "$active" == "active" && "$sub" == "exited" && ( "$result" == "success" || "$mainstatus" == "0" ) ]]; then
    return 0
  fi
  if [[ "$active" == "active" && "$sub" == "dead" && "$result" == "success" ]]; then
    return 0
  fi
  return 1
}

manifest_fetch_snapshot() {
  "$PY" - "$MANIFEST_PATH" <<'PY'
import sys
from collections import Counter
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore
store = ManifestStore(sys.argv[1])
records = list(store.iter_records())
counts = Counter(record.fetch_status for record in records)
pending = sum(1 for record in records if record.fetch_status not in ("success", "empty", "failed"))
print(f"{pending} {counts.get('failed', 0)} {counts.get('success', 0)} {counts.get('empty', 0)} {counts.get('running', 0)}")
PY
}

log "watcher start; unit=$FETCH_UNIT root=$ROOT"
while true; do
  if fetch_process_running; then
    unit_state="process_running"
    done_like=0
  else
    if fetch_unit_done; then
      unit_state="unit_done"
      done_like=1
    else
      active=$(systemctl show "$FETCH_UNIT" -p ActiveState --value 2>/dev/null || echo unknown)
      sub=$(systemctl show "$FETCH_UNIT" -p SubState --value 2>/dev/null || echo unknown)
      result=$(systemctl show "$FETCH_UNIT" -p Result --value 2>/dev/null || echo unknown)
      mainstatus=$(systemctl show "$FETCH_UNIT" -p ExecMainStatus --value 2>/dev/null || echo unknown)
      unit_state="unit_${active}/${sub}/result=${result}/main=${mainstatus}"
      done_like=0
    fi
  fi

  read -r pending failed success empty running < <(manifest_fetch_snapshot)
  log "fetch_state=$unit_state pending=$pending failed=$failed success=$success empty=$empty running=$running"

  if [[ "$done_like" == "1" && "$pending" == "0" ]]; then
    if [[ "$failed" != "0" ]]; then
      log "ERROR fetch complete but failed=$failed; refuse clean/load until re-fetch"
      exit 3
    fi
    break
  fi

  if [[ "$done_like" == "1" && "$pending" != "0" ]]; then
    log "ERROR fetch unit done but manifest still has $pending non-terminal fetch stages"
    exit 2
  fi
  sleep 60
done

log "fetch complete; starting offline clean,load with raw-gate+backup"
set +e
"$PY" -m qrp_atlas.pipeline.pit_backfill \
  --mode full --resume --stages clean,load --offline-only \
  --run-tag "$TAG" \
  --log-path "$LOG_DIR/pit_backfill_${TAG}.cleanload.log" \
  --audit \
  >>"$LOG" 2>&1
rc=$?
set -e
log "clean/load finished rc=$rc"

"$PY" - "$MANIFEST_PATH" <<'PY' | tee -a "$LOG"
import sys
from collections import Counter
from pathlib import Path
import duckdb
from qrp_atlas.config.settings import get_settings
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore

settings = get_settings()
store = ManifestStore(sys.argv[1])
records = list(store.iter_records())
print("agg", dict(Counter(record.status for record in records)))
print("fetch", dict(Counter(record.fetch_status for record in records)))
print("clean", dict(Counter(record.clean_status for record in records)))
print("load", dict(Counter(record.load_status for record in records)))
failed = [record.batch_id for record in records if record.status == "failed" or "failed" in (record.fetch_status, record.clean_status, record.load_status)]
print("failed_batches", len(failed))
print("failed_sample", failed[:20])
connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
for table in ["income_statement", "balance_sheet", "cashflow_statement", "financial_indicator", "industry_membership_history", "index_component_history"]:
    try:
        print(table, connection.execute(f"select count(*) from {table}").fetchone()[0])
    except Exception as exc:
        print(table, type(exc).__name__)
connection.close()
print("db_size_gb", round(Path(settings.paths.duckdb_path).stat().st_size / 1024**3, 2))
PY
log "all done"
exit $rc
