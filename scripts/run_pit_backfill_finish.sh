#!/usr/bin/env bash
# Wait for fetch completion, raw-gate + backup + offline clean/load + audit.
set -euo pipefail

# Configurable root; default resolves from this script location.
ROOT="${QRP_PROJECT_ROOT:-${QRP_ATLAS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}}"
TAG="${1:-20260714}"
PY="${QRP_ATLAS_PYTHON:-$ROOT/.venv/bin/python}"
LOG="$ROOT/data/logs/pit_backfill_${TAG}.finish.log"
FETCH_UNIT="${QRP_PIT_FETCH_UNIT:-qrp-pit-backfill-${TAG}-fetch.service}"
cd "$ROOT"
set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a
mkdir -p "$ROOT/data/logs"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

fetch_process_running() {
  pgrep -f "python -m qrp_atlas.pipeline.pit_backfill --mode full --resume --stages fetch --run-tag ${TAG}" >/dev/null 2>&1
}

fetch_unit_done() {
  # oneshot + RemainAfterExit=yes stays active after success; use SubState/Result.
  local active sub result mainstatus
  active=$(systemctl --user show "$FETCH_UNIT" -p ActiveState --value 2>/dev/null || echo unknown)
  sub=$(systemctl --user show "$FETCH_UNIT" -p SubState --value 2>/dev/null || echo unknown)
  result=$(systemctl --user show "$FETCH_UNIT" -p Result --value 2>/dev/null || echo unknown)
  mainstatus=$(systemctl --user show "$FETCH_UNIT" -p ExecMainStatus --value 2>/dev/null || echo unknown)

  if [[ "$active" == "inactive" || "$active" == "failed" ]]; then
    return 0
  fi
  # active (exited) after successful oneshot RemainAfterExit
  if [[ "$active" == "active" && "$sub" == "exited" && ( "$result" == "success" || "$mainstatus" == "0" ) ]]; then
    return 0
  fi
  if [[ "$active" == "active" && "$sub" == "dead" && "$result" == "success" ]]; then
    return 0
  fi
  return 1
}

manifest_fetch_snapshot() {
  "$PY" - <<PY
from collections import Counter
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore
s = ManifestStore("$ROOT/data/state/pit_backfill_${TAG}/manifest.jsonl")
c = Counter(r.fetch_status for r in s.iter_records())
pending = sum(1 for r in s.iter_records() if r.fetch_status not in ("success", "empty", "failed"))
failed = c.get("failed", 0)
print(f"{pending} {failed} {c.get('success',0)} {c.get('empty',0)} {c.get('running',0)}")
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
      active=$(systemctl --user show "$FETCH_UNIT" -p ActiveState --value 2>/dev/null || echo unknown)
      sub=$(systemctl --user show "$FETCH_UNIT" -p SubState --value 2>/dev/null || echo unknown)
      result=$(systemctl --user show "$FETCH_UNIT" -p Result --value 2>/dev/null || echo unknown)
      mainstatus=$(systemctl --user show "$FETCH_UNIT" -p ExecMainStatus --value 2>/dev/null || echo unknown)
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
  --log-path "$ROOT/data/logs/pit_backfill_${TAG}.cleanload.log" \
  --audit \
  >>"$LOG" 2>&1
rc=$?
set -e
log "clean/load finished rc=$rc"

"$PY" - <<PY | tee -a "$LOG"
from collections import Counter
from pathlib import Path
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore
from qrp_atlas.config import DB_PATH
import duckdb
s = ManifestStore("$ROOT/data/state/pit_backfill_${TAG}/manifest.jsonl")
print("agg", dict(Counter(r.status for r in s.iter_records())))
print("fetch", dict(Counter(r.fetch_status for r in s.iter_records())))
print("clean", dict(Counter(r.clean_status for r in s.iter_records())))
print("load", dict(Counter(r.load_status for r in s.iter_records())))
failed = [r.batch_id for r in s.iter_records() if r.status == "failed" or "failed" in (r.fetch_status, r.clean_status, r.load_status)]
print("failed_batches", len(failed))
print("failed_sample", failed[:20])
con = duckdb.connect(str(DB_PATH), read_only=True)
for t in ["income_statement","balance_sheet","cashflow_statement","financial_indicator","industry_membership_history","index_component_history"]:
    try:
        print(t, con.execute(f"select count(*) from {t}").fetchone()[0])
    except Exception as e:
        print(t, e)
con.close()
print("db_size_gb", round(Path(DB_PATH).stat().st_size / 1024**3, 2))
PY
log "all done"
exit $rc
