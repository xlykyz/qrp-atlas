#!/usr/bin/env bash
# Explicit privileged supplement: transient services only, no installed units,
# new users, production connections, service restarts or database operations.
set -euo pipefail
test "$(id -u)" = 0
test_root=/home/claire/qrp-lake-tests/review-86fb751-20261102
test "$(realpath "$test_root")" = "$test_root"
getent passwd claire > /dev/null
getent passwd nobody > /dev/null
python="$test_root/venv/bin/python"
probe="$test_root/source/tests/pipeline/lake/linux_raw_probe.py"
test -x "$python"
test -f "$probe"
sandbox=$(mktemp -d "$test_root/privileged-raw.XXXXXX")
chown claire:claire "$sandbox"
chmod 0755 "$sandbox"
for name in raw core core-runtime extension extension-runtime dagster; do
  install -d -o claire -g claire -m 0755 "$sandbox/$name"
done
chmod 0700 "$sandbox/raw"
prefix="qrp-lake-review-system-$$"
run_core() {
  systemd-run --wait --pipe --collect --unit="$prefix-$1" \
    -p User=claire -p Group=claire -p ProtectSystem=strict -p ProtectHome=read-only \
    -p PrivateTmp=yes -p NoNewPrivileges=yes -p KillMode=control-group \
    -p MemoryMax=1G -p CPUQuota=100% -p RuntimeMaxSec=30 \
    -p "ReadWritePaths=$2" --setenv=PYTHONDONTWRITEBYTECODE=1 \
    "$python" "$probe" "$sandbox/raw"
}
paths=$(sed -n 's/^ReadWritePaths=//p' "$test_root/source/deploy/lake/qrp-lake-core.service.example")
paths=${paths//\/var\/lib\/qrp-lake/$sandbox}
old_paths="$sandbox/core $sandbox/core-runtime $sandbox/dagster"
if run_core old-template "$old_paths" > "$sandbox/old.log" 2>&1; then
  echo 'ERROR: original template unexpectedly allowed Raw write'; exit 1
fi
grep -q '"errno": 30' "$sandbox/old.log"
run_core fixed-template "$paths" > "$sandbox/fixed.log" 2>&1
test "$(find "$sandbox/raw" -name batch.json | wc -l)" = 1
if systemd-run --wait --pipe --collect --unit="$prefix-extension" \
  -p User=nobody -p ProtectSystem=strict -p NoNewPrivileges=yes -p RuntimeMaxSec=15 \
  -p "ReadWritePaths=$sandbox/extension" /usr/bin/touch "$sandbox/raw/forbidden" \
  > "$sandbox/extension.log" 2>&1; then
  echo 'ERROR: extension identity unexpectedly wrote Raw'; exit 1
fi
test ! -e "$sandbox/raw/forbidden"
printf '%s\n' '{"old_template_raw_denied":true,"fixed_template_real_raw_saved":true,"distinct_identity_raw_denied":true,"scope":"transient system services; User=claire/nobody; ProtectSystem=strict; ProtectHome=read-only for isolated code"}' > "$sandbox/result.json"
cat "$sandbox/result.json"
printf 'Evidence directory: %s\n' "$sandbox"
