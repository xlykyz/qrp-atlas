#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"
TOOL_DIR="$ROOT_DIR/tools/remote_access"
PYTHON_BIN="${QRP_REMOTE_ACCESS_PYTHON:-$ROOT_DIR/.venv/bin/python}"
PORT="${QRP_REMOTE_ACCESS_PORT:-8765}"
DURATION="${QRP_SHARE_DURATION_MINUTES:-30}"

# Validate duration
if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [ "$DURATION" -lt 1 ] || [ "$DURATION" -gt 1440 ]; then
    echo "Error: QRP_SHARE_DURATION_MINUTES must be between 1 and 1440." >&2
    exit 1
fi

# Generate session via Python
SESSION_JSON="$("$PYTHON_BIN" - "$DURATION" <<'PYEOF'
import json, sys, secrets
from datetime import datetime, timedelta, timezone

duration = int(sys.argv[1])
now = datetime.now(timezone.utc)
session = {
    "session_id": secrets.token_urlsafe(32),
    "created_at": now.isoformat(),
    "expires_at": (now + timedelta(minutes=duration)).isoformat(),
    "revoked": False,
    "max_rows": 50,
    "duration_minutes": duration,
}
print(json.dumps(session, ensure_ascii=False))
PYEOF
)"

# Persist
mkdir -p "$RUNTIME_DIR"
umask 077
printf '%s\n' "$SESSION_JSON" > "$RUNTIME_DIR/capability_session.json"
chmod 600 "$RUNTIME_DIR/capability_session.json"

SESSION_ID="$(echo "$SESSION_JSON" | "$PYTHON_BIN" -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')"
EXPIRES_AT="$(echo "$SESSION_JSON" | "$PYTHON_BIN" -c 'import json,sys; print(json.load(sys.stdin)["expires_at"])')"

# Determine public URL
PUBLIC_URL=""
if [[ -f "$RUNTIME_DIR/public_url" ]]; then
    PUBLIC_URL="$(<"$RUNTIME_DIR/public_url")"
fi

# Print ChatGPT-ready info block
echo ""
echo "============================================================"
echo " Capability Session 已生成"
echo "============================================================"
echo ""

if [[ -n "$PUBLIC_URL" ]] && curl --fail --silent --max-time 5 "${PUBLIC_URL}/health" >/dev/null 2>&1; then
    ENTRY_URL="${PUBLIC_URL}/share/${SESSION_ID}"
    echo "这是我的本地 QRP 临时只读 Capability Session。"
    echo "入口地址：${ENTRY_URL}"
    echo "到期时间：${EXPIRES_AT}"
    echo "元信息完整地址：${ENTRY_URL}/meta"
    echo "表列表完整地址：${ENTRY_URL}/tables"
    echo "该地址只允许读取白名单内的数据，单次最多返回50行。"
    echo "请先读取元信息和表列表，再进行数据检查。"
    echo ""
    echo "⚠️  该地址和会话 ID 是临时凭证，请勿公开传播。"
    echo "到期或撤销后自动失效。"
else
    echo "⚠️  网关尚未就绪或无法从公网访问。"
    echo "   会话已保存，网关就绪后入口地址为："
    echo ""
    echo "   https://<public-url>/share/${SESSION_ID}"
    echo ""
    echo "   到期时间：${EXPIRES_AT}"
    echo ""
    echo "   先运行 tools/remote_access/start.sh 启动网关。"
    echo "   然后运行本脚本重新生成完整入口信息。"
fi

echo ""
echo "会话 ID（仅供确认，请勿传播）：${SESSION_ID}"
echo "到期时间：${EXPIRES_AT}"
echo "持续时间：${DURATION} 分钟"
echo ""
echo "撤销会话：tools/remote_access/share_stop.sh"
echo "查看状态：tools/remote_access/share_status.sh"
echo "重新生成：tools/remote_access/share_start.sh"
echo "============================================================"