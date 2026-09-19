#!/usr/bin/env bash
# 不用 Docker 时的本地启动脚本（macOS / Linux / WSL）。
# 同时拉起 API 与 Web UI，Ctrl-C 一起退出。
#
# 对外只暴露 PANEL_PORT（默认 3008）：面板会把 /v1/* 反代给内网的 API。
set -euo pipefail

cd "$(dirname "$0")"

# ---- 可按需覆盖 ----
export CODEBUDDY_AUTH_DIR="${CODEBUDDY_AUTH_DIR:-$PWD/data/auth}"
export PANEL_PORT="${PANEL_PORT:-3008}"
export API_PORT="${API_PORT:-3009}"
export CONVERTER_BASE="${CONVERTER_BASE:-http://127.0.0.1:$API_PORT}"

mkdir -p "$CODEBUDDY_AUTH_DIR"

PY="${PYTHON:-python3}"

if ! "$PY" -c "import fastapi, uvicorn, httpx" >/dev/null 2>&1; then
  echo "[start.sh] 缺少依赖，正在安装…"
  "$PY" -m pip install -r requirements.txt
fi

echo "[start.sh] auth dir : $CODEBUDDY_AUTH_DIR"
echo "[start.sh] 入口     : http://0.0.0.0:$PANEL_PORT  （面板 + /v1 API）"
echo "[start.sh] 内网 API : http://127.0.0.1:$API_PORT  （由面板反代）"

cd app
# API 只监听回环，不直接对外暴露
"$PY" converter.py --host 127.0.0.1 --port "$API_PORT" --skip-check --desensitize \
  --api-key-file "$CODEBUDDY_AUTH_DIR/.api_key" &
API_PID=$!
"$PY" panel.py &
PANEL_PID=$!

cleanup() { kill "$API_PID" "$PANEL_PID" 2>/dev/null || true; }
trap cleanup INT TERM EXIT
wait
