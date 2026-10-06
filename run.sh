#!/usr/bin/env bash
# =============================================================================
# 一键启动脚本：同时启动前端（静态页面）与后端（Flask API）。
#
# 本项目为前后端同源部署：Flask 一个进程既提供后端 API（/api/*），
# 也托管前端页面（/page/*），因此只需启动一次即可。
#
# 用法：
#   ./run.sh                 # 默认 http://127.0.0.1:8000，前台运行，Ctrl+C 退出
#   ./run.sh --port 9000     # 指定端口
#   ./run.sh --host 0.0.0.0  # 允许局域网访问
#   ./run.sh --no-browser    # 不自动打开浏览器
#   ./run.sh --install       # 强制重新安装依赖后启动
#   ./run.sh --help          # 查看帮助
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")"

HOST=127.0.0.1
PORT=8000
OPEN_BROWSER=1
DO_INSTALL=""
PYTHON=python3
VENV_DIR=".venv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="${2:-127.0.0.1}"; shift 2 ;;
    --port|-p)
      PORT="${2:-8000}"; shift 2 ;;
    --no-browser)
      OPEN_BROWSER=0; shift ;;
    --install|-i)
      DO_INSTALL=1; shift ;;
    --help|-h)
      cat <<'EOF'
一键启动自动化测试与持续集成平台（前端页面 + 后端 API 同源部署）

用法:
  ./run.sh                 # 默认 http://127.0.0.1:8000，前台运行，Ctrl+C 退出
  ./run.sh --port 9000     # 指定端口
  ./run.sh --host 0.0.0.0  # 绑定所有网卡，允许局域网访问
  ./run.sh --no-browser    # 不自动打开浏览器
  ./run.sh --install       # 强制重新安装依赖后启动
  ./run.sh --help          # 查看帮助
EOF
      exit 0 ;;
    *)
      echo "未知参数: $1（用 --help 查看帮助）" >&2; exit 1 ;;
  esac
done

# -- 依赖准备 -------------------------------------------------------------
if [[ -x "$VENV_DIR/bin/python" ]]; then
  PYTHON="$VENV_DIR/bin/python"
  echo "[run] 使用已存在的虚拟环境 $VENV_DIR"
elif [[ -n "$DO_INSTALL" ]] || ! "$PYTHON" -c "import flask" >/dev/null 2>&1; then
  echo "[run] 创建虚拟环境并安装依赖..."
  "$PYTHON" -m venv "$VENV_DIR"
  PYTHON="$VENV_DIR/bin/python"
  "$PYTHON" -m pip install --upgrade pip >/dev/null
  "$PYTHON" -m pip install -r requirements.txt
fi

# -- 端口占用检查 -----------------------------------------------------------
if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$PORT" 2>/dev/null | grep -q LISTEN; then
  echo "[run] 端口 $PORT 已被占用，请换端口（如 ./run.sh --port 9000）" >&2
  exit 1
fi

# -- 启动信息 ---------------------------------------------------------------
FRONTEND_URL="http://$HOST:$PORT/"
BACKEND_URL="http://$HOST:$PORT/api"

echo "[run] 自动化测试与持续集成平台启动中..."
echo "──────────────────────────────────────────────────────────"
echo "  前端地址（浏览器打开）:  $FRONTEND_URL"
echo "      → 项目列表页:        http://$HOST:$PORT/page/projects"
echo "  后端 API 根路径:         $BACKEND_URL"
echo "  数据存储目录:            $(pwd)/data"
echo "  停止服务:                Ctrl+C"
echo "──────────────────────────────────────────────────────────"

# 局域网访问提示
if [[ "$HOST" == "0.0.0.0" ]] && command -v hostname >/dev/null 2>&1; then
  LAN_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
  if [[ -n "$LAN_IP" ]]; then
    echo "  局域网访问地址:          http://$LAN_IP:$PORT/"
  fi
fi
echo ""

# -- 自动打开浏览器 ---------------------------------------------------------
if [[ "$OPEN_BROWSER" == "1" ]]; then
  ( sleep 1.5
    if command -v xdg-open >/dev/null 2>&1; then xdg-open "http://127.0.0.1:$PORT"
    elif command -v open >/dev/null 2>&1; then open "http://127.0.0.1:$PORT"
    fi ) >/dev/null 2>&1 &
fi

exec "$PYTHON" app.py --host "$HOST" --port "$PORT"
