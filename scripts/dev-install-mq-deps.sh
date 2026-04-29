#!/usr/bin/env bash
# 安装 dev/message_queue_worker.py 所需依赖（pika、requests）
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
if [[ -z "${PYTHON_CMD:-}" ]]; then
  echo "未找到 python" >&2
  exit 1
fi
exec "$PYTHON_CMD" -m pip install -r "$REPO_ROOT/dev/requirements.txt"
