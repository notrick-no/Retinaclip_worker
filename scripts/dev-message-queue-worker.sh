#!/usr/bin/env bash
# 本地跑 dev/message_queue_worker.py（消费 RabbitMQ，与 PPIO 容器内逻辑一致）
# 可设 BACKEND_URL、RABBITMQ_URL、MEDIA_QUEUE 等，见该文件内注释
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
if [[ -z "${PYTHON_CMD:-}" ]]; then
  echo "未找到 python/python3，请先: conda activate quzimu" >&2
  exit 1
fi
exec "$PYTHON_CMD" "$REPO_ROOT/dev/message_queue_worker.py"
