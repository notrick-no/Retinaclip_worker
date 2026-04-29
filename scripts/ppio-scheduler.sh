#!/usr/bin/env bash
# 启动 PPIO 调度器：有队列消息起算力、无消息关实例
# 必需环境变量见 env_example 中「派欧云 PPIO 调度器」一段
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
if [[ -z "${PYTHON_CMD:-}" ]]; then
  echo "未找到 python" >&2
  exit 1
fi
exec "$PYTHON_CMD" -m ppio
