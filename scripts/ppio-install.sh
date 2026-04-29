#!/usr/bin/env bash
# 安装 PPIO 调度器 Python 依赖
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
if [[ -z "${PYTHON_CMD:-}" ]]; then
  echo "未找到 python" >&2
  exit 1
fi
"$PYTHON_CMD" -m pip install -r "$REPO_ROOT/ppio/requirements.txt"
echo "ok: ppio requirements installed"
