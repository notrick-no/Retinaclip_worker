#!/usr/bin/env bash
# 列出派欧云 GPU 产品（需 PPIO_API_KEY，无需 PPIO_PRODUCT_ID）
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
if [[ -z "${PYTHON_CMD:-}" ]]; then
  echo "未找到 python" >&2
  exit 1
fi
exec "$PYTHON_CMD" -m ppio list-products
