#!/usr/bin/env bash
# 检查 PPIO 调度器所需环境变量是否已设置（不发起网络请求）
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

ok=0
need() {
  local n="$1"
  # shellcheck disable=SC2154
  local v="${!n-}"
  if [[ -z "${v}" ]]; then
    echo "缺少: $n" >&2
    ok=1
  else
    echo "已设置: $n"
  fi
}

echo "== PPIO 调度器 必需 =="
if [[ -n "${PPIO_API_KEY:-}" ]]; then
  echo "已设置: PPIO_API_KEY"
elif [[ -n "${PPINFRA_API_KEY:-}" ]]; then
  echo "已设置: PPINFRA_API_KEY"
else
  echo "缺少: PPIO_API_KEY 或 PPINFRA_API_KEY" >&2
  ok=1
fi
if [[ -n "${PPIO_MANAGED_INSTANCE_ID:-}" ]]; then
  echo "已设置: PPIO_MANAGED_INSTANCE_ID（仅启停模式）"
else
  need PPIO_PRODUCT_ID
fi
need RABBITMQ_URL
echo "== 管理 API =="
if [[ -n "${PPIO_ADMIN_TOKEN:-}" ]]; then
  echo "已设置: PPIO_ADMIN_TOKEN"
else
  echo "未设置 PPIO_ADMIN_TOKEN（不启用 HTTP 管理口；仅本机调试用可开）" >&2
fi
echo "== 建议 =="
if [[ -n "${PPIO_IMAGE_URL:-}" ]]; then
  echo "已设置: PPIO_IMAGE_URL"
else
  echo "未设置 PPIO_IMAGE_URL（走 create 模式时 create 会用到；仅启停可忽略）" >&2
fi
if [[ -n "${BACKEND_URL:-}" ]]; then
  echo "已设置: BACKEND_URL"
fi
if [[ -n "${PPIO_ADMIN_TOKEN:-}" && -n "${PPIO_MANAGED_INSTANCE_ID:-}" ]]; then
  :
elif [[ -n "${PPIO_ADMIN_TOKEN:-}" ]]; then
  echo "提示: 有管理 Token 时建议同时用 PPIO_MANAGED_INSTANCE_ID 做启停" >&2
fi

if [[ "$ok" -ne 0 ]]; then
  echo "" >&2
  echo "请复制 env_example 为 .env 并填写，或 export 上述变量。" >&2
  exit 1
fi
echo "check-ppio-env: 通过"
