#!/usr/bin/env bash
# 在仓库根目录执行；可选加载 .env
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
export REPO_ROOT
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
# conda 下优先 `python`（当前环境）；系统上常只有 `python3`
if [[ -n "${PYTHON_CMD:-}" ]]; then
  :
elif command -v python >/dev/null 2>&1; then
  export PYTHON_CMD="$(command -v python)"
elif command -v python3 >/dev/null 2>&1; then
  export PYTHON_CMD="$(command -v python3)"
else
  export PYTHON_CMD=""
fi
