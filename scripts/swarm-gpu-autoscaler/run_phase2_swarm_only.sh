#!/usr/bin/env bash
set -euo pipefail

# Phase 2：解开 Docker 部分，只测试 Swarm 扩缩容（不启停阿里云）

cd "$(dirname "$0")"

if [[ ! -f ".venv/bin/python" ]]; then
  echo "请先创建虚拟环境并安装依赖："
  echo "  python3 -m venv .venv"
  echo "  source .venv/bin/activate"
  echo "  pip install -r requirements.txt"
  exit 1
fi

if [[ ! -f ".env" ]]; then
  echo "错误：未找到 .env，请先在当前目录创建 .env（可从 .env.example 复制）。"
  exit 1
fi

echo "[Phase2] 加载 .env ..."
set -a
source .env
set +a

: "${RABBITMQ_URL:?RABBITMQ_URL 未设置}"
: "${RABBITMQ_QUEUE:?RABBITMQ_QUEUE 未设置}"
: "${SWARM_STACK_NAME:?SWARM_STACK_NAME 未设置}"
: "${SWARM_SERVICE_NAME:?SWARM_SERVICE_NAME 未设置}"

# 不做 DryRun，要实际执行 docker service scale；不配置 SWARM_GPU_INSTANCE_ID，避免调阿里云
unset SWARM_GPU_INSTANCE_ID || true
export AUTOSCALE_DRY_RUN=false
export AUTOSCALE_SINGLE_RUN=true

echo "[Phase2] 将根据队列长度实际执行 docker service scale（不启停 ECS 实例）。"
echo "[Phase2] Swarm 服务 = ${SWARM_STACK_NAME}_${SWARM_SERVICE_NAME}"

# 检查本机能否连上 Docker（Phase2 需在 Swarm Manager 节点或已启动 Docker 的本机运行）
if ! docker info &>/dev/null; then
  echo ""
  echo "错误：无法连接 Docker。"
  echo "Phase2 会真实执行 docker service scale，必须在以下环境之一运行："
  echo "  1) 云上 Swarm Manager 节点（已初始化 swarm 且部署了 stack）；"
  echo "  2) 本机已安装并启动 Docker Desktop，且已加入 Swarm。"
  echo "本机请先启动 Docker，或把此目录拷到 Manager 节点后再执行。"
  exit 1
fi
if ! docker node ls &>/dev/null; then
  echo ""
  echo "提示：当前环境不是 Swarm Manager（docker node ls 失败）。"
  echo "Phase2 需在 Manager 节点上运行才能 scale 服务。"
  exit 1
fi

exec ./.venv/bin/python autoscaler.py

