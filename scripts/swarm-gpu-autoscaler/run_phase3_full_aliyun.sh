#!/usr/bin/env bash
set -euo pipefail

# Phase 3：接入阿里云启停，完整链路测试

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

echo "[Phase3] 加载 .env ..."
set -a
source .env
set +a

: "${RABBITMQ_URL:?RABBITMQ_URL 未设置}"
: "${RABBITMQ_QUEUE:?RABBITMQ_QUEUE 未设置}"
: "${SWARM_STACK_NAME:?SWARM_STACK_NAME 未设置}"
: "${SWARM_SERVICE_NAME:?SWARM_SERVICE_NAME 未设置}"
: "${SWARM_GPU_NODE_ID:?SWARM_GPU_NODE_ID 未设置}"
: "${SWARM_GPU_INSTANCE_ID:?SWARM_GPU_INSTANCE_ID 未设置}"
: "${ALIBABA_CLOUD_ACCESS_KEY_ID:?ALIBABA_CLOUD_ACCESS_KEY_ID 未设置}"
: "${ALIBABA_CLOUD_ACCESS_KEY_SECRET:?ALIBABA_CLOUD_ACCESS_KEY_SECRET 未设置}"
: "${ALIYUN_ECS_REGION:?ALIYUN_ECS_REGION 未设置}"

export AUTOSCALE_DRY_RUN=false
export AUTOSCALE_SINGLE_RUN=true

echo "[Phase3] 将根据队列长度："
echo "  - 有任务：必要时 StartInstance + 节点 active + service scale > 0"
echo "  - 空闲超时：service scale=0 + 节点 drain + StopInstance"
echo "[Phase3] 请在另一终端向 RabbitMQ 的 ${RABBITMQ_QUEUE} 队列发送几条消息观察行为。"

exec ./.venv/bin/python autoscaler.py

