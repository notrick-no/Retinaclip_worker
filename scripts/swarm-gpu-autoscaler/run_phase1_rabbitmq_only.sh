#!/usr/bin/env bash
set -euo pipefail

# Phase 1：只测试 RabbitMQ + autoscaler 逻辑（不真正 scale / 不调阿里云）
# 前置：在当前目录准备好 .env（可以从 .env.example 复制修改）

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

echo "[Phase1] 加载 .env ..."
set -a
source .env
set +a

: "${RABBITMQ_URL:?RABBITMQ_URL 未设置}"
: "${RABBITMQ_QUEUE:?RABBITMQ_QUEUE 未设置}"

export AUTOSCALE_DRY_RUN=true
export AUTOSCALE_SINGLE_RUN=true

echo "[Phase1] 仅连接 RabbitMQ 并计算队列长度，不会执行 docker / 阿里云操作。"
echo "[Phase1] RABBITMQ_URL=$RABBITMQ_URL"
echo "[Phase1] RABBITMQ_QUEUE=$RABBITMQ_QUEUE"

exec ./.venv/bin/python autoscaler.py

