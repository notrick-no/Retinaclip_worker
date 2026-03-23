#!/usr/bin/env bash
# 超短命令：在项目根执行  ./scripts/rc.sh <子命令>
# 例：./scripts/rc.sh s    ./scripts/rc.sh p    ./scripts/rc.sh p subtitle
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cmd="${1:-}"
shift || true
case "$cmd" in
  s|start|w|worker)
    exec npx tsx index.ts
    ;;
  p|pool)
    exec npx tsx scripts/test-ecs-pool.ts "$@"
    ;;
  c|config)
    exec npx tsx scripts/verify-config.ts
    ;;
  r|mq|rabbit)
    exec npx tsx scripts/test-rabbitmq.ts
    ;;
  e|ecs)
    exec npx tsx scripts/test-ecs-api.ts
    ;;
  m|smoke)
    exec npm run test:smoke
    ;;
  t|test|unit)
    exec npm test
    ;;
  '')
    echo "用法: ./scripts/rc.sh <命令> [参数]"
    echo "  s|w|start        启动 Worker"
    echo "  p|pool [profile] 检查 ECS 池"
    echo "  c|config         校验 .env（无网络）"
    echo "  r|mq             测 RabbitMQ 连接"
    echo "  e|ecs            测 ECS API + 镜像"
    echo "  m|smoke          config+mq+ecs 三连"
    echo "  t|unit           vitest 单元测试"
    exit 1
    ;;
  *)
    echo "未知命令: $cmd  （用无参数查看帮助）"
    exit 1
    ;;
esac
