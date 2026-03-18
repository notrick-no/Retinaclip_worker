/**
 * Cloud-Init 用户数据脚本生成器
 * 
 * 生成 ECS 实例的启动脚本（通过 UserData 注入）
 * 
 * ECS 实例启动后自动执行的流程：
 * ┌─────────────────────────────────────────────┐
 * │ 1. cloud-init 解析 UserData 脚本            │
 * │ 2. 写入任务参数到环境变量文件               │
 * │ 3. 拉取并启动算法处理 Docker 容器           │
 * │ 4. 容器内：下载视频 → 处理 → 上传结果      │
 * │ 5. 通过 Webhook 回报处理结果                │
 * │ 6. 写入完成标记文件（供编排器轮询检测）     │
 * └─────────────────────────────────────────────┘
 * 
 * 完成标记约定：
 *   /tmp/mingle-task-done    - 任务完成标记
 *   /tmp/mingle-task-result  - 结果 JSON 文件
 */

import { WorkerConfig } from './config'

/** 传递给 ECS 实例的任务参数 */
export interface TaskParams {
  messageId: string
  userId?: string
  videoDownloadUrl: string
  webhookUrl: string
  detectType: string
  targetRegions?: string   // JSON 字符串
}

/**
 * 生成 cloud-init UserData 脚本
 * 
 * 阿里云 ECS UserData 要求：
 * - 必须是 Base64 编码
 * - 脚本类型由第一行 shebang 决定
 * - 最大 16KB
 * 
 * @returns Base64 编码的 UserData 字符串
 */
export function generateUserData(
  task: TaskParams,
  config: WorkerConfig,
  processingImage: string,
): string {
  const script = generateStartupScript(task, config, processingImage)
  return Buffer.from(script).toString('base64')
}

/**
 * 生成 Bash 启动脚本（真实 Docker 或 Mock）
 */
function generateStartupScript(
  task: TaskParams,
  config: WorkerConfig,
  processingImage: string,
): string {
  if (config.ecs.mockProcessing) {
    return generateMockStartupScript(task, config)
  }
  return generateDockerStartupScript(task, config, processingImage)
}

/**
 * Mock 模式：不拉取/不运行算法镜像，延迟后直接返回原视频 URL
 */
function generateMockStartupScript(task: TaskParams, config: WorkerConfig): string {
  const esc = (s: string) => s.replace(/'/g, "'\\''")
  const delaySec = Math.max(1, config.ecs.mockDelaySeconds ?? 60)
  const resultJson = JSON.stringify({ output_video_url: task.videoDownloadUrl })

  return `#!/bin/bash
set -euo pipefail

# =========================================================
# Mingle Worker - Mock 模式（算法占位，延迟后返回原视频）
# 任务 ID: ${task.messageId}
# =========================================================

RESULT_FILE="/tmp/mingle-task-result"
DONE_FILE="/tmp/mingle-task-done"
LOG_FILE="/var/log/mingle-worker.log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date -Iseconds)] ===== Mingle Worker Mock 启动 ====="
echo "[$(date -Iseconds)] 任务 ID: ${task.messageId}"
echo "[$(date -Iseconds)] Mock: 延迟 ${delaySec}s 后返回原视频 URL"

# 写入任务环境变量（与真实模式一致，便于后续切换）
cat > /tmp/mingle-task.env << 'ENVEOF'
TASK_MESSAGE_ID=${esc(task.messageId)}
TASK_VIDEO_URL=${esc(task.videoDownloadUrl)}
TASK_WEBHOOK_URL=${esc(task.webhookUrl)}
TASK_DETECT_TYPE=${esc(task.detectType)}
${task.userId ? `TASK_USER_ID=${esc(task.userId)}` : ''}
${task.targetRegions ? `TASK_TARGET_REGIONS=${esc(task.targetRegions)}` : ''}
ENVEOF

sleep ${delaySec}

# 写入结果：原视频 URL 作为 output_video_url
RESULT_JSON='${esc(resultJson)}'
echo "$RESULT_JSON" > "$RESULT_FILE"
echo "SUCCESS" > "$DONE_FILE"

rm -f /tmp/mingle-task.env
echo "[$(date -Iseconds)] ===== Mock 任务完成 ====="
`
}

/**
 * 真实模式：安装 Docker、拉取镜像、运行处理容器
 */
function generateDockerStartupScript(
  task: TaskParams,
  config: WorkerConfig,
  processingImage: string,
): string {
  // 转义 shell 特殊字符
  const esc = (s: string) => s.replace(/'/g, "'\\''")

  return `#!/bin/bash
set -euo pipefail

# =========================================================
# Mingle Worker - ECS 自动化启动脚本
# 由 Worker 编排器自动生成，请勿手动修改
# 
# 任务 ID: ${task.messageId}
# 生成时间: ${new Date().toISOString()}
# =========================================================

LOG_FILE="/var/log/mingle-worker.log"
RESULT_FILE="/tmp/mingle-task-result"
DONE_FILE="/tmp/mingle-task-done"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date -Iseconds)] ===== Mingle Worker 启动 ====="
echo "[$(date -Iseconds)] 任务 ID: ${task.messageId}"

# ---------------------------------------------------------
# 1. 写入任务环境变量
# ---------------------------------------------------------
cat > /tmp/mingle-task.env << 'ENVEOF'
TASK_MESSAGE_ID=${esc(task.messageId)}
TASK_VIDEO_URL=${esc(task.videoDownloadUrl)}
TASK_WEBHOOK_URL=${esc(task.webhookUrl)}
TASK_DETECT_TYPE=${esc(task.detectType)}
${task.userId ? `TASK_USER_ID=${esc(task.userId)}` : ''}
${task.targetRegions ? `TASK_TARGET_REGIONS=${esc(task.targetRegions)}` : ''}
WEBHOOK_SECRET=${esc(config.webhook.secret)}
ENVEOF

echo "[$(date -Iseconds)] 任务参数已写入"

# ---------------------------------------------------------
# 1.5 若未安装 Docker 则安装（公共镜像如 CentOS 需此步）
# ---------------------------------------------------------
if ! command -v docker > /dev/null 2>&1; then
  echo "[$(date -Iseconds)] 未检测到 Docker，尝试安装..."
  if command -v dnf > /dev/null 2>&1; then
    dnf install -y docker-ce docker-ce-cli containerd.io 2>/dev/null || true
    systemctl start docker
    systemctl enable docker
  elif command -v yum > /dev/null 2>&1; then
    yum install -y docker 2>/dev/null || true
    systemctl start docker
    systemctl enable docker
  fi
fi

# ---------------------------------------------------------
# 2. 等待 Docker 就绪
# ---------------------------------------------------------
echo "[$(date -Iseconds)] 等待 Docker 服务就绪..."
for i in $(seq 1 60); do
  if docker info > /dev/null 2>&1; then
    echo "[$(date -Iseconds)] Docker 已就绪"
    break
  fi
  if [ $i -eq 60 ]; then
    echo "[$(date -Iseconds)] ERROR: Docker 启动超时"
    echo '{"success":false,"error":"Docker service timeout"}' > "$RESULT_FILE"
    echo "FAILED" > "$DONE_FILE"
    exit 1
  fi
  sleep 2
done

# ---------------------------------------------------------
# 2.5 若镜像是「内网 Registry」host:port 形式，配置 insecure-registry 以便 HTTP 拉取
# ---------------------------------------------------------
IMAGE="${esc(processingImage)}"
REGISTRY_PART=$(echo "$IMAGE" | cut -d/ -f1)
if echo "$REGISTRY_PART" | grep -qE ':[0-9]+$'; then
  echo "[$(date -Iseconds)] 检测到内网 Registry ($REGISTRY_PART)，配置 insecure-registry..."
  mkdir -p /etc/docker
  export REGISTRY_PART
  python3 -c '
import json, os
p = "/etc/docker/daemon.json"
d = {}
if os.path.exists(p):
  try:
    with open(p) as f: d = json.load(f)
  except Exception: pass
r = os.environ.get("REGISTRY_PART", "")
reg = list(d.get("insecure-registries") or [])
if r and r not in reg:
  reg.append(r)
  d["insecure-registries"] = reg
  with open(p, "w") as f: json.dump(d, f, indent=2)
'
  systemctl restart docker || true
  for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 2; done
fi

# ---------------------------------------------------------
# 3. 拉取处理镜像（如果本地不存在）
# ---------------------------------------------------------
echo "[$(date -Iseconds)] 检查处理镜像: $IMAGE"

if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
  echo "[$(date -Iseconds)] 正在拉取镜像..."
  if ! docker pull "$IMAGE"; then
    echo "[$(date -Iseconds)] ERROR: 镜像拉取失败"
    echo '{"success":false,"error":"Image pull failed: '"$IMAGE"'"}' > "$RESULT_FILE"
    echo "FAILED" > "$DONE_FILE"
    exit 1
  fi
  echo "[$(date -Iseconds)] 镜像拉取完成"
else
  echo "[$(date -Iseconds)] 镜像已存在，跳过拉取"
fi

# ---------------------------------------------------------
# 4. 启动处理容器
# ---------------------------------------------------------
echo "[$(date -Iseconds)] 启动处理容器..."

CONTAINER_NAME="mingle-task-${task.messageId}"

# 运行容器，使用 --env-file 传递任务参数
# --rm: 容器退出后自动删除
# --network host: 使用宿主机网络（方便访问 webhook）
docker run --rm \\
  --name "$CONTAINER_NAME" \\
  --env-file /tmp/mingle-task.env \\
  --network host \\
  --tmpfs /tmp:rw,noexec,nosuid,size=4g \\
  --memory=${Math.floor(config.ecs.maxInstances > 1 ? 8 : 16)}g \\
  --cpus=${config.ecs.maxInstances > 1 ? 4 : 8} \\
  ${config.ecs.instanceType.includes('gn') ? '--gpus all' : ''} \\
  "$IMAGE" \\
  > /tmp/mingle-container-stdout.log 2> /tmp/mingle-container-stderr.log

CONTAINER_EXIT_CODE=$?

echo "[$(date -Iseconds)] 容器退出码: $CONTAINER_EXIT_CODE"

# ---------------------------------------------------------
# 5. 收集结果
# ---------------------------------------------------------
if [ $CONTAINER_EXIT_CODE -eq 0 ]; then
  # 容器成功：从 stdout 最后一行取出结果 JSON
  RESULT_JSON=$(tail -1 /tmp/mingle-container-stdout.log)
  
  # 验证是否为有效 JSON
  if echo "$RESULT_JSON" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "$RESULT_JSON" > "$RESULT_FILE"
    echo "SUCCESS" > "$DONE_FILE"
    echo "[$(date -Iseconds)] 任务处理成功"
  else
    echo '{"success":false,"error":"Invalid output JSON from container"}' > "$RESULT_FILE"
    echo "FAILED" > "$DONE_FILE"
    echo "[$(date -Iseconds)] ERROR: 容器输出格式异常"
  fi
else
  # 容器失败
  STDERR_TAIL=$(tail -5 /tmp/mingle-container-stderr.log 2>/dev/null || echo "No stderr")
  # 转义 JSON 中的特殊字符
  STDERR_ESCAPED=$(echo "$STDERR_TAIL" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))" 2>/dev/null || echo '"Unknown error"')
  echo '{"success":false,"error":'"$STDERR_ESCAPED"',"exit_code":'"$CONTAINER_EXIT_CODE"'}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  echo "[$(date -Iseconds)] ERROR: 容器执行失败"
fi

# ---------------------------------------------------------
# 6. 清理敏感文件
# ---------------------------------------------------------
rm -f /tmp/mingle-task.env

echo "[$(date -Iseconds)] ===== Mingle Worker 脚本结束 ====="
`
}
