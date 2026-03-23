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
import { TaskParams } from './domain/task'

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
  /** 本次创建实例的实际规格，用于决定是否加 `--gpus all`（与降级链一致） */
  hostInstanceTypeForGpu?: string,
): string {
  const script = generateTaskRunnerShellScript(task, config, processingImage, hostInstanceTypeForGpu)
  return Buffer.from(script).toString('base64')
}

/**
 * 生成在 ECS 宿主机上执行任务的 Bash 脚本（与 UserData 内容一致）。
 *
 * - 新建实例：经 {@link generateUserData} Base64 注入，开机由 cloud-init 执行。
 * - 池内实例：停机再开机后 UserData 通常不会再次执行，需由编排器通过云助手下发同一脚本。
 */
export function generateTaskRunnerShellScript(
  task: TaskParams,
  config: WorkerConfig,
  processingImage: string,
  hostInstanceTypeForGpu?: string,
): string {
  return generateStartupScript(task, config, processingImage, hostInstanceTypeForGpu)
}

/**
 * 生成 Bash 启动脚本（真实 Docker 或 Mock）
 */
function generateStartupScript(
  task: TaskParams,
  config: WorkerConfig,
  processingImage: string,
  hostInstanceTypeForGpu?: string,
): string {
  if (config.ecs.mockProcessing) {
    return generateMockStartupScript(task, config)
  }
  return generateDockerStartupScript(task, config, processingImage, hostInstanceTypeForGpu)
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
${task.videoUploadUrl ? `TASK_VIDEO_UPLOAD_URL=${esc(task.videoUploadUrl)}` : ''}
TASK_WEBHOOK_URL=${esc(task.webhookUrl)}
TASK_DETECT_TYPE=${esc(task.detectType)}
${task.userId ? `TASK_USER_ID=${esc(task.userId)}` : ''}
${task.targetRegions ? `TASK_TARGET_REGIONS=${esc(task.targetRegions)}` : ''}
${task.operations?.length ? `TASK_OPERATIONS=${esc(JSON.stringify(task.operations))}` : ''}
${task.qualityPreset ? `TASK_QUALITY_PRESET=${esc(task.qualityPreset)}` : ''}
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
  hostInstanceTypeForGpu?: string,
): string {
  // 转义 shell 特殊字符
  const esc = (s: string) => s.replace(/'/g, "'\\''")
  const hostInst = hostInstanceTypeForGpu ?? config.ecs.instanceType
  const useGpuFlag = hostInst.includes('gn')
  const dockerPolicy = config.ecs.userdataDockerPolicy

  const dockerInstallBlock =
    dockerPolicy === 'require_host'
      ? `if ! command -v docker > /dev/null 2>&1; then
  echo "[$(date -Iseconds)] ERROR: 宿主未预装 Docker（WORKER_ECS_USERDATA_DOCKER_POLICY=require_host）。请使用自定义镜像预装 Docker。"
  echo '{"success":false,"error":"Docker not installed on host (require_host policy)"}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  exit 1
fi
`
      : `if ! command -v docker > /dev/null 2>&1; then
  echo "[$(date -Iseconds)] 未检测到 Docker，尝试安装 Docker CE..."

  INSTALL_OK=1
  if command -v dnf > /dev/null 2>&1; then
    # dnf config-manager 属于 dnf-plugins-core
    if ! dnf -y install dnf-plugins-core; then
      INSTALL_OK=0
    fi

    # AlmaLinux 9 基本与 RHEL/CentOS 生态兼容：使用 Docker 官方 CentOS repo
    if ! dnf -y config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo; then
      # fallback：直接下载 repo 文件（避免 config-manager 不存在/不可用）
      if command -v curl > /dev/null 2>&1; then
        mkdir -p /etc/yum.repos.d
        if ! curl -fsSL https://download.docker.com/linux/centos/docker-ce.repo -o /etc/yum.repos.d/docker-ce.repo; then
          INSTALL_OK=0
        fi
      elif command -v wget > /dev/null 2>&1; then
        mkdir -p /etc/yum.repos.d
        if ! wget -qO /etc/yum.repos.d/docker-ce.repo https://download.docker.com/linux/centos/docker-ce.repo; then
          INSTALL_OK=0
        fi
      else
        INSTALL_OK=0
      fi
    fi

    if [ "$INSTALL_OK" -eq 1 ]; then
      if ! dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin; then
        INSTALL_OK=0
      fi
    fi
  else
    INSTALL_OK=0
  fi

  if [ "$INSTALL_OK" -ne 1 ] || ! command -v docker > /dev/null 2>&1; then
    echo "[$(date -Iseconds)] ERROR: Docker 安装失败"
    echo '{"success":false,"error":"Docker install failed"}' > "$RESULT_FILE"
    echo "FAILED" > "$DONE_FILE"
    exit 1
  fi

  if command -v systemctl > /dev/null 2>&1; then
    systemctl enable --now docker >/dev/null 2>&1 || systemctl start docker >/dev/null 2>&1 || true
  fi
fi
`

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
${task.videoUploadUrl ? `TASK_VIDEO_UPLOAD_URL=${esc(task.videoUploadUrl)}` : ''}
TASK_WEBHOOK_URL=${esc(task.webhookUrl)}
TASK_DETECT_TYPE=${esc(task.detectType)}
${task.userId ? `TASK_USER_ID=${esc(task.userId)}` : ''}
${task.targetRegions ? `TASK_TARGET_REGIONS=${esc(task.targetRegions)}` : ''}
${task.operations?.length ? `TASK_OPERATIONS=${esc(JSON.stringify(task.operations))}` : ''}
${task.qualityPreset ? `TASK_QUALITY_PRESET=${esc(task.qualityPreset)}` : ''}
WEBHOOK_SECRET=${esc(config.webhook.secret)}
ENVEOF

echo "[$(date -Iseconds)] 任务参数已写入"

# ---------------------------------------------------------
# 1.5 Docker：require_host 要求自定义镜像预装；auto 时缺失则尝试安装
# ---------------------------------------------------------
${dockerInstallBlock}

# Docker 命令存在时：尝试启动服务
if command -v systemctl > /dev/null 2>&1; then
  systemctl start docker >/dev/null 2>&1 || true
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
  ${useGpuFlag ? '--gpus all' : ''} \\
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
