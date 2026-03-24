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
 * 完成标记约定：见 {@link WORKER_HOST_PATHS}
 */

import { WorkerConfig } from './config'
import { TaskParams } from './domain/task'
import { WORKER_HOST_PATHS } from './worker-branding'

/**
 * 选择可写的宿主机日志路径（云助手有时对 /var/log 无写权限），再交给 tee。
 */
function shellSelectWorkerLogSnippet(): string {
  const preferred = WORKER_HOST_PATHS.workerLog
  const fallback = WORKER_HOST_PATHS.workerLogFallback
  return `
LOG_FILE='${preferred}'
if ! touch "$LOG_FILE" 2>/dev/null || [ ! -w "$LOG_FILE" ]; then
  LOG_FILE='${fallback}'
  touch "$LOG_FILE" 2>/dev/null || true
fi
exec > >(tee -a "$LOG_FILE") 2>&1
echo "[$(date -Iseconds)] RetinaClip 宿主机日志: $LOG_FILE"
`.trim()
}

/**
 * 自动挂载 NAS（NFS）。
 *
 * 约定（按用户需求固定）：
 * - 导出路径：/
 * - 本地挂载目录：/mnt
 * - NAS ID：3e41f4bcd1（仅用于日志定位）
 * - 挂载点域名：worker 的域名（优先 $WORKER_NAS_MOUNT_DOMAIN，否则取 hostname -f）
 */
function shellNasAutoMountSnippet(
  nasMountDomain: string,
  nasExportPath: string,
  nasMountPoint: string,
  nasId: string,
): string {
  const nasServerLine = nasMountDomain
    ? `NAS_SERVER='${nasMountDomain.replace(/'/g, "'\\''")}'`
    : `NAS_SERVER="$(hostname -f 2>/dev/null || hostname)"`

  const escPath = nasExportPath.replace(/'/g, "'\\''")
  const escPoint = nasMountPoint.replace(/'/g, "'\\''")
  const escId = nasId.replace(/'/g, "'\\''")

  return `
# ---------------------------------------------------------
# NAS 自动挂载（NFS）
# ---------------------------------------------------------
NAS_ID='${escId}'
NAS_EXPORT_PATH='${escPath}'
NAS_MOUNT_POINT='${escPoint}'
${nasServerLine}

echo "[$(date -Iseconds)] NAS auto-mount start: id=$NAS_ID server=$NAS_SERVER export=$NAS_EXPORT_PATH -> $NAS_MOUNT_POINT"

if [ -z "$NAS_SERVER" ]; then
  echo "[$(date -Iseconds)] ERROR: NAS server（挂载点域名）为空，无法挂载"
  echo '{"success":false,"error":"NAS mount failed: NAS_SERVER empty"}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  exit 1
fi

mkdir -p "$NAS_MOUNT_POINT"

# DNS 解析（便于排查）
if command -v getent >/dev/null 2>&1; then
  getent hosts "$NAS_SERVER" >/dev/null 2>&1 || true
fi
if command -v nslookup >/dev/null 2>&1; then
  nslookup "$NAS_SERVER" >/dev/null 2>&1 || true
fi

# 2049 端口探测（提示安全组/监听/网络）
if command -v nc >/dev/null 2>&1; then
  nc -vz -w 3 "$NAS_SERVER" 2049 >/dev/null 2>&1 || true
else
  python3 - <<PY >/dev/null 2>&1 || true
import socket
h="$NAS_SERVER"; p=2049
s=socket.socket(); s.settimeout(3)
try:
  s.connect((h,p))
except Exception:
  pass
finally:
  s.close()
PY
fi

# 1) 安装 NFS 客户端（mount.nfs）
if ! command -v mount.nfs >/dev/null 2>&1; then
  echo "[$(date -Iseconds)] 未检测到 nfs client（mount.nfs），开始安装 nfs-utils..."
  if command -v dnf >/dev/null 2>&1; then
    dnf -y install nfs-utils || true
  elif command -v yum >/dev/null 2>&1; then
    yum -y install nfs-utils || true
  fi
fi

if ! command -v mount.nfs >/dev/null 2>&1; then
  echo "[$(date -Iseconds)] ERROR: nfs-utils 安装失败或 mount.nfs 不可用"
  echo '{"success":false,"error":"NAS mount failed: nfs-utils not ready"}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  exit 1
fi

# 2) 幂等：若已挂载到目标 server:export，则跳过 mount；否则（/mnt 已被挂载但不匹配）先卸载重挂
NEED_MOUNT=1
if grep -qF "$NAS_SERVER:$NAS_EXPORT_PATH $NAS_MOUNT_POINT" /proc/mounts 2>/dev/null; then
  echo "[$(date -Iseconds)] NAS 已挂载：$NAS_MOUNT_POINT（server/export 匹配，跳过挂载）"
  NEED_MOUNT=0
elif mountpoint -q "$NAS_MOUNT_POINT" 2>/dev/null; then
  echo "[$(date -Iseconds)] 检测到 /mnt 已挂载但不匹配当前 NAS server/export，先卸载重试..."
  umount -f "$NAS_MOUNT_POINT" >/dev/null 2>&1 || true
fi

# 3) 写入 /etc/fstab（避免重启后丢失）
FSTAB_LINE="$NAS_SERVER:$NAS_EXPORT_PATH $NAS_MOUNT_POINT nfs4 _netdev,nofail,vers=4.0,soft,timeo=600,retrans=2 0 0"
if ! grep -qF "$NAS_SERVER:$NAS_EXPORT_PATH $NAS_MOUNT_POINT nfs4" /etc/fstab 2>/dev/null; then
  echo "$FSTAB_LINE" >> /etc/fstab
fi
tail -n 5 /etc/fstab 2>/dev/null || true

# 4) 挂载（带超时，防卡死；仅当 NEED_MOUNT=1 时执行）
if [ "$NEED_MOUNT" -eq 1 ]; then
  echo "[$(date -Iseconds)] 开始挂载（带 20 秒超时）..."
  set +e
  timeout 20 mount -v -t nfs4 -o vers=4.0,soft,timeo=600,retrans=2 "$NAS_SERVER:$NAS_EXPORT_PATH" "$NAS_MOUNT_POINT"
  MOUNT_EC=$?
  set -e

  if [ "$MOUNT_EC" -ne 0 ]; then
    echo "[$(date -Iseconds)] ERROR: mount 失败，exit=$MOUNT_EC"
    dmesg 2>/dev/null | tail -n 80 || true
    journalctl -n 80 --no-pager 2>/dev/null || true
    echo '{"success":false,"error":"NAS mount failed: mount timeout/connection error"}' > "$RESULT_FILE"
    echo "FAILED" > "$DONE_FILE"
    exit 1
  fi
fi

mount | grep "$NAS_MOUNT_POINT" || true
df -h "$NAS_MOUNT_POINT" 2>/dev/null || true
`.trim()
}

/**
 * 从内网镜像名 `host:port/repo:tag` 提取 registry，并与配置的列表合并（去重）。
 * 用于写入 Docker `insecure-registries`（HTTP 私有仓）。
 */
export function mergeDockerInsecureRegistries(
  processingImage: string,
  configured: string[],
): string[] {
  const set = new Set<string>()
  for (const c of configured) {
    const t = c.trim()
    if (t) set.add(t)
  }
  const firstSegment = processingImage.split('/')[0]?.trim() ?? ''
  if (/:[0-9]+$/.test(firstSegment)) {
    set.add(firstSegment)
  }
  return [...set]
}

/** 用于 docker login 的默认 registry（镜像第一段为 host:port 时） */
export function defaultDockerRegistryServerFromImage(processingImage: string): string {
  const firstSegment = processingImage.split('/')[0]?.trim() ?? ''
  return /:[0-9]+$/.test(firstSegment) ? firstSegment : ''
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
  const H = WORKER_HOST_PATHS

  return `#!/bin/bash
set -euo pipefail

# =========================================================
# RetinaClip Worker - Mock 模式（算法占位，延迟后返回原视频）
# 任务 ID: ${task.messageId}
# =========================================================

RESULT_FILE="${H.taskResult}"
DONE_FILE="${H.taskDone}"

trap 'rc=$?; \
  if [ "$rc" -ne 0 ]; then \
    if [ ! -f "$DONE_FILE" ]; then \
      echo "{\"success\":false,\"error\":\"host script failed (exit=$rc)\"}" > "$RESULT_FILE"; \
      echo "FAILED" > "$DONE_FILE"; \
    elif [ ! -f "$RESULT_FILE" ]; then \
      echo "{\"success\":false,\"error\":\"host script failed (exit=$rc)\"}" > "$RESULT_FILE"; \
    fi; \
  fi' EXIT

${shellSelectWorkerLogSnippet()}

${shellNasAutoMountSnippet(
    config.ecs.nasMountDomain,
    config.ecs.nasExportPath,
    config.ecs.nasMountPoint,
    config.ecs.nasId,
  )}

echo "[$(date -Iseconds)] ===== RetinaClip Worker Mock 启动 ====="
echo "[$(date -Iseconds)] 任务 ID: ${task.messageId}"
echo "[$(date -Iseconds)] Mock: 延迟 ${delaySec}s 后返回原视频 URL"

# 写入任务环境变量（与真实模式一致，便于后续切换）
cat > ${H.taskEnv} << 'ENVEOF'
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

rm -f ${H.taskEnv}
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
  const H = WORKER_HOST_PATHS
  const hostInst = hostInstanceTypeForGpu ?? config.ecs.instanceType
  const useGpuFlag = hostInst.includes('gn')
  const dockerPolicy = config.ecs.userdataDockerPolicy

  const insecureRegs = mergeDockerInsecureRegistries(
    processingImage,
    config.ecs.dockerInsecureRegistries ?? [],
  )
  const regJsonB64 = Buffer.from(JSON.stringify(insecureRegs), 'utf8').toString('base64')
  const loginServer =
    config.ecs.dockerRegistryServer?.trim() || defaultDockerRegistryServerFromImage(processingImage)
  const hasDockerLogin = Boolean(
    config.ecs.dockerRegistryUsername && config.ecs.dockerRegistryPassword && loginServer,
  )

  const netMode = config.ecs.dockerNetworkMode
  const publishPorts = config.ecs.dockerPublishPorts ?? []
  const volHost = (config.ecs.dockerVolumeHost ?? '').trim()
  const volContainer = (config.ecs.dockerVolumeContainer ?? '/DiffuEraser').trim() || '/DiffuEraser'
  const bashC = (config.ecs.dockerBashCommand ?? '').trim()

  const dockerRunNetworkAndPublishLines: string[] = []
  if (netMode === 'host') {
    dockerRunNetworkAndPublishLines.push('  --network host \\')
  }
  if (netMode === 'bridge' && publishPorts.length > 0) {
    for (const p of publishPorts) {
      dockerRunNetworkAndPublishLines.push(`  -p '${esc(p)}' \\`)
    }
  }
  const dockerRunVolumeLine =
    volHost.length > 0 ? `  -v '${esc(volHost)}:${esc(volContainer)}' \\` : ''
  const dockerRunImageTail = bashC.length > 0 ? `"$IMAGE" /bin/bash -c '${esc(bashC)}'` : `"$IMAGE"`
  const dockerRunMidLines = [...dockerRunNetworkAndPublishLines, ...(volHost.length ? [dockerRunVolumeLine] : [])]
  const dockerRunMidBlock = dockerRunMidLines.length > 0 ? `${dockerRunMidLines.join('\n')}\n` : ''

  const insecureRegistryShellBlock =
    insecureRegs.length === 0
      ? `echo "[$(date -Iseconds)] 无需合并 Docker insecure-registries（非 host:port 私有仓或列表为空）"
DOCKER_DAEMON_CHANGED=0`
      : `
echo "[$(date -Iseconds)] 合并 Docker insecure-registries（内网 HTTP 仓库）: ${insecureRegs.map((r) => r.replace(/"/g, '\\"')).join(', ')}"
REG_JSON=$(printf '%s' '${regJsonB64}' | base64 -d)
export REG_JSON
mkdir -p /etc/docker
[ -f /etc/docker/daemon.json ] && cp /etc/docker/daemon.json /tmp/daemon.retinaclip.bak 2>/dev/null || true
if ! command -v python3 >/dev/null 2>&1; then
  echo "[$(date -Iseconds)] ERROR: 需要 python3 以合并 /etc/docker/daemon.json（请预装或改用自带 Docker 的镜像）"
  echo '{"success":false,"error":"python3 missing for Docker insecure-registries merge"}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  exit 1
fi
python3 <<'PYMERGE'
import json, os
extra = json.loads(os.environ.get("REG_JSON", "[]"))
p = "/etc/docker/daemon.json"
d = {}
if os.path.exists(p):
    try:
        with open(p) as f:
            d = json.load(f)
    except Exception:
        d = {}
regs = list(d.get("insecure-registries") or [])
for r in extra:
    if r and r not in regs:
        regs.append(r)
d["insecure-registries"] = regs
os.makedirs("/etc/docker", exist_ok=True)
with open(p, "w") as f:
    json.dump(d, f, indent=2)
PYMERGE
DOCKER_DAEMON_CHANGED=0
if [ ! -f /tmp/daemon.retinaclip.bak ]; then
  DOCKER_DAEMON_CHANGED=1
elif ! cmp -s /etc/docker/daemon.json /tmp/daemon.retinaclip.bak 2>/dev/null; then
  DOCKER_DAEMON_CHANGED=1
fi
if [ "$DOCKER_DAEMON_CHANGED" -eq 1 ]; then
  echo "[$(date -Iseconds)] daemon.json 已更新 insecure-registries"
fi
`.trim()

  const dockerLoginShellBlock = hasDockerLogin
    ? `
echo "[$(date -Iseconds)] docker login '${esc(loginServer)}' ..."
if ! printf '%s\\n' '${esc(config.ecs.dockerRegistryPassword!)}' | docker login '${esc(loginServer)}' -u '${esc(config.ecs.dockerRegistryUsername!)}' --password-stdin; then
  echo "[$(date -Iseconds)] ERROR: docker login 失败"
  echo '{"success":false,"error":"docker login failed"}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  exit 1
fi
`.trim()
    : ''

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
fi
`

  return `#!/bin/bash
set -euo pipefail

# =========================================================
# RetinaClip Worker - ECS 自动化启动脚本
# 由 Worker 编排器自动生成，请勿手动修改
# 
# 任务 ID: ${task.messageId}
# 生成时间: ${new Date().toISOString()}
# =========================================================

RESULT_FILE="${H.taskResult}"
DONE_FILE="${H.taskDone}"

trap 'rc=$?; \
  if [ "$rc" -ne 0 ]; then \
    if [ ! -f "$DONE_FILE" ]; then \
      echo "{\"success\":false,\"error\":\"host script failed (exit=$rc)\"}" > "$RESULT_FILE"; \
      echo "FAILED" > "$DONE_FILE"; \
    elif [ ! -f "$RESULT_FILE" ]; then \
      echo "{\"success\":false,\"error\":\"host script failed (exit=$rc)\"}" > "$RESULT_FILE"; \
    fi; \
  fi' EXIT

${shellSelectWorkerLogSnippet()}

${shellNasAutoMountSnippet(
    config.ecs.nasMountDomain,
    config.ecs.nasExportPath,
    config.ecs.nasMountPoint,
    config.ecs.nasId,
  )}

echo "[$(date -Iseconds)] ===== RetinaClip Worker 启动 ====="
echo "[$(date -Iseconds)] 任务 ID: ${task.messageId}"

# ---------------------------------------------------------
# 1. 写入任务环境变量
# ---------------------------------------------------------
cat > ${H.taskEnv} << 'ENVEOF'
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

# ---------------------------------------------------------
# 1.55 内网 HTTP Registry：合并 insecure-registries（须在首次启动 Docker 前完成）
# ---------------------------------------------------------
${insecureRegistryShellBlock}

# ---------------------------------------------------------
# 1.6 启动 Docker；若已在跑且 daemon.json 有变则重启以应用 insecure-registries
# ---------------------------------------------------------
if command -v systemctl > /dev/null 2>&1; then
  if systemctl is-active --quiet docker 2>/dev/null; then
    if [ "\${DOCKER_DAEMON_CHANGED:-0}" -eq 1 ]; then
      echo "[$(date -Iseconds)] 重启 Docker 以应用 insecure-registries..."
      systemctl restart docker >/dev/null 2>&1 || true
      for j in $(seq 1 45); do docker info >/dev/null 2>&1 && break; sleep 2; done
    fi
  else
    systemctl enable docker >/dev/null 2>&1 || true
    systemctl start docker >/dev/null 2>&1 || true
  fi
fi

# ---------------------------------------------------------
# 2. 等待 Docker 就绪
# ---------------------------------------------------------
echo "[$(date -Iseconds)] 等待 Docker 服务就绪..."
for i in $(seq 1 60); do
  if docker info > /dev/null 2>&1; then
    echo "[$(date -Iseconds)] Docker 已就绪"
    docker info 2>/dev/null | grep -i 'Insecure Registries' || true
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

${dockerLoginShellBlock}

# ---------------------------------------------------------
# 3. 拉取处理镜像（如果本地不存在）
# ---------------------------------------------------------
IMAGE="${esc(processingImage)}"
REGISTRY_ENDPOINT=$(echo "$IMAGE" | cut -d/ -f1)
if echo "$REGISTRY_ENDPOINT" | grep -qE ':[0-9]+$'; then
  echo "[$(date -Iseconds)] 探测 Registry v2 (HTTP): http://$REGISTRY_ENDPOINT/v2/"
  if command -v curl >/dev/null 2>&1; then
    if curl -sS -o /dev/null --connect-timeout 5 --max-time 15 "http://$REGISTRY_ENDPOINT/v2/"; then
      echo "[$(date -Iseconds)] Registry /v2/ 可达"
    else
      echo "[$(date -Iseconds)] WARN: curl http://$REGISTRY_ENDPOINT/v2/ 失败（检查安全组、仓库监听与路由）"
    fi
  fi
fi

echo "[$(date -Iseconds)] 检查处理镜像: $IMAGE"

if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
  echo "[$(date -Iseconds)] 正在拉取镜像..."
  set +e
  docker pull "$IMAGE" 2>&1 | tee /tmp/retinaclip-docker-pull.log
  PULL_EC=\${PIPESTATUS[0]}
  set -e
  if [ "$PULL_EC" -ne 0 ]; then
    echo "[$(date -Iseconds)] ERROR: 镜像拉取失败 (exit $PULL_EC)，最近日志:"
    tail -n 30 /tmp/retinaclip-docker-pull.log 2>/dev/null || true
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

CONTAINER_NAME="retinaclip-task-${task.messageId}"

# 运行容器，使用 --env-file 传递任务参数
# 网络：WORKER_DOCKER_NETWORK_MODE=host（默认，宿主机网络）或 bridge（可配 WORKER_DOCKER_PUBLISH_PORTS）
# 卷：WORKER_DOCKER_VOLUME_HOST 非空时增加 -v host:container（如 DiffuEraser）
# 启动命令：WORKER_DOCKER_BASH_COMMAND 非空时追加 /bin/bash -c '...'（如 ./deploy.sh）
# GPU 参数在运行时二次校验：仅当实例类型推断为 GPU（useGpuFlag）且宿主机存在 /dev/nvidia* 或 nvidia-smi 时才启用。
USE_GPU_BY_TYPE='${useGpuFlag ? '1' : '0'}'
GPU_RUN_OPTS=''
if [ "$USE_GPU_BY_TYPE" = "1" ]; then
  if [ -e /dev/nvidia0 ] || command -v nvidia-smi >/dev/null 2>&1; then
    GPU_RUN_OPTS='--gpus all'
  else
    echo "[$(date -Iseconds)] WARN: 检测不到 NVIDIA 设备/驱动，跳过 --gpus all"
  fi
fi
docker run \\
  --name "$CONTAINER_NAME" \\
  --env-file ${H.taskEnv} \\
${dockerRunMidBlock}  --tmpfs /tmp:rw,noexec,nosuid,size=4g \\
  --memory=${Math.floor(config.ecs.maxInstances > 1 ? 8 : 16)}g \\
  --cpus=${config.ecs.maxInstances > 1 ? 4 : 8} \\
  $GPU_RUN_OPTS \\
  ${dockerRunImageTail} \\
  > ${H.containerStdout} 2> ${H.containerStderr}
#
# 调试/排障期间不自动移除容器（避免丢失容器对象，方便 docker ps / logs / inspect）。
# 容器退出后将保留，避免使用 --rm。

CONTAINER_EXIT_CODE=$?

echo "[$(date -Iseconds)] 容器退出码: $CONTAINER_EXIT_CODE"

# ---------------------------------------------------------
# 5. 收集结果
# ---------------------------------------------------------
if [ $CONTAINER_EXIT_CODE -eq 0 ]; then
  # 容器成功：从 stdout 最后一行取出结果 JSON
  RESULT_JSON=$(tail -1 ${H.containerStdout})
  
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
  STDERR_TAIL=$(tail -5 ${H.containerStderr} 2>/dev/null || echo "No stderr")
  # 转义 JSON 中的特殊字符
  STDERR_ESCAPED=$(echo "$STDERR_TAIL" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))" 2>/dev/null || echo '"Unknown error"')
  echo '{"success":false,"error":'"$STDERR_ESCAPED"',"exit_code":'"$CONTAINER_EXIT_CODE"'}' > "$RESULT_FILE"
  echo "FAILED" > "$DONE_FILE"
  echo "[$(date -Iseconds)] ERROR: 容器执行失败"
fi

# ---------------------------------------------------------
# 6. 清理敏感文件
# ---------------------------------------------------------
rm -f ${H.taskEnv}

echo "[$(date -Iseconds)] ===== RetinaClip Worker 脚本结束 ====="
`
}
