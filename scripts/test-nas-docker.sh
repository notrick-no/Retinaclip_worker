#!/bin/bash
set -euo pipefail

# =========================================================
# RetinaClip - NAS 挂载 + Docker 拉取/启动测试
# =========================================================
#
# 默认行为（按你描述）：
# - NFS 导出：/
# - 本地挂载目录：/mnt
# - 挂载点域名：worker 域名（默认 hostname -f；也可用 WORKER_NAS_MOUNT_DOMAIN 覆盖）
#
# 镜像/启动参数（按你给的 docker run 命令）：
# - registry：172.16.0.70:5000
# - 镜像：172.16.0.70:5000/quzimu-app:v3
# - 容器：quzimu-container
#

NAS_EXPORT_PATH="/"
NAS_MOUNT_POINT="/mnt"
NAS_ID="3e41f4bcd1" # 仅用于日志定位

NAS_SERVER="${WORKER_NAS_MOUNT_DOMAIN:-$(hostname -f 2>/dev/null || hostname)}"
IMAGE="172.16.0.70:5000/quzimu-app:v3"
REGISTRY="172.16.0.70:5000"
CONTAINER_NAME="quzimu-container"

HOST_VOLUME="/mnt/DiffuEraser/DiffuEraser/"
CONTAINER_VOLUME="/DiffuEraser"

# GPU 默认开启；若调度器宿主无 GPU，把 USE_GPU=false
USE_GPU="${USE_GPU:-true}"

# “接收任务”所需的环境变量：
# - 若不传 TASK_ENV_FILE，会从当前 shell 的环境变量自动生成 /tmp/retinaclip-task.env
# - 可直接传：TASK_ENV_FILE=/path/to/task.env
TASK_ENV_FILE="${TASK_ENV_FILE:-}"
TASK_ENV_OUT="${TASK_ENV_OUT:-/tmp/retinaclip-task.env}"

echo "[$(date -Iseconds)] ===== NAS+Docker 测试开始 ====="
echo "NAS_ID=${NAS_ID}"
echo "NAS_SERVER=${NAS_SERVER}"
echo "NAS_EXPORT_PATH=${NAS_EXPORT_PATH}"
echo "NAS_MOUNT_POINT=${NAS_MOUNT_POINT}"
echo "IMAGE=${IMAGE}"
echo "CONTAINER_NAME=${CONTAINER_NAME}"
echo "HOST_VOLUME=${HOST_VOLUME} -> ${CONTAINER_VOLUME}"
echo ""

if [ -z "${NAS_SERVER}" ]; then
  echo "ERROR: NAS_SERVER 为空，无法挂载"
  exit 1
fi

need_root_hint() {
  echo "提示：可能需要 root 权限（请用 sudo 运行）"
}

ensure_nfs_client() {
  if command -v mount.nfs >/dev/null 2>&1; then
    return 0
  fi
  echo "[$(date -Iseconds)] 未检测到 mount.nfs，尝试安装 nfs-utils..."
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf -y install nfs-utils || true
  elif command -v yum >/dev/null 2>&1; then
    sudo yum -y install nfs-utils || true
  else
    echo "ERROR: 未找到 dnf/yum，无法安装 nfs-utils"
    need_root_hint
    exit 1
  fi

  if ! command -v mount.nfs >/dev/null 2>&1; then
    echo "ERROR: 安装后仍未找到 mount.nfs"
    need_root_hint
    exit 1
  fi
}

test_and_mount_nas() {
  mkdir -p "${NAS_MOUNT_POINT}"

  if grep -qE "[[:space:]]${NAS_MOUNT_POINT}[[:space:]]" /proc/mounts 2>/dev/null; then
    echo "[$(date -Iseconds)] NAS 已挂载：${NAS_MOUNT_POINT}"
    df -h "${NAS_MOUNT_POINT}" || true
    return 0
  fi

  # 不写 fstab：这里只做测试（避免你环境里已有 fstab 冲突）
  echo "[$(date -Iseconds)] 尝试挂载 NAS -> ${NAS_MOUNT_POINT}"
  ensure_nfs_client

  # 优先 NFS4（你的业务一般用 nfs4）
  if ! mount -t nfs4 -o _netdev,vers=4.0,soft,timeo=600,retrans=2 \
    "${NAS_SERVER}:${NAS_EXPORT_PATH}" "${NAS_MOUNT_POINT}"; then
    echo "ERROR: mount 失败。请检查：NFS 端口/协议、安全组、挂载点域名解析是否正确。"
    echo "提示：可以尝试在宿主机上手动执行 mount 命令并观察报错。"
    exit 1
  fi

  if ! grep -qE "[[:space:]]${NAS_MOUNT_POINT}[[:space:]]" /proc/mounts 2>/dev/null; then
    echo "ERROR: mount 命令执行了，但 /proc/mounts 未看到挂载。"
    exit 1
  fi

  echo "[$(date -Iseconds)] NAS 挂载成功"
  df -h "${NAS_MOUNT_POINT}" || true
}

ensure_task_env_file() {
  # 若用户显式提供 env 文件：直接使用
  if [ -n "${TASK_ENV_FILE}" ]; then
    if [ ! -f "${TASK_ENV_FILE}" ]; then
      echo "ERROR: TASK_ENV_FILE 不存在: ${TASK_ENV_FILE}"
      exit 1
    fi
    echo "[$(date -Iseconds)] 使用 TASK_ENV_FILE: ${TASK_ENV_FILE}"
    export TASK_ENV_OUT="${TASK_ENV_FILE}"
    return 0
  fi

  echo "[$(date -Iseconds)] 生成任务 env 文件: ${TASK_ENV_OUT}"
  : > "${TASK_ENV_OUT}"

  maybe_write() {
    # $1: key  $2: shell变量名
    local k="$1"
    local v="${!2:-}"
    if [ -n "$v" ]; then
      echo "${k}=${v}" >> "${TASK_ENV_OUT}"
    fi
  }

  maybe_write "TASK_MESSAGE_ID" "TASK_MESSAGE_ID"
  maybe_write "TASK_VIDEO_URL" "TASK_VIDEO_URL"
  maybe_write "TASK_VIDEO_UPLOAD_URL" "TASK_VIDEO_UPLOAD_URL"
  maybe_write "TASK_WEBHOOK_URL" "TASK_WEBHOOK_URL"
  maybe_write "TASK_DETECT_TYPE" "TASK_DETECT_TYPE"
  maybe_write "TASK_USER_ID" "TASK_USER_ID"
  maybe_write "TASK_TARGET_REGIONS" "TASK_TARGET_REGIONS"
  maybe_write "TASK_OPERATIONS" "TASK_OPERATIONS"
  maybe_write "TASK_QUALITY_PRESET" "TASK_QUALITY_PRESET"
  maybe_write "WEBHOOK_SECRET" "WEBHOOK_SECRET"

  # 如果完全没有写入任何内容，就不强制传 env-file（避免容器读取失败/误读）
  if [ ! -s "${TASK_ENV_OUT}" ]; then
    echo "[$(date -Iseconds)] WARN: 任务 env 文件为空；将不会传 --env-file 给容器"
    export TASK_ENV_OUT=""
  else
    echo "[$(date -Iseconds)] 任务 env 已生成（非空）"
  fi
}

ensure_docker_insecure_registry_for_http() {
  # Docker 需要 insecure-registries 才能拉 HTTP 私有仓（非 HTTPS）
  # 这里会尝试合并 /etc/docker/daemon.json，而不是覆盖。
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 未检测到 docker 命令"
    need_root_hint
    exit 1
  fi

  if [ ! -f /etc/docker/daemon.json ]; then
    echo "[$(date -Iseconds)] /etc/docker/daemon.json 不存在，准备创建以写入 insecure-registries..."
  fi

  python3 - <<'PY' || true
import json, os, sys
p = "/etc/docker/daemon.json"
data = {}
if os.path.exists(p):
  try:
    with open(p, "r") as f:
      data = json.load(f)
  except Exception:
    data = {}
regs = data.get("insecure-registries") or []
reg = os.environ.get("RETINACLIP_TEST_REGISTRY", "")
if reg and reg not in regs:
  regs.append(reg)
  data["insecure-registries"] = regs
os.makedirs("/etc/docker", exist_ok=True)
with open(p, "w") as f:
  json.dump(data, f, indent=2)
PY

  if [ ! -f /etc/docker/daemon.json ]; then
    echo "WARNING: daemon.json 写入可能失败，继续往下尝试 docker pull。"
    return 0
  fi

  echo "[$(date -Iseconds)] 重启 Docker 以应用 insecure-registries（如需要）..."
  sudo systemctl restart docker >/dev/null 2>&1 || sudo systemctl start docker >/dev/null 2>&1 || true

  for i in $(seq 1 30); do
    if docker info >/dev/null 2>&1; then
      echo "[$(date -Iseconds)] Docker 已就绪"
      break
    fi
    sleep 2
  done
}

docker_pull_and_run() {
  echo "[$(date -Iseconds)] docker pull: ${IMAGE}"
  if ! docker pull "${IMAGE}"; then
    echo "ERROR: docker pull 失败"
    echo "请检查：registry 可达性、端口 5000 放行、安全组、daemon.json insecure-registries、以及是否需要认证（docker login）。"
    exit 1
  fi

  if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    echo "[$(date -Iseconds)] 容器已存在，停止并删除旧容器：${CONTAINER_NAME}"
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi

  echo "[$(date -Iseconds)] docker run 启动：${CONTAINER_NAME}"
  mkdir -p "${HOST_VOLUME}" || true

  local gpuFlag=""
  if [ "${USE_GPU}" = "true" ] || [ "${USE_GPU}" = "1" ]; then
    gpuFlag="--gpus all"
  fi

  local envFileArg=""
  if [ -n "${TASK_ENV_OUT}" ]; then
    envFileArg="--env-file ${TASK_ENV_OUT}"
  fi

  # 注意：这里的 docker run 命令尽量贴近你给的示例
  docker run -d --name "${CONTAINER_NAME}" \
    ${gpuFlag} \
    -p 8080:8080 \
    -p 6006:6006 \
    -p 5000:5000 \
    -v "${HOST_VOLUME}:${CONTAINER_VOLUME}" \
    "${IMAGE}" \
    /bin/bash -c "./deploy.sh"

  echo ""
  echo "[$(date -Iseconds)] docker ps:"
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep -n "${CONTAINER_NAME}" || true

  echo ""
  echo "[$(date -Iseconds)] docker logs（前 80 行）："
  docker logs --tail 80 "${CONTAINER_NAME}" || true
}

main() {
  test_and_mount_nas

  # 仅当 registry 是 HTTP（你给的 172.16.0.70:5000 一般是 HTTP）时才需要 insecure 配置；
  # 这里不做额外判断，直接尝试写入。
  export RETINACLIP_TEST_REGISTRY="${REGISTRY}"
  ensure_docker_insecure_registry_for_http

  ensure_task_env_file
  docker_pull_and_run

  echo ""
  echo "[$(date -Iseconds)] ===== NAS+Docker 测试完成 ====="
}

main "$@"

