/**
 * 与阿里云控制台标签、宿主机路径统一的命名（retinaclip:*）
 */

export const WORKER_ECS_TAGS = {
  role: 'retinaclip:role',
  lifecycle: 'retinaclip:lifecycle',
  messageId: 'retinaclip:message-id',
  createdAt: 'retinaclip:created-at',
} as const

/** 默认池 profile 标签键，可用 WORKER_ECS_POOL_PROFILE_TAG_KEY 覆盖 */
export const DEFAULT_POOL_PROFILE_TAG_KEY = 'retinaclip:pool-profile'

/** 任务在 ECS 宿主机上的约定路径（cloud-init 与编排器轮询必须一致） */
export const WORKER_HOST_PATHS = {
  taskResult: '/tmp/retinaclip-task-result',
  taskDone: '/tmp/retinaclip-task-done',
  taskEnv: '/tmp/retinaclip-task.env',
  /** 首选路径；若云助手/权限无法写 /var/log，启动脚本会回退到 {@link WORKER_HOST_PATHS.workerLogFallback} */
  workerLog: '/var/log/retinaclip-worker.log',
  /** 与 cloud-init 内 bash 回退路径保持一致（排查时可 tail 此文件） */
  workerLogFallback: '/tmp/retinaclip-worker.log',
  containerStdout: '/tmp/retinaclip-container-stdout.log',
  containerStderr: '/tmp/retinaclip-container-stderr.log',
} as const
