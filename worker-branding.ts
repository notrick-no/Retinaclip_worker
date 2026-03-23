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
  workerLog: '/var/log/retinaclip-worker.log',
  containerStdout: '/tmp/retinaclip-container-stdout.log',
  containerStderr: '/tmp/retinaclip-container-stderr.log',
} as const
