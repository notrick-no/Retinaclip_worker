/**
 * 任务契约（领域层）
 *
 * TaskParams 用于 Worker 编排器 -> ECS 启动脚本 (cloud-init) -> 容器进程。
 * 这里不包含具体实现细节，只定义跨模块的稳定类型与字段含义。
 */

export interface TaskParams {
  messageId: string
  userId?: string
  videoDownloadUrl: string
  webhookUrl: string
  detectType: string
  targetRegions?: string // JSON 字符串
}

