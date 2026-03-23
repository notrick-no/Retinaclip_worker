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
  /** 结果上传 URL（若算法容器支持） */
  videoUploadUrl?: string
  webhookUrl: string
  detectType: string
  targetRegions?: string // JSON 字符串
  /** 队列消息中的 operation 列表（字典序已用于路由键，此处保留原始顺序供容器使用） */
  operations?: string[]
  qualityPreset?: string
  /**
   * 池实例第二维标签 `mingle:pool-profile` 等的取值；由 WORKER_ECS_POOL_PROFILE_MAP 解析。
   * 未设置时池查询不按 profile 过滤（兼容仅打 lifecycle=pool 的旧池）。
   */
  poolProfile?: string
}

