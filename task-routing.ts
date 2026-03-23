/**
 * 根据队列消息中的 operation / quality_preset 解析处理镜像与池 profile。
 *
 * 映射表来自环境变量 JSON（见 config.processing）。
 * 查找键顺序（先命中先用）：
 * 1. sortedOps@quality   例如 "remove subtitles@normal"（多 operation 时按字典序用 | 连接： "a|b@normal"）
 * 2. sortedOps
 * 3. @quality（仅质量档）
 * 4. 默认 WORKER_PROCESSING_IMAGE
 */

import type { WorkerConfig } from './config'

export interface TaskRoutingMessageSlice {
  /** 队列 JSON 字段名 `operation` */
  operation?: string[]
  quality_preset?: string
  processing_image?: string
}

export interface ResolvedTaskRouting {
  processingImage: string
  /** 打在池实例上的 mingle:pool-profile 值；未配置映射时不返回 */
  poolProfile?: string
  /** 用于日志的查找说明 */
  resolvedFrom: string
}

function sortedOpKey(operations: string[] | undefined): string {
  if (!operations?.length) return ''
  return [...operations]
    .map((s) => s.trim())
    .filter(Boolean)
    .sort()
    .join('|')
}

/**
 * 从任务消息切片解析镜像与池 profile（不依赖 RabbitMQ 类型，便于单测）。
 */
export function resolveTaskRouting(
  task: TaskRoutingMessageSlice,
  config: WorkerConfig,
): ResolvedTaskRouting {
  const override = task.processing_image?.trim()
  if (override) {
    return {
      processingImage: override,
      resolvedFrom: 'message.processing_image',
    }
  }

  const quality = (task.quality_preset ?? 'default').trim() || 'default'
  const opKey = sortedOpKey(task.operation)
  const imageMap = config.processing.imageMap
  const profileMap = config.processing.poolProfileMap

  const imageKeys: string[] = []
  if (opKey) {
    imageKeys.push(`${opKey}@${quality}`)
    imageKeys.push(opKey)
  }
  imageKeys.push(`@${quality}`)

  let processingImage = config.processing.defaultImage
  let resolvedFrom = 'default'

  for (const k of imageKeys) {
    if (imageMap[k]) {
      processingImage = imageMap[k]
      resolvedFrom = `imageMap[${k}]`
      break
    }
  }

  let poolProfile: string | undefined
  for (const k of imageKeys) {
    if (profileMap[k]) {
      poolProfile = profileMap[k]
      break
    }
  }

  return { processingImage, poolProfile, resolvedFrom }
}
