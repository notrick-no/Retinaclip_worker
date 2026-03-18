/**
 * Webhook 回调发送器
 * 
 * 负责向 Next.js 应用发送任务状态更新回调
 * - 支持 HMAC-SHA256 签名验证
 * - 指数退避重试
 * - 支持所有事件类型（进度、完成、失败等）
 */

import * as crypto from 'crypto'
import { WorkerConfig } from './config'
import { createLogger } from './logger'

const log = createLogger('Webhook')

/**
 * Webhook 事件类型
 */
export type WebhookEventType =
  | 'queue_position_update'
  | 'download_started'
  | 'download_progress'
  | 'processing_started'
  | 'processing_progress'
  | 'upload_started'
  | 'upload_progress'
  | 'completed'
  | 'failed'
  | 'retrying'

/**
 * Webhook Payload
 */
export interface WebhookPayload {
  job_id: string
  user_id?: string
  status: string
  event_type: WebhookEventType
  // 进度信息
  current_stage?: string
  progress_percentage?: number
  progress_message?: string
  estimated_time_remaining?: number
  queue_position?: number
  // 完成信息
  output_video_url?: string
  completed_at?: string
  processing_time_seconds?: number
  // 错误信息
  error_message?: string
  retry_count?: number
}

/**
 * 生成 Webhook 签名
 */
function generateSignature(payload: string, secret: string): string {
  return 'sha256=' + crypto
    .createHmac('sha256', secret)
    .update(payload)
    .digest('hex')
}

/**
 * 发送 Webhook 回调（带重试）
 */
export async function sendWebhook(
  webhookUrl: string,
  payload: WebhookPayload,
  config: WorkerConfig
): Promise<boolean> {
  const payloadStr = JSON.stringify(payload)
  const signature = generateSignature(payloadStr, config.webhook.secret)

  for (let attempt = 0; attempt <= config.webhook.maxRetries; attempt++) {
    try {
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), config.webhook.timeout)

      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Webhook-Signature': signature,
          'X-Webhook-Event': payload.event_type,
          'X-Webhook-Delivery': crypto.randomUUID(),
        },
        body: payloadStr,
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (response.ok) {
        log.debug('Webhook 发送成功', {
          url: webhookUrl,
          event: payload.event_type,
          jobId: payload.job_id,
        })
        return true
      }

      log.warn(`Webhook 返回非 200 状态码: ${response.status}`, {
        attempt: attempt + 1,
        jobId: payload.job_id,
      })
    } catch (error) {
      log.warn(`Webhook 发送失败 (第 ${attempt + 1} 次)`, {
        jobId: payload.job_id,
        error: error instanceof Error ? error.message : String(error),
      })
    }

    // 如果不是最后一次尝试，等待后重试（指数退避）
    if (attempt < config.webhook.maxRetries) {
      const delay = Math.min(
        config.webhook.retryBaseInterval * Math.pow(2, attempt),
        30000 // 最大 30 秒
      )
      log.debug(`等待 ${delay}ms 后重试 webhook...`)
      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }

  log.error(`Webhook 发送最终失败，已重试 ${config.webhook.maxRetries} 次`, undefined, {
    url: webhookUrl,
    jobId: payload.job_id,
  })
  return false
}

// ===== 便捷方法：构造特定事件的 payload =====

export function buildDownloadStartedPayload(jobId: string, userId?: string): WebhookPayload {
  return {
    job_id: jobId,
    user_id: userId,
    status: 'DOWNLOADING',
    event_type: 'download_started',
    current_stage: 'downloading',
    progress_percentage: 0,
    progress_message: '正在下载视频...',
  }
}

export function buildProcessingStartedPayload(jobId: string, userId?: string): WebhookPayload {
  return {
    job_id: jobId,
    user_id: userId,
    status: 'PROCESSING',
    event_type: 'processing_started',
    current_stage: 'processing',
    progress_percentage: 0,
    progress_message: '正在处理视频...',
  }
}

export function buildProgressPayload(
  jobId: string,
  stage: 'downloading' | 'processing' | 'uploading',
  percentage: number,
  message: string,
  estimatedTime?: number,
  userId?: string,
): WebhookPayload {
  const eventMap = {
    downloading: 'download_progress' as const,
    processing: 'processing_progress' as const,
    uploading: 'upload_progress' as const,
  }

  return {
    job_id: jobId,
    user_id: userId,
    status: stage === 'downloading' ? 'DOWNLOADING' : stage === 'uploading' ? 'UPLOADING' : 'PROCESSING',
    event_type: eventMap[stage],
    current_stage: stage,
    progress_percentage: percentage,
    progress_message: message,
    estimated_time_remaining: estimatedTime,
  }
}

export function buildCompletedPayload(
  jobId: string,
  outputUrl: string,
  processingTimeSec: number,
  userId?: string,
): WebhookPayload {
  return {
    job_id: jobId,
    user_id: userId,
    status: 'COMPLETED',
    event_type: 'completed',
    output_video_url: outputUrl,
    completed_at: new Date().toISOString(),
    processing_time_seconds: processingTimeSec,
    progress_percentage: 100,
  }
}

export function buildFailedPayload(
  jobId: string,
  errorMessage: string,
  retryCount?: number,
  userId?: string,
): WebhookPayload {
  return {
    job_id: jobId,
    user_id: userId,
    status: 'FAILED',
    event_type: 'failed',
    error_message: errorMessage,
    completed_at: new Date().toISOString(),
    retry_count: retryCount,
  }
}

export function buildRetryingPayload(
  jobId: string,
  retryCount: number,
  errorMessage: string,
  userId?: string,
): WebhookPayload {
  return {
    job_id: jobId,
    user_id: userId,
    status: 'PROCESSING',
    event_type: 'retrying',
    retry_count: retryCount,
    error_message: errorMessage,
    progress_message: `任务重试中 (第 ${retryCount} 次)...`,
  }
}
