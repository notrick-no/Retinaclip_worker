/**
 * Webhook 契约（Node 侧统一来源）
 *
 * 目标：避免事件类型/字段构造在不同位置“写死”导致不一致。
 * 其中 `processor/process.py` 需要保持与这里的 progress event_type 映射完全一致。
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

export interface WebhookPayload {
  /** RabbitMQ 消息的 message_id，与 Prisma VideoProcessJob.queueJobId 对应 */
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

export type ProgressStage = 'downloading' | 'processing' | 'uploading'

export const progressStageToEventType: Record<ProgressStage, WebhookEventType> = {
  downloading: 'download_progress',
  processing: 'processing_progress',
  uploading: 'upload_progress',
}

