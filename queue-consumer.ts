/**
 * RabbitMQ 消费者
 * 
 * 职责：
 * 1. 连接 RabbitMQ 并消费 media.uploaded 队列
 * 2. 将任务分发给 ECS 编排器
 * 3. 通过 Webhook 回报任务进度
 * 4. 处理消息确认 (ack/nack)
 * 5. 自动重连
 * 
 * 背压策略：
 * - prefetchCount = maxInstances
 * - 当所有 ECS 实例槽位被占用时，RabbitMQ 自动停止推送消息
 * - 实例完成后 ack 消息，RabbitMQ 继续推送
 */

import amqp from 'amqplib'
import type { ConsumeMessage } from 'amqplib'
import { WorkerConfig } from './config'
import { ECSOrchestrator, TaskResult } from './ecs-orchestrator'
import { TaskParams } from './domain/task'
import {
  sendWebhook,
  buildProcessingStartedPayload,
  buildCompletedPayload,
  buildFailedPayload,
  buildRetryingPayload,
} from './webhook-sender'
import { createLogger } from './logger'

const log = createLogger('Consumer')

/** 从队列消息中解析出的任务数据 */
interface TaskMessage {
  message_id: string
  user_id?: string
  video_download_url: string
  webhook_url: string
  detect_type?: 'auto' | 'manual'
  target_regions?: Array<{
    id: string
    x: number
    y: number
    width: number
    height: number
  }>
}

type TaskResultWithAttempt = TaskResult & { attempt: number }

/**
 * RabbitMQ 消费者
 */
export class QueueConsumer {
  private config: WorkerConfig
  private orchestrator: ECSOrchestrator
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private connection: any = null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private channel: any = null
  private consumerTag: string | null = null
  private reconnectAttempts: number = 0
  private shuttingDown: boolean = false
  /** 当前正在处理的任务数 */
  private processingCount: number = 0

  constructor(config: WorkerConfig, orchestrator: ECSOrchestrator) {
    this.config = config
    this.orchestrator = orchestrator
  }

  /**
   * 启动消费者
   */
  async start(): Promise<void> {
    await this.connect()
    await this.startConsuming()
    log.info('队列消费者已启动', {
      queue: this.config.rabbitmq.queue,
      prefetch: this.config.rabbitmq.prefetchCount,
    })
  }

  /**
   * 连接 RabbitMQ
   */
  private async connect(): Promise<void> {
    try {
      log.info('正在连接 RabbitMQ...', { url: this.maskUrl(this.config.rabbitmq.url) })

      this.connection = await amqp.connect(this.config.rabbitmq.url)
      this.channel = await this.connection.createChannel()

      // 设置预取数量（背压控制的关键）
      await this.channel.prefetch(this.config.rabbitmq.prefetchCount)

      // 声明队列
      await this.channel.assertQueue(this.config.rabbitmq.queue, { durable: true })

      // 连接断开事件
      this.connection.on('close', () => {
        if (!this.shuttingDown) {
          log.warn('RabbitMQ 连接断开，准备重连...')
          this.scheduleReconnect()
        }
      })

      this.connection.on('error', (err: Error) => {
        log.error('RabbitMQ 连接错误', err)
      })

      this.channel.on('error', (err: Error) => {
        log.error('RabbitMQ Channel 错误', err)
      })

      this.reconnectAttempts = 0
      log.info('RabbitMQ 连接成功')

    } catch (error) {
      log.error('RabbitMQ 连接失败', error)
      throw error
    }
  }

  /**
   * 开始消费队列消息
   */
  private async startConsuming(): Promise<void> {
    if (!this.channel) throw new Error('Channel 未初始化')

    const result = await this.channel.consume(
      this.config.rabbitmq.queue,
      async (msg: ConsumeMessage | null) => {
        if (!msg) return
        await this.handleMessage(msg)
      },
      { noAck: false }
    )

    this.consumerTag = result.consumerTag
    log.info('开始消费队列', { consumerTag: this.consumerTag })
  }

  /**
   * 处理单条消息
   * 
   * 流程：
   * 1. 解析消息
   * 2. 发送 "处理中" webhook
   * 3. 创建 ECS 实例执行任务
   * 4. 根据结果发送 "完成/失败" webhook
   * 5. ACK 消息
   */
  private async handleMessage(msg: ConsumeMessage): Promise<void> {
    this.processingCount++
    let task: TaskMessage | null = null

    try {
      // 1. 解析消息
      const content = msg.content.toString('utf-8')
      log.info('收到新消息', {
        messageId: msg.properties.messageId,
        contentLength: content.length,
      })

      try {
        task = JSON.parse(content) as TaskMessage
      } catch {
        log.error('消息 JSON 解析失败，丢弃消息', undefined, { content: content.substring(0, 200) })
        this.channel?.ack(msg)
        this.processingCount--
        return
      }

      const messageId = task.message_id || msg.properties.messageId || 'unknown'
      task.message_id = messageId

      // 验证必需字段
      if (!task.video_download_url || !task.webhook_url) {
        log.error('消息缺少必需字段', undefined, { messageId })
        this.channel?.ack(msg)
        this.processingCount--
        return
      }

      log.info('任务解析成功', {
        messageId,
        userId: task.user_id,
        detectType: task.detect_type || 'auto',
        hasRegions: !!(task.target_regions && task.target_regions.length > 0),
      })

      // 2. 发送 "处理中" webhook
      await sendWebhook(
        task.webhook_url,
        buildProcessingStartedPayload(messageId, task.user_id),
        this.config
      )

      // 3. 执行任务（带重试）
      const result = await this.executeWithRetry(task, messageId)

      // 4. 根据结果处理
      if (result.success) {
        await sendWebhook(
          task.webhook_url,
          buildCompletedPayload(
            messageId,
            result.outputVideoUrl || '',
            result.durationSeconds,
            task.user_id,
          ),
          this.config
        )

        log.info('任务处理成功', {
          messageId,
          instanceId: result.instanceId,
          attempt: result.attempt,
          durationSeconds: result.durationSeconds.toFixed(2),
          outputUrl: result.outputVideoUrl,
        })
      } else {
        await sendWebhook(
          task.webhook_url,
          buildFailedPayload(
            messageId,
            result.error || '处理失败',
            undefined,
            task.user_id,
          ),
          this.config
        )

        log.error('任务处理失败', undefined, {
          messageId,
          instanceId: result.instanceId,
          attempt: result.attempt,
          error: result.error,
        })
      }

      // 5. ACK 消息
      this.channel?.ack(msg)

    } catch (error) {
      log.error('消息处理异常', error, { messageId: task?.message_id })

      // 发送失败 webhook
      if (task?.webhook_url) {
        await sendWebhook(
          task.webhook_url,
          buildFailedPayload(
            task.message_id || 'unknown',
            error instanceof Error ? error.message : '未知内部错误',
            undefined,
            task.user_id,
          ),
          this.config
        ).catch(() => {})
      }

      this.channel?.ack(msg)
    } finally {
      this.processingCount--
    }
  }

  /**
   * 带重试的任务执行
   */
  private async executeWithRetry(
    task: TaskMessage,
    messageId: string,
  ): Promise<TaskResultWithAttempt> {
    let lastResult: TaskResult | null = null
    let lastAttempt = 0
    const processingImage = process.env.WORKER_PROCESSING_IMAGE || 'mingle-processor:latest'

    for (let attempt = 0; attempt <= this.config.retry.maxAttempts; attempt++) {
      lastAttempt = attempt
      if (attempt > 0) {
        // 发送重试 webhook
        await sendWebhook(
          task.webhook_url,
          buildRetryingPayload(messageId, attempt, lastResult?.error || '未知错误', task.user_id),
          this.config
        )

        // 指数退避
        const delay = Math.min(
          this.config.retry.baseInterval * Math.pow(2, attempt - 1),
          this.config.retry.maxInterval
        )
        log.info(`等待 ${delay}ms 后重试 (第 ${attempt} 次)`, { messageId, attempt })
        await new Promise(resolve => setTimeout(resolve, delay))

        await sendWebhook(
          task.webhook_url,
          buildProcessingStartedPayload(messageId, task.user_id),
          this.config
        )
      }

      log.info('开始执行任务', {
        messageId,
        attempt,
        detectType: task.detect_type || 'auto',
      })

      // 构建任务参数
      const taskParams: TaskParams = {
        messageId,
        userId: task.user_id,
        videoDownloadUrl: task.video_download_url,
        webhookUrl: task.webhook_url,
        detectType: task.detect_type || 'auto',
        targetRegions: task.target_regions ? JSON.stringify(task.target_regions) : undefined,
      }

      // 运行 ECS 实例
      lastResult = await this.orchestrator.runTask(taskParams, processingImage, attempt)

      if (lastResult.success) {
        return { ...lastResult, attempt }
      }

      // 如果是抢占式回收导致的失败，直接重试
      const isSpotReclaim = lastResult.error?.includes('抢占式回收') || lastResult.error?.includes('Stopped')
      if (isSpotReclaim) {
        log.warn('抢占式实例被回收，将重试', { messageId, attempt })
      }

      log.warn(`任务执行失败 (第 ${attempt + 1}/${this.config.retry.maxAttempts + 1} 次)`, {
        messageId,
        error: lastResult.error,
        instanceId: lastResult.instanceId,
        attempt,
      })
    }

    if (!lastResult) {
      throw new Error('executeWithRetry 返回 lastResult 为空')
    }
    return { ...lastResult, attempt: lastAttempt }
  }

  /**
   * 自动重连
   */
  private scheduleReconnect(): void {
    if (this.shuttingDown) return

    const maxAttempts = this.config.rabbitmq.maxReconnectAttempts
    if (maxAttempts > 0 && this.reconnectAttempts >= maxAttempts) {
      log.error(`重连失败次数超过上限 (${maxAttempts})，停止重连`)
      process.exit(1)
    }

    this.reconnectAttempts++
    const delay = Math.min(
      this.config.rabbitmq.reconnectInterval * Math.pow(1.5, this.reconnectAttempts - 1),
      60000
    )

    log.info(`${delay}ms 后尝试第 ${this.reconnectAttempts} 次重连...`)

    setTimeout(async () => {
      try {
        await this.connect()
        await this.startConsuming()
        log.info('重连成功')
      } catch (error) {
        log.error('重连失败', error)
        this.scheduleReconnect()
      }
    }, delay)
  }

  /**
   * 获取当前处理状态
   */
  getStatus() {
    return {
      connected: !!this.connection && !!this.channel,
      processingCount: this.processingCount,
      reconnectAttempts: this.reconnectAttempts,
      shuttingDown: this.shuttingDown,
    }
  }

  /**
   * 优雅关闭
   * @param graceMs 最多等待任务完成的时长 (ms)，超时后仍会关闭连接（未 ack 的消息会重新投递）
   */
  async shutdown(graceMs?: number): Promise<void> {
    this.shuttingDown = true
    log.info('正在关闭队列消费者...')

    // 1. 停止消费
    if (this.channel && this.consumerTag) {
      try {
        await this.channel.cancel(this.consumerTag)
        log.info('已停止消费新消息')
      } catch (error) {
        log.warn('取消消费失败', { error: String(error) })
      }
    }

    // 2. 等待处理中的任务（不超过传入的 graceMs，由主进程统一控制关机超时）
    const maxWait = typeof graceMs === 'number' ? graceMs : 60000
    const start = Date.now()
    while (this.processingCount > 0 && Date.now() - start < maxWait) {
      log.info(`等待 ${this.processingCount} 个任务处理完成...`)
      await new Promise(resolve => setTimeout(resolve, 5000))
    }

    if (this.processingCount > 0) {
      log.warn(`仍有 ${this.processingCount} 个任务未完成，强制关闭（未 ack 的消息会被重新投递）`)
    }

    // 3. 关闭连接
    try {
      await this.channel?.close()
      await this.connection?.close()
      log.info('RabbitMQ 连接已关闭')
    } catch (error) {
      log.warn('关闭 RabbitMQ 连接失败', { error: String(error) })
    }
  }

  /**
   * 遮盖 URL 中的密码
   */
  private maskUrl(url: string): string {
    try {
      const parsed = new URL(url)
      if (parsed.password) parsed.password = '***'
      return parsed.toString()
    } catch {
      return url
    }
  }
}
