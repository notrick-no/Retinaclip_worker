/**
 * 池模式 RabbitMQ 调度：只 passive 检查队列深度，不消费、不 ack。
 * 根据深度调用 ECSOrchestrator.scalePoolToQueueDepth 启动 Stopped 池机。
 */

import amqp from 'amqplib'
import { WorkerConfig } from './config'
import { ECSOrchestrator } from './ecs-orchestrator'
import { createLogger } from './logger'

const log = createLogger('PoolScheduler')

export class PoolQueueScheduler {
  private config: WorkerConfig
  private orchestrator: ECSOrchestrator
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private connection: any = null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private channel: any = null
  private reconnectAttempts = 0
  private shuttingDown = false
  private pollTimer: ReturnType<typeof setInterval> | null = null
  private pollInFlight = false

  lastMessageCount: number | null = null
  lastConsumerCount: number | null = null
  lastPollAt: number | null = null

  constructor(config: WorkerConfig, orchestrator: ECSOrchestrator) {
    this.config = config
    this.orchestrator = orchestrator
  }

  async start(): Promise<void> {
    await this.connect()
    await this.runPollOnce()
    this.pollTimer = setInterval(() => {
      void this.runPollOnce()
    }, this.config.scheduler.pollIntervalMs)
    log.info('池队列调度已启动（仅观测队列深度，不消费）', {
      queue: this.config.rabbitmq.queue,
      pollIntervalMs: this.config.scheduler.pollIntervalMs,
      poolProfile: this.config.scheduler.poolProfile ?? '(未指定)',
    })
  }

  private async connect(): Promise<void> {
    log.info('正在连接 RabbitMQ（调度通道）...', { url: this.maskUrl(this.config.rabbitmq.url) })
    this.connection = await amqp.connect(this.config.rabbitmq.url)
    this.channel = await this.connection.createChannel()

    this.connection.on('close', () => {
      if (!this.shuttingDown) {
        log.warn('RabbitMQ 连接断开（调度），准备重连...')
        this.scheduleReconnect()
      }
    })

    this.connection.on('error', (err: Error) => {
      log.error('RabbitMQ 连接错误（调度）', err)
    })

    this.channel.on('error', (err: Error) => {
      log.error('RabbitMQ Channel 错误（调度）', err)
    })

    this.reconnectAttempts = 0
    log.info('RabbitMQ 调度通道已连接')
  }

  private scheduleReconnect(): void {
    if (this.shuttingDown) return

    const maxAttempts = this.config.rabbitmq.maxReconnectAttempts
    if (maxAttempts > 0 && this.reconnectAttempts >= maxAttempts) {
      log.error(`调度通道重连失败次数超过上限 (${maxAttempts})，停止重连`)
      process.exit(1)
    }

    this.reconnectAttempts++
    const delay = Math.min(
      this.config.rabbitmq.reconnectInterval * Math.pow(1.5, this.reconnectAttempts - 1),
      60000,
    )

    log.info(`${delay}ms 后尝试调度通道第 ${this.reconnectAttempts} 次重连...`)

    setTimeout(async () => {
      try {
        await this.teardownConnection()
        await this.connect()
        await this.runPollOnce()
        log.info('调度通道重连成功')
      } catch (error) {
        log.error('调度通道重连失败', error)
        this.scheduleReconnect()
      }
    }, delay)
  }

  private async teardownConnection(): Promise<void> {
    try {
      await this.channel?.close()
    } catch {
      /* ignore */
    }
    try {
      await this.connection?.close()
    } catch {
      /* ignore */
    }
    this.channel = null
    this.connection = null
  }

  private async runPollOnce(): Promise<void> {
    if (this.shuttingDown || this.pollInFlight) return
    this.pollInFlight = true
    try {
      if (!this.channel) return

      const q = await this.channel.checkQueue(this.config.rabbitmq.queue)
      this.lastMessageCount = q.messageCount
      this.lastConsumerCount = q.consumerCount
      this.lastPollAt = Date.now()

      log.debug('队列观测', {
        queue: this.config.rabbitmq.queue,
        messageCount: q.messageCount,
        consumerCount: q.consumerCount,
      })

      await this.orchestrator.scalePoolToQueueDepth(
        q.messageCount,
        this.config.scheduler.poolProfile,
      )
    } catch (error) {
      log.error('队列观测或扩缩失败', error, { queue: this.config.rabbitmq.queue })
    } finally {
      this.pollInFlight = false
    }
  }

  getStatus() {
    return {
      connected: !!this.connection && !!this.channel,
      reconnectAttempts: this.reconnectAttempts,
      shuttingDown: this.shuttingDown,
      lastMessageCount: this.lastMessageCount,
      lastConsumerCount: this.lastConsumerCount,
      lastPollAt: this.lastPollAt,
    }
  }

  async shutdown(): Promise<void> {
    this.shuttingDown = true
    if (this.pollTimer) {
      clearInterval(this.pollTimer)
      this.pollTimer = null
    }
    log.info('正在关闭池队列调度 RabbitMQ 连接...')
    await this.teardownConnection()
    log.info('池队列调度已关闭')
  }

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
