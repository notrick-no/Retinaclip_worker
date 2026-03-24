/**
 * RetinaClip Worker - 阿里云 ECS 实例编排任务处理器
 * 
 * 入口文件，负责：
 * 1. 加载配置
 * 2. 初始化各子系统
 * 3. 启动健康检查定时器
 * 4. 处理信号（优雅退出）
 * 
 * ┌─────────────────────────────────────────────────────────┐
 * │                     Worker 架构                          │
 * │                                                          │
 * │   池模式：RabbitMQ checkQueue ─► PoolQueueScheduler ─► ECSOrchestrator │
 * │          （不消费）     仅按队列深度启动 Stopped 池机                    │
 * │   非池：RabbitMQ ──► QueueConsumer ──► ECSOrchestrator（每消息一台任务） │
 * │                     │                │                    │
 * │                     │          ┌─────┼──────┐            │
 * │                     │          │ ECS 实例 1  │            │
 * │                     │          │ ECS 实例 2  │            │
 * │                     │          │  ...        │            │
 * │                     │          └─────┼──────┘            │
 * │                     │                │                    │
 * │                     │          cloud-init / 池内容器       │
 * │                     │          （池内镜像自消费 MQ + webhook）          │
 * │                     └── Webhook ─────┘（非池编排路径）      │
 * │                          │                                │
 * │                     Next.js App                           │
 * └─────────────────────────────────────────────────────────┘
 * 
 * 启动方式：
 *   pnpm worker
 *   # 或
 *   tsx worker/index.ts
 */

import 'dotenv/config'
import type { WorkerConfig } from './config'
import { loadConfig } from './config'
import { ECSOrchestrator } from './ecs-orchestrator'
import { QueueConsumer } from './queue-consumer'
import { PoolQueueScheduler } from './pool-queue-scheduler'
import { createLogger, setLogLevel } from './logger'

const log = createLogger('Main')

// ===== 全局状态 =====
let consumer: QueueConsumer | null = null
let poolScheduler: PoolQueueScheduler | null = null
let orchestrator: ECSOrchestrator | null = null
let healthCheckTimer: NodeJS.Timeout | null = null
let zombieCheckTimer: NodeJS.Timeout | null = null
let isShuttingDown = false
/** 关闭超时定时器：超时后强制退出，避免因任务未完成而永远关不掉 */
let shutdownDeadlineTimer: NodeJS.Timeout | null = null

/**
 * 主函数
 */
async function main() {
  // 1. 加载配置
  const config = loadConfig()
  setLogLevel(config.log.level)

  log.info('═══════════════════════════════════════════════')
  log.info('  RetinaClip Worker - 阿里云 ECS 实例编排器 启动中')
  log.info('═══════════════════════════════════════════════')
  log.info('配置信息', {
    queue: config.rabbitmq.queue,
    prefetch: config.rabbitmq.prefetchCount,
    poolModeScheduler: config.ecs.poolEnabled,
    schedulerPollMs: config.scheduler.pollIntervalMs,
    maxInstances: config.ecs.maxInstances,
    region: config.ecs.regionId,
    instanceType: config.ecs.instanceType,
    imageId: config.ecs.imageId,
    spotInstance: config.ecs.useSpotInstance,
    spotStrategy: config.ecs.spotStrategy,
    taskTimeout: `${config.ecs.taskTimeout / 60000} min`,
    retryAttempts: config.retry.maxAttempts,
    mockProcessing: config.ecs.mockProcessing,
    ...(config.ecs.mockProcessing && { mockDelaySeconds: config.ecs.mockDelaySeconds }),
  })
  if (config.ecs.mockProcessing) {
    log.warn('已启用 Mock 处理：ECS 内不运行算法镜像，延迟后返回原视频 URL')
  }

  // 2. 初始化 ECS 编排器
  log.info('初始化 ECS 编排器...')
  orchestrator = new ECSOrchestrator(config)
  await orchestrator.initialize()

  // 3. 池模式：只观测队列深度并启动池机；非池模式：消费队列并编排每任务 ECS
  if (config.ecs.poolEnabled) {
    log.info('池模式：初始化队列深度调度（不消费消息）...')
    poolScheduler = new PoolQueueScheduler(config, orchestrator)
    await poolScheduler.start()
  } else {
    log.info('初始化队列消费者...')
    consumer = new QueueConsumer(config, orchestrator)
    await consumer.start()
  }

  // 4. 启动健康检查
  healthCheckTimer = setInterval(() => {
    const ecsStatus = orchestrator!.getStatus()

    if (poolScheduler) {
      const s = poolScheduler.getStatus()
      log.info('健康检查', {
        ecs: {
          active: `${ecsStatus.activeSlots}/${ecsStatus.maxSlots}`,
          running: ecsStatus.runningInstances,
        },
        poolScheduler: {
          connected: s.connected,
          queueMessages: s.lastMessageCount,
          queueConsumers: s.lastConsumerCount,
        },
      })
    } else if (consumer) {
      const consumerStatus = consumer.getStatus()
      log.info('健康检查', {
        ecs: {
          active: `${ecsStatus.activeSlots}/${ecsStatus.maxSlots}`,
          running: ecsStatus.runningInstances,
        },
        consumer: {
          connected: consumerStatus.connected,
          processing: consumerStatus.processingCount,
        },
      })
    }
  }, config.healthCheck.interval)

  // 5. 启动僵尸实例清理
  zombieCheckTimer = setInterval(() => {
    orchestrator!.cleanupZombieInstances().catch(err => {
      log.error('僵尸实例清理失败', err)
    })
  }, config.healthCheck.zombieCheckInterval)

  // 6. 注册信号处理器（传入配置以使用关机超时）
  setupSignalHandlers(config)

  log.info('═══════════════════════════════════════════════')
  log.info(
    config.ecs.poolEnabled
      ? '  Worker 已就绪（池模式：按队列深度启停池机，消息由池内容器消费）...'
      : '  Worker 已就绪，等待任务...',
  )
  log.info('═══════════════════════════════════════════════')
}

/**
 * 注册进程信号处理（优雅退出 + 超时强制退出 + 二次信号立即退出）
 */
function setupSignalHandlers(config: WorkerConfig) {
  const { graceMs } = config.shutdown

  const gracefulShutdown = async (signal: string) => {
    // 第二次收到信号：立即强制退出，便于用户“关不掉就再按一次 Ctrl+C”
    if (isShuttingDown) {
      log.warn('已在关闭中，再次收到信号，强制退出')
      process.exit(1)
    }

    isShuttingDown = true
    log.info(`收到 ${signal} 信号，开始优雅关闭（最多等待 ${graceMs / 1000} 秒）...`)

    // 停止定时器
    if (healthCheckTimer) clearInterval(healthCheckTimer)
    if (zombieCheckTimer) clearInterval(zombieCheckTimer)

    // 超时后强制退出，确保进程不会因任务未完成而永远关不掉
    shutdownDeadlineTimer = setTimeout(() => {
      log.warn(`已达关闭超时 ${graceMs}ms，强制退出（未完成的任务可能被重新投递）`)
      process.exit(1)
    }, graceMs)

    try {
      // 1. 停止消费 / 关闭调度 RabbitMQ + 在 graceMs 内等待处理中的任务（仅非池消费者）
      if (consumer) {
        await consumer.shutdown(graceMs)
      }
      if (poolScheduler) {
        await poolScheduler.shutdown()
      }

      // 2. 关闭 ECS 编排器（等待/强制释放实例，同样受 graceMs 约束）
      if (orchestrator) {
        await orchestrator.shutdown(graceMs)
      }

      if (shutdownDeadlineTimer) clearTimeout(shutdownDeadlineTimer)
      shutdownDeadlineTimer = null
      log.info('Worker 已安全关闭')
      process.exit(0)
    } catch (error) {
      log.error('关闭过程出错', error)
      process.exit(1)
    }
  }

  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'))
  process.on('SIGINT', () => gracefulShutdown('SIGINT'))

  process.on('uncaughtException', (error) => {
    log.error('未捕获异常', error)
    gracefulShutdown('uncaughtException')
  })

  process.on('unhandledRejection', (reason) => {
    log.error('未处理的 Promise 拒绝', reason as Error)
  })
}

// ===== 启动 =====
main().catch((error) => {
  log.error('Worker 启动失败', error)
  process.exit(1)
})
