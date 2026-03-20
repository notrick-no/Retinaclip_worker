/**
 * Worker 配置管理
 * 
 * 阿里云 ECS 抢占式实例编排策略配置：
 * - 并发控制：通过 MAX_INSTANCES 限制同时运行的 ECS 实例数
 * - 抢占式实例：使用 SpotAsPriceGo 策略自动竞价，大幅降低成本
 * - 超时管理：任务超时自动释放实例
 * - 重试策略：实例被回收或失败时重新创建
 */

export interface WorkerConfig {
  // ===== RabbitMQ 配置 =====
  rabbitmq: {
    url: string
    queue: string
    /** 预取数量，等于最大并发实例数（背压控制） */
    prefetchCount: number
    /** 连接断开后重连间隔 (ms) */
    reconnectInterval: number
    /** 最大重连次数，0 表示无限 */
    maxReconnectAttempts: number
  }

  // ===== 阿里云 ECS 编排策略 =====
  ecs: {
    /** 阿里云 AccessKey ID */
    accessKeyId: string
    /** 阿里云 AccessKey Secret */
    accessKeySecret: string
    /** ECS 地域 ID，如 cn-shanghai */
    regionId: string
    /** ECS 镜像 ID（预装算法环境的自定义镜像） */
    imageId: string
    /** 实例规格，如 ecs.gn5i-c2g1.large */
    instanceType: string
    /** 安全组 ID */
    securityGroupId: string
    /** VSwitch ID（VPC 内） */
    vswitchId: string
    /** 可用区 ID（可选） */
    zoneId?: string
    /** 实例名称前缀 */
    instanceNamePrefix: string
    /** 最大同时运行的 ECS 实例数 */
    maxInstances: number
    /** 系统盘大小 (GB) */
    systemDiskSize: number
    /** 系统盘类型 */
    systemDiskCategory: string
    /** 是否使用抢占式实例 */
    useSpotInstance: boolean
    /** 抢占式实例策略：SpotAsPriceGo（系统自动出价）| SpotWithPriceLimit（设置上限） */
    spotStrategy: 'SpotAsPriceGo' | 'SpotWithPriceLimit'
    /** 抢占式实例价格上限（仅 SpotWithPriceLimit 时有效） */
    spotPriceLimit?: number
    /** 实例公网带宽 (Mbps)，0 表示不分配公网 IP */
    internetMaxBandwidthOut: number
    /** 任务处理超时时间 (ms)，超时将释放实例 */
    taskTimeout: number
    /** 实例启动等待超时 (ms) */
    instanceStartTimeout: number
    /** 实例状态轮询间隔 (ms) */
    pollInterval: number
    /** 密钥对名称（可选，用于 SSH 登录调试） */
    keyPairName?: string
    /** 实例 RAM 角色名称（可选，授予 OSS 等访问权限） */
    ramRoleName?: string
    /** 是否启用 Mock 处理（不跑算法镜像，延迟后直接返回原视频 URL） */
    mockProcessing?: boolean
    /** Mock 模式下延迟秒数，用于模拟处理耗时 */
    mockDelaySeconds?: number
  }

  // ===== Webhook 回调配置 =====
  webhook: {
    /** Webhook 签名密钥 */
    secret: string
    /** 回调超时时间 (ms) */
    timeout: number
    /** 回调失败最大重试次数 */
    maxRetries: number
    /** 重试间隔基数 (ms)，实际间隔 = base * 2^attempt */
    retryBaseInterval: number
  }

  // ===== 任务重试策略 =====
  retry: {
    /** 任务失败最大重试次数 */
    maxAttempts: number
    /** 重试间隔基数 (ms) */
    baseInterval: number
    /** 最大重试间隔 (ms) */
    maxInterval: number
  }

  // ===== 健康检查 =====
  healthCheck: {
    /** 健康检查间隔 (ms) */
    interval: number
    /** 僵尸实例检测间隔 (ms) */
    zombieCheckInterval: number
  }

  // ===== 优雅关闭 =====
  shutdown: {
    /** 收到关闭信号后最多等待多久再强制退出 (ms)，超时后 process.exit(1) */
    graceMs: number
  }

  // ===== 日志配置 =====
  log: {
    level: 'debug' | 'info' | 'warn' | 'error'
  }
}

/**
 * 从环境变量加载配置，带默认值
 */
export function loadConfig(): WorkerConfig {
  // ===== 必填项（启动期直接校验）=====
  const accessKeyId = requireEnv('ALIBABA_CLOUD_ACCESS_KEY_ID')
  const accessKeySecret = requireEnv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
  const imageId = requireEnv('ALIYUN_ECS_IMAGE_ID')
  const securityGroupId = requireEnv('ALIYUN_ECS_SECURITY_GROUP_ID')
  const vswitchId = requireEnv('ALIYUN_ECS_VSWITCH_ID')
  const webhookSecret = requireEnv('WEBHOOK_SECRET')

  // ===== 解析/校验工具函数 =====
  const prefetchCount = parseIntEnv('WORKER_PREFETCH_COUNT', 2, { min: 1 })
  const reconnectInterval = parseIntEnv('WORKER_RECONNECT_INTERVAL', 5000, { min: 1 })
  const maxReconnectAttempts = parseIntEnv('WORKER_MAX_RECONNECT_ATTEMPTS', 0, { min: 0 })

  const instanceType = env('ALIYUN_ECS_INSTANCE_TYPE', 'ecs.gn5i-c2g1.large')
  const regionId = env('ALIYUN_ECS_REGION', 'cn-shanghai')
  const instanceNamePrefix = env('ALIYUN_ECS_INSTANCE_NAME_PREFIX', 'mingle-worker')

  const maxInstances = parseIntEnv('WORKER_MAX_INSTANCES', 5, { min: 1 })
  const systemDiskSize = parseIntEnv('ALIYUN_ECS_SYSTEM_DISK_SIZE', 40, { min: 1 })
  const systemDiskCategory = env('ALIYUN_ECS_SYSTEM_DISK_CATEGORY', 'cloud_essd')

  const useSpotInstance = parseBoolEnv('ALIYUN_ECS_USE_SPOT', true)
  const spotStrategy = parseEnumEnv<'SpotAsPriceGo' | 'SpotWithPriceLimit'>('ALIYUN_ECS_SPOT_STRATEGY', {
    defaultValue: 'SpotAsPriceGo',
    allowed: ['SpotAsPriceGo', 'SpotWithPriceLimit'],
  })
  const spotPriceLimitRaw = process.env['ALIYUN_ECS_SPOT_PRICE_LIMIT']
  const spotPriceLimit =
    spotStrategy === 'SpotWithPriceLimit' && spotPriceLimitRaw && spotPriceLimitRaw.trim() !== ''
      ? parseFloatEnv('ALIYUN_ECS_SPOT_PRICE_LIMIT', { min: 0 })
      : undefined

  return {
    rabbitmq: {
      url: env('RABBITMQ_URL', 'amqp://localhost:5672'),
      queue: env('RABBITMQ_QUEUE', 'media.uploaded'),
      prefetchCount,
      reconnectInterval,
      maxReconnectAttempts,
    },

    ecs: {
      accessKeyId,
      accessKeySecret,
      regionId,
      imageId,
      instanceType,
      securityGroupId,
      vswitchId,
      zoneId: env('ALIYUN_ECS_ZONE_ID', '') || undefined,
      instanceNamePrefix,
      maxInstances,
      systemDiskSize,
      systemDiskCategory,
      useSpotInstance,
      spotStrategy,
      spotPriceLimit,
      internetMaxBandwidthOut: parseIntEnv('ALIYUN_ECS_BANDWIDTH_OUT', 100, { min: 0 }),
      taskTimeout: parseIntEnv('WORKER_TASK_TIMEOUT', 30 * 60 * 1000, { min: 1000 }), // 30 分钟
      instanceStartTimeout: parseIntEnv('WORKER_INSTANCE_START_TIMEOUT', 5 * 60 * 1000, { min: 1000 }), // 5 分钟
      pollInterval: parseIntEnv('WORKER_POLL_INTERVAL', 10000, { min: 1000 }), // 10 秒
      keyPairName: env('ALIYUN_ECS_KEY_PAIR_NAME', '') || undefined,
      ramRoleName: env('ALIYUN_ECS_RAM_ROLE_NAME', '') || undefined,
      mockProcessing: parseBoolEnv('WORKER_MOCK_PROCESSING', false),
      mockDelaySeconds: parseIntEnv('WORKER_MOCK_DELAY_SECONDS', 60, { min: 1 }),
    },

    webhook: {
      secret: webhookSecret,
      timeout: parseIntEnv('WORKER_WEBHOOK_TIMEOUT', 10000, { min: 1 }),
      maxRetries: parseIntEnv('WORKER_WEBHOOK_MAX_RETRIES', 3, { min: 0 }),
      retryBaseInterval: parseIntEnv('WORKER_WEBHOOK_RETRY_INTERVAL', 1000, { min: 0 }),
    },

    retry: {
      maxAttempts: parseIntEnv('WORKER_RETRY_MAX_ATTEMPTS', 3, { min: 0 }),
      baseInterval: parseIntEnv('WORKER_RETRY_BASE_INTERVAL', 5000, { min: 0 }),
      maxInterval: parseIntEnv('WORKER_RETRY_MAX_INTERVAL', 60000, { min: 0 }),
    },

    healthCheck: {
      interval: parseIntEnv('WORKER_HEALTH_CHECK_INTERVAL', 30000, { min: 1000 }),
      zombieCheckInterval: parseIntEnv('WORKER_ZOMBIE_CHECK_INTERVAL', 60000, { min: 1000 }),
    },

    shutdown: {
      graceMs: parseIntEnv('WORKER_SHUTDOWN_GRACE_MS', 60000, { min: 1000 }), // 默认 1 分钟
    },

    log: {
      level: parseEnumEnv<WorkerConfig['log']['level']>('WORKER_LOG_LEVEL', {
        defaultValue: 'info',
        allowed: ['debug', 'info', 'warn', 'error'],
      }),
    },
  }
}

// ===== 辅助函数 =====

function env(key: string, defaultValue: string): string {
  return process.env[key] ?? defaultValue
}

function requireEnv(key: string): string {
  const v = process.env[key]
  if (!v || !v.trim()) {
    throw new Error(`缺少必填配置：${key}`)
  }
  return v.trim()
}

function parseIntEnv(
  key: string,
  defaultValue: number,
  opts?: { min?: number; max?: number },
): number {
  const raw = process.env[key]
  if (raw === undefined || raw.trim() === '') return defaultValue

  const parsed = Number.parseInt(raw, 10)
  if (Number.isNaN(parsed)) {
    throw new Error(`配置 ${key} 不是合法整数：${raw}`)
  }
  if (opts?.min !== undefined && parsed < opts.min) {
    throw new Error(`配置 ${key} 不能小于 ${opts.min}：${parsed}`)
  }
  if (opts?.max !== undefined && parsed > opts.max) {
    throw new Error(`配置 ${key} 不能大于 ${opts.max}：${parsed}`)
  }
  return parsed
}

function parseFloatEnv(key: string, opts?: { min?: number; max?: number }): number {
  const raw = process.env[key]
  if (raw === undefined || raw.trim() === '') {
    throw new Error(`缺少必填数值配置：${key}`)
  }
  const parsed = Number.parseFloat(raw)
  if (Number.isNaN(parsed)) {
    throw new Error(`配置 ${key} 不是合法浮点数：${raw}`)
  }
  if (opts?.min !== undefined && parsed < opts.min) {
    throw new Error(`配置 ${key} 不能小于 ${opts.min}：${parsed}`)
  }
  if (opts?.max !== undefined && parsed > opts.max) {
    throw new Error(`配置 ${key} 不能大于 ${opts.max}：${parsed}`)
  }
  return parsed
}

function parseBoolEnv(key: string, defaultValue: boolean): boolean {
  const raw = process.env[key]
  if (raw === undefined || raw.trim() === '') return defaultValue
  const v = raw.trim().toLowerCase()
  if (v === 'true' || v === '1' || v === 'yes') return true
  if (v === 'false' || v === '0' || v === 'no') return false
  throw new Error(`配置 ${key} 不是合法布尔值（true/false/1/0/yes/no）：${raw}`)
}

function parseEnumEnv<T extends string>(key: string, params: { defaultValue: T; allowed: T[] }): T {
  const raw = process.env[key]
  const v = raw === undefined || raw.trim() === '' ? params.defaultValue : (raw.trim() as T)
  if (!params.allowed.includes(v)) {
    throw new Error(`配置 ${key} 非法：${raw}，允许值=${params.allowed.join(',')}`)
  }
  return v
}
