/**
 * Worker 配置管理
 * 
 * 阿里云 ECS 抢占式实例编排策略配置：
 * - 并发控制：通过 MAX_INSTANCES 限制同时运行的 ECS 实例数
 * - 抢占式实例：使用 SpotAsPriceGo 策略自动竞价，大幅降低成本
 * - 超时管理：任务超时自动释放实例
 * - 重试策略：实例被回收或失败时重新创建
 */

import { DEFAULT_POOL_PROFILE_TAG_KEY } from './worker-branding'

/** 解析自 WORKER_PROCESSING_IMAGE_MAP / WORKER_ECS_POOL_PROFILE_MAP 的 JSON 对象 */
export type ProcessingRouteMap = Record<string, string>

export interface WorkerConfig {
  // ===== 按任务路由处理镜像（operation / quality_preset）=====
  processing: {
    /** WORKER_PROCESSING_IMAGE，映射未命中时使用 */
    defaultImage: string
    /** 键：sortedOps@quality、sortedOps、@quality */
    imageMap: ProcessingRouteMap
    /** 键同上，值为池标签 `retinaclip:pool-profile`（可配置）的 Tag Value */
    poolProfileMap: ProcessingRouteMap
  }

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
    /**
     * 实例公网带宽 (Mbps)。RunInstances 新建实例时使用；0 表示创建时不配公网带宽。
     * 池机若此前为 0，可在 `WORKER_ECS_AUTO_ALLOCATE_PUBLIC_IP=true` 时由编排器尝试升带宽并分配公网 IP。
     */
    internetMaxBandwidthOut: number
    /**
     * 实例进入 Running 后是否要求具备公网出口（公网 IP 或已绑定 EIP），便于访问公网 RabbitMQ。
     * 若 MQ 在 VPC 内网，可设 `WORKER_ECS_REQUIRE_PUBLIC_IP=false`。
     */
    requirePublicIpForTasks: boolean
    /**
     * 在 requirePublicIpForTasks 为 true 且当前无公网 IP/EIP 时，是否调用阿里云 API 尝试修复：
     * 带宽为 0 时 `ModifyInstanceNetworkSpec`（按流量计费 + 分配公网 IP），否则先 `AllocatePublicIpAddress`。
     */
    autoAllocatePublicIp: boolean
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
    /**
     * 是否启用 ECS「池」：优先启动已存在且已停止的池实例；任务结束后仅 Stop，不释放。
     * 池内机器需打标签 `retinaclip:lifecycle=<poolLifecycleTagValue>`；不校验镜像/规格是否与 ALIYUN_* 一致。
     */
    poolEnabled: boolean
    /** 池实例标签 `retinaclip:lifecycle` 的值，默认 `pool` */
    poolLifecycleTagValue: string
    /**
     * 池实例可选第二维标签键，用于区分业务池（如字幕 / 放大）。
     * 值为 WORKER_ECS_POOL_PROFILE_MAP 解析结果。
     * 是否在挑选池实例时按此标签过滤见 poolProfileFilterEnabled。
     */
    poolProfileTagKey: string
    /**
     * 为 true 时 findIdlePoolInstance 会要求实例带 poolProfileTagKey=任务解析出的 profile。
     * 为 false 时仅按 lifecycle 池标签（适合全池共用、实例可不打 profile）。
     */
    poolProfileFilterEnabled: boolean
    /**
     * 在 `ALIYUN_ECS_INSTANCE_TYPE` 之后依次尝试的规格（逗号分隔），用于库存不足降级。
     */
    instanceTypeFallback: string[]
    /**
     * UserData 中 Docker：`auto` 缺失则尝试 dnf 安装；`require_host` 要求镜像预装 Docker，否则失败（推荐自定义镜像）。
     */
    userdataDockerPolicy: 'auto' | 'require_host'
    /**
     * 创建实例前调用 DescribeAvailableResource，将更有库存的规格排到前面（失败则忽略）。
     */
    prefilterAvailableResource: boolean
    /**
     * 合并进宿主机 `/etc/docker/daemon.json` 的 `insecure-registries`（逗号分隔）。
     * 与镜像名中自动识别的 `host:port` 合并；用于内网 HTTP Registry（如 172.16.0.70:5000）。
     */
    dockerInsecureRegistries: string[]
    /** 拉取前 `docker login` 的用户名（可选，私有仓库） */
    dockerRegistryUsername?: string
    /** 拉取前 `docker login` 的密码（可选） */
    dockerRegistryPassword?: string
    /** `docker login` 的 registry 地址，默认取镜像第一段 `host:port` */
    dockerRegistryServer?: string
    /**
     * NAS（NFS）挂载配置：用于宿主机启动时自动把 NAS 挂到本地目录。
     * 建议将挂载点域名填成 NAS 控制台给的“挂载点/挂载目标”域名或 IP（非 ECS 实例 ID）。
     */
    nasMountDomain: string
    /** NAS 导出路径，默认 `/` */
    nasExportPath: string
    /** 本地挂载目录，默认 `/mnt` */
    nasMountPoint: string
    /** NAS ID：仅用于日志定位 */
    nasId: string
    /**
     * 池机 `docker run` 网络模式：`host`（默认，与历史行为一致）或 `bridge`（可配合端口映射，贴近手动 `-p` 示例）。
     */
    dockerNetworkMode: 'host' | 'bridge'
    /**
     * 仅 `dockerNetworkMode=bridge` 时写入 `-p`；每项形如 `8080:8080`（环境变量逗号分隔）。
     */
    dockerPublishPorts: string[]
    /**
     * 宿主机待挂载目录（如 NAS 下的 DiffuEraser）；空字符串表示不加 `-v`。
     */
    dockerVolumeHost: string
    /** 与 dockerVolumeHost 对应的容器内路径，默认 `/DiffuEraser` */
    dockerVolumeContainer: string
    /**
     * 非空时在镜像名后追加 `/bin/bash -c '<本字段>'`（与手动 `docker run ... /bin/bash -c "./deploy.sh"` 一致）；
     * 空则使用镜像默认 ENTRYPOINT/CMD。
     */
    dockerBashCommand?: string
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

  // ===== 池模式调度（只观测队列深度，不消费消息）=====
  scheduler: {
    /** 轮询 RabbitMQ 队列深度间隔 (ms)，仅 passive checkQueue */
    pollIntervalMs: number
    /**
     * 与 WORKER_ECS_POOL_PROFILE_FILTER_ENABLED 配合：只统计/启动带该 pool profile 标签的池机。
     * 未设置时行为与编排器 findIdlePoolInstance(poolProfile 为空) 一致。
     */
    poolProfile?: string
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
  const instanceNamePrefix = env('ALIYUN_ECS_INSTANCE_NAME_PREFIX', 'retinaclip-worker')

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

  const defaultProcessingImage = env('WORKER_PROCESSING_IMAGE', 'retinaclip-processor:latest')
  const processingImageMap = parseJsonObjectEnv('WORKER_PROCESSING_IMAGE_MAP', {})
  const poolProfileMap = parseJsonObjectEnv('WORKER_ECS_POOL_PROFILE_MAP', {})
  const instanceTypeFallback = env('ALIYUN_ECS_INSTANCE_TYPE_FALLBACK', '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)

  const dockerInsecureRegistries = env('WORKER_DOCKER_INSECURE_REGISTRIES', '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  const dockerRegistryUsername = env('WORKER_DOCKER_REGISTRY_USERNAME', '').trim() || undefined
  const dockerRegistryPassword = env('WORKER_DOCKER_REGISTRY_PASSWORD', '').trim() || undefined
  const dockerRegistryServer = env('WORKER_DOCKER_REGISTRY_SERVER', '').trim() || undefined

  // ===== NAS（NFS）挂载配置 =====
  const nasMountDomain = env('WORKER_NAS_MOUNT_DOMAIN', '').trim()
  const nasExportPath = env('WORKER_NAS_EXPORT_PATH', '/').trim() || '/'
  const nasMountPoint = env('WORKER_NAS_MOUNT_POINT', '/mnt').trim() || '/mnt'
  const nasId = env('WORKER_NAS_ID', '3e41f4bcd1').trim() || '3e41f4bcd1'

  const dockerNetworkMode = parseEnumEnv<'host' | 'bridge'>('WORKER_DOCKER_NETWORK_MODE', {
    defaultValue: 'host',
    allowed: ['host', 'bridge'],
  })
  const dockerPublishPorts = env('WORKER_DOCKER_PUBLISH_PORTS', '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  const dockerVolumeHost = env('WORKER_DOCKER_VOLUME_HOST', '').trim()
  const dockerVolumeContainer = env('WORKER_DOCKER_VOLUME_CONTAINER', '/DiffuEraser').trim() || '/DiffuEraser'
  const dockerBashCommandRaw = env('WORKER_DOCKER_BASH_COMMAND', '').trim()
  const dockerBashCommand = dockerBashCommandRaw || undefined

  return {
    processing: {
      defaultImage: defaultProcessingImage,
      imageMap: processingImageMap,
      poolProfileMap,
    },

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
      requirePublicIpForTasks: parseBoolEnv('WORKER_ECS_REQUIRE_PUBLIC_IP', true),
      autoAllocatePublicIp: parseBoolEnv('WORKER_ECS_AUTO_ALLOCATE_PUBLIC_IP', true),
      taskTimeout: parseIntEnv('WORKER_TASK_TIMEOUT', 30 * 60 * 1000, { min: 1000 }), // 30 分钟
      instanceStartTimeout: parseIntEnv('WORKER_INSTANCE_START_TIMEOUT', 5 * 60 * 1000, { min: 1000 }), // 5 分钟
      pollInterval: parseIntEnv('WORKER_POLL_INTERVAL', 10000, { min: 1000 }), // 10 秒
      keyPairName: env('ALIYUN_ECS_KEY_PAIR_NAME', '') || undefined,
      ramRoleName: env('ALIYUN_ECS_RAM_ROLE_NAME', '') || undefined,
      mockProcessing: parseBoolEnv('WORKER_MOCK_PROCESSING', false),
      mockDelaySeconds: parseIntEnv('WORKER_MOCK_DELAY_SECONDS', 60, { min: 1 }),
      poolEnabled: parseBoolEnv('WORKER_ECS_POOL_ENABLED', false),
      poolLifecycleTagValue: env('WORKER_ECS_POOL_TAG_VALUE', 'pool').trim() || 'pool',
      poolProfileTagKey:
        env('WORKER_ECS_POOL_PROFILE_TAG_KEY', DEFAULT_POOL_PROFILE_TAG_KEY).trim() ||
        DEFAULT_POOL_PROFILE_TAG_KEY,
      poolProfileFilterEnabled: parseBoolEnv('WORKER_ECS_POOL_PROFILE_FILTER_ENABLED', false),
      instanceTypeFallback,
      userdataDockerPolicy: parseEnumEnv<'auto' | 'require_host'>('WORKER_ECS_USERDATA_DOCKER_POLICY', {
        defaultValue: 'auto',
        allowed: ['auto', 'require_host'],
      }),
      prefilterAvailableResource: parseBoolEnv('WORKER_ECS_PREFILTER_AVAILABLE_RESOURCE', false),
      dockerInsecureRegistries,
      dockerRegistryUsername,
      dockerRegistryPassword,
      dockerRegistryServer,
      nasMountDomain,
      nasExportPath,
      nasMountPoint,
      nasId,
      dockerNetworkMode,
      dockerPublishPorts,
      dockerVolumeHost,
      dockerVolumeContainer,
      dockerBashCommand,
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

    scheduler: {
      pollIntervalMs: parseIntEnv('WORKER_SCHEDULER_POLL_INTERVAL_MS', 15000, { min: 1000 }),
      poolProfile: env('WORKER_SCHEDULER_POOL_PROFILE', '').trim() || undefined,
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

/** 解析 JSON 对象环境变量；非法或空则返回 defaultObj */
function parseJsonObjectEnv(key: string, defaultObj: ProcessingRouteMap): ProcessingRouteMap {
  const raw = process.env[key]
  if (raw === undefined || !raw.trim()) return { ...defaultObj }
  try {
    const v = JSON.parse(raw) as unknown
    if (v === null || typeof v !== 'object' || Array.isArray(v)) {
      throw new Error('不是 JSON 对象')
    }
    const out: ProcessingRouteMap = {}
    for (const [k, val] of Object.entries(v)) {
      if (typeof val === 'string' && val.trim()) out[k] = val.trim()
    }
    return out
  } catch (e) {
    throw new Error(`配置 ${key} 必须是 JSON 对象（字符串键到非空字符串值）：${e instanceof Error ? e.message : e}`)
  }
}
