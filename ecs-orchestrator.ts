/**
 * 阿里云 ECS 实例编排器
 * 
 * ╔══════════════════════════════════════════════════════════════╗
 * ║                ECS 实例生命周期管理策略                       ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║                                                              ║
 * ║  1. 池 + 按需扩容                                            ║
 * ║     - 可选：优先复用带 retinaclip:lifecycle=pool 的已停止实例      ║
 * ║       任务结束仅 Stop，保留在池                               ║
 * ║     - 池无可用时 RunInstances 新建（标签 ephemeral）          ║
 * ║       任务结束 DeleteInstance 释放                            ║
 * ║     - 并发上限：MAX_INSTANCES                                  ║
 * ║                                                              ║
 * ║  2. 抢占式实例 (Spot Instance)                                ║
 * ║     - 使用 SpotAsPriceGo 自动竞价，按市场价付费               ║
 * ║     - 成本约为按量付费的 10%~30%                              ║
 * ║     - 实例可能被回收，Worker 自动重试                          ║
 * ║                                                              ║
 * ║  3. 自动化流程                                                ║
 * ║     - Cloud-Init 脚本自动拉取镜像 + 运行容器                  ║
 * ║     - 通过 RunCommand (云助手) 查询任务完成状态                ║
 * ║     - Webhook 回调实时汇报进度                                 ║
 * ║                                                              ║
 * ║  4. 安全清理                                                  ║
 * ║     - 超时实例自动释放                                        ║
 * ║     - 僵尸实例定期扫描清理                                    ║
 * ║     - 优雅退出时释放所有实例                                  ║
 * ║                                                              ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

import ECS20140526, * as $ECS from '@alicloud/ecs20140526'
import OpenApi, * as $OpenApi from '@alicloud/openapi-client'
import * as $Util from '@alicloud/tea-util'
import { WorkerConfig } from './config'
import { generateTaskRunnerShellScript, generateUserData } from './cloud-init'
import type { TaskParams } from './domain/task'
import { createLogger } from './logger'
import { resolveTaskRouting } from './task-routing'
import { WORKER_ECS_TAGS, WORKER_HOST_PATHS } from './worker-branding'

const log = createLogger('ECS')

/** 库存或可用区不支持时换规格 / 抢占式改按量 */
function isStockOrZoneInstanceError(code: string): boolean {
  return (
    code === 'OperationDenied.NoStock' ||
    code === 'Zone.NotOnSale' ||
    code === 'InvalidInstanceType.ZoneNotSupported'
  )
}

/** ECS 实例运行上下文 */
export interface InstanceContext {
  /** 实例 ID */
  instanceId: string
  /** 任务消息 ID */
  messageId: string
  /** 创建时间戳 */
  createdAt: number
  /** 任务结束后：池内实例仅停机；临时实例释放删除 */
  disposeMode: 'stop' | 'delete'
  /** 超时定时器 */
  timeoutTimer?: NodeJS.Timeout
  /** 轮询定时器 */
  pollTimer?: NodeJS.Timeout
}

/** 任务运行结果 */
export interface TaskResult {
  success: boolean
  /** 输出视频 URL */
  outputVideoUrl?: string
  /** 处理耗时（秒） */
  durationSeconds: number
  /** 错误信息 */
  error?: string
  /** 实例 ID */
  instanceId: string
}

/**
 * 阿里云 ECS 实例编排器
 */
export class ECSOrchestrator {
  // @alicloud/ecs20140526 在 ESM 下默认导入并不等价于构造函数本体，
  // 实际构造函数在 default 属性上。
  private client: any
  private config: WorkerConfig
  /** 当前运行中的实例 Map<instanceId, InstanceContext> */
  private runningInstances: Map<string, InstanceContext> = new Map()
  /** 当前占用的槽位数 */
  private activeSlots: number = 0
  /** 是否正在关闭 */
  private shuttingDown: boolean = false

  constructor(config: WorkerConfig) {
    this.config = config

    // 初始化阿里云 ECS 客户端
    const openApiConfig = new $OpenApi.Config({
      accessKeyId: config.ecs.accessKeyId,
      accessKeySecret: config.ecs.accessKeySecret,
      regionId: config.ecs.regionId,
      endpoint: `ecs.${config.ecs.regionId}.aliyuncs.com`,
    })

    this.client = new (ECS20140526 as any).default(openApiConfig)
  }

  /**
   * 初始化：验证配置 + 清理残留实例
   */
  async initialize(): Promise<void> {
    log.info('初始化 ECS 编排器...')

    // 1. 验证 API 连通性
    try {
      const request = new $ECS.DescribeRegionsRequest({})
      const runtime = new $Util.RuntimeOptions({})
      await this.client.describeRegionsWithOptions(request, runtime)
      log.info('阿里云 ECS API 连接成功', { region: this.config.ecs.regionId })
    } catch (error: any) {
      throw new Error(`阿里云 ECS API 连接失败: ${error.message || error}`)
    }

    // 2. 验证镜像存在
    try {
      const request = new $ECS.DescribeImagesRequest({
        regionId: this.config.ecs.regionId,
        imageId: this.config.ecs.imageId,
      })
      const runtime = new $Util.RuntimeOptions({})
      const response = await this.client.describeImagesWithOptions(request, runtime)
      const images = response.body?.images?.image || []
      if (images.length === 0) {
        throw new Error(`镜像 ${this.config.ecs.imageId} 不存在`)
      }
      log.info('镜像验证通过', {
        imageId: this.config.ecs.imageId,
        imageName: images[0].imageName,
        platform: images[0].platform,
      })
    } catch (error: any) {
      if (error.message?.includes('不存在')) throw error
      log.warn('镜像验证失败（可能权限不足，继续运行）', { error: error.message })
    }

    // 3. 清理残留的 worker 实例
    await this.cleanupStaleInstances()

    log.info('ECS 编排器初始化完成')
  }

  /**
   * 检查是否有可用的实例槽位
   */
  hasAvailableSlot(): boolean {
    return !this.shuttingDown && this.activeSlots < this.config.ecs.maxInstances
  }

  /**
   * 获取当前运行状态
   */
  getStatus() {
    return {
      activeSlots: this.activeSlots,
      maxSlots: this.config.ecs.maxInstances,
      runningInstances: this.runningInstances.size,
      shuttingDown: this.shuttingDown,
    }
  }

  /**
   * 核心方法：为一个任务分配 ECS 并等待完成
   *
   * 流程：
   *   获取槽位 →（可选）池内已停止实例 Start + 云助手跑任务脚本
   *   或 RunInstances + UserData → 等待任务完成 → 池：Stop / 临时：Delete → 释放槽位
   */
  async runTask(
    taskParams: TaskParams,
    processingImage: string,
    attempt: number = 0,
  ): Promise<TaskResult> {
    if (this.shuttingDown) {
      throw new Error('Worker 正在关闭，拒绝新任务')
    }

    // 1. 获取槽位
    this.activeSlots++
    log.info(`获取实例槽位 [${this.activeSlots}/${this.config.ecs.maxInstances}]`, {
      messageId: taskParams.messageId,
      attempt,
    })

    const startTime = Date.now()
    let instanceId: string | null = null
    let disposeMode: 'stop' | 'delete' = 'delete'

    try {
      // 任务脚本里 GPU 等参数按配置主规格生成；池实例可与配置镜像/规格不同，需自行保证能跑任务镜像
      const taskScript = generateTaskRunnerShellScript(
        taskParams,
        this.config,
        processingImage,
        this.config.ecs.instanceType,
      )

      // 2. 优先复用池内已停止实例（任务结束只 Stop，不释放）
      if (this.config.ecs.poolEnabled) {
        const poolId = await this.findIdlePoolInstance(taskParams.poolProfile)
        if (poolId) {
          instanceId = poolId
          disposeMode = 'stop'
          log.info('复用池内 ECS（Stopped → Running）', {
            instanceId,
            messageId: taskParams.messageId,
            tag: `${WORKER_ECS_TAGS.lifecycle}=${this.config.ecs.poolLifecycleTagValue}`,
            poolProfile: taskParams.poolProfile,
          })

          this.runningInstances.set(instanceId, {
            instanceId,
            messageId: taskParams.messageId,
            createdAt: startTime,
            disposeMode: 'stop',
          })

          await this.startInstance(instanceId)
          await this.waitForInstanceRunning(instanceId, taskParams.messageId, attempt)
          await this.ensureInstancePublicEgressForMq(instanceId, taskParams.messageId, attempt)

          // 云助手就绪需要短暂时间
          await this.sleep(20000)
          await this.runRemoteCommand(
            instanceId,
            `rm -f ${WORKER_HOST_PATHS.taskDone} ${WORKER_HOST_PATHS.taskResult}`,
          )

          await this.runRemoteLongRunningScript(instanceId, taskScript, taskParams.messageId, attempt)

          const result = await this.readTaskResultWithRetries(instanceId, taskParams.messageId, attempt)
          const durationSeconds = (Date.now() - startTime) / 1000
          return {
            ...result,
            durationSeconds,
            instanceId,
          }
        }
      }

      // 3. 池无可用或关闭池：新建临时实例（任务结束 Delete）
      instanceId = await this.createInstance(taskParams, processingImage, attempt)
      disposeMode = 'delete'

      this.runningInstances.set(instanceId, {
        instanceId,
        messageId: taskParams.messageId,
        createdAt: startTime,
        disposeMode: 'delete',
      })

      await this.waitForInstanceRunning(instanceId, taskParams.messageId, attempt)
      await this.ensureInstancePublicEgressForMq(instanceId, taskParams.messageId, attempt)
      const result = await this.waitForTaskCompletion(instanceId, taskParams.messageId, attempt)
      const durationSeconds = (Date.now() - startTime) / 1000

      return {
        ...result,
        durationSeconds,
        instanceId,
      }
    } catch (error) {
      const durationSeconds = (Date.now() - startTime) / 1000

      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        durationSeconds,
        instanceId: instanceId || 'N/A',
      }
    } finally {
      if (instanceId) {
        await this.disposeAfterTask(instanceId, taskParams.messageId, disposeMode, attempt)
        this.runningInstances.delete(instanceId)
      }
      this.activeSlots--
      log.info(`释放实例槽位 [${this.activeSlots}/${this.config.ecs.maxInstances}]`, {
        messageId: taskParams.messageId,
        attempt,
      })
    }
  }

  /**
   * 创建 ECS 实例：主规格 + ALIYUN_ECS_INSTANCE_TYPE_FALLBACK 降级；
   * 抢占式下同一规格先 Spot 再按量；库存/可用区类错误尝试下一规格。
   */
  private async createInstance(
    taskParams: TaskParams,
    processingImage: string,
    attempt: number,
  ): Promise<string> {
    const messageId = taskParams.messageId
    const instanceName = `${this.config.ecs.instanceNamePrefix}-${messageId.substring(0, 8)}`
    const runtime = new $Util.RuntimeOptions({})

    const primary = this.config.ecs.instanceType
    const fallbacks = this.config.ecs.instanceTypeFallback || []
    const candidateTypes = [...new Set([primary, ...fallbacks].filter(Boolean))]

    let orderedTypes = candidateTypes
    if (this.config.ecs.prefilterAvailableResource && candidateTypes.length > 1) {
      try {
        orderedTypes = await this.rankInstanceTypesByAvailability(candidateTypes)
        log.info('DescribeAvailableResource 已调整规格尝试顺序', { orderedTypes })
      } catch (e: any) {
        log.warn('DescribeAvailableResource 预排序失败，使用配置顺序', { error: e?.message || String(e) })
      }
    }

    const buildTags = () => {
      const tags = [
        new $ECS.RunInstancesRequestTag({ key: WORKER_ECS_TAGS.role, value: 'worker' }),
        new $ECS.RunInstancesRequestTag({ key: WORKER_ECS_TAGS.lifecycle, value: 'ephemeral' }),
        new $ECS.RunInstancesRequestTag({ key: WORKER_ECS_TAGS.messageId, value: messageId }),
        new $ECS.RunInstancesRequestTag({ key: WORKER_ECS_TAGS.createdAt, value: new Date().toISOString() }),
      ]
      if (taskParams.poolProfile?.trim()) {
        tags.push(
          new $ECS.RunInstancesRequestTag({
            key: this.config.ecs.poolProfileTagKey,
            value: taskParams.poolProfile.trim(),
          }),
        )
      }
      return tags
    }

    const runOnce = async (instanceType: string, useSpot: boolean): Promise<string> => {
      const userData = generateUserData(taskParams, this.config, processingImage, instanceType)
      const request = new $ECS.RunInstancesRequest({
        regionId: this.config.ecs.regionId,
        imageId: this.config.ecs.imageId,
        instanceType,
        securityGroupId: this.config.ecs.securityGroupId,
        vSwitchId: this.config.ecs.vswitchId,
        instanceName,
        hostName: instanceName,
        systemDiskSize: String(this.config.ecs.systemDiskSize),
        systemDiskCategory: this.config.ecs.systemDiskCategory,
        internetMaxBandwidthOut: this.config.ecs.internetMaxBandwidthOut,
        internetChargeType: 'PayByTraffic',
        instanceChargeType: 'PostPaid',
        userData,
        ...(this.config.ecs.zoneId && { zoneId: this.config.ecs.zoneId }),
        ...(this.config.ecs.keyPairName && { keyPairName: this.config.ecs.keyPairName }),
        ...(this.config.ecs.ramRoleName && { ramRoleName: this.config.ecs.ramRoleName }),
        amount: 1,
        tag: buildTags(),
        ...(useSpot && {
          spotStrategy: this.config.ecs.spotStrategy,
          ...(this.config.ecs.spotPriceLimit && {
            spotPriceLimit: this.config.ecs.spotPriceLimit,
          }),
          spotInterruptionBehavior: 'Terminate',
        }),
      })

      const response = await this.client.runInstancesWithOptions(request, runtime)
      const instanceIds = response.body?.instanceIdSets?.instanceIdSet || []
      if (instanceIds.length === 0) {
        throw new Error('创建实例成功但未返回实例 ID')
      }
      const instanceId = instanceIds[0]
      log.info('ECS 实例创建成功', {
        instanceId,
        messageId,
        attempt,
        instanceType,
        spot: useSpot,
      })
      return instanceId
    }

    let lastError = ''

    for (const instanceType of orderedTypes) {
      log.info('正在创建 ECS 实例...', {
        messageId,
        attempt,
        instanceType,
        spot: this.config.ecs.useSpotInstance,
      })

      const trySpotThenOndemand = async (): Promise<string | null> => {
        if (this.config.ecs.useSpotInstance) {
          try {
            return await runOnce(instanceType, true)
          } catch (error: any) {
            const code = error.code || error.Code || ''
            const msg = error.message || error.Message || String(error)
            if (code === 'InvalidSpotPriceLimit.LowerThanPublicPrice') {
              throw new Error('抢占式实例出价低于市场价')
            }
            if (!isStockOrZoneInstanceError(code)) {
              throw new Error(`创建 ECS 实例失败: [${code}] ${msg}`)
            }
            log.warn('抢占式创建失败，尝试同规格按量', { instanceType, code, msg })
            try {
              return await runOnce(instanceType, false)
            } catch (error2: any) {
              const c2 = error2.code || error2.Code || ''
              const m2 = error2.message || error2.Message || String(error2)
              lastError = `[${c2}] ${m2}`
              if (isStockOrZoneInstanceError(c2)) return null
              if (c2 === 'InvalidSpotPriceLimit.LowerThanPublicPrice') {
                throw new Error('抢占式实例出价低于市场价')
              }
              throw new Error(`创建 ECS 实例失败: [${c2}] ${m2}`)
            }
          }
        } else {
          try {
            return await runOnce(instanceType, false)
          } catch (error: any) {
            const code = error.code || error.Code || ''
            const msg = error.message || error.Message || String(error)
            lastError = `[${code}] ${msg}`
            if (isStockOrZoneInstanceError(code)) return null
            throw new Error(`创建 ECS 实例失败: [${code}] ${msg}`)
          }
        }
      }

      const id = await trySpotThenOndemand()
      if (id) return id
      log.warn('该规格无库存或不可用，尝试下一备选规格', { instanceType, lastError })
    }

    throw new Error(
      `所有备选规格均创建失败（已试: ${orderedTypes.join(', ')}）。最后错误: ${lastError || 'unknown'}`,
    )
  }

  /**
   * 按 DescribeAvailableResource 推断的库存优先级排序（分高在前）。
   */
  private async rankInstanceTypesByAvailability(types: string[]): Promise<string[]> {
    const runtime = new $Util.RuntimeOptions({})
    const scored: { t: string; score: number }[] = []

    for (const t of types) {
      let score = 0
      try {
        const req = new $ECS.DescribeAvailableResourceRequest({
          regionId: this.config.ecs.regionId,
          destinationResource: 'InstanceType',
          instanceType: t,
          systemDiskCategory: this.config.ecs.systemDiskCategory,
          ...(this.config.ecs.zoneId && { zoneId: this.config.ecs.zoneId }),
        })
        const res = await this.client.describeAvailableResourceWithOptions(req, runtime)
        score = this.scoreInstanceTypeAvailability(res, t)
      } catch {
        score = 0
      }
      scored.push({ t, score })
    }

    scored.sort((a, b) => b.score - a.score)
    return scored.map((s) => s.t)
  }

  /** 解析 DescribeAvailableResource 响应，返回 0–3 的库存优先级分 */
  private scoreInstanceTypeAvailability(res: any, instanceType: string): number {
    const zones = res.body?.availableZones?.availableZone || []
    let best = 0
    for (const z of zones) {
      if (this.config.ecs.zoneId && z.zoneId && z.zoneId !== this.config.ecs.zoneId) continue
      const ars = z.availableResources?.availableResource || []
      for (const ar of ars) {
        if (ar.type !== 'InstanceType') continue
        const srs = ar.supportedResources?.supportedResource || []
        for (const sr of srs) {
          if (sr.value !== instanceType) continue
          const cat = sr.statusCategory || ''
          const st = sr.status || ''
          if (cat === 'WithStock' || st === 'Available') best = Math.max(best, 3)
          else if (cat === 'ClosedWithStock') best = Math.max(best, 2)
          else if (cat === 'WithoutStock') best = Math.max(best, 1)
        }
      }
    }
    return best
  }

  /**
   * 启动 ECS 实例
   */
  private async startInstance(instanceId: string): Promise<void> {
    log.info('正在启动 ECS 实例...', { instanceId })

    const request = new $ECS.StartInstanceRequest({ instanceId })
    const runtime = new $Util.RuntimeOptions({})

    try {
      await this.client.startInstanceWithOptions(request, runtime)
      log.info('实例启动命令已发送', { instanceId })
    } catch (error: any) {
      // 如果实例已经在 Running 状态，忽略错误
      if (error.code === 'IncorrectInstanceStatus' && error.message?.includes('Running')) {
        log.info('实例已在运行中', { instanceId })
        return
      }
      throw new Error(`启动实例失败: ${error.message || error}`)
    }
  }

  /**
   * DescribeInstances 用的池标签条件（与 findIdlePoolInstance / 池调度一致）。
   */
  private poolDescribeTags(poolProfile?: string): $ECS.DescribeInstancesRequestTag[] {
    const tags = [
      new $ECS.DescribeInstancesRequestTag({
        key: WORKER_ECS_TAGS.lifecycle,
        value: this.config.ecs.poolLifecycleTagValue,
      }),
    ]
    if (this.config.ecs.poolProfileFilterEnabled && poolProfile?.trim()) {
      tags.push(
        new $ECS.DescribeInstancesRequestTag({
          key: this.config.ecs.poolProfileTagKey,
          value: poolProfile.trim(),
        }),
      )
    }
    return tags
  }

  /**
   * 列出符合池标签的实例 ID（分页）。
   */
  private async listAllPoolInstanceIds(
    status: 'Running' | 'Stopped',
    poolProfile?: string,
  ): Promise<string[]> {
    const tags = this.poolDescribeTags(poolProfile)
    const runtime = new $Util.RuntimeOptions({})
    const ids: string[] = []
    let page = 1
    for (;;) {
      const request = new $ECS.DescribeInstancesRequest({
        regionId: this.config.ecs.regionId,
        status,
        pageNumber: page,
        pageSize: 50,
        tag: tags,
      })
      const response = await this.client.describeInstancesWithOptions(request, runtime)
      const batch = response.body?.instances?.instance || []
      for (const i of batch) {
        if (i.instanceId) ids.push(i.instanceId)
      }
      const total = response.body?.totalCount ?? 0
      if (ids.length >= total || batch.length === 0) break
      page++
      if (page > 100) break
    }
    return ids
  }

  /**
   * 池模式：根据队列中「待消费」消息数，将 Stopped 池机启动为 Running，
   * 并默认走与 runTask 池路径相同的云助手脚本（NAS、docker pull、docker run）。
   */
  async scalePoolToQueueDepth(messageCount: number, poolProfile?: string): Promise<void> {
    if (!this.config.ecs.poolEnabled || this.shuttingDown) return

    const prof = poolProfile?.trim() || undefined
    const runningIds = await this.listAllPoolInstanceIds('Running', prof)
    const running = runningIds.length
    const target = Math.min(this.config.ecs.maxInstances, Math.max(0, messageCount))
    const need = target - running
    if (need <= 0) {
      log.debug('池机数量已满足当前队列深度策略', {
        messageCount,
        running,
        target,
        poolProfile: prof ?? '(none)',
      })
      return
    }

    const stoppedIds = await this.listAllPoolInstanceIds('Stopped', prof)
    const toStart = Math.min(need, stoppedIds.length)
    if (toStart <= 0) {
      log.warn('队列需要更多池机但无 Stopped 实例可启', {
        need,
        stoppedAvailable: stoppedIds.length,
        messageCount,
        running,
        poolProfile: prof ?? '(none)',
      })
      return
    }

    log.info('根据队列深度启动池机（ECS Start + 公网 + 云助手：默认同 runTask 池脚本）', {
      messageCount,
      running,
      target,
      starting: toStart,
      poolProfile: prof ?? '(none)',
    })

    for (let i = 0; i < toStart; i++) {
      const instanceId = stoppedIds[i]!
      try {
        await this.startInstance(instanceId)
        await this.waitForInstanceRunning(instanceId, 'pool-scheduler', 0)
        await this.ensureInstancePublicEgressForMq(instanceId, 'pool-scheduler', 0)
        await this.runPoolBootHook(instanceId, prof)
      } catch (error) {
        log.error('启动池机失败', error, { instanceId, index: i })
      }
    }
  }

  /**
   * 队列空闲时按需收缩池机：仅 Stop，不删除。
   * @returns 实际停止的实例数
   */
  async scaleDownPoolRunningInstances(
    poolProfile?: string,
    minRunningInstances: number = 0,
  ): Promise<number> {
    if (!this.config.ecs.poolEnabled || this.shuttingDown) return 0

    const prof = poolProfile?.trim() || undefined
    const keep = Math.max(0, minRunningInstances)
    const runningIds = await this.listAllPoolInstanceIds('Running', prof)
    if (runningIds.length <= keep) {
      return 0
    }

    let stopped = 0
    const candidates = runningIds.slice(keep)
    for (const instanceId of candidates) {
      // 避免影响 runTask 正在使用的实例
      if (this.runningInstances.has(instanceId)) continue
      try {
        await this.stopInstanceAndWait(instanceId, 'pool-scheduler-scale-down')
        stopped++
      } catch (error) {
        log.warn('池机空闲收缩失败', {
          instanceId,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }
    return stopped
  }

  /**
   * 池机开机后：默认同 runTask 池路径的完整宿主机脚本；可被 WORKER_ECS_POOL_BOOT_COMMAND /
   * WORKER_ECS_POOL_DOCKER_CONTAINER 覆盖。
   */
  private async runPoolBootHook(instanceId: string, poolProfileHint?: string): Promise<void> {
    if (this.config.ecs.mockProcessing) {
      log.warn('池机云助手跳过：WORKER_MOCK_PROCESSING=true', { instanceId })
      return
    }

    const ecs = this.config.ecs

    if (ecs.poolBootCommand?.trim()) {
      const script = ecs.poolBootCommand.trim()
      if (ecs.poolBootDelayMs > 0) {
        log.info('等待池机就绪后执行自定义 boot 云助手', { instanceId, delayMs: ecs.poolBootDelayMs })
        await this.sleep(ecs.poolBootDelayMs)
      }
      log.info('执行 WORKER_ECS_POOL_BOOT_COMMAND', { instanceId })
      const out = await this.runRemoteCommand(instanceId, script, { timeoutSec: 180 })
      if (out === null) {
        log.warn('自定义池机 boot 云助手未返回或失败', { instanceId })
      } else {
        log.info('自定义池机 boot 完成', { instanceId, outputTail: out.trim().slice(-800) })
      }
      return
    }

    if (ecs.poolDockerContainer?.trim()) {
      const c = ecs.poolDockerContainer.trim().replace(/'/g, "'\\''")
      const script = `set -e
for i in $(seq 1 45); do docker info >/dev/null 2>&1 && break; sleep 2; done
CONTAINER='${c}'
if ! docker ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER"; then
  echo "POOL_BOOT: container not found: $CONTAINER" >&2
  exit 1
fi
exec docker start "$CONTAINER"`
      if (ecs.poolBootDelayMs > 0) {
        await this.sleep(ecs.poolBootDelayMs)
      }
      log.info('执行 docker start（WORKER_ECS_POOL_DOCKER_CONTAINER）', { instanceId, container: ecs.poolDockerContainer })
      const out = await this.runRemoteCommand(instanceId, script, { timeoutSec: 180 })
      if (out === null) {
        log.warn('docker start 云助手失败', { instanceId })
      }
      return
    }

    await this.runPoolBootstrapSameAsRunTask(instanceId, poolProfileHint)
  }

  /**
   * 与 runTask 复用池机时一致：清结果文件 → 云助手跑 generateTaskRunnerShellScript 全文。
   */
  private async runPoolBootstrapSameAsRunTask(instanceId: string, poolProfileHint?: string): Promise<void> {
    const ecs = this.config.ecs
    const routing = resolveTaskRouting({}, this.config)
    let poolProfile = routing.poolProfile
    if (ecs.poolProfileFilterEnabled && poolProfileHint?.trim()) {
      poolProfile = poolProfileHint.trim()
    } else if (this.config.scheduler.poolProfile?.trim()) {
      poolProfile = this.config.scheduler.poolProfile.trim()
    }

    const taskParams: TaskParams = {
      messageId: ecs.poolBootstrapMessageId,
      videoDownloadUrl: ecs.poolBootstrapVideoUrl,
      webhookUrl: ecs.poolBootstrapWebhookUrl,
      detectType: 'auto',
      ...(poolProfile ? { poolProfile } : {}),
    }

    const processingImage = ecs.poolBootstrapProcessingImage?.trim() || routing.processingImage
    const detached = ecs.poolBootstrapDockerDetached

    const taskScript = generateTaskRunnerShellScript(
      taskParams,
      this.config,
      processingImage,
      this.config.ecs.instanceType,
      { dockerDetached: detached },
    )

    log.info('池机云助手：执行与 runTask 池路径同源的宿主机脚本', {
      instanceId,
      messageId: taskParams.messageId,
      processingImage,
      dockerDetached: detached,
      resolvedFrom: routing.resolvedFrom,
    })

    if (ecs.poolBootDelayMs > 0) {
      await this.sleep(ecs.poolBootDelayMs)
    }

    await this.runRemoteCommand(
      instanceId,
      `rm -f ${WORKER_HOST_PATHS.taskDone} ${WORKER_HOST_PATHS.taskResult}`,
    )

    await this.runRemoteLongRunningScript(instanceId, taskScript, taskParams.messageId, 0)

    const result = await this.readTaskResultWithRetries(instanceId, taskParams.messageId, 0)
    if (!result.success) {
      log.error('池机 bootstrap 脚本未成功', undefined, { instanceId, error: result.error })
    } else {
      log.info('池机 bootstrap 脚本成功', { instanceId })
    }
  }

  /**
   * 查找池内空闲实例：已停止 + retinaclip:lifecycle 池标签（不校验镜像/规格是否与配置一致）。
   * 若 poolProfileFilterEnabled 且任务带 poolProfile，再要求 poolProfileTagKey 匹配。
   */
  private async findIdlePoolInstance(poolProfile?: string): Promise<string | null> {
    try {
      const request = new $ECS.DescribeInstancesRequest({
        regionId: this.config.ecs.regionId,
        status: 'Stopped',
        pageSize: 50,
        tag: this.poolDescribeTags(poolProfile),
      })
      const runtime = new $Util.RuntimeOptions({})
      const response = await this.client.describeInstancesWithOptions(request, runtime)
      const instances = response.body?.instances?.instance || []
      const first = instances.find((i: any) => i.instanceId)
      return first?.instanceId ?? null
    } catch (error: any) {
      log.warn('查询池内实例失败', { error: error.message || String(error) })
      return null
    }
  }

  /**
   * 池内实例任务结束后停机（保留实例与系统盘，供下次 Start）
   */
  private async stopInstanceAndWait(instanceId: string, messageId: string): Promise<void> {
    log.info('池内实例任务结束，执行 Stop（不释放）', { instanceId, messageId })

    const request = new $ECS.StopInstanceRequest({
      instanceId,
      forceStop: false,
    })
    const runtime = new $Util.RuntimeOptions({})

    try {
      await this.client.stopInstanceWithOptions(request, runtime)
    } catch (error: any) {
      if (error.code === 'IncorrectInstanceStatus' && error.message?.includes('Stopped')) {
        log.info('实例已处于 Stopped', { instanceId })
        return
      }
      log.error('停止实例失败', error, { instanceId, messageId })
      return
    }

    const deadline = Date.now() + 180000
    while (Date.now() < deadline) {
      const status = await this.getInstanceStatus(instanceId)
      if (status === 'Stopped') {
        log.info('实例已停止', { instanceId })
        return
      }
      await this.sleep(3000)
    }
    log.warn('等待实例停止超时', { instanceId })
  }

  /**
   * 任务结束后：池 → Stop；临时扩容 → Delete
   */
  private async disposeAfterTask(
    instanceId: string,
    messageId: string,
    mode: 'stop' | 'delete',
    attempt: number = 0,
  ): Promise<void> {
    if (mode === 'stop') {
      await this.stopInstanceAndWait(instanceId, messageId)
    } else {
      await this.releaseInstance(instanceId, messageId, attempt)
    }
    const ctx = this.runningInstances.get(instanceId)
    if (ctx?.timeoutTimer) clearTimeout(ctx.timeoutTimer)
    if (ctx?.pollTimer) clearInterval(ctx.pollTimer)
  }

  /**
   * 云助手执行完整任务脚本（与 UserData 同源）。超时上限与阿里云 RunCommand 一致取较小值。
   */
  private async runRemoteLongRunningScript(
    instanceId: string,
    commandContent: string,
    messageId: string,
    attempt: number,
  ): Promise<void> {
    const timeoutSec = Math.min(
      36000,
      Math.max(120, Math.ceil(this.config.ecs.taskTimeout / 1000) + 900),
    )

    log.info('通过云助手执行宿主机任务脚本', {
      instanceId,
      messageId,
      attempt,
      timeoutSec,
    })

    const runRequest = new $ECS.RunCommandRequest({
      regionId: this.config.ecs.regionId,
      type: 'RunShellScript',
      commandContent,
      instanceId: [instanceId],
      timeout: timeoutSec,
    })
    const runtime = new $Util.RuntimeOptions({})

    const runResponse = await this.client.runCommandWithOptions(runRequest, runtime)
    const invokeId = runResponse.body?.invokeId
    if (!invokeId) {
      throw new Error('云助手未返回 invokeId')
    }

    const wallDeadline = Date.now() + this.config.ecs.taskTimeout + 180000

    while (Date.now() < wallDeadline) {
      await this.sleep(5000)

      const resultRequest = new $ECS.DescribeInvocationResultsRequest({
        regionId: this.config.ecs.regionId,
        invokeId,
      })

      const resultResponse = await this.client.describeInvocationResultsWithOptions(resultRequest, runtime)
      const results = resultResponse.body?.invocation?.invocationResults?.invocationResult || []

      if (results.length === 0) continue

      const result = results[0]
      const invokeStatus = result.invokeRecordStatus

      if (invokeStatus === 'Finished') {
        log.info('宿主机任务脚本执行结束（云助手）', { instanceId, messageId })
        return
      }
      if (invokeStatus === 'Failed' || invokeStatus === 'Timeout') {
        const out = result.output ? Buffer.from(result.output, 'base64').toString('utf-8') : ''
        throw new Error(`云助手执行任务脚本失败: ${invokeStatus}${out ? ` — ${out.slice(-500)}` : ''}`)
      }
    }

    throw new Error('云助手执行超时（任务脚本未在预期时间内结束）')
  }

  /**
   * 解析云助手上 cat 完成标记文件的输出
   */
  private parseTaskCheckOutput(
    checkResult: string | null,
  ): Omit<TaskResult, 'durationSeconds' | 'instanceId'> | 'PENDING' | null {
    if (checkResult === null) return null

    const lines = checkResult.trim().split('\n')
    const doneStatus = lines[0]?.trim()

    if (doneStatus === 'SUCCESS') {
      const resultJson = lines.slice(1).join('\n').trim()
      try {
        const result = JSON.parse(resultJson)
        return {
          success: true,
          outputVideoUrl: result.output_video_url,
        }
      } catch {
        return {
          success: true,
          outputVideoUrl: resultJson,
        }
      }
    }

    if (doneStatus === 'FAILED') {
      const resultJson = lines.slice(1).join('\n').trim()
      let errorMsg = '任务处理失败'
      try {
        const result = JSON.parse(resultJson)
        errorMsg = result.error || errorMsg
      } catch {
        // ignore
      }
      return {
        success: false,
        error: errorMsg,
      }
    }

    if (checkResult.includes('PENDING') || doneStatus === '' || !doneStatus) {
      return 'PENDING'
    }

    return 'PENDING'
  }

  private async readTaskResultWithRetries(
    instanceId: string,
    messageId: string,
    attempt: number,
  ): Promise<Omit<TaskResult, 'durationSeconds' | 'instanceId'>> {
    const cmd = `cat ${WORKER_HOST_PATHS.taskDone} 2>/dev/null && cat ${WORKER_HOST_PATHS.taskResult} 2>/dev/null || echo "PENDING"`

    for (let i = 0; i < 24; i++) {
      const raw = await this.runRemoteCommand(instanceId, cmd)
      const parsed = this.parseTaskCheckOutput(raw)

      if (parsed === null || parsed === 'PENDING') {
        log.debug('等待任务结果文件…', { instanceId, messageId, attempt, round: i + 1 })
        await this.sleep(5000)
        continue
      }

      return parsed
    }

    return {
      success: false,
      error: `任务结束后未读到 ${WORKER_HOST_PATHS.taskDone} 结果`,
    }
  }

  /**
   * 等待实例进入 Running 状态
   */
  private async waitForInstanceRunning(
    instanceId: string,
    messageId: string,
    attempt: number,
  ): Promise<void> {
    const deadline = Date.now() + this.config.ecs.instanceStartTimeout

    log.info('等待实例进入 Running 状态...', { instanceId, messageId, attempt })

    while (Date.now() < deadline) {
      const status = await this.getInstanceStatus(instanceId)

      if (status === 'Running') {
        log.info('实例已进入 Running 状态', { instanceId, attempt })
        return
      }

      if (status === 'Stopped' || status === 'Deleted') {
        throw new Error(`实例意外进入 ${status} 状态`)
      }

      log.debug(`实例状态: ${status}，等待中...`, { instanceId, attempt })
      await this.sleep(this.config.ecs.pollInterval)
    }

    throw new Error(`实例启动超时 (${this.config.ecs.instanceStartTimeout / 1000}s)`)
  }

  /**
   * 等待任务完成
   * 
   * 通过阿里云 "云助手" (RunCommand) 远程执行命令检查完成标记文件
   * 
   * 轮询策略：
   * - 前 2 分钟：每 10 秒检查一次
   * - 之后：按配置的 pollInterval 检查
   * - 超过 taskTimeout：超时失败
   */
  private async waitForTaskCompletion(
    instanceId: string,
    messageId: string,
    attempt: number,
  ): Promise<Omit<TaskResult, 'durationSeconds' | 'instanceId'>> {
    const deadline = Date.now() + this.config.ecs.taskTimeout

    log.info('等待任务完成...', {
      instanceId,
      messageId,
      attempt,
      timeoutMinutes: (this.config.ecs.taskTimeout / 60000).toFixed(1),
    })

    // 先等待一段时间让 cloud-init 开始执行
    await this.sleep(30000) // 30 秒

    while (Date.now() < deadline) {
      if (this.shuttingDown) {
        throw new Error('Worker 正在关闭')
      }

      try {
        // 检查实例是否还在运行
        const status = await this.getInstanceStatus(instanceId)
        if (status !== 'Running') {
          // 实例被回收或停止了
          log.warn(`实例状态异常: ${status}`, { instanceId, messageId, attempt })
          return {
            success: false,
            error: `实例状态异常: ${status}（可能被抢占式回收）`,
          }
        }

        // 通过云助手检查任务完成标记
        const checkResult = await this.runRemoteCommand(
          instanceId,
          `cat ${WORKER_HOST_PATHS.taskDone} 2>/dev/null && cat ${WORKER_HOST_PATHS.taskResult} 2>/dev/null || echo "PENDING"`,
        )

        if (checkResult === null) {
          // 云助手可能未安装或未就绪，等待
          log.debug('云助手未就绪，等待...', { instanceId, attempt })
          await this.sleep(this.config.ecs.pollInterval)
          continue
        }

        const parsed = this.parseTaskCheckOutput(checkResult)
        if (parsed === null) {
          await this.sleep(this.config.ecs.pollInterval)
          continue
        }
        if (parsed === 'PENDING') {
          // 任务还在处理中
        } else if (parsed.success) {
          log.info('任务处理成功', { instanceId, messageId, attempt })
          return { success: true, outputVideoUrl: parsed.outputVideoUrl }
        } else {
          log.error('任务处理失败', undefined, {
            instanceId,
            messageId,
            error: parsed.error,
          })
          return { success: false, error: parsed.error }
        }

        log.debug('任务处理中...', {
          instanceId,
          messageId,
          attempt,
          elapsed: ((Date.now() - (this.runningInstances.get(instanceId)?.createdAt || Date.now())) / 60000).toFixed(1) + ' min',
        })
      } catch (error) {
        log.warn('检查任务状态失败', {
          instanceId,
          attempt,
          error: error instanceof Error ? error.message : String(error),
        })
      }

      await this.sleep(this.config.ecs.pollInterval)
    }

    // 超时
    return {
      success: false,
      error: `任务处理超时 (${this.config.ecs.taskTimeout / 60000} 分钟)`,
    }
  }

  /**
   * 通过阿里云云助手远程执行命令
   * 
   * 使用 RunCommand API 在 ECS 实例上执行 shell 命令
   * 并等待命令执行结果
   */
  private async runRemoteCommand(
    instanceId: string,
    command: string,
    options?: { timeoutSec?: number },
  ): Promise<string | null> {
    const timeoutSec = options?.timeoutSec ?? 30
    try {
      // 发送命令
      const runRequest = new $ECS.RunCommandRequest({
        regionId: this.config.ecs.regionId,
        type: 'RunShellScript',
        commandContent: command,
        instanceId: [instanceId],
        timeout: timeoutSec,
      })

      const runtime = new $Util.RuntimeOptions({})
      const runResponse = await this.client.runCommandWithOptions(runRequest, runtime)
      const invokeId = runResponse.body?.invokeId

      if (!invokeId) {
        return null
      }

      // 等待命令执行完成
      await this.sleep(3000) // 等 3 秒

      const maxAttempts = Math.max(8, Math.ceil((timeoutSec + 25) / 2))
      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        const resultRequest = new $ECS.DescribeInvocationResultsRequest({
          regionId: this.config.ecs.regionId,
          invokeId,
        })

        const resultResponse = await this.client.describeInvocationResultsWithOptions(resultRequest, runtime)
        const results = resultResponse.body?.invocation?.invocationResults?.invocationResult || []

        if (results.length > 0) {
          const result = results[0]
          const invokeStatus = result.invokeRecordStatus

          if (invokeStatus === 'Finished') {
            // 命令执行完成，返回输出
            const output = result.output || ''
            // 阿里云返回的 output 是 Base64 编码
            return Buffer.from(output, 'base64').toString('utf-8')
          }

          if (invokeStatus === 'Failed' || invokeStatus === 'Timeout') {
            log.warn('远程命令执行失败', { instanceId, status: invokeStatus })
            return null
          }
        }

        await this.sleep(2000)
      }

      return null
    } catch (error: any) {
      // 云助手未安装的情况
      if (error.code === 'InvalidInstance.NotFound' || 
          error.code === 'CloudAssistant.NotInstalled') {
        return null
      }
      log.debug('远程命令执行异常', { error: error.message })
      return null
    }
  }

  /**
   * 查询实例状态
   */
  private async getInstanceStatus(instanceId: string): Promise<string> {
    const request = new $ECS.DescribeInstanceStatusRequest({
      regionId: this.config.ecs.regionId,
      instanceId: [instanceId],
    })
    const runtime = new $Util.RuntimeOptions({})

    const response = await this.client.describeInstanceStatusWithOptions(request, runtime)
    const statuses = response.body?.instanceStatuses?.instanceStatus || []

    if (statuses.length === 0) {
      return 'Deleted' // 实例不存在
    }

    return statuses[0].status || 'Unknown'
  }

  /**
   * DescribeInstances 取单台详情（公网 IP / EIP / 带宽等）。
   */
  private async fetchInstanceDescribe(instanceId: string): Promise<any | null> {
    const request = new $ECS.DescribeInstancesRequest({
      regionId: this.config.ecs.regionId,
      instanceIds: JSON.stringify([instanceId]),
      pageSize: 10,
    })
    const runtime = new $Util.RuntimeOptions({})
    const response = await this.client.describeInstancesWithOptions(request, runtime)
    const instances = response.body?.instances?.instance || []
    return instances[0] ?? null
  }

  /** 是否已有公网 IPv4（公网 IP 列表非空或已绑定 EIP） */
  private instanceHasPublicEgress(inst: any): boolean {
    const ips = inst?.publicIpAddress?.ipAddress
    const list = Array.isArray(ips) ? ips : []
    if (list.some((ip: unknown) => typeof ip === 'string' && ip.trim().length > 0)) {
      return true
    }
    const eip = inst?.eipAddress?.ipAddress
    return typeof eip === 'string' && eip.trim().length > 0
  }

  /**
   * 池机 / 临时实例启动后：若配置要求公网出口，则检查公网 IP 或 EIP；
   * 缺失时可选自动调用 ModifyInstanceNetworkSpec / AllocatePublicIpAddress（需账号余额与权限）。
   */
  private async ensureInstancePublicEgressForMq(
    instanceId: string,
    messageId: string,
    attempt: number,
  ): Promise<void> {
    if (!this.config.ecs.requirePublicIpForTasks) {
      log.debug('已跳过公网出口检查（WORKER_ECS_REQUIRE_PUBLIC_IP=false）', { instanceId, messageId })
      return
    }

    const runtime = new $Util.RuntimeOptions({})
    let inst = await this.fetchInstanceDescribe(instanceId)
    if (!inst) {
      throw new Error(`DescribeInstances 未返回实例 ${instanceId}，无法检查公网 IP`)
    }

    if (this.instanceHasPublicEgress(inst)) {
      const pub = inst.publicIpAddress?.ipAddress
      const eip = inst.eipAddress?.ipAddress
      log.info('实例已具备公网出口', {
        instanceId,
        messageId,
        attempt,
        publicIpAddress: Array.isArray(pub) ? pub : pub,
        eipAddress: eip,
      })
      return
    }

    if (!this.config.ecs.autoAllocatePublicIp) {
      throw new Error(
        '实例无公网 IP 且未绑定 EIP，无法访问公网消息队列。请在 ECS 控制台为该实例分配公网带宽并分配公网 IP 或绑定 EIP；' +
          '或设置 WORKER_ECS_AUTO_ALLOCATE_PUBLIC_IP=true 由编排器自动尝试；若 RabbitMQ 在 VPC 内可设 WORKER_ECS_REQUIRE_PUBLIC_IP=false。',
      )
    }

    const bwConfigured = typeof inst.internetMaxBandwidthOut === 'number' ? inst.internetMaxBandwidthOut : 0
    const targetBw = Math.max(1, this.config.ecs.internetMaxBandwidthOut || 1)

    log.warn('实例无公网出口，尝试自动开通/分配公网 IP', {
      instanceId,
      messageId,
      attempt,
      internetMaxBandwidthOut: bwConfigured,
      targetBw,
    })

    try {
      if (bwConfigured <= 0) {
        await this.client.modifyInstanceNetworkSpecWithOptions(
          new $ECS.ModifyInstanceNetworkSpecRequest({
            instanceId,
            internetMaxBandwidthOut: targetBw,
            networkChargeType: 'PayByTraffic',
            allocatePublicIp: true,
            autoPay: true,
          }),
          runtime,
        )
        log.info('已提交 ModifyInstanceNetworkSpec（公网带宽 + 分配公网 IP）', {
          instanceId,
          targetBw,
        })
      } else {
        await this.client.allocatePublicIpAddressWithOptions(
          new $ECS.AllocatePublicIpAddressRequest({ instanceId }),
          runtime,
        )
        log.info('已调用 AllocatePublicIpAddress', { instanceId })
      }
    } catch (error: any) {
      const code = error?.code || error?.data?.Code
      const msg = error?.message || String(error)
      log.warn('首次自动分配公网 IP 失败，将尝试备用策略或轮询', {
        instanceId,
        code,
        message: msg,
      })

      if (bwConfigured > 0) {
        try {
          await this.client.modifyInstanceNetworkSpecWithOptions(
            new $ECS.ModifyInstanceNetworkSpecRequest({
              instanceId,
              internetMaxBandwidthOut: Math.max(bwConfigured, targetBw),
              networkChargeType: 'PayByTraffic',
              allocatePublicIp: true,
              autoPay: true,
            }),
            runtime,
          )
          log.info('已提交 ModifyInstanceNetworkSpec（allocatePublicIp，备用）', { instanceId })
        } catch (e2: any) {
          throw new Error(
            `自动分配公网 IP 失败: ${e2?.message || e2?.code || e2}。请在控制台检查实例网络或绑定 EIP。原始错误: ${msg}`,
          )
        }
      } else {
        throw new Error(`开通公网带宽/分配公网 IP 失败: ${msg}（${code || 'no code'}）`)
      }
    }

    const ok = await this.pollInstancePublicEgress(instanceId, messageId, 180000)
    if (!ok) {
      throw new Error(
        '等待公网 IP 超时（约 3 分钟）。请在阿里云控制台为该实例确认公网带宽、公网 IP 或 EIP；' +
          '若 MQ 在 VPC 内网请设置 WORKER_ECS_REQUIRE_PUBLIC_IP=false。',
      )
    }

    inst = await this.fetchInstanceDescribe(instanceId)
    log.info('实例已获得公网出口', {
      instanceId,
      messageId,
      publicIpAddress: inst?.publicIpAddress?.ipAddress,
      eipAddress: inst?.eipAddress?.ipAddress,
    })
  }

  /** 轮询 DescribeInstances，直到出现公网 IP 或 EIP */
  private async pollInstancePublicEgress(
    instanceId: string,
    messageId: string,
    maxWaitMs: number,
  ): Promise<boolean> {
    const deadline = Date.now() + maxWaitMs
    while (Date.now() < deadline) {
      await this.sleep(4000)
      const inst = await this.fetchInstanceDescribe(instanceId)
      if (inst && this.instanceHasPublicEgress(inst)) {
        return true
      }
      log.debug('等待公网 IP 生效…', { instanceId, messageId })
    }
    return false
  }

  /**
   * 释放 ECS 实例
   * 
   * 先停止再释放，确保计费停止。
   * 若实例仍在 Initializing/Pending/Starting，会轮询等待后再删除，避免 403。
   */
  async releaseInstance(
    instanceId: string,
    messageId: string,
    attempt: number = 0,
  ): Promise<void> {
    log.info('正在释放 ECS 实例...', { instanceId, messageId, attempt })

    const maxRetries = 24 // 约 2 分钟（每 5 秒重试）
    const retryInterval = 5000

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const request = new $ECS.DeleteInstanceRequest({
          instanceId,
          force: true,
        })
        const runtime = new $Util.RuntimeOptions({})
        await this.client.deleteInstanceWithOptions(request, runtime)
        log.info('ECS 实例已释放', { instanceId, messageId })
        break
      } catch (error: any) {
        if (error.code === 'InvalidInstanceId.NotFound') {
          log.info('实例已不存在（可能已被回收）', { instanceId })
          return
        }
        // 实例仍在初始化/启动中，无法删除，等待后重试
        if (
          error.code === 'IncorrectInstanceStatus.Initializing' ||
          error.code === 'IncorrectInstanceStatus' ||
          (error.message && String(error.message).includes('IncorrectInstanceStatus'))
        ) {
          if (attempt < maxRetries - 1) {
            log.debug(`实例尚未可释放，${retryInterval / 1000}s 后重试`, {
              instanceId,
              attempt: attempt + 1,
              maxRetries,
            })
            await this.sleep(retryInterval)
            continue
          }
        }
        log.error('释放实例失败', error, { instanceId, messageId })
        return
      }
    }

    // 清除定时器
    const context = this.runningInstances.get(instanceId)
    if (context) {
      if (context.timeoutTimer) clearTimeout(context.timeoutTimer)
      if (context.pollTimer) clearInterval(context.pollTimer)
    }
  }

  /**
   * 清理残留的 worker 实例（来自上次异常退出）
   */
  async cleanupStaleInstances(): Promise<void> {
    try {
      const request = new $ECS.DescribeInstancesRequest({
        regionId: this.config.ecs.regionId,
        tag: [
          new $ECS.DescribeInstancesRequestTag({
            key: WORKER_ECS_TAGS.lifecycle,
            value: 'ephemeral',
          }),
        ],
        status: 'Running',
        pageSize: 100,
      })
      const runtime = new $Util.RuntimeOptions({})

      const response = await this.client.describeInstancesWithOptions(request, runtime)
      const instances = response.body?.instances?.instance || []

      if (instances.length === 0) {
        log.debug('没有发现残留的 worker 实例')
        return
      }

      log.warn(`发现 ${instances.length} 个残留 worker 实例`, {
        instanceIds: instances.map((i: any) => i.instanceId),
      })

      for (const instance of instances) {
        const iid = instance.instanceId
        if (!iid) continue

        // 检查创建时间，超过 2 倍任务超时时间的才清理
        const createdAt = new Date(instance.creationTime || '').getTime()
        const age = Date.now() - createdAt

        if (age > this.config.ecs.taskTimeout * 2) {
          log.info('清理残留实例', {
            instanceId: iid,
            ageMinutes: (age / 60000).toFixed(1),
          })
          await this.releaseInstance(iid, 'cleanup', 0)
        } else {
          log.info('残留实例创建时间较近，跳过清理', {
            instanceId: iid,
            ageMinutes: (age / 60000).toFixed(1),
          })
        }
      }
    } catch (error) {
      log.warn('列出残留实例失败', { error: String(error) })
    }
  }

  /**
   * 检测并清理僵尸实例
   */
  async cleanupZombieInstances(): Promise<void> {
    const now = Date.now()

    const entries = Array.from(this.runningInstances.entries())
    for (const [instanceId, context] of entries) {
      const runningTime = now - context.createdAt

      // 超过超时时间的 1.5 倍，认为是僵尸实例
      if (runningTime > this.config.ecs.taskTimeout * 1.5) {
        log.warn('检测到僵尸实例，正在释放', {
          instanceId,
          messageId: context.messageId,
          runningMinutes: (runningTime / 60000).toFixed(1),
        })

        await this.disposeAfterTask(
          instanceId,
          context.messageId,
          context.disposeMode ?? 'delete',
          0,
        )
        this.runningInstances.delete(instanceId)
        this.activeSlots = Math.max(0, this.activeSlots - 1)
      }
    }
  }

  /**
   * 优雅关闭：等待所有实例完成，超时后强制释放
   */
  async shutdown(forceTimeoutMs: number = 120000): Promise<void> {
    this.shuttingDown = true
    log.info('正在优雅关闭 ECS 编排器...', {
      runningInstances: this.runningInstances.size,
    })

    if (this.runningInstances.size === 0) {
      log.info('没有运行中的实例，立即关闭')
      return
    }

    // 等待实例完成
    const deadline = Date.now() + forceTimeoutMs
    while (this.runningInstances.size > 0 && Date.now() < deadline) {
      log.info(`等待 ${this.runningInstances.size} 个实例完成...`)
      await this.sleep(5000)
    }

    // 超时后强制释放
    if (this.runningInstances.size > 0) {
      log.warn(`强制释放 ${this.runningInstances.size} 个实例`)
      const entries = Array.from(this.runningInstances.entries())
      for (const [instanceId, context] of entries) {
        await this.disposeAfterTask(
          instanceId,
          context.messageId,
          context.disposeMode ?? 'delete',
          0,
        )
      }
      this.runningInstances.clear()
      this.activeSlots = 0
    }

    log.info('ECS 编排器已关闭')
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}
