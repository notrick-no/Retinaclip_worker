/**
 * 阿里云 ECS 实例编排器
 * 
 * ╔══════════════════════════════════════════════════════════════╗
 * ║                ECS 实例生命周期管理策略                       ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║                                                              ║
 * ║  1. 按需创建 (On-Demand)                                     ║
 * ║     - 收到 RabbitMQ 消息时，创建 ECS 抢占式实例               ║
 * ║     - 通过信号量控制并发数（不超过 MAX_INSTANCES）             ║
 * ║     - 任务完成后立即释放实例                                  ║
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
import { generateUserData, TaskParams } from './cloud-init'
import { createLogger } from './logger'

const log = createLogger('ECS')

/** ECS 实例运行上下文 */
export interface InstanceContext {
  /** 实例 ID */
  instanceId: string
  /** 任务消息 ID */
  messageId: string
  /** 创建时间戳 */
  createdAt: number
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
  private client: ECS20140526
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

    this.client = new ECS20140526(openApiConfig)
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
   * 核心方法：为一个任务创建 ECS 实例并等待完成
   * 
   * 完整流程：
   *   获取槽位 → 创建实例 → 等待启动 → 轮询任务状态
   *   → 收集结果 → 释放实例 → 释放槽位
   */
  async runTask(taskParams: TaskParams, processingImage: string): Promise<TaskResult> {
    if (this.shuttingDown) {
      throw new Error('Worker 正在关闭，拒绝新任务')
    }

    // 1. 获取槽位
    this.activeSlots++
    log.info(`获取实例槽位 [${this.activeSlots}/${this.config.ecs.maxInstances}]`, {
      messageId: taskParams.messageId,
    })

    const startTime = Date.now()
    let instanceId: string | null = null

    try {
      // 2. 生成 UserData（Cloud-Init 启动脚本）
      const userData = generateUserData(taskParams, this.config, processingImage)

      // 3. 创建 ECS 实例
      instanceId = await this.createInstance(taskParams.messageId, userData)

      // 4. 注册运行中的实例
      const context: InstanceContext = {
        instanceId,
        messageId: taskParams.messageId,
        createdAt: startTime,
      }
      this.runningInstances.set(instanceId, context)

      // 5. RunInstances 创建的实例会自动启动，无需再调 StartInstance（否则会 403）
      // 6. 等待实例进入 Running 状态
      await this.waitForInstanceRunning(instanceId, taskParams.messageId)

      // 7. 等待任务完成（轮询实例上的完成标记）
      const result = await this.waitForTaskCompletion(instanceId, taskParams.messageId)

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
      // 8. 释放实例 + 释放槽位
      if (instanceId) {
        await this.releaseInstance(instanceId, taskParams.messageId)
        this.runningInstances.delete(instanceId)
      }
      this.activeSlots--
      log.info(`释放实例槽位 [${this.activeSlots}/${this.config.ecs.maxInstances}]`, {
        messageId: taskParams.messageId,
      })
    }
  }

  /**
   * 创建 ECS 抢占式实例
   */
  private async createInstance(messageId: string, userData: string): Promise<string> {
    log.info('正在创建 ECS 实例...', {
      messageId,
      instanceType: this.config.ecs.instanceType,
      spot: this.config.ecs.useSpotInstance,
    })

    const instanceName = `${this.config.ecs.instanceNamePrefix}-${messageId.substring(0, 8)}`

    const request = new $ECS.RunInstancesRequest({
      regionId: this.config.ecs.regionId,
      imageId: this.config.ecs.imageId,
      instanceType: this.config.ecs.instanceType,
      securityGroupId: this.config.ecs.securityGroupId,
      vSwitchId: this.config.ecs.vswitchId,
      instanceName,
      hostName: instanceName,
      // 系统盘
      systemDiskSize: String(this.config.ecs.systemDiskSize),
      systemDiskCategory: this.config.ecs.systemDiskCategory,
      // 公网
      internetMaxBandwidthOut: this.config.ecs.internetMaxBandwidthOut,
      internetChargeType: 'PayByTraffic',
      // 按量付费 + 抢占式
      instanceChargeType: 'PostPaid',
      ...(this.config.ecs.useSpotInstance && {
        spotStrategy: this.config.ecs.spotStrategy,
        ...(this.config.ecs.spotPriceLimit && {
          spotPriceLimit: this.config.ecs.spotPriceLimit,
        }),
        // 抢占式实例中断后的操作：释放
        spotInterruptionBehavior: 'Terminate',
      }),
      // Cloud-Init 用户数据
      userData,
      // 可用区
      ...(this.config.ecs.zoneId && { zoneId: this.config.ecs.zoneId }),
      // 密钥对（调试用）
      ...(this.config.ecs.keyPairName && { keyPairName: this.config.ecs.keyPairName }),
      // RAM 角色
      ...(this.config.ecs.ramRoleName && { ramRoleName: this.config.ecs.ramRoleName }),
      // 实例数量
      amount: 1,
      // 标签（用于识别 worker 实例）
      tag: [
        new $ECS.RunInstancesRequestTag({ key: 'mingle:role', value: 'worker' }),
        new $ECS.RunInstancesRequestTag({ key: 'mingle:message-id', value: messageId }),
        new $ECS.RunInstancesRequestTag({ key: 'mingle:created-at', value: new Date().toISOString() }),
      ],
    })

    const runtime = new $Util.RuntimeOptions({})

    try {
      const response = await this.client.runInstancesWithOptions(request, runtime)
      const instanceIds = response.body?.instanceIdSets?.instanceIdSet || []

      if (instanceIds.length === 0) {
        throw new Error('创建实例成功但未返回实例 ID')
      }

      const instanceId = instanceIds[0]
      log.info('ECS 实例创建成功', { instanceId, messageId })
      return instanceId

    } catch (error: any) {
      const errorCode = error.code || error.Code || ''
      const errorMsg = error.message || error.Message || String(error)

      // 处理常见错误
      if (errorCode === 'OperationDenied.NoStock') {
        throw new Error(`实例规格 ${this.config.ecs.instanceType} 在 ${this.config.ecs.zoneId || this.config.ecs.regionId} 库存不足`)
      }
      if (errorCode === 'InvalidSpotPriceLimit.LowerThanPublicPrice') {
        throw new Error('抢占式实例出价低于市场价')
      }

      throw new Error(`创建 ECS 实例失败: [${errorCode}] ${errorMsg}`)
    }
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
   * 等待实例进入 Running 状态
   */
  private async waitForInstanceRunning(instanceId: string, messageId: string): Promise<void> {
    const deadline = Date.now() + this.config.ecs.instanceStartTimeout

    log.info('等待实例进入 Running 状态...', { instanceId, messageId })

    while (Date.now() < deadline) {
      const status = await this.getInstanceStatus(instanceId)

      if (status === 'Running') {
        log.info('实例已进入 Running 状态', { instanceId })
        return
      }

      if (status === 'Stopped' || status === 'Deleted') {
        throw new Error(`实例意外进入 ${status} 状态`)
      }

      log.debug(`实例状态: ${status}，等待中...`, { instanceId })
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
  private async waitForTaskCompletion(instanceId: string, messageId: string): Promise<Omit<TaskResult, 'durationSeconds' | 'instanceId'>> {
    const deadline = Date.now() + this.config.ecs.taskTimeout

    log.info('等待任务完成...', {
      instanceId,
      messageId,
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
          log.warn(`实例状态异常: ${status}`, { instanceId, messageId })
          return {
            success: false,
            error: `实例状态异常: ${status}（可能被抢占式回收）`,
          }
        }

        // 通过云助手检查任务完成标记
        const checkResult = await this.runRemoteCommand(
          instanceId,
          'cat /tmp/mingle-task-done 2>/dev/null && cat /tmp/mingle-task-result 2>/dev/null || echo "PENDING"'
        )

        if (checkResult === null) {
          // 云助手可能未安装或未就绪，等待
          log.debug('云助手未就绪，等待...', { instanceId })
          await this.sleep(this.config.ecs.pollInterval)
          continue
        }

        const lines = checkResult.trim().split('\n')
        const doneStatus = lines[0]?.trim()

        if (doneStatus === 'SUCCESS') {
          // 任务成功
          const resultJson = lines.slice(1).join('\n').trim()
          try {
            const result = JSON.parse(resultJson)
            log.info('任务处理成功', { instanceId, messageId })
            return {
              success: true,
              outputVideoUrl: result.output_video_url,
            }
          } catch {
            log.warn('结果 JSON 解析失败', { instanceId, resultJson })
            return {
              success: true,
              outputVideoUrl: resultJson, // 尝试直接用原始内容
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
            // 忽略
          }
          log.error('任务处理失败', undefined, { instanceId, messageId, error: errorMsg })
          return {
            success: false,
            error: errorMsg,
          }
        }

        // PENDING - 任务还在处理中
        log.debug('任务处理中...', {
          instanceId,
          messageId,
          elapsed: ((Date.now() - (this.runningInstances.get(instanceId)?.createdAt || Date.now())) / 60000).toFixed(1) + ' min',
        })
      } catch (error) {
        log.warn('检查任务状态失败', {
          instanceId,
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
  private async runRemoteCommand(instanceId: string, command: string): Promise<string | null> {
    try {
      // 发送命令
      const runRequest = new $ECS.RunCommandRequest({
        regionId: this.config.ecs.regionId,
        type: 'RunShellScript',
        commandContent: command,
        instanceId: [instanceId],
        timeout: 30,
      })

      const runtime = new $Util.RuntimeOptions({})
      const runResponse = await this.client.runCommandWithOptions(runRequest, runtime)
      const invokeId = runResponse.body?.invokeId

      if (!invokeId) {
        return null
      }

      // 等待命令执行完成
      await this.sleep(3000) // 等 3 秒

      for (let attempt = 0; attempt < 5; attempt++) {
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
   * 释放 ECS 实例
   * 
   * 先停止再释放，确保计费停止。
   * 若实例仍在 Initializing/Pending/Starting，会轮询等待后再删除，避免 403。
   */
  async releaseInstance(instanceId: string, messageId: string): Promise<void> {
    log.info('正在释放 ECS 实例...', { instanceId, messageId })

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
            key: 'mingle:role',
            value: 'worker',
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
          await this.releaseInstance(iid, 'cleanup')
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

        await this.releaseInstance(instanceId, context.messageId)
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
        await this.releaseInstance(instanceId, context.messageId)
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
