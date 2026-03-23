#!/usr/bin/env npx tsx
/**
 * 调度器侧端到端测试（不手动登录池机）：
 * - 调度器上直接调用 ECSOrchestrator.runTask()
 * - 将任务分配到池实例（Started -> 远程云助手执行 -> 池机内 NAS 挂载 + docker pull/run）
 * - 在本脚本里拿到 TaskResult（成功/失败 + 错误信息）
 *
 * 用法（项目根目录）：
 * 1) 最小示例（需你提供可访问的视频 URL + Webhook URL）：
 *    npx tsx scripts/test-scheduler-run-pool-task.ts \
 *      --videoDownloadUrl http://... \
 *      --webhookUrl http://... \
 *      --processingImage 172.16.0.70:5000/quzimu-app:v3
 *
 * 2) 可选：指定 poolProfile（当 WORKER_ECS_POOL_PROFILE_FILTER_ENABLED=true 才会生效）
 *    npx tsx scripts/test-scheduler-run-pool-task.ts \
 *      --videoDownloadUrl http://... \
 *      --webhookUrl http://... \
 *      --processingImage 172.16.0.70:5000/quzimu-app:v3 \
 *      --poolProfile subtitle
 */

import 'dotenv/config'
import { loadConfig } from '../config'
import { ECSOrchestrator } from '../ecs-orchestrator'
import type { TaskParams } from '../domain/task'
import { createLogger } from '../logger'

const log = createLogger('SchedulerE2E')

function getArg(name: string): string | undefined {
  const idx = process.argv.indexOf(name)
  if (idx === -1) return undefined
  return process.argv[idx + 1]
}

async function main() {
  // 关键：调度器侧必须跑真实 docker pull/run，所以强制关闭 mock
  process.env.WORKER_MOCK_PROCESSING = 'false'

  const videoDownloadUrl = getArg('--videoDownloadUrl')
  const webhookUrl = getArg('--webhookUrl')
  const processingImage = getArg('--processingImage')
  const poolProfile = getArg('--poolProfile')

  if (!videoDownloadUrl) throw new Error('缺少参数 --videoDownloadUrl')
  if (!webhookUrl) throw new Error('缺少参数 --webhookUrl')
  if (!processingImage) throw new Error('缺少参数 --processingImage')

  const config = loadConfig()
  log.info('加载配置完成，开始初始化 ECSOrchestrator...')

  const orchestrator = new ECSOrchestrator(config)
  await orchestrator.initialize()

  const taskParams: TaskParams = {
    messageId: `sched-e2e-${Date.now()}`,
    videoDownloadUrl,
    webhookUrl,
    detectType: 'auto',
    operations: ['remove subtitles'],
    qualityPreset: 'normal',
    ...(poolProfile ? { poolProfile } : {}),
  }

  log.info('发起 runTask（将触发池机复用 + 云助手执行）', {
    messageId: taskParams.messageId,
    processingImage,
    poolProfile: taskParams.poolProfile ?? '(none)',
  })

  const result = await orchestrator.runTask(taskParams, processingImage, 0)

  log.info('runTask 返回结果', result)

  // 用非 0 退出码便于 CI/脚本判断
  if (!result.success) process.exit(1)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})

