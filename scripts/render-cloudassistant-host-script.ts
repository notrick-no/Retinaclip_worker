#!/usr/bin/env npx tsx
/**
 * 渲染云助手（RunCommand）在 ECS 宿主机上执行的那段 bash 脚本（非 base64）。
 *
 * 目的：你可以把脚本原文复制到池机/调度机的 ECS 宿主机上执行一次，
 *       以复现 cloud-init + docker pull/run 的行为，便于定位 NAS/Registry 等问题。
 *
 * 注意：
 * - 云助手脚本是在「宿主机」执行，不是在「容器内」执行。
 * - 容器内只会跑镜像里的 `./deploy.sh`（作为 docker run 的 command）。
 *
 * 用法（项目根目录）：
 * npx tsx scripts/render-cloudassistant-host-script.ts \
 *   --messageId test-1 \
 *   --videoDownloadUrl http://... \
 *   --webhookUrl http://... \
 *   --processingImage 172.16.0.70:5000/quzimu-container:v4 \
 *   --qualityPreset normal \
 *   --operations '["remove subtitles"]'
 *
 * 如果你只想验证 NAS + docker pull/run，可用任意占位 videoDownloadUrl/webhookUrl
 * （脚本仍会先执行 NAS 挂载与 docker pull，失败点能看出来）。
 */

import 'dotenv/config'
import { loadConfig } from '../config'
import type { TaskParams } from '../domain/task'
import { generateTaskRunnerShellScript } from '../cloud-init'

function getArg(name: string): string | undefined {
  const idx = process.argv.indexOf(name)
  if (idx === -1) return undefined
  return process.argv[idx + 1]
}

function parseJsonMaybe<T>(s: string | undefined, fallback: T): T {
  if (!s) return fallback
  try {
    return JSON.parse(s) as T
  } catch {
    return fallback
  }
}

async function main() {
  const config = loadConfig()

  // 为了让脚本一定走真实模式：强制关闭 mock
  config.ecs.mockProcessing = false

  const messageId = getArg('--messageId') || `render-local-${Date.now()}`
  const videoDownloadUrl = getArg('--videoDownloadUrl') || 'http://127.0.0.1/placeholder.mp4'
  const webhookUrl = getArg('--webhookUrl') || 'http://127.0.0.1/placeholder-webhook'
  const processingImage = getArg('--processingImage') || config.processing.defaultImage
  const qualityPreset = getArg('--qualityPreset') || 'normal'
  const detectType = (getArg('--detectType') || 'auto') as TaskParams['detectType']
  const poolProfile = getArg('--poolProfile')

  const operations = parseJsonMaybe<string[]>(getArg('--operations'), ['remove subtitles'])

  const taskParams: TaskParams = {
    messageId,
    videoDownloadUrl,
    webhookUrl,
    detectType,
    operations,
    qualityPreset,
    ...(poolProfile ? { poolProfile } : {}),
  }

  const hostInstanceTypeForGpu = config.ecs.instanceType
  const script = generateTaskRunnerShellScript(taskParams, config, processingImage, hostInstanceTypeForGpu)
  process.stdout.write(script)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})

