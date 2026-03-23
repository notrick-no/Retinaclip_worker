#!/usr/bin/env npx tsx
/**
 * 检查当前 .env 配置下 ECS「池」与临时实例概况（只读，不创建/不删除实例）
 *
 * 用法（项目根目录）：
 *   npm run pool
 *   npm run pool -- <pool_profile_tag_value>
 *   ./scripts/rc.sh p [profile]
 *
 * 第二个参数可选：在 WORKER_ECS_POOL_PROFILE_FILTER_ENABLED=true 时，模拟按 pool profile 筛选；默认 false 时 CLI 参数会被忽略（与编排器一致）。
 */

import 'dotenv/config'
import ECS20140526, * as $ECS from '@alicloud/ecs20140526'
import OpenApi, * as $OpenApi from '@alicloud/openapi-client'
import * as $Util from '@alicloud/tea-util'
import type { WorkerConfig } from '../config'
import { loadConfig } from '../config'
import { WORKER_ECS_TAGS } from '../worker-branding'

function createEcsClient(config: WorkerConfig) {
  const openApiConfig = new $OpenApi.Config({
    accessKeyId: config.ecs.accessKeyId,
    accessKeySecret: config.ecs.accessKeySecret,
    regionId: config.ecs.regionId,
    endpoint: `ecs.${config.ecs.regionId}.aliyuncs.com`,
  })
  return new (ECS20140526 as any).default(openApiConfig)
}

async function describeAllPages(
  client: any,
  buildRequest: (pageNumber: number) => InstanceType<typeof $ECS.DescribeInstancesRequest>,
): Promise<any[]> {
  const runtime = new $Util.RuntimeOptions({})
  const out: any[] = []
  let page = 1
  for (;;) {
    const response = await client.describeInstancesWithOptions(buildRequest(page), runtime)
    const batch = response.body?.instances?.instance || []
    out.push(...batch)
    const total = response.body?.totalCount ?? 0
    if (out.length >= total || batch.length === 0) break
    page++
    if (page > 50) break
  }
  return out
}

function tagMap(instance: any): Record<string, string> {
  const m: Record<string, string> = {}
  const tags = instance.tags?.tag || instance.tag?.tag || []
  for (const t of tags) {
    if (t.tagKey && t.tagValue !== undefined) m[String(t.tagKey)] = String(t.tagValue)
  }
  return m
}

async function main() {
  const poolProfileArg = process.argv[2]?.trim() || ''

  let config: WorkerConfig
  try {
    config = loadConfig()
  } catch (e) {
    console.error('加载配置失败（请确认已配置 .env）：', e instanceof Error ? e.message : e)
    process.exit(1)
  }

  const client = createEcsClient(config)
  const runtime = new $Util.RuntimeOptions({})

  console.log('══════════════════════════════════════════════════════════════')
  console.log('  RetinaClip — ECS 池状态检查（只读）')
  console.log('══════════════════════════════════════════════════════════════')
  console.log('')
  console.log('配置摘要:')
  console.log(`  regionId          : ${config.ecs.regionId}`)
  console.log(`  zoneId            : ${config.ecs.zoneId || '(未固定，由交换机决定)'}`)
  console.log(`  WORKER_ECS_POOL_ENABLED : ${config.ecs.poolEnabled}`)
  console.log(`  池 lifecycle 标签   : ${WORKER_ECS_TAGS.lifecycle} = ${config.ecs.poolLifecycleTagValue}`)
  console.log(`  pool profile 键     : ${config.ecs.poolProfileTagKey}`)
  console.log(
    `  pool profile 筛选   : ${config.ecs.poolProfileFilterEnabled ? '开启（WORKER_ECS_POOL_PROFILE_FILTER_ENABLED）' : '关闭（仅 lifecycle 池标签）'}`,
  )
  console.log(`  配置 imageId（新建 ephemeral 用）: ${config.ecs.imageId}`)
  console.log(`  配置 instanceType（新建 ephemeral 用）: ${config.ecs.instanceType}`)
  if (poolProfileArg) {
    console.log(`  本次模拟 poolProfile : ${poolProfileArg}`)
  }
  console.log('')

  // ----- 带 lifecycle=pool 的全部实例 -----
  const poolInstances = await describeAllPages(client, (pageNumber) => {
    return new $ECS.DescribeInstancesRequest({
      regionId: config.ecs.regionId,
      pageNumber,
      pageSize: 50,
      tag: [
        new $ECS.DescribeInstancesRequestTag({
          key: WORKER_ECS_TAGS.lifecycle,
          value: config.ecs.poolLifecycleTagValue,
        }),
      ],
    })
  })

  console.log(`── 标签为「${WORKER_ECS_TAGS.lifecycle}=${config.ecs.poolLifecycleTagValue}」的实例: ${poolInstances.length} 台 ──`)
  if (poolInstances.length === 0) {
    console.log('  (无) 请确认池实例已打上述标签；控制台若用 retinaclip:lifecycle 需与 WORKER_ECS_POOL_TAG_VALUE 一致。')
  } else {
    for (const i of poolInstances) {
      const id = i.instanceId || '?'
      const name = i.instanceName || ''
      const st = i.status || '?'
      const img = i.imageId || '?'
      const typ = i.instanceType || '?'
      const zone = i.zoneId || '?'
      const tags = tagMap(i)
      const prof = tags[config.ecs.poolProfileTagKey] || '(无 profile 标签)'
      const flags: string[] = []
      if (st === 'Stopped') flags.push('Stopped：可被编排器选为空闲池（不校验镜像/规格）')
      if (st === 'Running') flags.push('运行中')
      console.log(`  • ${id}  ${name}`)
      console.log(`      status=${st}  zone=${zone}`)
      console.log(`      imageId=${img}`)
      console.log(`      instanceType=${typ}`)
      console.log(`      ${config.ecs.poolProfileTagKey}=${prof}`)
      if (flags.length) console.log(`      → ${flags.join('；')}`)
    }
  }
  console.log('')

  // ----- 与 findIdlePoolInstance 一致：Stopped + lifecycle（+ 可选 profile）-----
  const idleReq = new $ECS.DescribeInstancesRequest({
    regionId: config.ecs.regionId,
    status: 'Stopped',
    pageSize: 50,
    tag: [
      new $ECS.DescribeInstancesRequestTag({
        key: WORKER_ECS_TAGS.lifecycle,
        value: config.ecs.poolLifecycleTagValue,
      }),
    ],
  })
  if (config.ecs.poolProfileFilterEnabled && poolProfileArg) {
    idleReq.tag!.push(
      new $ECS.DescribeInstancesRequestTag({
        key: config.ecs.poolProfileTagKey,
        value: poolProfileArg,
      }),
    )
  }

  const idleRes = await client.describeInstancesWithOptions(idleReq, runtime)
  const idleList = idleRes.body?.instances?.instance || []

  console.log('── 编排器「下一任务」可立即复用的 Stopped 池实例（与代码 findIdlePoolInstance 条件一致）──')
  if (!config.ecs.poolProfileFilterEnabled) {
    console.log('  （WORKER_ECS_POOL_PROFILE_FILTER_ENABLED=false：不按 profile 标签筛选池实例）')
    if (poolProfileArg) {
      console.log(`  （已忽略 CLI 参数「${poolProfileArg}」；若需模拟 profile 筛选请先设 WORKER_ECS_POOL_PROFILE_FILTER_ENABLED=true）`)
    }
  } else if (poolProfileArg) {
    console.log(`  （已加 profile 过滤: ${config.ecs.poolProfileTagKey}=${poolProfileArg}）`)
  } else {
    console.log('  （未加 profile 过滤；若任务带了 poolProfile，请用: npx tsx scripts/test-ecs-pool.ts <profile值>）')
  }
  console.log(`  命中数量: ${idleList.length}`)
  for (const i of idleList) {
    console.log(`  • ${i.instanceId}  ${i.instanceName || ''}  zone=${i.zoneId}`)
  }
  if (!config.ecs.poolEnabled) {
    console.log('')
    console.log('  ⚠ WORKER_ECS_POOL_ENABLED=false：Worker 不会使用池，仍会新建 ephemeral 实例。')
  }
  console.log('')

  // ----- ephemeral 运行中（残留排查）-----
  const ephem = await describeAllPages(client, (pageNumber) => {
    return new $ECS.DescribeInstancesRequest({
      regionId: config.ecs.regionId,
      pageNumber,
      pageSize: 50,
      tag: [
        new $ECS.DescribeInstancesRequestTag({
          key: WORKER_ECS_TAGS.lifecycle,
          value: 'ephemeral',
        }),
      ],
    })
  })

  console.log(`── 标签 ${WORKER_ECS_TAGS.lifecycle}=ephemeral 的实例: ${ephem.length} 台 ──`)
  for (const i of ephem.slice(0, 20)) {
    console.log(`  • ${i.instanceId}  ${i.instanceName || ''}  status=${i.status}  type=${i.instanceType}`)
  }
  if (ephem.length > 20) console.log(`  … 其余 ${ephem.length - 20} 台省略`)
  console.log('')
  console.log('检查完成。')
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
