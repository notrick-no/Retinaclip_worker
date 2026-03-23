#!/usr/bin/env npx tsx
/**
 * 测试阿里云 ECS OpenAPI：地域、自定义镜像是否存在（只读）
 * 用法: npm run test:ecs
 */
import 'dotenv/config'
import ECS20140526, * as $ECS from '@alicloud/ecs20140526'
import OpenApi, * as $OpenApi from '@alicloud/openapi-client'
import * as $Util from '@alicloud/tea-util'
import type { WorkerConfig } from '../config'
import { loadConfig } from '../config'

function createEcsClient(config: WorkerConfig) {
  const openApiConfig = new $OpenApi.Config({
    accessKeyId: config.ecs.accessKeyId,
    accessKeySecret: config.ecs.accessKeySecret,
    regionId: config.ecs.regionId,
    endpoint: `ecs.${config.ecs.regionId}.aliyuncs.com`,
  })
  return new (ECS20140526 as any).default(openApiConfig)
}

async function main() {
  let cfg: WorkerConfig
  try {
    cfg = loadConfig()
  } catch (e) {
    console.error('[失败] 配置加载:', e instanceof Error ? e.message : e)
    process.exit(1)
  }

  const client = createEcsClient(cfg)
  const runtime = new $Util.RuntimeOptions({})

  console.log('DescribeRegions…')
  try {
    const regionsRes = await client.describeRegionsWithOptions(new $ECS.DescribeRegionsRequest({}), runtime)
    const regions = regionsRes.body?.regions?.region || []
    const hit = regions.find((r: any) => r.regionId === cfg.ecs.regionId)
    if (!hit) {
      console.error('[失败] 配置中的 regionId 不在返回列表中:', cfg.ecs.regionId)
      process.exit(1)
    }
    console.log('[通过] 地域可用:', cfg.ecs.regionId, hit.localName || '')
  } catch (e: any) {
    console.error('[失败] DescribeRegions:', e.message || e)
    process.exit(1)
  }

  console.log('DescribeImages…', cfg.ecs.imageId)
  try {
    const imgRes = await client.describeImagesWithOptions(
      new $ECS.DescribeImagesRequest({
        regionId: cfg.ecs.regionId,
        imageId: cfg.ecs.imageId,
      }),
      runtime,
    )
    const images = imgRes.body?.images?.image || []
    if (images.length === 0) {
      console.warn('[警告] 未查询到该镜像（可能 ID 错误或无权限），Worker 仍可能启动但创建实例会失败。')
    } else {
      const img = images[0]
      console.log('[通过] 镜像:', img.imageName, '|', img.platform, '|', img.status)
    }
  } catch (e: any) {
    console.warn('[警告] DescribeImages:', e.message || e, '（可检查 RAM 权限）')
  }

  console.log('[完成] ECS API 连通性检查结束。')
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
