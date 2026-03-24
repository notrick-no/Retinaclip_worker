#!/usr/bin/env npx tsx
/**
 * 校验 .env 能否被 loadConfig() 正确解析（不调用外网 API）
 * 用法: npm run test:config
 */
import 'dotenv/config'
import { loadConfig } from '../config'

function mask(s: string, keepStart = 4): string {
  if (!s || s.length <= keepStart) return '***'
  return `${s.slice(0, keepStart)}…(${s.length} chars)`
}

function main() {
  let cfg
  try {
    cfg = loadConfig()
  } catch (e) {
    console.error('[失败] 配置解析错误:', e instanceof Error ? e.message : e)
    console.error('请对照 env_example 检查必填项与 JSON 环境变量格式。')
    process.exit(1)
  }

  console.log('[通过] loadConfig() 解析成功\n')
  console.log('── RabbitMQ ──')
  console.log('  url            ', cfg.rabbitmq.url.replace(/:[^:@/]+@/, ':***@'))
  console.log('  queue          ', cfg.rabbitmq.queue)
  console.log('  prefetchCount  ', cfg.rabbitmq.prefetchCount)

  console.log('\n── 池调度（仅 poolEnabled 时使用）──')
  console.log('  pollIntervalMs ', cfg.scheduler.pollIntervalMs)
  console.log('  poolProfile    ', cfg.scheduler.poolProfile ?? '(未设置)')

  console.log('\n── ECS ──')
  console.log('  regionId       ', cfg.ecs.regionId)
  console.log('  zoneId         ', cfg.ecs.zoneId || '(未设置)')
  console.log('  imageId        ', cfg.ecs.imageId)
  console.log('  instanceType   ', cfg.ecs.instanceType)
  console.log('  instanceFallback', cfg.ecs.instanceTypeFallback.length ? cfg.ecs.instanceTypeFallback.join(', ') : '(无)')
  console.log('  securityGroup  ', cfg.ecs.securityGroupId)
  console.log('  vswitch        ', cfg.ecs.vswitchId)
  console.log('  namePrefix     ', cfg.ecs.instanceNamePrefix)
  console.log('  maxInstances   ', cfg.ecs.maxInstances)
  console.log('  requirePubIp   ', cfg.ecs.requirePublicIpForTasks)
  console.log('  autoAllocPubIp ', cfg.ecs.autoAllocatePublicIp)
  console.log('  spot           ', cfg.ecs.useSpotInstance, cfg.ecs.spotStrategy)
  console.log('  poolEnabled    ', cfg.ecs.poolEnabled)
  console.log('  poolLifecycle  ', cfg.ecs.poolLifecycleTagValue)
  console.log('  poolProfileKey ', cfg.ecs.poolProfileTagKey)
  console.log('  poolProfileFilter', cfg.ecs.poolProfileFilterEnabled)
  console.log('  poolBootDelayMs ', cfg.ecs.poolBootDelayMs)
  console.log('  poolBootstrapMsgId', cfg.ecs.poolBootstrapMessageId)
  console.log('  poolBootDetached ', cfg.ecs.poolBootstrapDockerDetached)
  console.log('  poolDockerCtr   ', cfg.ecs.poolDockerContainer ?? '(未设置)')
  console.log('  poolBootCmd set ', Boolean(cfg.ecs.poolBootCommand))
  console.log('  mockProcessing ', cfg.ecs.mockProcessing ?? false)
  console.log('  userdataDocker ', cfg.ecs.userdataDockerPolicy)
  console.log('  prefilterStock ', cfg.ecs.prefilterAvailableResource)
  console.log(
    '  dockerInsecure ',
    cfg.ecs.dockerInsecureRegistries.length
      ? cfg.ecs.dockerInsecureRegistries.join(', ')
      : '(无，仍会从镜像 host:port 自动推断)',
  )
  console.log('  dockerNetwork   ', cfg.ecs.dockerNetworkMode)
  console.log(
    '  dockerPublish   ',
    cfg.ecs.dockerPublishPorts.length ? cfg.ecs.dockerPublishPorts.join(', ') : '(无)',
  )
  console.log(
    '  dockerVolume    ',
    cfg.ecs.dockerVolumeHost
      ? `${cfg.ecs.dockerVolumeHost} -> ${cfg.ecs.dockerVolumeContainer}`
      : '(无)',
  )
  console.log('  dockerBashCmd   ', cfg.ecs.dockerBashCommand || '(镜像默认 CMD)')

  console.log('\n── 处理镜像路由 ──')
  console.log('  defaultImage   ', cfg.processing.defaultImage)
  const imKeys = Object.keys(cfg.processing.imageMap)
  const ppKeys = Object.keys(cfg.processing.poolProfileMap)
  console.log('  imageMap 条数  ', imKeys.length, imKeys.length ? `e.g. ${imKeys[0]}` : '')
  console.log('  poolProfile条数', ppKeys.length, ppKeys.length ? `e.g. ${ppKeys[0]}` : '')

  console.log('\n── 凭证（已打码）──')
  console.log('  ALIBABA_CLOUD_ACCESS_KEY_ID    ', mask(cfg.ecs.accessKeyId))
  console.log('  ALIBABA_CLOUD_ACCESS_KEY_SECRET', mask(cfg.ecs.accessKeySecret, 0))
  console.log('  WEBHOOK_SECRET                 ', mask(cfg.webhook.secret, 0))

  console.log('\n[完成] 仅内存校验，未发起网络请求。')
}

main()
