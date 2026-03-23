import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { loadConfig } from '../config'

function setEnv(values: Record<string, string | undefined>) {
  for (const [k, v] of Object.entries(values)) {
    if (v === undefined) delete process.env[k]
    else process.env[k] = v
  }
}

describe('config.ts', () => {
  beforeEach(() => {
    setEnv({
      ALIBABA_CLOUD_ACCESS_KEY_ID: 'ak',
      ALIBABA_CLOUD_ACCESS_KEY_SECRET: 'sk',
      ALIYUN_ECS_IMAGE_ID: 'img-123',
      ALIYUN_ECS_SECURITY_GROUP_ID: 'sg-1',
      ALIYUN_ECS_VSWITCH_ID: 'vs-1',
      WEBHOOK_SECRET: 'wh-secret',

      // 可选项留空，使用默认值
      WORKER_PREFETCH_COUNT: undefined,
      WORKER_RETRY_MAX_ATTEMPTS: undefined,
      WORKER_TASK_TIMEOUT: undefined,
    })
  })

  afterEach(() => {
    // 清理关键 env，避免影响其它用例
    setEnv({
      ALIBABA_CLOUD_ACCESS_KEY_ID: undefined,
      ALIBABA_CLOUD_ACCESS_KEY_SECRET: undefined,
      ALIYUN_ECS_IMAGE_ID: undefined,
      ALIYUN_ECS_SECURITY_GROUP_ID: undefined,
      ALIYUN_ECS_VSWITCH_ID: undefined,
      WEBHOOK_SECRET: undefined,
      WORKER_PROCESSING_IMAGE_MAP: undefined,
    })
  })

  it('缺少 WEBHOOK_SECRET 时启动失败（可读错误）', () => {
    setEnv({ WEBHOOK_SECRET: undefined })
    expect(() => loadConfig()).toThrow(/缺少必填配置：WEBHOOK_SECRET/)
  })

  it('WORKER_PREFETCH_COUNT < 1 会报错', () => {
    setEnv({ WORKER_PREFETCH_COUNT: '0' })
    expect(() => loadConfig()).toThrow(/不能小于 1/)
  })

  it('解析成功并带默认值', () => {
    const cfg = loadConfig()
    expect(cfg.rabbitmq.queue).toBe('media.uploaded')
    expect(cfg.rabbitmq.prefetchCount).toBe(2)
    expect(cfg.webhook.secret).toBe('wh-secret')
    expect(cfg.processing.defaultImage).toBe('retinaclip-processor:latest')
    expect(cfg.ecs.poolProfileTagKey).toBe('retinaclip:pool-profile')
    expect(cfg.ecs.userdataDockerPolicy).toBe('auto')
  })

  it('WORKER_PROCESSING_IMAGE_MAP 解析为对象', () => {
    setEnv({
      WORKER_PROCESSING_IMAGE_MAP: JSON.stringify({ 'a@normal': 'img:1', skip: 1 }),
    })
    const cfg = loadConfig()
    expect(cfg.processing.imageMap['a@normal']).toBe('img:1')
    expect(cfg.processing.imageMap['skip']).toBeUndefined()
  })
})

