import { beforeEach, describe, expect, it, vi } from 'vitest'

const connectMock = vi.fn()
let consumeCallback: ((msg: any) => Promise<void>) | null = null
const ackMock = vi.fn()

const sendWebhookMock = vi.fn().mockResolvedValue(true)

vi.mock('amqplib', () => {
  return {
    default: {
      connect: connectMock,
    },
  }
})

vi.mock('../ecs-orchestrator', () => {
  return {
    ECSOrchestrator: class {},
  }
})

vi.mock('../webhook-sender', async () => {
  const actual = await vi.importActual('../webhook-sender')
  return {
    ...actual,
    sendWebhook: sendWebhookMock,
  }
})

const { QueueConsumer } = await import('../queue-consumer')

describe('queue-consumer.ts', () => {
  beforeEach(() => {
    connectMock.mockReset()
    consumeCallback = null
    ackMock.mockReset()
    sendWebhookMock.mockReset()
  })

  it('收到合法消息时应 ack，并先发送 processing_started 再 completed；且调用 orchestrator.runTask(attempt=0)', async () => {
    const fakeChannel = {
      prefetch: vi.fn().mockResolvedValue(undefined),
      assertQueue: vi.fn().mockResolvedValue(undefined),
      consume: vi.fn().mockImplementation(async (_queue: string, cb: any) => {
        consumeCallback = cb
        return { consumerTag: 'ctag-1' }
      }),
      ack: ackMock,
      on: vi.fn(),
      cancel: vi.fn(),
      close: vi.fn(),
    }

    const fakeConnection = {
      createChannel: vi.fn().mockResolvedValue(fakeChannel),
      on: vi.fn(),
      close: vi.fn(),
    }

    connectMock.mockResolvedValue(fakeConnection)

    const orchestratorMock = {
      runTask: vi.fn().mockResolvedValue({
        success: true,
        outputVideoUrl: 'http://out.mp4',
        durationSeconds: 1.23,
        instanceId: 'i-1',
      }),
    }

    const config: any = {
      processing: {
        defaultImage: 'mingle-processor:latest',
        imageMap: {},
        poolProfileMap: {},
      },
      rabbitmq: {
        url: 'amqp://x',
        queue: 'media.uploaded',
        prefetchCount: 1,
        reconnectInterval: 1000,
        maxReconnectAttempts: 0,
      },
      ecs: {
        accessKeyId: 'ak',
        accessKeySecret: 'sk',
        regionId: 'cn-shanghai',
        imageId: 'img',
        instanceType: 'ecs.gn5i-c2g1.large',
        securityGroupId: 'sg-1',
        vswitchId: 'vs-1',
        instanceNamePrefix: 'mingle',
        maxInstances: 1,
        systemDiskSize: 40,
        systemDiskCategory: 'cloud_essd',
        useSpotInstance: true,
        spotStrategy: 'SpotAsPriceGo',
        internetMaxBandwidthOut: 100,
        taskTimeout: 30 * 60 * 1000,
        instanceStartTimeout: 5 * 60 * 1000,
        pollInterval: 1000,
        mockProcessing: true,
        poolEnabled: false,
        poolLifecycleTagValue: 'pool',
        poolProfileTagKey: 'mingle:pool-profile',
        instanceTypeFallback: [],
        userdataDockerPolicy: 'auto',
        prefilterAvailableResource: false,
      },
      webhook: {
        secret: 'wh-secret',
        timeout: 10000,
        maxRetries: 0,
        retryBaseInterval: 1000,
      },
      retry: {
        maxAttempts: 0,
        baseInterval: 1000,
        maxInterval: 1000,
      },
      healthCheck: {
        interval: 10000,
        zombieCheckInterval: 10000,
      },
      shutdown: { graceMs: 2000 },
      log: { level: 'error' },
    }

    const consumer = new QueueConsumer(config, orchestratorMock as any)
    await consumer.start()

    expect(typeof consumeCallback).toBe('function')

    const message: any = {
      content: Buffer.from(
        JSON.stringify({
          message_id: 'm-1',
          video_download_url: 'http://in.mp4',
          webhook_url: 'http://hook',
          detect_type: 'auto',
          target_regions: [],
          user_id: 'u1',
        }),
      ),
      properties: { messageId: 'prop-1' },
    }

    await consumeCallback!(message)

    expect(ackMock).toHaveBeenCalledTimes(1)

    // sendWebhook 调用 2 次：processing_started + completed
    expect(sendWebhookMock).toHaveBeenCalledTimes(2)
    const firstPayload = sendWebhookMock.mock.calls[0][1]
    const secondPayload = sendWebhookMock.mock.calls[1][1]
    expect(firstPayload.event_type).toBe('processing_started')
    expect(secondPayload.event_type).toBe('completed')

    expect(orchestratorMock.runTask).toHaveBeenCalledTimes(1)
    expect(orchestratorMock.runTask.mock.calls[0][2]).toBe(0)
  })

  it('非法 JSON 时应 ack 且不调用 runTask', async () => {
    const fakeChannel = {
      prefetch: vi.fn().mockResolvedValue(undefined),
      assertQueue: vi.fn().mockResolvedValue(undefined),
      consume: vi.fn().mockImplementation(async (_queue: string, cb: any) => {
        consumeCallback = cb
        return { consumerTag: 'ctag-1' }
      }),
      ack: ackMock,
      on: vi.fn(),
      cancel: vi.fn(),
      close: vi.fn(),
    }

    const fakeConnection = {
      createChannel: vi.fn().mockResolvedValue(fakeChannel),
      on: vi.fn(),
      close: vi.fn(),
    }

    connectMock.mockResolvedValue(fakeConnection)

    const orchestratorMock = {
      runTask: vi.fn(),
    }

    const config: any = {
      processing: {
        defaultImage: 'mingle-processor:latest',
        imageMap: {},
        poolProfileMap: {},
      },
      rabbitmq: {
        url: 'amqp://x',
        queue: 'media.uploaded',
        prefetchCount: 1,
        reconnectInterval: 1000,
        maxReconnectAttempts: 0,
      },
      ecs: {
        accessKeyId: 'ak',
        accessKeySecret: 'sk',
        regionId: 'cn-shanghai',
        imageId: 'img',
        instanceType: 'ecs.gn5i-c2g1.large',
        securityGroupId: 'sg-1',
        vswitchId: 'vs-1',
        instanceNamePrefix: 'mingle',
        maxInstances: 1,
        systemDiskSize: 40,
        systemDiskCategory: 'cloud_essd',
        useSpotInstance: true,
        spotStrategy: 'SpotAsPriceGo',
        internetMaxBandwidthOut: 100,
        taskTimeout: 30 * 60 * 1000,
        instanceStartTimeout: 5 * 60 * 1000,
        pollInterval: 1000,
        mockProcessing: true,
        poolEnabled: false,
        poolLifecycleTagValue: 'pool',
        poolProfileTagKey: 'mingle:pool-profile',
        instanceTypeFallback: [],
        userdataDockerPolicy: 'auto',
        prefilterAvailableResource: false,
      },
      webhook: {
        secret: 'wh-secret',
        timeout: 10000,
        maxRetries: 0,
        retryBaseInterval: 1000,
      },
      retry: {
        maxAttempts: 0,
        baseInterval: 1000,
        maxInterval: 1000,
      },
      healthCheck: {
        interval: 10000,
        zombieCheckInterval: 10000,
      },
      shutdown: { graceMs: 2000 },
      log: { level: 'error' },
    }

    const consumer = new QueueConsumer(config, orchestratorMock as any)
    await consumer.start()
    await consumeCallback!({
      content: Buffer.from('NOT_JSON'),
      properties: { messageId: 'prop-1' },
    })

    expect(ackMock).toHaveBeenCalledTimes(1)
    expect(orchestratorMock.runTask).not.toHaveBeenCalled()
  })
})

