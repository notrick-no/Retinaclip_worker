#!/usr/bin/env npx tsx
/**
 * 测试 RabbitMQ：连接、声明队列（与 Worker 使用相同 .env）
 * 用法: npm run test:rabbitmq
 */
import 'dotenv/config'
import amqp from 'amqplib'
import { loadConfig } from '../config'

async function main() {
  let cfg
  try {
    cfg = loadConfig()
  } catch (e) {
    console.error('[失败] 配置加载:', e instanceof Error ? e.message : e)
    process.exit(1)
  }

  const masked = cfg.rabbitmq.url.replace(/:[^:@/]+@/, ':***@')
  console.log('正在连接 RabbitMQ…', masked)

  let conn: Awaited<ReturnType<typeof amqp.connect>>
  try {
    conn = await amqp.connect(cfg.rabbitmq.url)
  } catch (e) {
    console.error('[失败] 无法连接:', e instanceof Error ? e.message : e)
    process.exit(1)
  }

  try {
    const ch = await conn.createChannel()
    await ch.assertQueue(cfg.rabbitmq.queue, { durable: true })
    console.log('[通过] 队列已就绪:', cfg.rabbitmq.queue)
    await ch.close()
  } catch (e) {
    console.error('[失败] Channel/队列:', e instanceof Error ? e.message : e)
    await conn.close().catch(() => {})
    process.exit(1)
  }

  await conn.close()
  console.log('[完成] 连接已关闭。')
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
