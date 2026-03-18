import amqp from "amqplib"
import { createHash, randomUUID } from "crypto"
import { createReadStream, promises as fs } from "fs"
import path from "path"
import mime from "mime-types"
import { prisma } from "@/lib/prisma"
import { BaiduDriveClient } from "@/lib/baidu-drive/client"
import { OSSStorage } from "@/lib/storage/oss"
import { DetectType, publishUploadSuccess } from "@/lib/queue/rabbitmq"
import {
  assertBaiduDownloadQueues,
  BAIDU_DOWNLOAD_QUEUE,
  DEFAULT_MAX_RETRIES,
  publishDeadLetterMessage,
  publishRetryMessage,
  type BaiduDownloadJobMessage,
} from "@/lib/queue/baidu-queue"

const DOWNLOAD_DIR =
  process.env.BAIDU_DOWNLOAD_DIR || path.join(process.cwd(), "tmp", "baidu")
const CHUNK_SIZE_BYTES = Number(
  process.env.BAIDU_DOWNLOAD_CHUNK_SIZE_BYTES || 8 * 1024 * 1024
)
const RETRY_BASE_DELAY_MS = Number(
  process.env.BAIDU_DOWNLOAD_RETRY_BASE_DELAY_MS || 30_000
)
const MAX_RETRIES = Number(
  process.env.BAIDU_DOWNLOAD_MAX_RETRIES || DEFAULT_MAX_RETRIES
)

interface DownloadInfo {
  dlink: string
  size: number
  fileName: string
  md5?: string
}

function sanitizeFileName(name: string) {
  return name.replace(/[\\/:*?"<>|]/g, "_")
}

function normalizeMd5(md5: string | undefined | null): string | null {
  if (!md5) return null
  // 移除非十六进制字符，只保留 0-9, a-f
  const normalized = md5.toLowerCase().replace(/[^0-9a-f]/g, "")
  // 如果长度不是 32，说明格式无效
  if (normalized.length !== 32) {
    console.warn(`[BaiduDownloadWorker] ⚠️  MD5 格式无效: ${md5} (规范化后长度=${normalized.length})`)
    return null
  }
  return normalized
}

async function getDownloadInfo(
  userId: string,
  fsid: number
): Promise<DownloadInfo> {
  const metadata = await BaiduDriveClient.getFileMetadata(userId, [fsid])
  if (!metadata.list || metadata.list.length === 0) {
    throw new Error("Baidu file metadata not found")
  }

  const file = metadata.list[0]
  if (!file.dlink) {
    throw new Error("Baidu dlink missing")
  }

  const accessToken = await BaiduDriveClient.getValidAccessToken(userId)
  return {
    dlink: `${file.dlink}&access_token=${accessToken}`,
    size: file.size,
    fileName: file.filename,
    md5: file.md5,
  }
}

async function ensureDir(dirPath: string) {
  await fs.mkdir(dirPath, { recursive: true })
}

interface DownloadSink {
  prepare(): Promise<number>
  write(offset: number, buffer: Buffer): Promise<void>
  finalize(): Promise<string>
  abort?(): Promise<void>
  getPartialPath(): string
}

class LocalFileDownloadSink implements DownloadSink {
  private fileHandle: Awaited<ReturnType<typeof fs.open>> | null = null

  constructor(
    private partialPath: string,
    private finalPath: string
  ) {}

  getPartialPath() {
    return this.partialPath
  }

  async prepare(): Promise<number> {
    await ensureDir(path.dirname(this.partialPath))
    this.fileHandle = await fs.open(this.partialPath, "a+")
    const stat = await this.fileHandle.stat()
    return stat.size
  }

  async write(offset: number, buffer: Buffer) {
    if (!this.fileHandle) {
      throw new Error("Download sink not prepared")
    }
    await this.fileHandle.write(buffer, 0, buffer.length, offset)
  }

  async finalize(): Promise<string> {
    if (this.fileHandle) {
      await this.fileHandle.close()
      this.fileHandle = null
    }
    await fs.rename(this.partialPath, this.finalPath).catch(async () => {
      await fs.copyFile(this.partialPath, this.finalPath)
      await fs.unlink(this.partialPath)
    })
    return this.finalPath
  }

  async abort() {
    if (this.fileHandle) {
      await this.fileHandle.close()
      this.fileHandle = null
    }
  }
}

async function downloadWithRange(
  dlink: string,
  sink: DownloadSink,
  expectedSize: number,
  onProgress?: (downloaded: number, total: number) => Promise<void>
) {
  let offset = await sink.prepare()
  try {
    while (offset < expectedSize) {
      const end = Math.min(offset + CHUNK_SIZE_BYTES - 1, expectedSize - 1)
      const response = await fetch(dlink, {
        method: "GET",
        headers: {
          "User-Agent": "pan.baidu.com",
          Range: `bytes=${offset}-${end}`,
        },
        redirect: "follow",
      })

      if (!response.ok) {
        const text = await response.text().catch(() => "")
        throw new Error(
          `Baidu download failed: ${response.status} ${response.statusText} ${text}`
        )
      }

      if (offset > 0 && response.status !== 206) {
        throw new Error(`Baidu range response invalid: ${response.status}`)
      }

      const buffer = Buffer.from(await response.arrayBuffer())
      await sink.write(offset, buffer)
      offset += buffer.length

      if (onProgress) {
        await onProgress(offset, expectedSize)
      }
    }
  } finally {
    await sink.abort?.()
  }
}

async function computeMd5(filePath: string) {
  const hash = createHash("md5")
  const stream = createReadStream(filePath)
  for await (const chunk of stream) {
    hash.update(chunk as Buffer)
  }
  return hash.digest("hex")
}

function buildDownloadPaths(jobId: string, fileName: string) {
  const safeName = sanitizeFileName(fileName)
  const baseName = `${jobId}-${safeName}`
  return {
    partial: path.join(DOWNLOAD_DIR, `${baseName}.part`),
    final: path.join(DOWNLOAD_DIR, `${baseName}`),
  }
}

function calcRetryDelayMs(retryCount: number) {
  return RETRY_BASE_DELAY_MS * Math.pow(2, Math.max(0, retryCount - 1))
}

export async function startBaiduDownloadWorker() {
  console.log("[BaiduDownloadWorker] 正在连接 RabbitMQ...")
  const conn = await amqp.connect(process.env.RABBITMQ_URL!)
  const channel = await conn.createChannel()

  await assertBaiduDownloadQueues(channel)
  await channel.prefetch(1)
  console.log("[BaiduDownloadWorker] ✅ Worker 已启动，等待消息...")

  channel.consume(
    BAIDU_DOWNLOAD_QUEUE,
    async (msg) => {
      if (!msg) return
      const raw = msg.content.toString()
      const headers = msg.properties.headers || {}
      const retryCount = Number(headers["x-retry-count"] || 0)
      let payload: BaiduDownloadJobMessage | null = null

      try {
        payload = JSON.parse(raw) as BaiduDownloadJobMessage
        const jobId = payload.job_id
        const fsid = parseInt(payload.baidu_file_id, 10)

        console.log(`[BaiduDownloadWorker] 📥 收到任务 jobId=${jobId}, fsid=${fsid}, retryCount=${retryCount}`)

        await prisma.videoProcessJob.update({
          where: { id: jobId },
          data: {
            status: "DOWNLOADING",
            currentStage: "downloading",
            progressPercentage: 0,
            progressMessage: "准备下载",
            startedAt: new Date(),
          },
        })

        console.log(`[BaiduDownloadWorker] 🔍 获取文件元数据 userId=${payload.user_id}, fsid=${fsid}`)
        const downloadInfo = await getDownloadInfo(payload.user_id, fsid)
        console.log(`[BaiduDownloadWorker] ✅ 文件信息: ${downloadInfo.fileName}, 大小=${downloadInfo.size} bytes`)
        const { partial, final } = buildDownloadPaths(
          jobId,
          downloadInfo.fileName || payload.file_name || `${randomUUID()}.mp4`
        )
        console.log(`[BaiduDownloadWorker] 📁 下载路径: ${final}`)
        const sink = new LocalFileDownloadSink(partial, final)

        console.log(`[BaiduDownloadWorker] ⬇️  开始下载，分片大小=${CHUNK_SIZE_BYTES} bytes`)
        await downloadWithRange(
          downloadInfo.dlink,
          sink,
          downloadInfo.size,
          async (downloaded, total) => {
            const percent = Math.floor((downloaded / total) * 100)
            if (percent % 10 === 0 || downloaded === total) {
              console.log(`[BaiduDownloadWorker] 📊 下载进度: ${percent}% (${downloaded}/${total} bytes)`)
            }
            await prisma.videoProcessJob.update({
              where: { id: jobId },
              data: {
                progressPercentage: percent,
                progressMessage: `已下载 ${percent}%`,
                processOptions: {
                  downloadedBytes: downloaded,
                  totalBytes: total,
                  downloadPath: sink.getPartialPath(),
                },
              },
            })
          }
        )
        console.log(`[BaiduDownloadWorker] ✅ 下载完成: ${final}`)

        const finalPath = await sink.finalize()

        const normalizedExpectedMd5 = normalizeMd5(downloadInfo.md5)
        if (normalizedExpectedMd5) {
          console.log(`[BaiduDownloadWorker] 🔐 开始 MD5 校验...`)
          const localMd5 = await computeMd5(finalPath)
          if (localMd5 !== normalizedExpectedMd5) {
            console.error(`[BaiduDownloadWorker] ❌ MD5 校验失败: 期望=${normalizedExpectedMd5}, 实际=${localMd5}`)
            throw new Error("MD5 mismatch after download")
          }
          console.log(`[BaiduDownloadWorker] ✅ MD5 校验通过: ${localMd5}`)
        } else {
          console.log(`[BaiduDownloadWorker] ⚠️  跳过 MD5 校验（MD5 格式无效或缺失）`)
        }

        await prisma.videoProcessJob.update({
          where: { id: jobId },
          data: {
            status: "UPLOADING",
            currentStage: "uploading",
            progressMessage: "开始上传到 OSS",
          },
        })

        const contentType =
          mime.lookup(downloadInfo.fileName || finalPath) ||
          "application/octet-stream"
        console.log(`[BaiduDownloadWorker] ☁️  开始上传到 OSS, contentType=${contentType}`)
        const ossStorage = new OSSStorage()
        const ossKey = ossStorage.generateObjectKey(
          payload.user_id,
          downloadInfo.fileName || path.basename(finalPath),
          "uploads"
        )
        console.log(`[BaiduDownloadWorker] 📤 OSS Key: ${ossKey}`)
        const uploadUrl = await ossStorage.getPresignedUrl(ossKey, contentType)
        const fileBuffer = await fs.readFile(finalPath)
        console.log(`[BaiduDownloadWorker] 📦 上传文件大小: ${fileBuffer.length} bytes`)
        const uploadResponse = await fetch(uploadUrl, {
          method: "PUT",
          headers: {
            "Content-Type": contentType,
          },
          body: fileBuffer,
        })
        if (!uploadResponse.ok) {
          console.error(`[BaiduDownloadWorker] ❌ OSS 上传失败: ${uploadResponse.status} ${uploadResponse.statusText}`)
          throw new Error(
            `OSS upload failed: ${uploadResponse.status} ${uploadResponse.statusText}`
          )
        }
        console.log(`[BaiduDownloadWorker] ✅ OSS 上传成功，确认上传...`)
        const confirmResult = await ossStorage.confirmUpload(ossKey)
        if (!confirmResult.exists) {
          console.error(`[BaiduDownloadWorker] ❌ OSS 上传确认失败`)
          throw new Error("OSS upload not confirmed")
        }
        const ossUrl = await ossStorage.getDownloadUrl(ossKey, contentType)
        console.log(`[BaiduDownloadWorker] ✅ OSS 上传完成: ${ossUrl}`)

        const processedKey = ossStorage.generateObjectKey(
          payload.user_id,
          downloadInfo.fileName || path.basename(finalPath),
          "processed"
        )
        const processedUploadUrl = await ossStorage.getPresignedUrl(
          processedKey,
          contentType
        )
        const webhookUrl = `${process.env.NEXT_PUBLIC_APP_URL || "http://localhost:3000"}/api/webhooks/video-processing`

        console.log(`[BaiduDownloadWorker] 📩 发送处理任务到 RabbitMQ...`)
        const publishResult = await publishUploadSuccess({
          video_download_url: ossUrl,
          video_upload_url: processedUploadUrl,
          webhook_url: webhookUrl,
          user_id: payload.user_id,
          detect_type: DetectType.AUTO,
        })

        if (!publishResult.success) {
          throw new Error("RabbitMQ publish failed")
        }
        console.log(`[BaiduDownloadWorker] ✅ RabbitMQ 投递成功 messageId=${publishResult.messageId}`)

        await prisma.videoProcessJob.update({
          where: { id: jobId },
          data: {
            status: "COMPLETED",
            progressPercentage: 100,
            progressMessage: "上传完成",
            currentStage: "uploaded",
            outputVideoUrl: ossUrl,
            outputS3Key: ossKey,
            completedAt: new Date(),
            errorMessage: null,
            retryCount,
            processOptions: {
              downloadPath: finalPath,
              ossKey,
              ossUrl,
              downloadBytes: downloadInfo.size,
              md5: downloadInfo.md5,
            },
          },
        })

        console.log(`[BaiduDownloadWorker] 🎉 任务完成 jobId=${jobId}`)
        channel.ack(msg)
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error)
        if (!payload) {
          console.error(`[BaiduDownloadWorker] ❌ 消息解析失败:`, error)
          channel.nack(msg, false, false)
          return
        }

        console.error(`[BaiduDownloadWorker] ❌ 任务失败 jobId=${payload.job_id}, retryCount=${retryCount}, error=${errorMessage}`)

        if (retryCount < MAX_RETRIES) {
          const nextRetry = retryCount + 1
          const delayMs = calcRetryDelayMs(nextRetry)

          console.log(`[BaiduDownloadWorker] 🔄 准备重试 jobId=${payload.job_id}, 第${nextRetry}次重试, ${delayMs / 1000}s 后执行`)

          await prisma.videoProcessJob.update({
            where: { id: payload.job_id },
            data: {
              status: "QUEUED",
              retryCount: nextRetry,
              errorMessage: errorMessage,
              progressMessage: `下载失败，${delayMs / 1000}s 后重试`,
            },
          })

          await publishRetryMessage(
            channel,
            payload,
            nextRetry,
            delayMs,
            errorMessage
          )
          channel.ack(msg)
          return
        }

        console.error(`[BaiduDownloadWorker] 💀 达到最大重试次数，进入死信队列 jobId=${payload.job_id}`)
        await prisma.videoProcessJob.update({
          where: { id: payload.job_id },
          data: {
            status: "FAILED",
            retryCount,
            errorMessage: errorMessage,
            progressMessage: "下载失败，已进入死信队列",
          },
        })

        await publishDeadLetterMessage(
          channel,
          payload,
          retryCount,
          errorMessage
        )
        channel.ack(msg)
      }
    },
    { noAck: false }
  )
}
