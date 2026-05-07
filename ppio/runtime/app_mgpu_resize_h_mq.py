"""
消费远端 RabbitMQ media 队列：下载源视频至 mq_ingest → /upload-from-staging（同机 mv）或回退 /upload →
POST /process（携带 video_upload_url 由处理服务直传 OSS）→ 轮询 /status → webhook；
若后端未直传，则回退由本进程 GET /results 再 PUT OSS。
"""
import pika
from pika import exceptions as pika_exceptions
import json
import requests
import os
import logging
import random
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import time
import re
import uuid
import hmac
import hashlib
from datetime import datetime
from typing import Optional

from mgpu_mq_config import (
    MQ_HTTP_TIMEOUT_DOWNLOAD_SEC,
    MQ_HTTP_TIMEOUT_GPU_CAPACITY_SEC,
    MQ_HTTP_TIMEOUT_OSS_PUT_SEC,
    MQ_HTTP_TIMEOUT_PROCESS_POST_SEC,
    MQ_HTTP_TIMEOUT_RESULT_DOWNLOAD_SEC,
    MQ_HTTP_TIMEOUT_STATUS_POLL_SEC,
    MQ_HTTP_TIMEOUT_UPLOAD_BACKEND_SEC,
    MQ_HTTP_TIMEOUT_WEBHOOK_SEC,
    MQ_PREFETCH_COUNT,
    MQ_RABBITMQ_HEARTBEAT_SEC,
    MQ_UPLOAD_MAX_RETRIES,
    MQ_WORKER_PROGRESS_WEBHOOK_INTERVAL_POLLS,
    MQ_WORKER_STATUS_MAX_POLLS,
    MQ_WORKER_STATUS_POLL_INTERVAL_SEC,
    WEBHOOK_SECRET,
    get_backend_url,
    get_media_queue_name,
    get_mq_ingest_dir,
    get_mq_worker_gpu_wait_poll_sec,
    get_rabbitmq_url,
    mq_worker_should_wait_for_gpu,
)

# 与 app_mgpu_resize_h_mq / mgpu_mq_config 对齐（环境变量覆盖默认值）
RABBITMQ_URL = get_rabbitmq_url()
BACKEND_URL = get_backend_url()
QUEUE_NAME = get_media_queue_name()

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_QUEUE_WORKER_LOG = os.path.join(_APP_DIR, "queue_worker.log")

# 配置日志（文件始终写在 app 目录下，与启动 cwd 无关）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_QUEUE_WORKER_LOG, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)
# Pika 默认 INFO 会刷屏连接/AMQP 细节，无问题时压到 WARNING
logging.getLogger("pika").setLevel(logging.WARNING)


def _build_webhook_payload(*, message_id: str, user_id, status: str, event_type: str, **extra):
    """
    按本仓库 TypeScript/WebhookPayload 的语义 + Prisma 需要的 queueJobId 一起拼 payload。
    """
    payload = {
        "job_id": message_id,
        "queue_job_id": message_id,
        "queueJobId": message_id,
        "user_id": user_id,
        "status": status,
        "event_type": event_type,
    }
    payload.update(extra)
    # 避免把 None 序列化进请求体（有些服务端会做严格校验）
    return {k: v for k, v in payload.items() if v is not None}


def _resolve_queue_job_id(data: dict, properties) -> Optional[str]:
    """与 worker_simulation 一致：message_id ?? AMQP BasicProperties.message_id"""
    mid = data.get("message_id")
    if mid is not None and str(mid).strip():
        return str(mid).strip()
    if properties is None:
        return None
    raw = getattr(properties, "message_id", None)
    if not raw:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace").strip() or None
    return str(raw).strip() or None


def _normalize_detect_type(data: dict) -> str:
    """
    与 worker_simulation 的 detect_type?: 'auto' | 'manual' 对齐；
    内部仍用 manual / automatic 驱动坐标逻辑。
    """
    raw = str(data.get("detect_type") or "").strip().lower()
    if raw in ("", "auto", "automatic"):
        return "automatic"
    if raw == "manual":
        return "manual"
    logger.warning("detect_type 非法值: %s，已按 auto 处理", raw)
    return "automatic"


def _build_oss_presign_diag(upload_url: str):
    """提取预签名 URL 的关键字段，便于与签发端对齐。"""
    try:
        parsed = urlparse(upload_url or "")
        query = parse_qs(parsed.query or "", keep_blank_values=True)
        path_preview = parsed.path or ""
        if len(path_preview) > 120:
            path_preview = f"...{path_preview[-120:]}"
        credential = (query.get("x-oss-credential") or [""])[0]
        if credential and len(credential) > 36:
            credential = f"{credential[:18]}...{credential[-18:]}"
        return {
            "host": parsed.netloc,
            "path_preview": path_preview,
            "x_oss_date": (query.get("x-oss-date") or [""])[0],
            "x_oss_expires": (query.get("x-oss-expires") or [""])[0],
            "x_oss_signature_version": (query.get("x-oss-signature-version") or [""])[0],
            "x_oss_credential_preview": credential,
            "query_key_count": len(query.keys()),
        }
    except Exception:
        return {"parse_error": True}


class VideoQueueWorker:
    def __init__(self, rabbitmq_url, backend_url, queue_name=QUEUE_NAME):
        """
        初始化消息队列工作者

        Args:
            rabbitmq_url: RabbitMQ连接URL
            backend_url: 同机处理服务（Flask）基址，如 http://127.0.0.1:2026
            queue_name: 队列名称
        """
        self.rabbitmq_url = rabbitmq_url
        self.backend_url = backend_url.rstrip("/")
        self.queue_name = queue_name

        # 临时下载目录
        self.temp_dir = Path("./temp_downloads")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.ingest_dir = Path(get_mq_ingest_dir())
        self.ingest_dir.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.channel = None
        # 处理服务 HTTP（BACKEND_URL，常与 127.0.0.1 同机）：绕过代理，避免本机请求被转发成 503
        self.backend_session = requests.Session()
        self.backend_session.trust_env = False
        # 访问预签名 OSS URL 时禁用代理变量，避免签名链路被中间层改写
        self.oss_session = requests.Session()
        self.oss_session.trust_env = False

    def _yield_amqp_io(self):
        """
        BlockingConnection 在 basic_consume 回调里若长时间阻塞（下载/上传），
        同一线程无法处理 broker 心跳，易被踢。在耗时循环中周期性调用本方法。
        """
        conn = self.connection
        if not conn or not getattr(conn, "is_open", False):
            return
        try:
            if hasattr(conn, "sleep"):
                conn.sleep(0.001)
            else:
                conn.process_data_events(time_limit=0)
        except Exception:
            pass

    def _wait_backend_gpu_capacity(self):
        """
        在后端 GPU 处理槽位已满时阻塞等待，避免在占满 GPU 时仍继续下载/提交新任务。
        与 app_mgpu_resize_h_mq 中 /api/worker/gpu-capacity 及 gpu_admission_* 协同。
        """
        if not mq_worker_should_wait_for_gpu():
            return
        poll = get_mq_worker_gpu_wait_poll_sec()
        url = f"{self.backend_url}/api/worker/gpu-capacity"
        warned = False
        while True:
            try:
                r = self.backend_session.get(url, timeout=MQ_HTTP_TIMEOUT_GPU_CAPACITY_SEC)
                if r.status_code == 200:
                    j = r.json() if r.content else {}
                    if int(j.get("available_slots", 0)) > 0:
                        if warned:
                            logger.info("后端 GPU 槽位已可用，继续处理队列消息")
                        return
            except Exception as e:
                logger.warning("查询 GPU 容量失败: %s", e)
            if not warned:
                logger.info(
                    "后端无可用 GPU 处理槽位，暂停下载/提交（消息仍由 RabbitMQ prefetch 持有，不 ack）"
                )
                warned = True
            self._yield_amqp_io()
            time.sleep(poll)

    def connect(self):
        """连接到RabbitMQ"""
        try:
            logger.info("连接到RabbitMQ...")
            connection_params = pika.URLParameters(self.rabbitmq_url)
            connection_params.heartbeat = MQ_RABBITMQ_HEARTBEAT_SEC

            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=MQ_PREFETCH_COUNT)

            logger.info("✅  RabbitMQ连接成功")
            logger.info("队列: %s", self.queue_name)
            logger.info("后端API: %s", self.backend_url)
            return True
        except Exception as e:
            logger.error("❌  连接失败: %s", e)
            return False

    def download_video(self, url, save_path: Path):
        """下载视频文件"""
        try:
            logger.info("开始下载视频: %s...", url[:80])

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Accept-Encoding": "identity",
            }

            response = requests.get(url, headers=headers, stream=True, timeout=MQ_HTTP_TIMEOUT_DOWNLOAD_SEC)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            downloaded = 0
            start_time = time.time()

            chunk_i = 0
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    chunk_i += 1
                    if chunk_i % 512 == 0:
                        self._yield_amqp_io()

                    if total_size > 0 and downloaded % (5 * 1024 * 1024) < 8192:
                        percent = (downloaded / total_size) * 100
                        elapsed = time.time() - start_time
                        speed = downloaded / elapsed / 1024 / 1024 if elapsed > 0 else 0
                        logger.info("下载进度: %.1f%% | 速度: %.2f MB/s", percent, speed)

            if save_path.exists():
                actual_size = save_path.stat().st_size
                logger.info("✅  下载完成: %s", save_path)
                logger.info("文件大小: %.2f MB", actual_size / 1024 / 1024)
                return True

            logger.error("❌  下载后文件不存在")
            return False
        except Exception as e:
            logger.error("❌  下载失败: %s", e)
            return False

    def _staging_video_name(self, message_id: str) -> str:
        mid = re.sub(r"[^A-Za-z0-9_-]+", "_", str(message_id)).strip("_")[:100] or "job"
        return f"mq_{mid}_{uuid.uuid4().hex[:12]}.mp4"

    def upload_to_backend(self, file_path: Path, coordinates, mq_meta: dict):
        """
        优先 POST /upload-from-staging（同机 mv，零整文件 HTTP 拷贝）；
        若处理机无该 staging 文件（异机部署），回退 multipart /upload。
        POST /process 时附带 video_upload_url、job_id，由处理服务直接 PUT OSS，避免 worker 再 GET /results。
        """
        message_id = mq_meta.get("message_id")
        if not message_id:
            logger.error("upload_to_backend: mq_meta 缺少 message_id")
            return None
        video_upload_url = mq_meta.get("video_upload_url")
        bottom_process_height = mq_meta.get("bottom_process_height")

        upload_result = None
        staging_url = f"{self.backend_url}/upload-from-staging"
        try:
            r = self.backend_session.post(
                staging_url,
                json={"staging_filename": file_path.name},
                timeout=MQ_HTTP_TIMEOUT_UPLOAD_BACKEND_SEC,
            )
            if r.status_code == 200:
                upload_result = r.json()
                logger.info("已使用 upload-from-staging 登记输入视频（同机 mv）")
            else:
                err = {}
                try:
                    err = r.json() if r.content else {}
                except Exception:
                    pass
                if r.status_code == 404 and err.get("error") == "staging_not_found":
                    logger.info("staging 在处理机不存在，回退 multipart /upload（异机或路径不一致）")
                else:
                    logger.warning(
                        "upload-from-staging 失败 status=%s body=%s，回退 multipart",
                        r.status_code,
                        (r.text or "")[:400],
                    )
        except requests.RequestException as e:
            logger.warning("upload-from-staging 请求异常，回退 multipart: %s", e)

        if upload_result is None:
            upload_url = f"{self.backend_url}/upload"
            max_upload_retries = MQ_UPLOAD_MAX_RETRIES
            for attempt in range(1, max_upload_retries + 1):
                try:
                    with open(file_path, "rb") as f:
                        files = {"file": (os.path.basename(file_path), f, "video/mp4")}
                        response = self.backend_session.post(
                            upload_url, files=files, timeout=MQ_HTTP_TIMEOUT_UPLOAD_BACKEND_SEC
                        )

                    if response.status_code == 200:
                        upload_result = response.json()
                        break

                    body_preview = (response.text or "").strip()[:300]
                    logger.error(
                        "上传失败(第 %s/%s 次): status=%s, server=%s, body=%s",
                        attempt,
                        max_upload_retries,
                        response.status_code,
                        response.headers.get("Server", "unknown"),
                        body_preview or "<empty>",
                    )
                    if response.status_code < 500 or attempt == max_upload_retries:
                        return None
                except requests.RequestException as e:
                    logger.error(
                        "上传请求异常(第 %s/%s 次): %s",
                        attempt,
                        max_upload_retries,
                        e,
                    )
                    if attempt == max_upload_retries:
                        return None

                self._yield_amqp_io()
                time.sleep(min(2 ** (attempt - 1), 5))

        try:
            filename = (upload_result or {}).get("filename")
            if not filename:
                logger.error("上传响应中没有文件名: %s", upload_result)
                return None

            logger.info("✅  输入视频已就绪，文件名: %s", filename)

            process_url = f"{self.backend_url}/process"
            process_data = {
                "filename": filename,
                "coordinates": coordinates,
                "job_id": message_id,
                "message_id": message_id,
            }
            if video_upload_url:
                process_data["video_upload_url"] = video_upload_url
            if bottom_process_height is not None:
                process_data["bottom_process_height"] = bottom_process_height

            process_response = self.backend_session.post(
                process_url, json=process_data, timeout=MQ_HTTP_TIMEOUT_PROCESS_POST_SEC
            )
            if process_response.status_code != 202:
                logger.error(
                    "触发处理失败: %s - %s",
                    process_response.status_code,
                    process_response.text,
                )
                return None

            process_result = process_response.json()
            job_id = process_result.get("job_id")
            logger.info("✅  处理任务已提交，任务ID: %s", job_id)

            return {"job_id": job_id, "filename": filename, "upload_info": upload_result}

        except Exception as e:
            logger.error("上传到后端失败: %s", e)
            return None

    def resolve_coordinates(self, data: dict):
        """
        从队列消息解析坐标，优先级：
        1) target_regions[0]
        2) coordinates
        3) 默认坐标
        """
        default_coordinates = {"x": 100, "y": 100, "width": 400, "height": 100}

        def _normalize_tuple(region_list):
            # 兼容 schema 描述的数组格式: [identifier, x, y, width, height]
            if not isinstance(region_list, (list, tuple)) or len(region_list) < 5:
                return None
            try:
                x = int(region_list[1])
                y = int(region_list[2])
                width = int(region_list[3])
                height = int(region_list[4])
                if width <= 0 or height <= 0:
                    return None
                return {"x": x, "y": y, "width": width, "height": height}
            except (TypeError, ValueError):
                return None

        def _normalize(region):
            if not isinstance(region, dict):
                return None
            try:
                x = int(region.get("x"))
                y = int(region.get("y"))
                width = int(region.get("width"))
                height = int(region.get("height"))
                if width <= 0 or height <= 0:
                    return None
                return {"x": x, "y": y, "width": width, "height": height}
            except (TypeError, ValueError):
                return None

        target_regions = data.get("target_regions")
        if isinstance(target_regions, list) and target_regions:
            first_raw_region = target_regions[0]
            first_region = _normalize(first_raw_region)
            if not first_region:
                first_region = _normalize_tuple(first_raw_region)
            if first_region:
                return first_region

        coordinates = _normalize(data.get("coordinates"))
        if coordinates:
            return coordinates

        return default_coordinates

    def send_webhook_notification(self, webhook_url: str, payload: dict, *, event_type: str):
        """
        发送带 HMAC 签名的 Webhook 通知
        关键点：payload_str 必须和签名使用的是同一份字符串。
        """
        if not webhook_url:
            print("⚠️ 错误: Webhook URL 为空，跳过发送")
            return True

        webhook_secret = WEBHOOK_SECRET.strip()
        if not webhook_secret:
            logger.warning("WEBHOOK_SECRET 为空，跳过 Webhook")
            return False

        payload_str = json.dumps(payload)
        mac = hmac.new(webhook_secret.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256)
        signature = f"sha256={mac.hexdigest()}"

        print("🚀  正在发送 Webhook...")
        print(f"   目标地址: {webhook_url}")
        print(f"   请求头签名: {signature}")

        try:
            response = requests.post(
                webhook_url,
                data=payload_str.encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-webhook-signature": signature,
                    "X-Webhook-Signature": signature,
                    "X-Webhook-Event": str(event_type),
                    "X-Webhook-Delivery": str(uuid.uuid4()),
                },
                timeout=MQ_HTTP_TIMEOUT_WEBHOOK_SEC,
            )

            if response.status_code == 200:
                print(f"✅  Webhook 发送成功 | URL: {webhook_url} | 状态码: 200")
                return True

            error_msg = response.text[:200]
            print("❌  Webhook 发送失败!")
            print(f"   URL: {webhook_url}")
            print(f"   状态码: {response.status_code}")
            print(f"   服务器响应: {error_msg}")
            return False
        except Exception as e:
            print("💥  Webhook 请求发生异常!")
            print(f"   URL: {webhook_url}")
            print(f"   异常详情: {e}")
            return False

    def upload_result_video(self, result_url: str, video_upload_url: str) -> bool:
        """
        将后端结果视频上传到消息体指定的 video_upload_url（通常是预签名 OSS/S3 PUT URL）。

        与签发字据一致：仅 Bucket + Object Key + PUT + 过期；字据不含 Content-Type 时
        不得多带未签字头。若 requests 自动补 Content-Type 触发 403，用 Content-Type: None
        按库语义去掉（实现细节，与字据里写死某个 MIME 无关）。
        """
        if not result_url or not video_upload_url:
            logger.error("结果上传参数缺失: result_url=%s, video_upload_url=%s", bool(result_url), bool(video_upload_url))
            return False

        try:
            logger.info("开始下载处理结果: %s", result_url)
            download_resp = self.backend_session.get(result_url, stream=True, timeout=MQ_HTTP_TIMEOUT_RESULT_DOWNLOAD_SEC)
            if download_resp.status_code != 200:
                logger.error("下载处理结果失败: status=%s", download_resp.status_code)
                return False

            temp_filename = f"result_upload_{int(time.time())}_{uuid.uuid4().hex}.mp4"
            temp_video_path = self.temp_dir / temp_filename
            with open(temp_video_path, "wb") as temp_f:
                for chunk in download_resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        temp_f.write(chunk)
            download_resp.close()

            if not temp_video_path.exists():
                logger.error("下载处理结果后临时文件不存在")
                return False

            file_size = temp_video_path.stat().st_size
            diag_info = _build_oss_presign_diag(video_upload_url)
            logger.info("OSS预签名上传诊断: %s", diag_info)

            logger.info(
                "开始上传处理结果到 video_upload_url（%.2f MB，无未签字头；流式 PUT）",
                file_size / (1024 * 1024),
            )
            with open(temp_video_path, "rb") as video_f:
                upload_resp = self.oss_session.put(
                    video_upload_url,
                    data=video_f,
                    headers={"Content-Type": None},
                    timeout=MQ_HTTP_TIMEOUT_OSS_PUT_SEC,
                )
            if upload_resp.status_code in (200, 201, 204):
                logger.info(
                    "✅ 处理结果上传成功: %s",
                    video_upload_url[:120],
                )
                try:
                    if temp_video_path.exists():
                        temp_video_path.unlink()
                except Exception:
                    pass
                return True

            body_preview = (upload_resp.text or "").strip()[:300]
            logger.error(
                "❌ 处理结果上传失败: status=%s, req_id=%s, body=%s",
                upload_resp.status_code,
                (upload_resp.headers or {}).get("x-oss-request-id"),
                body_preview or "<empty>",
            )
            try:
                if temp_video_path.exists():
                    temp_video_path.unlink()
            except Exception:
                pass
            return False
        except Exception as e:
            logger.error("上传处理结果到 video_upload_url 异常: %s", e)
            return False

    def process_message(self, ch, method, properties, body):
        """处理消息队列中的消息"""
        job_info = None
        video_path = None
        try:
            logger.info("\n%s", "=" * 60)
            logger.info("📥  收到新消息 [ID: %s]", method.delivery_tag)
            logger.info("%s", "=" * 60)

            message_str = body.decode("utf-8")
            logger.info("消息内容: %s", json.dumps(json.loads(message_str), indent=2, ensure_ascii=False))

            data = json.loads(message_str)

            video_url = data.get("video_download_url")
            video_upload_url = data.get("video_upload_url")
            webhook_url = data.get("webhook_url")
            user_id = data.get("user_id")
            message_id = _resolve_queue_job_id(data, properties)
            detect_type = _normalize_detect_type(data)
            aliyun_region = data.get("aliyun_region")

            if detect_type == "manual" and not data.get("target_regions"):
                logger.warning("detect_type=manual 但 target_regions 为空，将使用默认坐标")
            if aliyun_region:
                logger.info("消息指定 aliyun_region: %s", aliyun_region)

            if not video_url:
                logger.error("❌  消息中没有视频URL")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            if not message_id:
                logger.error("❌  缺少 message_id 且 AMQP message_id 为空（无法回调）")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            staging_name = self._staging_video_name(message_id)
            video_path = self.ingest_dir / staging_name

            self._wait_backend_gpu_capacity()

            # 1) 下载开始
            payload = _build_webhook_payload(
                message_id=message_id,
                user_id=user_id,
                status="DOWNLOADING",
                event_type="download_started",
                current_stage="downloading",
                progress_percentage=0,
                progress_message="正在下载视频...",
            )
            self.send_webhook_notification(webhook_url, payload, event_type="download_started")

            if not self.download_video(video_url, video_path):
                logger.error("❌  视频下载失败")
                payload = _build_webhook_payload(
                    message_id=message_id,
                    user_id=user_id,
                    status="FAILED",
                    event_type="failed",
                    error_message="视频下载失败",
                    completed_at=datetime.now().isoformat(),
                )
                self.send_webhook_notification(webhook_url, payload, event_type="failed")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # 2) 上传开始
            payload = _build_webhook_payload(
                message_id=message_id,
                user_id=user_id,
                status="UPLOADING",
                event_type="upload_started",
                current_stage="uploading",
                progress_percentage=0,
                progress_message="视频下载完成，开始上传处理",
            )
            self.send_webhook_notification(webhook_url, payload, event_type="upload_started")

            coordinates = self.resolve_coordinates(data)
            logger.info("本次使用字幕区域坐标: %s", coordinates)
            job_info = self.upload_to_backend(
                video_path,
                coordinates,
                {
                    "message_id": message_id,
                    "video_upload_url": video_upload_url,
                    "bottom_process_height": data.get("bottom_process_height"),
                },
            )

            if not job_info:
                logger.error("❌  上传到后端处理失败")
                payload = _build_webhook_payload(
                    message_id=message_id,
                    user_id=user_id,
                    status="FAILED",
                    event_type="failed",
                    error_message="上传到处理服务失败",
                    completed_at=datetime.now().isoformat(),
                )
                self.send_webhook_notification(webhook_url, payload, event_type="failed")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            backend_job_id = job_info["job_id"]

            # 3) 处理开始（注意：job_id/queueJobId 仍是 message_id）
            payload = _build_webhook_payload(
                message_id=message_id,
                user_id=user_id,
                status="PROCESSING",
                event_type="processing_started",
                current_stage="processing",
                progress_percentage=0,
                progress_message="正在处理视频...",
            )
            self.send_webhook_notification(webhook_url, payload, event_type="processing_started")

            logger.info("✅  视频已提交处理，backend job_id: %s", backend_job_id)
            start_time = time.time()

            # 4) 监控处理进度（用 backend_job_id 轮询）
            def monitor_processing():
                max_retries = MQ_WORKER_STATUS_MAX_POLLS
                retry_count = 0

                while retry_count < max_retries:
                    try:
                        status_url = f"{self.backend_url}/status/{backend_job_id}"
                        response = self.backend_session.get(
                            status_url,
                            timeout=MQ_HTTP_TIMEOUT_STATUS_POLL_SEC,
                        )

                        if response.status_code != 200:
                            logger.warning("状态检查失败: %s", response.status_code)
                            continue

                        status_data = response.json()
                        status = status_data.get("status")
                        step = status_data.get("step", "unknown")

                        if status == "completed":
                            result_path = status_data.get("result_url") or ""
                            result_url = f"{self.backend_url}{result_path}" if result_path else ""
                            backend_uploaded_url = status_data.get("uploaded_video_url")
                            result_upload_ok = True
                            if backend_uploaded_url:
                                logger.info("后端已完成结果上传，跳过 worker 二次上传")
                            elif video_upload_url:
                                if not result_url:
                                    result_upload_ok = False
                                else:
                                    result_upload_ok = self.upload_result_video(result_url, video_upload_url)
                                if not result_upload_ok:
                                    payload = _build_webhook_payload(
                                        message_id=message_id,
                                        user_id=user_id,
                                        status="FAILED",
                                        event_type="failed",
                                        error_message="处理完成，但下载链接丢失",
                                        completed_at=datetime.now().isoformat(),
                                    )
                                    self.send_webhook_notification(webhook_url, payload, event_type="failed")
                                    return
                            else:
                                payload = _build_webhook_payload(
                                    message_id=message_id,
                                    user_id=user_id,
                                    status="FAILED",
                                    event_type="failed",
                                    error_message="处理完成，但下载链接丢失",
                                    completed_at=datetime.now().isoformat(),
                                )
                                self.send_webhook_notification(webhook_url, payload, event_type="failed")
                                return

                            out_url = backend_uploaded_url or result_url or None
                            payload = _build_webhook_payload(
                                message_id=message_id,
                                user_id=user_id,
                                status="COMPLETED",
                                event_type="completed",
                                output_video_url=out_url,
                                result_uploaded=bool(backend_uploaded_url or video_upload_url),
                                uploaded_video_url=backend_uploaded_url or (video_upload_url if video_upload_url else None),
                                completed_at=datetime.now().isoformat(),
                                processing_time_seconds=round(time.time() - start_time, 2),
                                progress_percentage=100,
                            )
                            self.send_webhook_notification(webhook_url, payload, event_type="completed")
                            return

                        if status == "failed":
                            error_msg = status_data.get("error", "Unknown error")
                            payload = _build_webhook_payload(
                                message_id=message_id,
                                user_id=user_id,
                                status="FAILED",
                                event_type="failed",
                                error_message=error_msg,
                                completed_at=datetime.now().isoformat(),
                            )
                            self.send_webhook_notification(webhook_url, payload, event_type="failed")
                            return

                        if status == "processing":
                            # 粗略进度：如果后端没有 percentage，这里用 50 起步
                            payload = _build_webhook_payload(
                                message_id=message_id,
                                user_id=user_id,
                                status="PROCESSING",
                                event_type="processing_progress",
                                current_stage="processing",
                                progress_percentage=50,
                                progress_message=f"视频处理中: {step}",
                            )
                            # 不要太频繁：每 60 秒发一次（5秒轮询 => 12 次）
                            if retry_count % MQ_WORKER_PROGRESS_WEBHOOK_INTERVAL_POLLS == 0:
                                self.send_webhook_notification(
                                    webhook_url,
                                    payload,
                                    event_type="processing_progress",
                                )

                    except Exception as e:
                        logger.warning("状态检查异常: %s", e)

                    retry_count += 1
                    time.sleep(MQ_WORKER_STATUS_POLL_INTERVAL_SEC)

                # 超时
                payload = _build_webhook_payload(
                    message_id=message_id,
                    user_id=user_id,
                    status="FAILED",
                    event_type="failed",
                    error_message="视频处理超时",
                    completed_at=datetime.now().isoformat(),
                )
                self.send_webhook_notification(webhook_url, payload, event_type="failed")

            # 同步等待处理到终态后再 ack，避免“任务仍在跑但队列已空”导致调度器误停机。
            # 注意：pika channel 非线程安全，因此不要在后台线程里 ack。
            monitor_processing()

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("✅  任务已到终态，消息已 ack")

        except Exception as e:
            logger.error("❌  处理消息时发生错误: %s", e)
            try:
                data = json.loads(body.decode("utf-8"))
                message_id = _resolve_queue_job_id(data, properties)
                webhook_url = data.get("webhook_url")
                user_id = data.get("user_id")
            except Exception:
                message_id = None
                webhook_url = None
                user_id = None

            if webhook_url and message_id:
                payload = _build_webhook_payload(
                    message_id=message_id,
                    user_id=user_id,
                    status="FAILED",
                    event_type="failed",
                    error_message=f"处理过程中发生错误: {str(e)}",
                    completed_at=datetime.now().isoformat(),
                )
                self.send_webhook_notification(webhook_url, payload, event_type="failed")

            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        finally:
            if video_path and video_path.exists():
                try:
                    video_path.unlink()
                    logger.info("已清理临时文件: %s", video_path)
                except Exception:
                    pass

    def start_consuming(self):
        """开始消费消息"""
        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.process_message,
                auto_ack=False,
            )

            logger.info("\n%s", "=" * 60)
            logger.info("🚀  消息队列工作者已启动")
            logger.info("📭  队列: %s", self.queue_name)
            logger.info("🔗  后端API: %s", self.backend_url)
            logger.info("💾  临时目录: %s", self.temp_dir)
            logger.info("⏳  等待消息中...")
            logger.info("按 Ctrl+C 退出")
            logger.info("%s\n", "=" * 60)

            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("\n👋  用户中断")
            raise
        except Exception as e:
            logger.error("消费失败: %s", e)
            raise
        finally:
            self.close()

    def close(self):
        """关闭连接"""
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
            logger.info("连接已关闭")
        except Exception:
            pass


def main():
    """
    云代理 / NAT 下 AMQP 易偶发 EOF；连接断开后自动退避重连，避免 8 个 worker 一起挂死。
    """
    backoff_sec = 5.0
    backoff_max = 120.0
    while True:
        logger.info("启动消息队列工作者...")
        worker = VideoQueueWorker(
            rabbitmq_url=RABBITMQ_URL,
            backend_url=BACKEND_URL,
            queue_name=QUEUE_NAME,
        )
        if not worker.connect():
            wait = backoff_sec + random.uniform(0, 2)
            logger.error("RabbitMQ 连接失败，%.1f 秒后重试", wait)
            time.sleep(wait)
            backoff_sec = min(backoff_sec * 2, backoff_max)
            continue

        backoff_sec = 5.0
        try:
            worker.start_consuming()
            break
        except KeyboardInterrupt:
            logger.info("收到中断信号，退出")
            break
        except (
            pika_exceptions.StreamLostError,
            pika_exceptions.AMQPConnectionError,
            pika_exceptions.ConnectionClosedByBroker,
            pika_exceptions.ChannelWrongStateError,
        ) as e:
            logger.warning("RabbitMQ 会话异常，将重连: %s", e)
            try:
                worker.close()
            except Exception:
                pass
            wait = min(backoff_sec + random.uniform(0, 2), backoff_max)
            time.sleep(wait)
            backoff_sec = min(backoff_sec * 2, backoff_max)
        except Exception as e:
            logger.error("消费循环未预期错误: %s", e)
            try:
                worker.close()
            except Exception:
                pass
            wait = min(backoff_sec + random.uniform(0, 2), backoff_max)
            time.sleep(wait)
            backoff_sec = min(backoff_sec * 2, backoff_max)


if __name__ == "__main__":
    main()

