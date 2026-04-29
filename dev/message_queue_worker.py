import pika
import json
import requests
import os
import logging
from urllib.parse import urlparse, unquote, parse_qs
from pathlib import Path
import time
import sys
import re
import threading
import uuid
import hmac
import hashlib
from datetime import datetime
from typing import Optional


# ----------------------------
# 连接等可仍用下面默认；队列名优先读 RABBITMQ_MEDIA_UPLOAD / MEDIA_QUEUE（与 worker_simulation 一致）
# ----------------------------
RABBITMQ_URL = "amqp://KJXCAcslUlenu3nh:-vt7YdxTMRLx9Bz.Ldyxr2zxduxfS8sC@shinkansen.proxy.rlwy.net:24253"
BACKEND_URL = "http://127.0.0.1:2026"
# 队列名与 worker_simulation 中 RABBITMQ_MEDIA_UPLOAD / 默认 media.uploaded 对齐（可用环境变量覆盖）
QUEUE_NAME = (
    os.environ.get("RABBITMQ_MEDIA_UPLOAD")
    or os.environ.get("MEDIA_QUEUE")
    or "media.uploaded"
)

# 注意：该密钥用于 webhook HMAC 签名校验，必须与 Next.js 配置一致
WEBHOOK_SECRET = "f6ed614f5b9444b4869d18e15433ef06"


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("queue_worker.log"),
    ],
)
logger = logging.getLogger(__name__)


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
            backend_url: 后端API地址
            queue_name: 队列名称
        """
        self.rabbitmq_url = rabbitmq_url
        self.backend_url = backend_url.rstrip("/")
        self.queue_name = queue_name

        # 临时下载目录
        self.temp_dir = Path("./temp_downloads")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.channel = None
        # 本地后端调用必须绕过 http_proxy/https_proxy，避免 127.0.0.1 被代理转发后返回 503
        self.local_session = requests.Session()
        self.local_session.trust_env = False
        # 访问预签名 OSS URL 时禁用代理变量，避免签名链路被中间层改写
        self.oss_session = requests.Session()
        self.oss_session.trust_env = False

    def connect(self):
        """连接到RabbitMQ"""
        try:
            logger.info("连接到RabbitMQ...")
            connection_params = pika.URLParameters(self.rabbitmq_url)
            connection_params.heartbeat = 600

            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)

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

            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            downloaded = 0
            start_time = time.time()

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)

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

    def extract_filename(self, url):
        """从URL提取文件名"""
        try:
            parsed_url = urlparse(url)
            path = unquote(parsed_url.path)
            filename = os.path.basename(path)

            if "?" in filename:
                filename = filename.split("?")[0]

            filename = re.sub(r'[<>:"/\\|?*]', "_", filename)

            if not filename or len(filename) < 5:
                filename = f"video_{int(time.time())}.mp4"

            return filename
        except Exception:
            return f"video_{int(time.time())}.mp4"

    def upload_to_backend(self, file_path: Path, coordinates):
        """上传视频到后端处理（获取 backend 内部 job_id 用于轮询）"""
        try:
            logger.info("上传视频到后端: %s", file_path)
            upload_url = f"{self.backend_url}/upload"
            upload_result = None

            # 上传阶段对 5xx/网络抖动做重试，避免后端瞬时不可用导致整任务失败
            max_upload_retries = 3
            for attempt in range(1, max_upload_retries + 1):
                try:
                    with open(file_path, "rb") as f:
                        files = {"file": (os.path.basename(file_path), f, "video/mp4")}
                        response = self.local_session.post(upload_url, files=files, timeout=60)

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

                time.sleep(min(2 ** (attempt - 1), 5))

            filename = (upload_result or {}).get("filename")
            if not filename:
                logger.error("上传响应中没有文件名: %s", upload_result)
                return None

            logger.info("✅  上传成功，文件名: %s", filename)

            process_url = f"{self.backend_url}/process"
            process_data = {"filename": filename, "coordinates": coordinates}

            process_response = self.local_session.post(process_url, json=process_data, timeout=30)
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
                timeout=10,
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
        将后端结果视频上传到消息体指定的 video_upload_url（通常是预签名 OSS URL）。
        """
        if not result_url or not video_upload_url:
            logger.error("结果上传参数缺失: result_url=%s, video_upload_url=%s", bool(result_url), bool(video_upload_url))
            return False

        try:
            logger.info("开始下载处理结果: %s", result_url)
            download_resp = self.local_session.get(result_url, stream=True, timeout=60)
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
            upload_attempts = [
                {
                    "name": "ct+cl+ua+oss",
                    "headers": {
                        "Content-Type": "video/mp4",
                        "Content-Length": str(file_size),
                        "User-Agent": "Mozilla/5.0",
                        "x-oss-forbid-overwrite": "false",
                    },
                },
                {
                    "name": "ct+cl+ua",
                    "headers": {
                        "Content-Type": "video/mp4",
                        "Content-Length": str(file_size),
                        "User-Agent": "Mozilla/5.0",
                    },
                },
                {
                    "name": "ct+ua",
                    "headers": {
                        "Content-Type": "video/mp4",
                        "User-Agent": "Mozilla/5.0",
                    },
                },
                {
                    "name": "no-custom-headers",
                    "headers": {},
                },
            ]

            logger.info("开始上传处理结果到 video_upload_url")
            upload_resp = None
            for attempt_idx, attempt in enumerate(upload_attempts, start=1):
                if attempt["name"] == "no-custom-headers":
                    # 使用原始字节体，避免文件流/额外头导致签名口径不一致
                    with open(temp_video_path, "rb") as video_f:
                        payload = video_f.read()
                    upload_resp = self.oss_session.put(
                        video_upload_url,
                        data=payload,
                        timeout=180,
                    )
                else:
                    with open(temp_video_path, "rb") as video_f:
                        upload_resp = self.oss_session.put(
                            video_upload_url,
                            data=video_f,
                            headers=attempt["headers"],
                            timeout=180,
                        )
                if upload_resp.status_code in (200, 201, 204):
                    logger.info(
                        "✅ 处理结果上传成功(尝试%s/%s, 模式=%s): %s",
                        attempt_idx,
                        len(upload_attempts),
                        attempt["name"],
                        video_upload_url[:120],
                    )
                    try:
                        if temp_video_path.exists():
                            temp_video_path.unlink()
                    except Exception:
                        pass
                    return True

                body_preview = (upload_resp.text or "").strip()[:300]
                logger.warning(
                    "处理结果上传失败(尝试%s/%s, 模式=%s): status=%s, req_id=%s, body=%s",
                    attempt_idx,
                    len(upload_attempts),
                    attempt["name"],
                    upload_resp.status_code,
                    (upload_resp.headers or {}).get("x-oss-request-id"),
                    body_preview or "<empty>",
                )
                if upload_resp.status_code not in (400, 403):
                    break

            logger.error(
                "❌ 处理结果上传最终失败: status=%s, x-oss-request-id=%s",
                getattr(upload_resp, "status_code", "n/a"),
                (getattr(upload_resp, "headers", {}) or {}).get("x-oss-request-id"),
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

            filename = self.extract_filename(video_url)
            video_path = self.temp_dir / filename

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
            job_info = self.upload_to_backend(video_path, coordinates)

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
                max_retries = 180  # 最多等待3小时
                retry_count = 0

                while retry_count < max_retries:
                    try:
                        status_url = f"{self.backend_url}/status/{backend_job_id}"
                        response = self.local_session.get(status_url, timeout=10)

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
                            if retry_count % 12 == 0:
                                self.send_webhook_notification(
                                    webhook_url,
                                    payload,
                                    event_type="processing_progress",
                                )

                    except Exception as e:
                        logger.warning("状态检查异常: %s", e)

                    retry_count += 1
                    time.sleep(5)

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

            monitor_thread = threading.Thread(target=monitor_processing, daemon=True)
            monitor_thread.start()

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("✅  消息处理完成")

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
    logger.info("启动消息队列工作者...")
    worker = VideoQueueWorker(
        rabbitmq_url=RABBITMQ_URL,
        backend_url=BACKEND_URL,
        queue_name=QUEUE_NAME,
    )

    if worker.connect():
        worker.start_consuming()
    else:
        logger.error("启动失败")
        sys.exit(1)


if __name__ == "__main__":
    main()

