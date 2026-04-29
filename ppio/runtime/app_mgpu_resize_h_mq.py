# 副本：与 dev/app_mgpu_resize_h_mq.py 同步部署用；生产环境以服务器上的代码为准。
# 下半部分 + 长视频切片处理（合并：H + Lanczos + seam）+ RabbitMQ 消息队列对接
# 本文件位于 AppFrontend/app/，与 message_queue 中 worker 用法一致
import os
import json
import subprocess
import uuid
import shutil
import logging
import math  # **[新增]**
import gc    # **[新增] 用于显存垃圾回收**
import threading
import time
import re
import hmac
import hashlib
import requests
import pika
from queue import Queue
from fractions import Fraction
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, unquote, urlunparse, parse_qs
from flask import Flask, request, jsonify, send_from_directory, render_template

try:
    from waitress import serve
except ImportError:  # pragma: no cover
    serve = None

# [新增] 引入 OpenCV 用于无痕拼接
try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None
    print("Warning: opencv-python not installed. Stitching step will fail.")
    

# # 1min-3D-30fps-720p.mp4================ 核心参数配置 (算力极限: 90帧/3秒) ================= 
# CLIP_LENGTH = 400    # 单次处理窗口 (90帧)。                            还可以再大，<700
#                     # 设定原因：这是你的显卡能处理的最大上限，尽量占满。

# OVERLAP_LEN = 20    # 重叠缓冲区 (15帧，约0.5秒)。
#                     # 设定原因：即使是短切片，保留少量重叠以防万一需要处理超过90帧的视频。

# STEP_LEN = 380      # 步长 (CLIP_LENGTH - OVERLAP_LEN)
#                     # 计算：600 - 20 = 580。
# # ====================================================================

# 1min-2D-30fps-1080p.mp4================ 核心参数配置 (算力极限: 90帧/3秒) =================
CLIP_LENGTH = 195    # 单次处理窗口 (200帧)。
                    # 设定原因：这是你的显卡能处理的最大上限，尽量占满。

OVERLAP_LEN = 15    # 重叠缓冲区 (15帧，约0.5秒)。
                    # 设定原因：即使是短切片，保留少量重叠以防万一需要处理超过90帧的视频。

STEP_LEN = 180       # 步长 (CLIP_LENGTH - OVERLAP_LEN)
                    # 计算：85 - 15 = 70。
# ====================================================================

# # 1min-2D-30fps-1080p-low.mp4================ 核心参数配置 (算力极限: 90帧/3秒) =================
# CLIP_LENGTH = 75    # 单次处理窗口 (90帧)。
#                     # 设定原因：这是你的显卡能处理的最大上限，尽量占满。

# OVERLAP_LEN = 15    # 重叠缓冲区 (15帧，约0.5秒)。
#                     # 设定原因：即使是短切片，保留少量重叠以防万一需要处理超过90帧的视频。

# STEP_LEN = 60       # 步长 (CLIP_LENGTH - OVERLAP_LEN)
#                     # 计算：85 - 15 = 70。
# # ====================================================================

# # 1min-3D-30fps-1080.mp4================ 核心参数配置 (算力极限: 90帧/3秒) =================
# CLIP_LENGTH = 75    # 单次处理窗口 (90帧)。
#                     # 设定原因：这是你的显卡能处理的最大上限，尽量占满。

# OVERLAP_LEN = 15    # 重叠缓冲区 (15帧，约0.5秒)。
#                     # 设定原因：即使是短切片，保留少量重叠以防万一需要处理超过90帧的视频。

# STEP_LEN = 60       # 步长 (CLIP_LENGTH - OVERLAP_LEN)
#                     # 计算：75 - 15 = 60。
# # ====================================================================

# ================= 多GPU并行配置 =================
## 8卡4090
GPU_IDS = ['0', '1', '2', '3', '4', '5', '6', '7']   # 指定使用的 GPU 编号；设为 [] 时自动探测
MAX_GPU_WORKERS = 8              # 并行 worker 数；设为 None 时使用全部可用 GPU

## 4卡3090
# GPU_IDS = ['0', '1', '2', '3']   # 指定使用的 GPU 编号；设为 [] 时自动探测
# MAX_GPU_WORKERS = 4 

# ================================================
# ================= DiffuEraser 输入 FPS 配置 =================
# DiffuEraser 内部严格要求 input_video / input_mask / priori 帧率完全一致；
# 用统一固定 FPS，避免 OpenCV 与 torchvision 读到的帧率存在细微差异导致报错。
DIFFUSERASER_INPUT_FPS = os.getenv('DIFFUSERASER_INPUT_FPS', '24').strip() or '24'
# ============================================================

# ================= Mask 膨胀配置 =================
MASK_SIZE = '25x25'             # 可选: 9x9 / 11x11 / 13x13 / 15x15 / 17x17
MASK_KERNEL_SIZES = [9, 11, 13, 25]  # 可选多个核尺寸，逗号分隔 (例如 '9,11,13')
# ================================================

# ================= AI 处理区域高度配置 =================
# 仅将底部指定高度（像素）送入 DiffuEraser；其余上半部分保持原视频并在最后拼接。
# 可在 /process 请求体中通过 bottom_process_height 覆盖。
BOTTOM_PROCESS_HEIGHT = 450
# ================================================
# ================= DiffuEraser 输入缩放（Lanczos） =================
# 仅缩放下半区 AI 输入以降低显存；最终 seam 前会放大回 Step2 原始下半区分辨率。
ENABLE_AI_RESIZE = True
AI_TARGET_HEIGHT = 360
AI_SCALE_MODE = 'lanczos'
# ==================================================================
# # ================= 核心参数配置 (基于 35s/1050帧 显存极限) =================
# CLIP_LENGTH = 960   # 单次处理窗口 (30秒)
# OVERLAP_LEN = 60    # 重叠缓冲区 (2秒)
# STEP_LEN = 900      # 步长 (CLIP_LENGTH - OVERLAP_LEN)
# # =======================================================================
# ================= 核心参数配置 (DEBUG 模式: 针对 12s/375帧 视频) =================
# CLIP_LENGTH = 150   # 单次处理 5秒 (150帧)，确保能切出 3 份
# OVERLAP_LEN = 30    # 重叠 1秒 (30帧)，测试拼接融合效果
# STEP_LEN = 120      # 步长 (150 - 30 = 120)
# ==============================================================================


# 本文件在 AppFrontend/app/，资源在上一级 AppFrontend（core_scripts、uploads 等）
_BASE_FILE = os.path.abspath(__file__)
BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(_BASE_FILE), '..'))

app = Flask(__name__)
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['PROCESSED_FOLDER'] = 'processed'
# app.config['FRAMES_FOLDER'] = 'frames'
# app.config['MASK_FOLDER'] = 'masks'
# app.config['RESULTS_FOLDER'] = 'results'

# 2. 使用绝对路径拼接
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'uploads')
app.config['PROCESSED_FOLDER'] = os.path.join(BASE_DIR, 'processed')
app.config['FRAMES_FOLDER'] = os.path.join(BASE_DIR, 'frames')
app.config['MASK_FOLDER'] = os.path.join(BASE_DIR, 'masks')
app.config['RESULTS_FOLDER'] = os.path.join(BASE_DIR, 'results')
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB 文件大小限制
app.config['RABBITMQ_URL'] = os.environ.get(
    'RABBITMQ_URL',
    'amqp://KJXCAcslUlenu3nh:-vt7YdxTMRLx9Bz.Ldyxr2zxduxfS8sC@shinkansen.proxy.rlwy.net:24253',
)
app.config['MEDIA_QUEUE'] = os.environ.get(
    'MEDIA_QUEUE',
) or os.environ.get(
    'RABBITMQ_MEDIA_UPLOAD',
    'media.uploaded',
)
app.config['STATUS_QUEUE'] = os.environ.get('STATUS_QUEUE', 'processing.status')
# 与 message_queue 内嵌 worker 中的密钥保持一致（可通过环境变量覆盖）
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'f6ed614f5b9444b4869d18e15433ef06')
tasks_store = {}
WEBHOOK_HOST_OVERRIDE_MAP = {'47.101.157.157': '47.101.177.221'}
# 为 true 时 /process 默认 202 异步；URL ?sync=1 或 body "sync": true 可强制同步
MGPU_ASYNC_DEFAULT = os.environ.get('MGPU_ASYNC_DEFAULT', 'true').lower() in ('1', 'true', 'yes')
executor = ThreadPoolExecutor(max_workers=8)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建必要的目录
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)
os.makedirs(app.config['FRAMES_FOLDER'], exist_ok=True)
os.makedirs(app.config['MASK_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

local_http = requests.Session()
local_http.trust_env = False
oss_http = requests.Session()
oss_http.trust_env = False


def normalize_queue_detect_type(data: dict):
    """
    与 worker_simulation 一致：detect_type 可选，取值 auto | manual（另兼容 automatic）。
    返回内部使用的 manual | automatic。
    """
    raw = str(data.get('detect_type') or '').strip().lower()
    if raw in ('', 'auto', 'automatic'):
        return 'automatic'
    if raw == 'manual':
        return 'manual'
    return None


def normalize_webhook_url(webhook_url):
    if not webhook_url:
        return webhook_url
    try:
        parsed = urlparse(webhook_url)
        if not parsed.netloc:
            return webhook_url
        host = parsed.hostname
        if not host or host not in WEBHOOK_HOST_OVERRIDE_MAP:
            return webhook_url
        new_host = WEBHOOK_HOST_OVERRIDE_MAP[host]
        if parsed.port:
            new_netloc = f"{new_host}:{parsed.port}"
        else:
            new_netloc = new_host
        return urlunparse(parsed._replace(netloc=new_netloc))
    except Exception:
        return webhook_url


def build_oss_presign_diag(upload_url):
    """提取预签名 URL 关键信息，便于排查 SignatureDoesNotMatch。"""
    try:
        parsed = urlparse(upload_url or '')
        query = parse_qs(parsed.query or '', keep_blank_values=True)
        path_preview = parsed.path or ''
        if len(path_preview) > 120:
            path_preview = f"...{path_preview[-120:]}"
        credential = (query.get('x-oss-credential') or [''])[0]
        if credential and len(credential) > 36:
            credential = f"{credential[:18]}...{credential[-18:]}"
        return {
            'host': parsed.netloc,
            'path_preview': path_preview,
            'x_oss_date': (query.get('x-oss-date') or [''])[0],
            'x_oss_expires': (query.get('x-oss-expires') or [''])[0],
            'x_oss_signature_version': (query.get('x-oss-signature-version') or [''])[0],
            'x_oss_credential_preview': credential,
            'query_key_count': len(query.keys()),
        }
    except Exception:
        return {'parse_error': True}


class RabbitMQManager:
    def __init__(self, rabbitmq_url, queue_name):
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = queue_name

    def send_task(self, task_data):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(self.rabbitmq_url))
            channel = connection.channel()
            channel.queue_declare(queue=self.queue_name, durable=True)
            if 'message_id' not in task_data:
                task_data['message_id'] = str(uuid.uuid4())
            task_data['timestamp'] = datetime.now().isoformat()
            pub_kw = {'delivery_mode': 2, 'content_type': 'application/json'}
            mid = task_data.get('message_id')
            if mid is not None:
                pub_kw['message_id'] = str(mid)
            channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(task_data),
                properties=pika.BasicProperties(**pub_kw),
            )
            connection.close()
            logger.info("任务已发送到消息队列: %s", task_data['message_id'])
            return True
        except Exception as e:
            logger.error("发送任务到RabbitMQ失败: %s", e)
            return False


class MessageQueueConsumer:
    def __init__(self, rabbitmq_url, queue_name, callback):
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = queue_name
        self.callback = callback
        self.running = False

    def start(self):
        self.running = True
        th = threading.Thread(target=self._consume, daemon=True)
        th.start()
        logger.info("消息队列消费者已启动: %s", self.queue_name)

    def _consume(self):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(self.rabbitmq_url))
            channel = connection.channel()
            channel.queue_declare(queue=self.queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)

            def on_message(ch, method, _properties, body):
                try:
                    self.callback(body)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as err:
                    logger.error("处理状态消息失败: %s", err)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(
                queue=self.queue_name, on_message_callback=on_message, auto_ack=False,
            )
            channel.start_consuming()
        except Exception as e:
            logger.error("消息队列消费者错误: %s", e)
            self.running = False

    def stop(self):
        self.running = False


mq_consumer = None


def init_message_queue():
    global mq_consumer

    def process_status_message(body):
        try:
            message = json.loads(body.decode('utf-8'))
            jid = message.get('job_id')
            if jid and jid in tasks_store:
                tasks_store[jid]['mq_status'] = message.get('status')
                tasks_store[jid]['mq_update_time'] = datetime.now().isoformat()
        except Exception as e:
            logger.error("处理状态消息失败: %s", e)

    mq_consumer = MessageQueueConsumer(
        rabbitmq_url=app.config['RABBITMQ_URL'],
        queue_name=app.config['STATUS_QUEUE'],
        callback=process_status_message,
    )
    mq_consumer.start()


rabbitmq_manager = RabbitMQManager(app.config['RABBITMQ_URL'], app.config['MEDIA_QUEUE'])


def send_status_to_queue(job_id, status, message=None, progress=None, result_url=None):
    try:
        connection = pika.BlockingConnection(pika.URLParameters(app.config['RABBITMQ_URL']))
        channel = connection.channel()
        channel.queue_declare(queue=app.config['STATUS_QUEUE'], durable=True)
        status_message = {
            'job_id': job_id, 'status': status, 'message': message,
            'progress': progress, 'timestamp': datetime.now().isoformat(), 'result_url': result_url,
        }
        if job_id in tasks_store:
            t = tasks_store[job_id]
            status_message.update({
                'user_id': t.get('user_id'),
                'webhook_url': t.get('webhook_url'),
                'step': t.get('step'),
                'source': t.get('source', 'app'),
            })
        channel.basic_publish(
            exchange='',
            routing_key=app.config['STATUS_QUEUE'],
            body=json.dumps(status_message),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type='application/json',
            ),
        )
        connection.close()
        if job_id in tasks_store and tasks_store[job_id].get('webhook_url'):
            try:
                send_webhook_notification(
                    job_id, status, message or '', progress=progress, result_url=result_url,
                )
            except Exception as hook_err:
                logger.warning("webhook 触发失败: %s", hook_err)
    except Exception as e:
        logger.error("发送状态到消息队列失败: %s", e)


def send_webhook_notification(job_id, status, message, progress=None, result_url=None):
    if job_id not in tasks_store:
        return False
    task_info = tasks_store[job_id]
    webhook_url = task_info.get('webhook_url')
    if not webhook_url:
        return False
    webhook_url = normalize_webhook_url(webhook_url)
    raw_status = str(status or '').strip().lower()
    if raw_status in {'completed', 'complete', 'success'}:
        mapped_status, event_type = 'COMPLETED', 'completed'
    elif raw_status in {'failed', 'error', 'failure'}:
        mapped_status, event_type = 'FAILED', 'failed'
    else:
        mapped_status, event_type = 'PROCESSING', 'progress'
    output_video_url = result_url or task_info.get('result_url')
    now_iso = datetime.now().isoformat()
    notification_data = {
        'job_id': job_id, 'queue_job_id': job_id, 'queueJobId': job_id, 'message_id': job_id,
        'user_id': task_info.get('user_id'),
        'status': mapped_status,
        'output_video_url': output_video_url,
        'result_url': result_url,
        'error_message': message if mapped_status == 'FAILED' else None,
        'event_type': event_type, 'current_stage': task_info.get('step') or raw_status,
        'progress_percentage': progress, 'progress_message': message, 'timestamp': now_iso,
        'uploaded_video_url': task_info.get('uploaded_video_url'),
        'result_uploaded': bool(task_info.get('uploaded_video_url')),
    }
    if mapped_status in ('COMPLETED', 'FAILED'):
        notification_data['completed_at'] = now_iso
    if mapped_status == 'COMPLETED' and task_info.get('process_time') is not None:
        try:
            notification_data['processing_time_seconds'] = max(
                0, int(round(float(task_info['process_time']))),
            )
        except (TypeError, ValueError):
            pass
    payload_str = json.dumps(notification_data)
    signature = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode('utf-8'), payload_str.encode('utf-8'), hashlib.sha256,
    ).hexdigest()
    try:
        r = requests.post(
            webhook_url, data=payload_str.encode('utf-8'), timeout=10,
            headers={
                'Content-Type': 'application/json',
                'x-webhook-signature': signature,
                'X-Webhook-Signature': signature,
                'X-Webhook-Event': event_type,
                'X-Webhook-Delivery': str(uuid.uuid4()),
            },
        )
        return 200 <= r.status_code < 300
    except Exception as e:
        logger.error("Webhook 通知失败: %s", e)
        return False


def upload_processed_video(video_path, upload_url, content_type=None, timeout=120):
    """
    阿里云 OSS 预签名 URL 上传（PUT），严格遵循签名约束。

    重要：
      - 如果预签名 URL 生成时未指定 Content-Type，则 content_type 必须为 None（默认）
      - 如果生成时指定了 Content-Type，则必须传入完全相同的值，否则会 403
      - 不要手动添加 Content-Length、User-Agent、x-oss-forbid-overwrite 等头部

    Args:
        video_path: 本地视频文件路径
        upload_url: OSS 预签名 PUT URL
        content_type: 可选，仅当签名包含 Content-Type 时才传入
        timeout: 超时时间（秒），默认 120

    Returns:
        bool: 成功返回 True，失败返回 False
    """
    try:
        if not video_path or not os.path.exists(video_path):
            logger.error(f"上传失败：视频文件不存在: {video_path}")
            return False

        if not upload_url:
            logger.error("上传失败：upload_url 为空")
            return False

        file_size = os.path.getsize(video_path)
        logger.info(f"准备上传: {video_path} ({file_size / 1024 / 1024:.2f} MB)")

        # 可选诊断信息（不参与请求）
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(upload_url)
        qs = parse_qs(parsed.query)
        logger.debug(f"目标 Host: {parsed.netloc}")
        logger.debug(f"签名版本: {qs.get('x-oss-signature-version', ['?'])[0]}")
        logger.debug(f"过期时间: {qs.get('x-oss-expires', ['?'])[0]}")

        # 构造请求头：仅当明确需要 Content-Type 时才添加
        headers = {}
        if content_type is not None:
            headers["Content-Type"] = content_type
            logger.info(f"携带 Content-Type: {content_type}")
        else:
            logger.info("未携带 Content-Type（假定签名时未包含该头）")

        start = time.time()
        with open(video_path, 'rb') as fp:
            resp = requests.put(upload_url, data=fp, headers=headers, timeout=timeout)

        elapsed = time.time() - start
        logger.info(f"上传耗时: {elapsed:.2f}s")
        logger.info(f"HTTP 状态码: {resp.status_code}")

        # 记录 OSS 返回的追踪信息
        etag = resp.headers.get('ETag')
        request_id = resp.headers.get('x-oss-request-id')
        if etag:
            logger.info(f"ETag: {etag}")
        if request_id:
            logger.info(f"x-oss-request-id: {request_id}")

        if 200 <= resp.status_code < 300:
            logger.info("✅ 视频上传成功")
            return True

        # 失败时解析响应体前 2000 字符
        body_sample = resp.text[:2000]
        logger.error(f"上传失败，响应体: {body_sample}{'...' if len(resp.text) > 2000 else ''}")

        if resp.status_code == 403 and "SignatureDoesNotMatch" in resp.text:
            logger.error(
                "💡 SignatureDoesNotMatch 可能原因：\n"
                "   - 签名时未指定 Content-Type，但上传时携带了该头\n"
                "   - 签名时指定了 Content-Type，但上传时未携带或不一致\n"
                "   - 预签名 URL 已过期\n"
                "   - 本地系统时间偏差过大\n"
                "   - URL 中的 Bucket/Object Key 与实际不匹配"
            )
        return False

    except requests.exceptions.ConnectionError as e:
        logger.error(f"连接失败: {e}")
        return False
    except requests.exceptions.Timeout:
        logger.error(f"上传超时（超过 {timeout} 秒）")
        return False
    except Exception as e:
        logger.error(f"上传异常: {e}")
        return False

def resolve_queue_coordinates(data: dict):
    default_coordinates = {"x": 100, "y": 100, "width": 400, "height": 100}

    def _norm_tuple(region):
        # 兼容 schema 描述的数组格式: [identifier, x, y, width, height]
        if not isinstance(region, (list, tuple)) or len(region) < 5:
            return None
        try:
            x, y, w, h = int(region[1]), int(region[2]), int(region[3]), int(region[4])
            if w <= 0 or h <= 0:
                return None
            return {"x": x, "y": y, "width": w, "height": h}
        except (TypeError, ValueError):
            return None

    def _norm(region):
        if not isinstance(region, dict):
            return None
        try:
            x, y, w, h = int(region['x']), int(region['y']), int(region['width']), int(region['height'])
            if w <= 0 or h <= 0:
                return None
            return {"x": x, "y": y, "width": w, "height": h}
        except (TypeError, ValueError, KeyError):
            return None

    tr = data.get('target_regions')
    if isinstance(tr, list) and tr:
        r0 = _norm(tr[0])
        if not r0:
            r0 = _norm_tuple(tr[0])
        if r0:
            return r0
    c = _norm(data.get('coordinates'))
    if c:
        return c
    return default_coordinates


@app.route('/')
def index():
    """主页面路由"""
    return render_template('index.html')

@app.route('/demo')
def demo():
    """研究展示页面（你刚才提供的 React 页面）"""
    return render_template('demo.html')

@app.route('/upload', methods=['POST'])
def upload_video():
    """处理视频上传"""
    logger.info("收到上传请求...")
    
    if 'file' not in request.files:
        logger.error("没有文件部分")
        return jsonify({'error': 'No file part'}), 400
        
    file = request.files['file']
    if file.filename == '':
        logger.error("没有选择文件")
        return jsonify({'error': 'No selected file'}), 400
        
    # 检查文件扩展名
    if not file.filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
        logger.error("不支持的文件格式: %s", file.filename)
        return jsonify({'error': 'Unsupported file format'}), 400
        
    # 生成唯一文件名
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.mp4"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    logger.info("保存文件到: %s", filepath)
    try:
        file.save(filepath)
        logger.info("文件保存成功")
        
        # 验证视频文件
        logger.info("验证视频文件...")
        check_video_cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', 
                          '-show_entries', 'stream=codec_name,width,height', 
                          '-of', 'json', filepath]
        result = subprocess.run(check_video_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error("视频验证失败: %s", result.stderr)
            os.remove(filepath)
            return jsonify({
                'error': 'Invalid video file',
                'details': result.stderr
            }), 400
            
        # 解析视频信息
        video_info = json.loads(result.stdout)
        streams = video_info.get('streams', [])
        if not streams:
            logger.error("没有找到视频流")
            os.remove(filepath)
            return jsonify({'error': 'No video stream found'}), 400
            
        video_stream = streams[0]
        logger.info("视频信息: %s", video_stream)
        return jsonify({
            'filename': filename,
            'width': video_stream.get('width', 0),
            'height': video_stream.get('height', 0)
        }), 200
        
    except Exception as e:
        logger.exception("文件上传失败")
        if os.path.exists(filepath):
            os.remove(filepath)
        return jsonify({'error': 'File upload failed', 'details': str(e)}), 500

def process_video_impl(data):
    """
    处理视频去字幕：可配置底部高度切割 →（可选）Lanczos 缩放入 DiffuEraser →
    多 GPU 切片推理 → xfade 缝合 →（启用缩放时）放大回原始下半区尺寸 → 亮度估计 + blend seam + vstack。
    data: dict，需含 filename、coordinates；可选 job_id、message_id、bottom_process_height 等。
    """
    job_start_total = datetime.now()
    if not data or 'filename' not in data or 'coordinates' not in data:
        return jsonify({'error': 'Invalid request data'}), 400

    filename = data['filename']
    coordinates = data['coordinates']
    video_upload_url = data.get('video_upload_url')

    # 验证坐标
    if not all(key in coordinates for key in ['x', 'y', 'width', 'height']):
        return jsonify({'error': 'Invalid coordinates format'}), 400

    # 工作目录 id：与外部 message_id / 上传约定一致
    ext_job = data.get('job_id') or data.get('message_id')
    job_id = ext_job if ext_job else f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    work_dir = os.path.join(app.config['PROCESSED_FOLDER'], job_id)
    os.makedirs(work_dir, exist_ok=True)
    
    # ================= **[新增] 日志工具函数** =================
    inference_log_path = os.path.join(work_dir, 'inference.log')  # **[新增] 提前定义日志路径**
    error_log_path = os.path.join(work_dir, 'error.log')
    
    def write_log(msg, time_cost=None):  # **[新增] 定义写日志内部函数**
        """写入日志 (追加模式)"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        with open(inference_log_path, 'a', encoding='utf-8') as f:
            if time_cost is not None:
                f.write(f"[{timestamp}] {msg} | 耗时: {time_cost:.2f}秒\n")
            else:
                f.write(f"[{timestamp}] {msg}\n")
    
    # **[新增] 初始化日志文件**
    with open(inference_log_path, 'w', encoding='utf-8') as f:
        f.write(f"=== 任务启动: {job_id} ===\n")
        f.write(f"=== 开始时间: {job_start_total.strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")
    # ========================================================

    # 原始完整视频路径
    input_video = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    # 定义清理函数
    def cleanup_work_dir():
        """清理当前任务的工作目录"""
        if os.path.exists(work_dir):
            try:
                shutil.rmtree(work_dir)
                logger.info(f"成功清理任务 {job_id} 的工作目录: {work_dir}")
            except Exception as e:
                logger.error(f"清理任务 {job_id} 的工作目录失败: {str(e)}")

    def parse_fps_value(fps_expr):
        """将 ffmpeg/ffprobe 的 FPS 表达式转换为 float。"""
        fps_expr = str(fps_expr).strip()
        try:
            return float(Fraction(fps_expr))
        except Exception:
            return float(fps_expr)

    def probe_video_frame_count(video_path):
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-count_packets',
            '-show_entries', 'stream=nb_read_packets,nb_frames',
            '-of', 'json',
            video_path
        ]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
        streams = json.loads(probe_result.stdout).get('streams', [])
        if not streams:
            raise RuntimeError(f'No video stream found in {video_path}')
        nb_frames_val = streams[0].get('nb_read_packets') or streams[0].get('nb_frames')
        if not nb_frames_val:
            raise RuntimeError(f'Could not read frame count for {video_path}')
        return int(nb_frames_val)

    def probe_video_fps_value(video_path):
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=avg_frame_rate,r_frame_rate',
            '-of', 'json',
            video_path
        ]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
        streams = json.loads(probe_result.stdout).get('streams', [])
        if not streams:
            raise RuntimeError(f'No video stream found in {video_path}')
        fps_expr = (streams[0].get('avg_frame_rate') or streams[0].get('r_frame_rate') or '').strip()
        if not fps_expr:
            raise RuntimeError(f'Could not read fps for {video_path}')
        return parse_fps_value(fps_expr)

    def probe_video_size(video_path):
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'json',
            video_path
        ]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
        streams = json.loads(probe_result.stdout).get('streams', [])
        if not streams:
            raise RuntimeError(f'No video stream found in {video_path}')
        return int(streams[0]['width']), int(streams[0]['height'])

    def parse_mask_kernel_from_size(size_name):
        """将 '17x17' 解析为 17。"""
        try:
            value = int(str(size_name).lower().split('x')[0])
            if value <= 0:
                raise ValueError("mask kernel size must be positive")
            if value % 2 == 0:
                value += 1
            return value
        except Exception as err:
            raise ValueError(f"Invalid MASK_SIZE={size_name}") from err

    def has_png_frames(dir_path):
        if not os.path.isdir(dir_path):
            return False
        for name in os.listdir(dir_path):
            if name.lower().endswith('.png'):
                return True
        return False

    def build_gen_mask_cmd(kernel_sizes):
        kernel_sizes = sorted(set(int(s) for s in kernel_sizes if int(s) > 0))
        return [
            'python', gen_mask_script,
            '--input_dir', frames_dir,
            '--output_dir', masks_dir,
            '--x', str(ai_mask_x),
            '--y', str(ai_mask_y),
            '--width', str(ai_mask_w),
            '--height', str(ai_mask_h),
            '--kernel_sizes', ','.join(str(size) for size in kernel_sizes)
        ]

    # ================= 1. 获取元数据 =================
    try:
        step_start = datetime.now()  # **[新增] 计时开始**
        write_log("Step 0: 开始获取视频元数据...")  # **[新增] 写日志**

        logger.info("Step 0: 获取视频元数据...")
        probe_cmd = [
            'ffprobe', '-v', 'error', 
            '-select_streams', 'v:0', 
            '-count_packets', # **[修改] 增加此参数以确保总帧数精确**
            '-show_entries', 'stream=r_frame_rate,width,height,nb_read_packets,nb_frames', # **[修改] 读取 packet 数**
            '-of', 'json', 
            input_video
        ]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
        video_info = json.loads(probe_result.stdout)['streams'][0]
        
        width = int(video_info['width'])
        height = int(video_info['height'])

        # **[修改] 优先读取 nb_read_packets (真实物理帧数)，其次 nb_frames (元数据)**
        nb_frames_val = video_info.get('nb_read_packets') or video_info.get('nb_frames')
        if nb_frames_val:
            total_frames = int(nb_frames_val)
        else:
            total_frames = 2000 # 保底
        
        source_fps_expr = str(video_info['r_frame_rate']).strip()
        source_fps_value = parse_fps_value(source_fps_expr)

        fps = DIFFUSERASER_INPUT_FPS
        fps_value = parse_fps_value(fps)

        logger.info(
            f"原视频: {width}x{height}, 原始FPS: {source_fps_expr} ({source_fps_value:.6f}), "
            f"DiffuEraser输入FPS: {fps} ({fps_value:.6f})"
        )

        step_cost = (datetime.now() - step_start).total_seconds()  # **[新增] 计算耗时**
        write_log("Step 0: 元数据获取完成", step_cost)  # **[新增] 记录耗时**
        
    except Exception as e:
        logger.error("元数据获取失败: %s", str(e))
        write_log(f"Step 0 ERROR: {str(e)}")  # **[新增] 错误记录**
        return jsonify({'error': 'Failed to probe video info'}), 500

    # ================= 2. 执行底部固定高度切割 =================
    step_start = datetime.now()  # **[新增] 计时开始**
    write_log("Step 2: 开始切割视频...")  # **[新增]**
    logger.info("Step 2: 切割视频...")

    requested_bottom_height = data.get('bottom_process_height', BOTTOM_PROCESS_HEIGHT)
    try:
        requested_bottom_height = int(requested_bottom_height)
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid bottom_process_height'}), 400

    if height < 4:
        return jsonify({'error': f'Video height too small: {height}'}), 400

    if requested_bottom_height <= 0:
        return jsonify({'error': 'bottom_process_height must be > 0'}), 400

    max_bottom_height = height - 2  # 保底留至少 2px 给上半部分
    bottom_height = min(requested_bottom_height, max_bottom_height)
    top_height = height - bottom_height
    write_log(
        f"Step 2: 底部处理高度请求={requested_bottom_height}px, 实际={bottom_height}px, 顶部高度={top_height}px"
    )
    
    top_video_path = os.path.join(work_dir, 'top_half.mp4')
    bottom_video_path = os.path.join(work_dir, 'bottom_half.mp4')
    
    try:
        # Top: 保留音频，同时统一到 DiffuEraser 输入 FPS，保证最终上下时基一致
        subprocess.run([
            'ffmpeg', '-y', '-i', input_video,
            '-vf', f'crop={width}:{top_height}:0:0,fps={fps},setpts=PTS-STARTPTS',
            '-c:v', 'libx264', '-c:a', 'copy', top_video_path
        ], check=True, capture_output=True)
        
        # Bottom: 去除音频，并统一到 DiffuEraser 输入 FPS
        subprocess.run([
            'ffmpeg', '-y', '-i', input_video,
            '-vf', f'crop={width}:{bottom_height}:0:{top_height},fps={fps},setpts=PTS-STARTPTS',
            '-c:v', 'libx264', '-an', bottom_video_path
        ], check=True, capture_output=True)

        # 以统一 FPS 后的下半视频为准，重新获取真实总帧数用于后续切片。
        total_frames = probe_video_frame_count(bottom_video_path)
        top_fps = probe_video_fps_value(top_video_path)
        bottom_fps = probe_video_fps_value(bottom_video_path)
        if abs(top_fps - fps_value) > 1e-3 or abs(bottom_fps - fps_value) > 1e-3:
            raise RuntimeError(
                f"切割后帧率不一致: top={top_fps:.6f}, bottom={bottom_fps:.6f}, expected={fps_value:.6f}"
            )
        write_log(f"Step 2: 统一FPS后总帧数={total_frames}, top_fps={top_fps:.6f}, bottom_fps={bottom_fps:.6f}")

        step_cost = (datetime.now() - step_start).total_seconds()  # **[新增]**
        write_log("Step 2: 视频切割完成", step_cost)  # **[新增]**
        
    except subprocess.CalledProcessError as e:
        logger.error(f"视频切割失败: {e.stderr}")
        return jsonify({'error': 'Video split failed'}), 500

    # ================= 3. Lanczos 缩放入 AI + Mask 坐标映射 =================
    input_video_for_ai = bottom_video_path
    mapped_y = int(coordinates['y']) - top_height
    if mapped_y < 0:
        mapped_y = 0

    ai_bottom_width = width
    ai_bottom_height = bottom_height
    ai_scale_x = 1.0
    ai_scale_y = 1.0

    try:
        if ENABLE_AI_RESIZE:
            step_start_s3 = datetime.now()
            write_log("Step 3: 开始缩放下半区 AI 输入 (Lanczos)...")

            bottom_video_for_ai_path = os.path.join(work_dir, 'bottom_half_ai_input.mp4')
            resize_cmd = [
                'ffmpeg', '-y', '-i', bottom_video_path,
                '-vf', f'scale=-2:{AI_TARGET_HEIGHT}:flags={AI_SCALE_MODE},fps={fps},setpts=PTS-STARTPTS',
                '-c:v', 'libx264', '-an',
                bottom_video_for_ai_path
            ]
            subprocess.run(resize_cmd, check=True, capture_output=True)
            input_video_for_ai = bottom_video_for_ai_path
            ai_bottom_width, ai_bottom_height = probe_video_size(input_video_for_ai)

            if ai_bottom_width <= 0 or ai_bottom_height <= 0:
                raise RuntimeError("缩放后 AI 输入分辨率无效")

            ai_scale_x = ai_bottom_width / float(width)
            ai_scale_y = ai_bottom_height / float(bottom_height)
            step_cost_s3 = (datetime.now() - step_start_s3).total_seconds()
            write_log(
                f"Step 3: AI 输入缩放完成 -> {ai_bottom_width}x{ai_bottom_height}, "
                f"scale_x={ai_scale_x:.6f}, scale_y={ai_scale_y:.6f}",
                step_cost_s3
            )
        else:
            write_log("Step 3: 未启用 AI 输入缩放，使用原下半区分辨率")
    except subprocess.CalledProcessError as e:
        logger.error(
            "Step 3 缩放失败: %s",
            e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr,
        )
        return jsonify({
            'error': 'AI input resize failed',
            'details': e.stderr.decode() if isinstance(e.stderr, bytes) else str(e.stderr),
        }), 500
    except Exception as e:
        logger.error("Step 3 参数或缩放构建失败: %s", str(e))
        return jsonify({'error': 'AI input resize failed', 'details': str(e)}), 500

    ai_mask_x = int(round(int(coordinates['x']) * ai_scale_x))
    ai_mask_y = int(round(mapped_y * ai_scale_y))
    ai_mask_w = int(round(int(coordinates['width']) * ai_scale_x))
    ai_mask_h = int(round(int(coordinates['height']) * ai_scale_y))

    ai_mask_x = max(0, min(ai_mask_x, max(0, ai_bottom_width - 1)))
    ai_mask_y = max(0, min(ai_mask_y, max(0, ai_bottom_height - 1)))
    ai_mask_w = max(2, ai_mask_w)
    ai_mask_h = max(2, ai_mask_h)
    if ai_mask_x + ai_mask_w > ai_bottom_width:
        ai_mask_w = max(2, ai_bottom_width - ai_mask_x)
    if ai_mask_y + ai_mask_h > ai_bottom_height:
        ai_mask_h = max(2, ai_bottom_height - ai_mask_y)

    write_log(
        f"Step 3: Mask 坐标映射 -> x={ai_mask_x}, y={ai_mask_y}, "
        f"w={ai_mask_w}, h={ai_mask_h}"
    )

    max_img_size_for_ai = str(ai_bottom_width if ENABLE_AI_RESIZE else 1920)

    # ================= 4. 提取帧 =================
    step_start = datetime.now()  # **[新增]**
    write_log("Step 4: 开始提取视频帧...")  # **[新增]**
    logger.info("Step 4: 提取视频帧...")

    frames_dir = os.path.join(work_dir, 'frames')
    os.makedirs(frames_dir, exist_ok=True)

    extract_frames_cmd = [
        'ffmpeg', '-y', '-i', input_video_for_ai,
        os.path.join(frames_dir, '%04d.png')
    ]

    try:
        subprocess.run(extract_frames_cmd, check=True, capture_output=True)
        logger.info("视频帧提取完成")
        extracted_frames = len([name for name in os.listdir(frames_dir) if name.lower().endswith('.png')])
        if extracted_frames <= 0:
            raise RuntimeError("提取到的帧数为 0")
        if extracted_frames != total_frames:
            logger.warning("提取帧数与预期不一致: extracted=%s, expected=%s", extracted_frames, total_frames)
            write_log(f"Step 4 WARNING: 提取帧数({extracted_frames})与预期({total_frames})不一致，后续按提取帧数处理")
            total_frames = extracted_frames

        step_cost = (datetime.now() - step_start).total_seconds()  # **[新增]**
        write_log(f"Step 4: 提取帧完成 (共{extracted_frames}帧)", step_cost)  # **[新增]**

    except subprocess.CalledProcessError as e:
        logger.error(f"提取视频帧失败: {e.stderr.decode()}")
        cleanup_work_dir()
        return jsonify({'error': 'Frame extraction failed', 'details': e.stderr.decode()}), 500
    
    # ================= 5. 生成 Mask =================
    step_start = datetime.now()  # **[新增]**
    write_log("Step 5: 开始生成 Mask...")  # **[新增]**
    logger.info("Step 5: 生成 Mask...")

    masks_dir = os.path.join(work_dir, 'masks')
    os.makedirs(masks_dir, exist_ok=True)

    gen_mask_script = os.path.join(BASE_DIR, 'core_scripts', 'gen_mask.py')
    
    # 检查脚本是否存在，避免瞎猜
    if not os.path.exists(gen_mask_script):
        logger.error(f"找不到脚本: {gen_mask_script}")
        return jsonify({'error': 'Script not found'}), 500

    target_mask_kernel = parse_mask_kernel_from_size(MASK_SIZE)
    configured_kernel_sizes = sorted(set(MASK_KERNEL_SIZES + [target_mask_kernel]))
    gen_mask_cmd = build_gen_mask_cmd(configured_kernel_sizes)

    try:
        result = subprocess.run(gen_mask_cmd, check=True, capture_output=True, text=True)
        logger.info(f"Mask 生成输出: {result.stdout}")

        step_cost = (datetime.now() - step_start).total_seconds()  # **[新增]**
        write_log("Step 5: Mask 生成完成", step_cost)  # **[新增]**

    except subprocess.CalledProcessError as e:
        logger.error(f"生成 Mask 失败: {e.stderr}")
        return jsonify({'error': 'Mask generation failed', 'details': e.stderr}), 500
    
    # ================= 6. 合成 Mask 视频 =================
    step_start = datetime.now()  # **[新增]**
    write_log("Step 6: 开始合成 Mask 视频...")  # **[新增]**
    logger.info("Step 6: 合成 Mask 视频...")

    mask_frames_dir = os.path.join(masks_dir, f'dilated_masks_{MASK_SIZE}')
    mask_video = os.path.join(work_dir, 'mask.mp4')
    if not has_png_frames(mask_frames_dir):
        write_log(
            f"Step 6 WARNING: 目标 mask=dilated_masks_{MASK_SIZE} 缺失或为空，开始自动创建对应目录"
        )
        regenerate_cmd = build_gen_mask_cmd([target_mask_kernel])
        try:
            regen_result = subprocess.run(regenerate_cmd, check=True, capture_output=True, text=True)
            logger.info("自动补建目标 mask 输出: %s", regen_result.stdout.strip())
        except subprocess.CalledProcessError as regen_err:
            regen_detail = regen_err.stderr if isinstance(regen_err.stderr, str) else str(regen_err)
            write_log(f"Step 6 ERROR: 自动创建 dilated_masks_{MASK_SIZE} 失败 - {regen_detail}")
            return jsonify({
                'error': 'Mask directory auto-create failed',
                'details': regen_detail
            }), 500

    if not has_png_frames(mask_frames_dir):
        detail_msg = f"自动创建后仍未找到可用 mask 帧目录: dilated_masks_{MASK_SIZE}"
        write_log(f"Step 6 ERROR: {detail_msg}")
        return jsonify({'error': 'No mask frames generated', 'details': detail_msg}), 500
    
    try:
        create_mask_video_cmd = [
            'ffmpeg', '-y', '-framerate', fps,
            '-i', os.path.join(mask_frames_dir, '%04d.png'),
            '-c:v', 'libx264', '-pix_fmt', 'yuv420p', mask_video
        ]
        subprocess.run(create_mask_video_cmd, check=True, capture_output=True)

        mask_fps = probe_video_fps_value(mask_video)
        if abs(mask_fps - fps_value) > 1e-3:
            raise RuntimeError(f"Mask 视频帧率异常: {mask_fps:.6f}, expected={fps_value:.6f}")

        step_cost = (datetime.now() - step_start).total_seconds()  # **[新增]**
        write_log(f"Step 6: Mask 视频合成完成 (mask_fps={mask_fps:.6f})", step_cost)  # **[新增]**

    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Mask video creation failed', 'details': e.stderr.decode()}), 500
    
    # ================= 7. DiffuEraser AI 推理（多GPU并行） =================
    write_log("\n=== Step 7: 进入 DiffuEraser AI 推理阶段 (多GPU切片并行) ===")
    logger.info("Step 7: DiffuEraser 多GPU并行推理...")

    diffueraser_start = datetime.now()
    results_dir = os.path.join(work_dir, 'results')
    os.makedirs(results_dir, exist_ok=True)

    # 临时目录，用于存放切片中间产物
    chunks_temp_dir = os.path.join(work_dir, 'chunks_temp')
    os.makedirs(chunks_temp_dir, exist_ok=True)

    diffueraser_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'DiffuEraser'))
    run_script = os.path.join(diffueraser_path, 'run_diffueraser.py')

    if not os.path.exists(diffueraser_path):
        return jsonify({'error': 'DiffuEraser directory not found', 'path': diffueraser_path}), 500
    if not os.path.exists(run_script):
        return jsonify({'error': 'DiffuEraser script not found', 'path': run_script}), 500

    # 切片规划
    slice_list = []
    current_start = 0
    while True:
        current_end = current_start + CLIP_LENGTH

        if current_end > total_frames:
            current_end = total_frames
            current_start = max(0, total_frames - CLIP_LENGTH)
            slice_list.append((current_start, current_end))
            break

        slice_list.append((current_start, current_end))
        if current_end == total_frames:
            break
        current_start += STEP_LEN

    logger.info(f"切片规划完成，共 {len(slice_list)} 个切片: {slice_list}")
    write_log(f"计划处理 {len(slice_list)} 个切片任务")

    def detect_available_gpu_ids():
        if GPU_IDS:
            gpu_ids = [str(item).strip() for item in GPU_IDS if str(item).strip()]
            if gpu_ids:
                return gpu_ids

        try:
            import torch
            count = torch.cuda.device_count()
            if count > 0:
                return [str(i) for i in range(count)]
        except Exception as gpu_err:
            logger.warning(f"torch 探测 GPU 失败: {gpu_err}")

        try:
            smi_cmd = ['nvidia-smi', '--query-gpu=index', '--format=csv,noheader']
            smi_result = subprocess.run(smi_cmd, capture_output=True, text=True, check=True)
            gpu_ids = [line.strip() for line in smi_result.stdout.splitlines() if line.strip()]
            return gpu_ids
        except Exception as smi_err:
            logger.warning(f"nvidia-smi 探测 GPU 失败: {smi_err}")
            return []

    all_gpu_ids = detect_available_gpu_ids()
    if not all_gpu_ids:
        write_log("Step 7 ERROR: 未检测到可用 GPU")
        return jsonify({'error': 'No available GPU detected'}), 500

    if MAX_GPU_WORKERS is None:
        max_workers = len(all_gpu_ids)
    else:
        try:
            max_workers = max(1, int(MAX_GPU_WORKERS))
        except ValueError:
            max_workers = len(all_gpu_ids)

    active_gpu_ids = all_gpu_ids[:min(len(all_gpu_ids), max_workers)]
    logger.info(f"可用 GPU: {all_gpu_ids}; 并行 GPU: {active_gpu_ids}")
    write_log(f"GPU 检测结果: {all_gpu_ids}")
    write_log(f"Step 7 将使用 {len(active_gpu_ids)} 张卡并行: {active_gpu_ids}")

    chunk_result_paths = []

    # 7.1 先串行准备全部切片输入，避免并发 ffmpeg 抢磁盘
    chunk_jobs = []
    try:
        for idx, (start_f, end_f) in enumerate(slice_list):
            chunk_log_msg = f">>> 准备 Slice {idx + 1}/{len(slice_list)}: Frames [{start_f}-{end_f}]"
            logger.info(chunk_log_msg)
            write_log(chunk_log_msg)

            chunk_in_name = f"chunk_in_{idx}.mp4"
            chunk_in_path = os.path.join(chunks_temp_dir, chunk_in_name)

            chunk_mask_name = f"chunk_mask_{idx}.mp4"
            chunk_mask_path = os.path.join(chunks_temp_dir, chunk_mask_name)

            chunk_out_dir = os.path.join(chunks_temp_dir, f"out_{idx}")
            os.makedirs(chunk_out_dir, exist_ok=True)

            trim_filter = f"trim=start_frame={start_f}:end_frame={end_f},setpts=PTS-STARTPTS"

            subprocess.run([
                'ffmpeg', '-y', '-i', input_video_for_ai,
                '-vf', trim_filter,
                '-c:v', 'libx264', '-an', chunk_in_path
            ], check=True, capture_output=True)

            subprocess.run([
                'ffmpeg', '-y', '-i', mask_video,
                '-vf', trim_filter,
                '-c:v', 'libx264', '-an', chunk_mask_path
            ], check=True, capture_output=True)

            chunk_in_fps = probe_video_fps_value(chunk_in_path)
            chunk_mask_fps = probe_video_fps_value(chunk_mask_path)
            if abs(chunk_in_fps - fps_value) > 1e-3 or abs(chunk_mask_fps - fps_value) > 1e-3:
                raise RuntimeError(
                    f"切片FPS不一致: slice={idx}, video={chunk_in_fps:.6f}, "
                    f"mask={chunk_mask_fps:.6f}, expected={fps_value:.6f}"
                )

            chunk_len = end_f - start_f
            run_cmd = [
                'python', 'run_diffueraser.py',
                '--input_video', os.path.abspath(chunk_in_path),
                '--input_mask', os.path.abspath(chunk_mask_path),
                '--save_path', os.path.abspath(chunk_out_dir),
                '--video_length', str(chunk_len),
                '--mask_dilation_iter', '8',
                '--max_img_size', max_img_size_for_ai,
                '--ref_stride', '10',
                '--neighbor_length', '10',
                '--subvideo_length', '50',
                '--base_model_path', 'weights/stable-diffusion-v1-5',
                '--vae_path', 'weights/sd-vae-ft-mse',
                '--diffueraser_path', 'weights/diffuEraser',
                '--propainter_model_dir', 'weights/propainter'
            ]

            chunk_jobs.append({
                'idx': idx,
                'start_f': start_f,
                'end_f': end_f,
                'chunk_out_dir': chunk_out_dir,
                'run_cmd': run_cmd
            })
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode() if isinstance(e.stderr, bytes) else str(e.stderr)
        write_log(f"Step 7 ERROR: 切片预处理失败 - {err_msg}")
        return jsonify({
            'error': 'Chunk pre-processing failed',
            'details': err_msg
        }), 500

    # 7.2 多GPU并行推理
    task_queue = Queue()
    for job in chunk_jobs:
        task_queue.put(job)
    for _ in active_gpu_ids:
        task_queue.put(None)

    log_lock = threading.Lock()
    result_lock = threading.Lock()
    process_lock = threading.Lock()
    error_lock = threading.Lock()
    chunk_result_map = {}
    failed_jobs = []
    active_processes = {}
    fail_fast_event = threading.Event()
    fail_fast_first_error = {'job': None}
    error_state = {'written': False}

    def append_inference_log(text):
        with log_lock:
            with open(inference_log_path, 'a', encoding='utf-8') as log_file:
                if text.endswith('\n'):
                    log_file.write(text)
                else:
                    log_file.write(text + '\n')
                log_file.flush()

    def write_error_once(text):
        if not text:
            text = 'Unknown error'
        with error_lock:
            if error_state['written']:
                return
            with open(error_log_path, 'w', encoding='utf-8') as f:
                f.write(str(text))
            error_state['written'] = True

    def is_cuda_oom_error(text):
        if not text:
            return False
        lower_text = str(text).lower()
        oom_markers = [
            'cuda out of memory',
            'torch.cuda.outofmemoryerror',
            'cublas_status_alloc_failed',
            'cuda error: out of memory'
        ]
        return any(marker in lower_text for marker in oom_markers)

    def stop_subprocess(proc):
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass
        except Exception:
            pass

    def stop_other_gpu_processes(exclude_gpu=None):
        with process_lock:
            running = list(active_processes.items())
        for running_gpu, running_proc in running:
            if exclude_gpu is not None and str(running_gpu) == str(exclude_gpu):
                continue
            stop_subprocess(running_proc)

    def stream_pipe_lines(pipe, prefix, collector=None, line_handler=None):
        if pipe is None:
            return
        try:
            for line in iter(pipe.readline, ''):
                clean_line = line.rstrip()
                if collector is not None:
                    collector.append(clean_line)
                if line_handler is not None:
                    try:
                        line_handler(clean_line)
                    except Exception:
                        pass
                append_inference_log(f"{prefix}{clean_line}")
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    def gpu_worker(gpu_id):
        while True:
            job = task_queue.get()
            if job is None:
                task_queue.task_done()
                break

            if fail_fast_event.is_set():
                task_queue.task_done()
                continue

            idx = job['idx']
            start_f = job['start_f']
            end_f = job['end_f']
            run_cmd = job['run_cmd']
            chunk_out_dir = job['chunk_out_dir']
            process = None
            stderr_lines = []
            realtime_oom = {'detected': False}

            append_inference_log(
                f"\n>>> [GPU {gpu_id}] Processing Slice {idx + 1}/{len(slice_list)}: Frames [{start_f}-{end_f}]"
            )

            try:
                process_env = os.environ.copy()
                process_env['CUDA_VISIBLE_DEVICES'] = str(gpu_id)

                process = subprocess.Popen(
                    run_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    cwd=diffueraser_path,
                    env=process_env
                )

                with process_lock:
                    active_processes[gpu_id] = process

                def stderr_line_handler(clean_line):
                    if realtime_oom['detected']:
                        return
                    if not is_cuda_oom_error(clean_line):
                        return

                    realtime_oom['detected'] = True
                    failed_job = {
                        'idx': idx,
                        'gpu': gpu_id,
                        'error': clean_line,
                        'cmd': run_cmd
                    }

                    trigger_fail_fast = False
                    with result_lock:
                        failed_jobs.append(failed_job)
                        if fail_fast_first_error['job'] is None:
                            fail_fast_first_error['job'] = failed_job
                        if not fail_fast_event.is_set():
                            fail_fast_event.set()
                            trigger_fail_fast = True

                    append_inference_log(f"[GPU {gpu_id}] Slice {idx} 实时检测到 CUDA OOM: {clean_line}")
                    write_log(f"Step 7 ERROR: 实时检测到 CUDA OOM (GPU={gpu_id}, Slice={idx})")
                    write_error_once(f"CUDA OOM detected on GPU {gpu_id}, slice {idx}: {clean_line}")

                    if trigger_fail_fast:
                        append_inference_log("[Step 7] 检测到 CUDA OOM，触发 fail-fast，正在终止其他 GPU 切片任务。")
                        stop_other_gpu_processes(exclude_gpu=gpu_id)

                    stop_subprocess(process)

                stdout_thread = threading.Thread(
                    target=stream_pipe_lines,
                    args=(process.stdout, f"[GPU {gpu_id}][Slice {idx}] ", None),
                    daemon=True
                )
                stderr_thread = threading.Thread(
                    target=stream_pipe_lines,
                    args=(process.stderr, f"[GPU {gpu_id}][Slice {idx} STDERR] ", stderr_lines, stderr_line_handler),
                    daemon=True
                )
                stdout_thread.start()
                stderr_thread.start()

                terminated_by_fail_fast = False
                while process.poll() is None:
                    if fail_fast_event.is_set():
                        terminated_by_fail_fast = True
                        stop_subprocess(process)
                        break
                    time.sleep(0.2)

                return_code = process.wait()
                stdout_thread.join(timeout=5)
                stderr_thread.join(timeout=5)
                stderr = '\n'.join(stderr_lines)

                if return_code != 0:
                    raise subprocess.CalledProcessError(
                        returncode=return_code,
                        cmd=run_cmd,
                        stderr=stderr
                    )

                if terminated_by_fail_fast:
                    raise RuntimeError("Cancelled by fail-fast due to another OOM task.")

                found_res = False
                for f in os.listdir(chunk_out_dir):
                    if f.endswith('.mp4'):
                        res_path = os.path.join(chunk_out_dir, f)
                        safe_res_name = f"chunk_res_{idx}.mp4"
                        safe_res_path = os.path.join(chunks_temp_dir, safe_res_name)
                        shutil.move(res_path, safe_res_path)
                        with result_lock:
                            chunk_result_map[idx] = safe_res_path
                        found_res = True
                        break

                if not found_res:
                    raise RuntimeError(f"Slice {idx} did not produce an output video.")

                append_inference_log(f"[GPU {gpu_id}] Slice {idx} 完成")

                gc.collect()
                try:
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                except Exception:
                    pass

            except Exception as worker_err:
                if isinstance(worker_err, subprocess.CalledProcessError):
                    stderr_text = worker_err.stderr.decode() if isinstance(worker_err.stderr, bytes) else str(worker_err.stderr or '')
                    err_text = stderr_text.strip() or str(worker_err)
                else:
                    err_text = str(worker_err)

                oom_hit = realtime_oom['detected'] or is_cuda_oom_error(err_text)
                if oom_hit:
                    if not realtime_oom['detected']:
                        trigger_fail_fast = False
                        append_inference_log(f"[GPU {gpu_id}] Slice {idx} 失败: {err_text}")
                        with result_lock:
                            failed_job = {
                                'idx': idx,
                                'gpu': gpu_id,
                                'error': err_text,
                                'cmd': run_cmd
                            }
                            failed_jobs.append(failed_job)
                            if fail_fast_first_error['job'] is None:
                                fail_fast_first_error['job'] = failed_job
                            if not fail_fast_event.is_set():
                                fail_fast_event.set()
                                trigger_fail_fast = True

                        write_log(f"Step 7 ERROR: 实时检测到 CUDA OOM (GPU={gpu_id}, Slice={idx})")
                        write_error_once(f"CUDA OOM detected on GPU {gpu_id}, slice {idx}: {err_text}")
                        if trigger_fail_fast:
                            append_inference_log("[Step 7] 检测到 CUDA OOM，触发 fail-fast，正在终止其他 GPU 切片任务。")
                            stop_other_gpu_processes(exclude_gpu=gpu_id)
                else:
                    if fail_fast_event.is_set():
                        append_inference_log(
                            f"[GPU {gpu_id}] Slice {idx} 已取消（其他切片触发 OOM fail-fast）。"
                        )
                    else:
                        append_inference_log(f"[GPU {gpu_id}] Slice {idx} 失败: {err_text}")
                        with result_lock:
                            failed_jobs.append({
                                'idx': idx,
                                'gpu': gpu_id,
                                'error': err_text,
                                'cmd': run_cmd
                            })
            finally:
                with process_lock:
                    active_processes.pop(gpu_id, None)
                task_queue.task_done()

    workers = []
    for gpu_id in active_gpu_ids:
        t = threading.Thread(target=gpu_worker, args=(gpu_id,), daemon=True)
        t.start()
        workers.append(t)

    task_queue.join()
    for t in workers:
        t.join()

    if failed_jobs:
        failed_jobs = sorted(failed_jobs, key=lambda item: item['idx'])
        first_failed = fail_fast_first_error['job'] or failed_jobs[0]
        if fail_fast_first_error['job'] is not None:
            write_log("Step 7 ERROR: 检测到 CUDA OOM，已触发 fail-fast 并中止剩余切片")
            write_error_once(first_failed['error'])
        else:
            write_log(f"Step 7 ERROR: 并行推理失败，失败切片数={len(failed_jobs)}")
            write_error_once(first_failed['error'])
        return jsonify({
            'error': 'Parallel video processing failed',
            'failed_jobs': failed_jobs,
            'details': first_failed['error'],
            'cmd': ' '.join(first_failed['cmd'])
        }), 500

    expected_indices = list(range(len(slice_list)))
    result_indices = sorted(chunk_result_map.keys())
    if result_indices != expected_indices:
        missing = sorted(set(expected_indices) - set(result_indices))
        write_log(f"Step 7 ERROR: 输出切片不完整，缺失索引: {missing}")
        return jsonify({
            'error': 'Missing chunk outputs after parallel processing',
            'missing_indices': missing
        }), 500

    chunk_result_paths = [chunk_result_map[i] for i in expected_indices]
    write_log("Step 7: 多GPU并行推理完成")

    diffueraser_end = datetime.now()
    diffueraser_duration = (diffueraser_end - diffueraser_start).total_seconds()
    process_time = diffueraser_duration
    logger.info(f"DiffuEraser处理完成，耗时: {diffueraser_duration:.2f}秒")

    # ================= 8. 智能自适应拼接 (Scale2Ref) =================
    step_start = datetime.now()
    write_log("Step 8: 开始拼接视频...")
    logger.info("Step 8: 智能拼接 (FFmpeg Xfade)...")

    # 定义最终 AI 合成视频路径
    ai_full_stitched_video = os.path.join(results_dir, 'ai_full_stitched.mp4')

    # **[重写] 使用 FFmpeg xfade 滤镜进行无痕拼接 (彻底解决色差问题)**
    def stitch_video_chunks(chunk_paths, slice_list, output_path, final_fps):
        if not chunk_paths: return False
        
        # 如果只有一个片段，直接复制
        if len(chunk_paths) == 1:
            shutil.copy(chunk_paths[0], output_path)
            return True

        # 构建 FFmpeg 命令
        # 格式: ffmpeg -i 0.mp4 -i 1.mp4 ... -filter_complex "[0][1]xfade=...[v1];[v1][2]xfade...[v]" ...
        
        input_args = []
        for path in chunk_paths:
            input_args.extend(['-i', path])
            
        filter_chains = []
        last_stream = "0:v" # 初始流是第0个输入
        
        # 遍历切片生成 xfade 链
        for i in range(1, len(chunk_paths)):
            # 下一段的输入流索引
            next_stream = f"{i}:v"
            
            # 计算 offset (偏移量)
            # 逻辑：下一段视频应该在时间轴的什么位置淡入？
            # 答案：就是下一段视频在原始长视频中的"开始时间点"。
            # slice_list[i][0] 是第 i 个切片的 start_frame
            start_frame = slice_list[i][0]
            offset_seconds = start_frame / float(final_fps)
            
            # 计算 duration (重叠时长)
            # 逻辑：上一段结束 - 这一段开始
            prev_end = slice_list[i-1][1]
            curr_start = slice_list[i][0]
            overlap_frames = prev_end - curr_start
            duration_seconds = overlap_frames / float(final_fps)
            
            # 构造滤镜节点
            # [上一个流][下一个流] xfade = transition=fade : duration=... : offset=... [新流]
            out_stream = f"v{i}"
            if i == len(chunk_paths) - 1:
                out_stream = "v_final" # 最后一个输出叫 v_final
                
            filter_cmd = (
                f"[{last_stream}][{next_stream}]"
                f"xfade=transition=fade:duration={duration_seconds:.6f}:offset={offset_seconds:.6f}"
                f"[{out_stream}]"
            )
            filter_chains.append(filter_cmd)
            last_stream = out_stream

        # 组合完整的 filter_complex
        full_filter = ";".join(filter_chains)
        
        cmd = [
            'ffmpeg', '-y',
            *input_args,
            '-filter_complex', full_filter,
            '-map', f"[{last_stream}]", # 映射最后一个输出流
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '18', # 高画质重编码
            '-pix_fmt', 'yuv420p',
            output_path
        ]
        
        # 控制台只保留摘要，完整命令写入任务日志，避免刷屏。
        logger.info("Step 8: 开始 xfade 缝合，共 %s 段", len(chunk_paths))
        write_log(f"Step 8 DEBUG: Xfade Stitch Cmd: {' '.join(cmd)}")
        
        try:
            # 增加 bufsize 避免死锁
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg Xfade Failed: {e.stderr.decode()}")
            return False

    try:
        # **[修改] 调用新的 FFmpeg 拼接函数**
        # 注意：这里不再需要 import cv2 了，因为我们是用 subprocess 调 ffmpeg
        stitch_success = stitch_video_chunks(chunk_result_paths, slice_list, ai_full_stitched_video, fps_value)
        
        if not stitch_success:
            raise Exception("Stitching returned False")
            
        logger.info(f"AI 结果缝合完成: {ai_full_stitched_video}")
        
    except Exception as e:
        logger.error(f"拼接失败: {e}")
        # 降级方案：直接取第一个
        if chunk_result_paths:
            shutil.copy(chunk_result_paths[0], ai_full_stitched_video)
        else:
            return jsonify({'error': 'Merge chunks failed', 'details': str(e)}), 500

    def estimate_boundary_luma_adjust(top_path, bottom_path, sample_band):
        """
        估计下半部分亮度修正系数，避免上下拼接出现明显亮度台阶。
        返回: (gain, offset)
        """
        if cv2 is None or np is None:
            return 1.0, 0.0

        top_cap = cv2.VideoCapture(top_path)
        bottom_cap = cv2.VideoCapture(bottom_path)
        try:
            ok_top, top_frame = top_cap.read()
            ok_bottom, bottom_frame = bottom_cap.read()
            if not ok_top or not ok_bottom:
                return 1.0, 0.0

            sample_h = max(8, min(sample_band, top_frame.shape[0], bottom_frame.shape[0]))
            top_strip = top_frame[-sample_h:, :, :]
            bottom_strip = bottom_frame[:sample_h, :, :]

            top_y = cv2.cvtColor(top_strip, cv2.COLOR_BGR2YCrCb)[:, :, 0].astype(np.float32)
            bottom_y = cv2.cvtColor(bottom_strip, cv2.COLOR_BGR2YCrCb)[:, :, 0].astype(np.float32)

            top_mean = float(np.mean(top_y))
            bottom_mean = float(np.mean(bottom_y))
            if bottom_mean < 1.0:
                return 1.0, 0.0

            gain = top_mean / bottom_mean
            # 只做轻量全局亮度匹配，避免过强修正带来分界线
            gain = max(0.98, min(1.02, gain))

            offset = top_mean - (bottom_mean * gain)
            offset = max(-6.0, min(6.0, offset))
            return gain, offset
        except Exception as luma_err:
            logger.warning("边界亮度估计失败，跳过亮度匹配: %s", luma_err)
            return 1.0, 0.0
        finally:
            top_cap.release()
            bottom_cap.release()

    # 最终输出文件名
    transcoded_result = os.path.join(results_dir, 'transcoded_result.mp4')
    
    try:
        if ENABLE_AI_RESIZE:
            bot_src_w, bot_src_h = probe_video_size(bottom_video_path)
            ai_full_upscaled_video = os.path.join(results_dir, 'ai_full_upscaled.mp4')
            upscale_cmd = [
                'ffmpeg', '-y',
                '-i', ai_full_stitched_video,
                '-vf', f'scale={bot_src_w}:{bot_src_h}:flags={AI_SCALE_MODE},fps={fps},setpts=PTS-STARTPTS',
                '-c:v', 'libx264', '-an',
                ai_full_upscaled_video
            ]
            subprocess.run(upscale_cmd, check=True, capture_output=True)
            ai_for_seam = ai_full_upscaled_video
        else:
            ai_for_seam = ai_full_stitched_video

        top_w, top_h = probe_video_size(top_video_path)
        bot_w, bot_h = probe_video_size(ai_for_seam)

        if top_w <= 0 or top_h <= 0 or bot_w <= 0 or bot_h <= 0:
            raise RuntimeError("Invalid video resolution for final stitching")

        scaled_bot_h = bot_h
        if bot_w != top_w:
            scaled_bot_h = int(round(bot_h * (top_w / float(bot_w))))
        scaled_bot_h = max(2, (scaled_bot_h // 2) * 2)

        # 用边界带采样亮度，仅用于估计整体亮度修正量
        sample_h = max(8, min(32, top_h // 20, scaled_bot_h // 20))

        y_gain, y_offset = estimate_boundary_luma_adjust(
            bottom_video_path,
            ai_for_seam,
            sample_band=sample_h
        )
        logger.info(
            "Step 8 seam params -> top=%sx%s, bot_scaled_h=%s, sample_h=%s, y_gain=%.4f, y_offset=%.2f",
            top_w, top_h, scaled_bot_h, sample_h, y_gain, y_offset
        )

        # 中线保护策略：
        # 1) 下半顶部先保留原始底图若干行，确保与上半无缝
        # 2) 再用渐变过渡到 AI 下半，避免硬分层
        seam_guard_h = max(24, min(96, scaled_bot_h // 10))
        seam_fade_h = max(40, min(180, scaled_bot_h // 6))
        seam_guard_end = seam_guard_h
        seam_fade_end = seam_guard_h + seam_fade_h

        stitch_filter = (
            # 三路视频先统一到同一 FPS / timebase，再进行下半混合与上下拼接，避免偶发上下错位
            f"[0:v]fps={fps},settb=AVTB,setpts=PTS-STARTPTS[top_sync];"
            f"[2:v]scale={top_w}:{scaled_bot_h},format=yuv420p,"
            f"fps={fps},settb=AVTB,setpts=PTS-STARTPTS[bot_src];"
            f"[1:v]scale={top_w}:{scaled_bot_h},format=yuv420p,"
            f"lut=y='clip(val*{y_gain:.6f}+{y_offset:.3f},0,255)',"
            f"fps={fps},settb=AVTB,setpts=PTS-STARTPTS[bot_ai_adj];"
            f"[bot_src][bot_ai_adj]"
            f"blend=all_expr='if(lte(Y,{seam_guard_end}),A,"
            f"if(gte(Y,{seam_fade_end}),B,"
            f"A*(1-(Y-{seam_guard_end})/{float(seam_fade_h):.6f})+B*((Y-{seam_guard_end})/{float(seam_fade_h):.6f})))'"
            f"[bot_mix];"
            f"[top_sync][bot_mix]vstack=inputs=2[v]"
        )

        stitch_cmd = [
            'ffmpeg', '-y',
            '-i', top_video_path,
            '-i', ai_for_seam,
            '-i', bottom_video_path,
            '-filter_complex', stitch_filter,
            '-map', '[v]',
            '-map', '0:a?',
            '-c:v', 'libx264', '-profile:v', 'main', '-preset', 'fast',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-pix_fmt', 'yuv420p',
            transcoded_result
        ]
        
        logger.info("Step 8: 开始最终上下拼接")
        write_log(f"Step 8 DEBUG: Final Stitch Cmd: {' '.join(stitch_cmd)}")
        subprocess.run(stitch_cmd, check=True, capture_output=True)
        logger.info("拼接完成")
        result_video = transcoded_result

        step_cost = (datetime.now() - step_start).total_seconds()
        write_log("Step 8: 最终拼接完成", step_cost)

    except subprocess.CalledProcessError as e:
        logger.error("拼接失败: %s", e.stderr.decode())
        write_log(f"Step 8 ERROR: 拼接失败 - {e.stderr.decode()}")
        return jsonify({'error': 'Merge failed', 'details': e.stderr.decode()}), 500
    except Exception as e:
        logger.error("Step 8 参数或拼接构建失败: %s", str(e))
        write_log(f"Step 8 ERROR: {str(e)}")
        return jsonify({'error': 'Merge failed', 'details': str(e)}), 500

    result_filename = f'result_{job_id}.mp4'
    final_result = os.path.join(app.config['RESULTS_FOLDER'], result_filename)
    
    try:
        shutil.copy(result_video, final_result)
        logger.info(f"结果已就绪: {final_result}")
    except Exception as e:
        logger.exception("复制结果视频失败")
        return jsonify({'error': 'Failed to copy result video'}), 500

    # 记录全流程总耗时
    total_duration = (datetime.now() - job_start_total).total_seconds()
    write_log(f"=== 全流程结束 ===", total_duration)
    
    # 返回结果URL
    result_url = f"/results/{result_filename}"
    
    response_payload = {
        'result_url': result_url,
        'job_id': job_id,
        'process_time': process_time,
    }

    if video_upload_url:
        upload_ok = upload_processed_video(final_result, video_upload_url)
        if upload_ok:
            response_payload['uploaded_video_url'] = video_upload_url
            response_payload['result_uploaded'] = True
            tasks_store.setdefault(job_id, {})
            tasks_store[job_id]['uploaded_video_url'] = video_upload_url
        else:
            response_payload['result_uploaded'] = False
            response_payload['upload_error'] = 'failed to upload processed video'

    return jsonify(response_payload), 200


def _process_video_from_url_impl(job_id, video_url, coordinates, extra=None, webhook_url=None, user_id=None):
    """下载 URL 到 uploads 后调用 process_video_impl（供消息队列工作线程使用）。"""
    if extra is None:
        extra = {}
    try:
        if job_id in tasks_store:
            tasks_store[job_id]['status'] = 'downloading'
            tasks_store[job_id]['step'] = 'downloading'
        send_status_to_queue(job_id, 'downloading', '正在下载视频', progress=5)

        parsed = urlparse(video_url)
        path = unquote(parsed.path)
        fn = os.path.basename(path)
        if '?' in fn:
            fn = fn.split('?')[0]
        fn = re.sub(r'[<>:"/\\|?*]', '_', fn)
        if not fn or len(fn) < 5:
            fn = f"video_{int(time.time())}.mp4"

        filepath = os.path.join(app.config['UPLOAD_FOLDER'], fn)
        with requests.get(video_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)

        if job_id in tasks_store:
            tasks_store[job_id]['status'] = 'processing'
            tasks_store[job_id]['step'] = 'processing'
        send_status_to_queue(job_id, 'processing', '开始去字幕处理', progress=20)

        payload = {
            'filename': fn,
            'coordinates': coordinates,
            'job_id': job_id,
            'webhook_url': webhook_url,
            'user_id': user_id,
            '_inner': True,
        }
        if extra:
            payload.update({k: v for k, v in extra.items() if v is not None})
        resp = process_video_impl(payload)
        j = resp.get_json(silent=True) or {}
        if resp.status_code == 200:
            if job_id in tasks_store:
                tasks_store[job_id].update(
                    {
                        'status': 'completed',
                        'step': 'completed',
                        'result_url': j.get('result_url'),
                        'uploaded_video_url': j.get('uploaded_video_url'),
                        'process_time': j.get('process_time'),
                        'completed_at': datetime.now().isoformat(),
                    },
                )
            send_status_to_queue(
                job_id, 'completed', '处理完成', progress=100, result_url=j.get('result_url'),
            )
        else:
            err = (j or {}).get('error') or (j or {}).get('details') or str(j)
            if job_id in tasks_store:
                tasks_store[job_id].update({'status': 'failed', 'step': 'failed', 'error': err})
            send_status_to_queue(job_id, 'failed', str(err), progress=0)
    except Exception as e:
        logger.exception("消息队列处理失败: %s", e)
        if job_id in tasks_store:
            tasks_store[job_id].update({'status': 'failed', 'error': str(e), 'step': 'failed'})
        send_status_to_queue(job_id, 'failed', str(e), progress=0)


@app.route('/process', methods=['POST'])
def process_video():
    """
    同步/异步由 body.async 与 MGPU_ASYNC_DEFAULT 控制；?sync=1 强制同步。
    与 message_queue/VideoQueueWorker 的 POST /process（202 + job_id 轮询）一致。
    """
    data = request.get_json() or {}
    if data.get('sync') is True or request.args.get('sync') == '1':
        return process_video_impl(data)

    use_async = data.get('async', MGPU_ASYNC_DEFAULT) and not data.get('_inner')
    if not use_async:
        return process_video_impl(data)

    out = {k: v for k, v in data.items() if not str(k).startswith('_')}
    ext_job = out.get('job_id') or out.get('message_id')
    job_id = ext_job or f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    tasks_store[job_id] = {
        'status': 'processing',
        'step': 'queued',
        'job_id': job_id,
        'created_at': datetime.now().isoformat(),
        'source': 'api',
        'filename': out.get('filename'),
        'coordinates': out.get('coordinates'),
        'webhook_url': out.get('webhook_url'),
        'user_id': out.get('user_id'),
        'video_upload_url': out.get('video_upload_url'),
    }
    d = {**out, 'job_id': job_id, '_inner': True}

    def _run():
        with app.app_context():
            try:
                raw_resp = process_video_impl(d)
                if isinstance(raw_resp, tuple):
                    resp = raw_resp[0]
                    status_code = raw_resp[1] if len(raw_resp) > 1 else getattr(resp, 'status_code', 500)
                else:
                    resp = raw_resp
                    status_code = getattr(resp, 'status_code', 500)

                if hasattr(resp, 'get_json'):
                    j = resp.get_json(silent=True) or {}
                else:
                    j = {}

                if status_code == 200:
                    tasks_store[job_id].update(
                        {
                            'status': 'completed',
                            'step': 'completed',
                            'result_url': j.get('result_url'),
                            'uploaded_video_url': j.get('uploaded_video_url'),
                            'process_time': j.get('process_time'),
                            'completed_at': datetime.now().isoformat(),
                        },
                    )
                    send_status_to_queue(
                        job_id, 'completed', '处理完成', progress=100, result_url=j.get('result_url'),
                    )
                else:
                    err = (j or {}).get('error') or (j or {}).get('details') or str(j)
                    tasks_store[job_id].update({'status': 'failed', 'error': err, 'step': 'failed'})
                    send_status_to_queue(job_id, 'failed', str(err), progress=0)
            except Exception as e:
                logger.exception("异步处理失败: %s", e)
                tasks_store[job_id].update({'status': 'failed', 'error': str(e), 'step': 'failed'})
                send_status_to_queue(job_id, 'failed', str(e), progress=0)

    executor.submit(_run)
    return jsonify(
        {
            'job_id': job_id,
            'status': 'queued',
            'message': 'Job accepted. Please poll /status/<job_id> for updates.',
        },
    ), 202


@app.route('/api/process-from-queue', methods=['POST'])
def process_from_queue():
    """
    与 worker_simulation / message_queue 一致：
    video_download_url、video_upload_url、webhook_url、message_id；
    detect_type 可选，取值与 TS 一致：auto | manual（另兼容 automatic）。
    """
    try:
        data = request.get_json() or {}
        video_url = data.get('video_download_url')
        message_id = data.get('message_id')
        webhook_url = data.get('webhook_url')
        user_id = data.get('user_id')
        video_upload_url = data.get('video_upload_url')
        detect_type = normalize_queue_detect_type(data)
        aliyun_region = data.get('aliyun_region')
        if not video_url:
            return jsonify({'error': 'No video URL provided'}), 400
        if not message_id:
            return jsonify({'error': 'message_id is required'}), 400
        if not video_upload_url:
            return jsonify({'error': 'video_upload_url is required'}), 400
        if not webhook_url:
            return jsonify({'error': 'webhook_url is required'}), 400
        if detect_type is None:
            return jsonify({'error': 'detect_type must be manual, auto, or automatic'}), 400

        job_id = str(message_id)
        coordinates = resolve_queue_coordinates(data)
        if detect_type == 'manual' and not data.get('target_regions') and not data.get('coordinates'):
            logger.warning("job=%s detect_type=manual 但未提供 target_regions/coordinates，已回退默认坐标", job_id)
        extra = {
            'bottom_process_height': data.get('bottom_process_height'),
            'video_upload_url': video_upload_url,
        }
        tasks_store[job_id] = {
            'status': 'queued',
            'step': 'queued',
            'created_at': datetime.now().isoformat(),
            'source': 'message_queue',
            'video_download_url': video_url,
            'webhook_url': webhook_url,
            'user_id': user_id,
            'video_upload_url': video_upload_url,
            'detect_type': detect_type,
            'aliyun_region': aliyun_region,
            'coordinates': coordinates,
        }
        executor.submit(
            _process_video_from_url_impl,
            job_id,
            video_url,
            coordinates,
            extra,
            webhook_url,
            user_id,
        )
        return jsonify(
            {
                'job_id': job_id,
                'status': 'accepted',
                'message': 'Video processing started from message queue',
            },
        ), 202
    except Exception as e:
        logger.error("api/process-from-queue: %s", e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/process', methods=['POST'])
def api_v1_process():
    """将任务写入 RabbitMQ media 队列（与 message_queue_worker 一致）。"""
    try:
        body = request.get_json() or {}
        if not body:
            return jsonify({'error': 'Empty request body'}), 400
        if rabbitmq_manager.send_task(body):
            return jsonify(
                {
                    'success': True,
                    'message': 'Task sent to processing queue',
                    'message_id': body.get('message_id', 'generated'),
                    'queue': app.config['MEDIA_QUEUE'],
                },
            ), 202
        return jsonify({'error': 'Failed to send task to queue'}), 500
    except Exception as e:
        logger.error("api/v1/process: %s", e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/queue-status', methods=['GET'])
def api_queue_status():
    try:
        con = pika.BlockingConnection(pika.URLParameters(app.config['RABBITMQ_URL']))
        ch = con.channel()
        mq = ch.queue_declare(queue=app.config['MEDIA_QUEUE'], passive=True)
        sq = ch.queue_declare(queue=app.config['STATUS_QUEUE'], passive=True)
        con.close()
        return jsonify(
            {
                'media_queue': {
                    'name': app.config['MEDIA_QUEUE'], 'message_count': mq.method.message_count,
                    'consumer_count': mq.method.consumer_count,
                },
                'status_queue': {
                    'name': app.config['STATUS_QUEUE'], 'message_count': sq.method.message_count,
                    'consumer_count': sq.method.consumer_count,
                },
            },
        )
    except Exception as e:
        logger.error("queue-status: %s", e)
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify(
        {
            'status': 'ok',
            'service': 'app_mgpu_resize_h_mq',
            'ts': datetime.now().isoformat(),
        },
    )


@app.route('/inference-log/<job_id>')
def get_inference_log(job_id):
    """获取指定任务的去字幕推理日志"""
    work_dir = os.path.join(app.config['PROCESSED_FOLDER'], job_id)
    log_path = os.path.join(work_dir, 'inference.log')
    
    # 检查日志文件是否存在
    if not os.path.exists(log_path):
        return jsonify({
            'status': 'not_ready',
            'log': '推理日志尚未生成...'
        }), 200
    
    # 读取日志内容（按行读取，便于前端渲染）
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            log_lines = f.readlines()
        # 提取关键信息：推理时间、进度（如 98%|█████████▊| 59/60）
        filtered_log = []
        for line in log_lines:
            line = line.strip()
            # 提取推理时间
            if "推理耗时:" in line:
                filtered_log.append(line)
            # 提取进度条（含 % 符号的行）
            elif "%" in line and ("|" in line or "/" in line):
                filtered_log.append(line)
            # 保留关键提示
            elif any(keyword in line for keyword in ["DiffuEraser inference", "Priori generating"]):
                filtered_log.append(line)
            # 保留错误与 OOM 信息，确保前端可第一时间看到
            elif any(keyword in line.lower() for keyword in ["error", "oom", "out of memory", "fail-fast"]):
                filtered_log.append(line)
        
        return jsonify({
            'status': 'ready',
            'log': filtered_log  # 返回过滤后的关键日志列表
        }), 200
    
    except Exception as e:
        logger.error(f"读取推理日志失败: {str(e)}")
        return jsonify({
            'status': 'error',
            'log': [f"日志读取失败: {str(e)}"]
        }), 500

@app.route('/results/<filename>')
def get_result(filename):
    """获取处理后的视频"""
    return send_from_directory(app.config['RESULTS_FOLDER'], filename)

@app.route('/status/<job_id>')
def job_status(job_id):
    """
    轮询任务状态。优先返回 tasks_store（与 message_queue 中 VideoQueueWorker 轮询 /status 一致）；
    含顶层 'status' 字段: completed / failed / processing，以及 result_url 供外部队列脚本使用。
    """
    t = tasks_store.get(job_id)
    if t:
        st = t.get('status', 'unknown')
        out = {
            'job_id': job_id,
            'status': st,
            'step': t.get('step', 'unknown'),
        }
        if st == 'completed' and t.get('result_url'):
            out['result_url'] = t.get('result_url')
            if t.get('uploaded_video_url'):
                out['uploaded_video_url'] = t.get('uploaded_video_url')
            out['process_time'] = t.get('process_time')
        if st == 'failed':
            out['error'] = t.get('error', 'Unknown error')
        if st in ('processing', 'queued', 'downloading') or st == 'unknown':
            out.setdefault('message', t.get('step'))
        if st in ('completed', 'failed'):
            return jsonify(out), 200
        if st in ('processing', 'queued', 'downloading', 'downloaded', 'unknown'):
            return jsonify(out), 200
        return jsonify(out), 200

    work_dir = os.path.join(app.config['PROCESSED_FOLDER'], job_id)
    if not os.path.exists(work_dir):
        return jsonify({'status': 'not_found', 'job_id': job_id}), 404

    error_file = os.path.join(work_dir, 'error.log')
    if os.path.exists(error_file):
        with open(error_file, 'r', encoding='utf-8', errors='ignore') as f:
            error_msg = f.read()
        return jsonify(
            {
                'job_id': job_id,
                'status': 'failed',
                'overall': 'failed',
                'error': error_msg,
            },
        ), 200

    status = {
        'job_id': job_id,
        'frames_extracted': os.path.exists(os.path.join(work_dir, 'frames'))
        and len(os.listdir(os.path.join(work_dir, 'frames'))) > 0,
        'masks_generated': os.path.exists(os.path.join(work_dir, 'masks'))
        and len(os.listdir(os.path.join(work_dir, 'masks'))) > 0,
        'mask_video_created': os.path.exists(os.path.join(work_dir, 'mask.mp4')),
        'processing_completed': os.path.exists(os.path.join(work_dir, 'results'))
        and any(f.endswith('.mp4') for f in os.listdir(os.path.join(work_dir, 'results'))),
    }
    process_time = 0
    log_path = os.path.join(work_dir, 'inference.log')
    if os.path.exists(log_path):
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if '推理耗时:' in line:
                    m = re.search(r'推理耗时: (\d+\.\d+)秒', line)
                    if m:
                        process_time = float(m.group(1))
                        break
    status['process_time'] = process_time
    if status['processing_completed']:
        result_filename = f'result_{job_id}.mp4'
        rpath = f"/results/{result_filename}"
        return jsonify(
            {
                'job_id': job_id,
                'status': 'completed',
                'overall': 'completed',
                'result_url': rpath,
            },
        ), 200
    if status['mask_video_created']:
        status['overall'] = 'processing'
    elif status['masks_generated']:
        status['overall'] = 'generating_masks'
    elif status['frames_extracted']:
        status['overall'] = 'generating_masks'
    else:
        status['overall'] = 'extracting_frames'
    status['status'] = 'processing'
    return jsonify(status), 200

if __name__ == '__main__':
    init_message_queue()
    port = int(os.environ.get('PORT', 2026))
    if serve is not None:
        logger.info("waitress 启动，端口: %s", port)
        serve(app, host='0.0.0.0', port=port, threads=8)
    else:
        logger.warning("未安装 waitress，使用 Flask 开发服；生产环境请: pip install waitress")
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
