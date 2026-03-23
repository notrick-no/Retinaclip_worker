#!/usr/bin/env python3
"""
Mingle 视频处理脚本 - 容器入口点

此脚本在 Docker 容器内运行，负责：
1. 从环境变量读取任务参数
2. 下载视频
3. 检测并去除字幕/水印
4. 上传处理后的视频
5. 向 stdout 输出结果 JSON

环境变量：
    TASK_MESSAGE_ID      - 任务消息 ID
    TASK_VIDEO_URL       - 视频下载 URL
    TASK_WEBHOOK_URL     - Webhook 回调 URL（可选，容器内可发送进度）
    TASK_DETECT_TYPE     - 检测类型 (auto/manual)
    TASK_USER_ID         - 用户 ID
    TASK_TARGET_REGIONS  - 目标区域 JSON (可选)
"""

import os
import sys
import json
import time
import hashlib
import uuid
import logging
import requests
from pathlib import Path

# 配置日志（输出到 stderr，不影响 stdout 的 JSON 输出）
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# 临时目录
INPUT_DIR = Path("/tmp/videos/input")
OUTPUT_DIR = Path("/tmp/videos/output")


def send_progress(webhook_url: str, message_id: str, stage: str,
                  percentage: int, message: str, user_id: str = None):
    """向 webhook 发送进度更新"""
    if not webhook_url:
        return

    # 与 Node 侧 webhook-sender.ts 的 WebhookEventType 保持一致：
    # downloading -> download_progress
    # processing -> processing_progress
    # uploading -> upload_progress
    progress_stage_to_event_type = {
        "downloading": "download_progress",
        "processing": "processing_progress",
        "uploading": "upload_progress",
    }
    event_type = progress_stage_to_event_type.get(stage)
    if not event_type:
        logger.warning(f"未知进度 stage={stage}，将回退为 event_type={stage}_progress")
        event_type = f"{stage}_progress"

    webhook_secret = os.environ.get("WEBHOOK_SECRET", "")
    payload = {
        "job_id": message_id,
        "queue_job_id": message_id,
        "queueJobId": message_id,
        "user_id": user_id,
        "status": stage.upper(),
        "event_type": event_type,
        "current_stage": stage,
        "progress_percentage": percentage,
        "progress_message": message,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    import hmac
    signature = "sha256=" + hmac.new(
        webhook_secret.encode(), payload_str.encode(), hashlib.sha256
    ).hexdigest()

    try:
        delivery = str(uuid.uuid4())
        requests.post(
            webhook_url,
            data=payload_str,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature,
                # 让 Node/服务端可选地依赖该头进行路由/观测
                "X-Webhook-Event": event_type,
                "X-Webhook-Delivery": delivery,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"发送进度更新失败: {e}")


def download_video(url: str, message_id: str) -> Path:
    """下载视频到本地临时目录"""
    logger.info(f"开始下载视频: {url[:100]}...")
    
    # 从 URL 提取文件扩展名
    ext = ".mp4"
    if "." in url.split("?")[0].split("/")[-1]:
        ext = "." + url.split("?")[0].split("/")[-1].rsplit(".", 1)[-1]
    
    output_path = INPUT_DIR / f"{message_id}{ext}"
    
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()
    
    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0
    
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                progress = int(downloaded / total_size * 100)
                if progress % 10 == 0:
                    logger.info(f"下载进度: {progress}%")
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"视频下载完成: {file_size_mb:.1f} MB")
    
    return output_path


def process_video(input_path: Path, message_id: str, detect_type: str,
                  target_regions: list = None) -> Path:
    """
    处理视频（去字幕/去水印）
    
    TODO: 这里需要替换为实际的视频处理逻辑
    当前为占位实现，演示容器的输入输出流程
    """
    logger.info(f"开始处理视频: {input_path.name}")
    logger.info(f"检测类型: {detect_type}")
    
    if target_regions:
        logger.info(f"目标区域: {len(target_regions)} 个")
        for i, region in enumerate(target_regions):
            logger.info(f"  区域 {i+1}: x={region['x']}, y={region['y']}, "
                       f"w={region['width']}, h={region['height']}")
    
    output_path = OUTPUT_DIR / f"processed_{message_id}.mp4"
    
    # ============================================================
    # TODO: 在这里实现实际的视频处理逻辑
    # 
    # 例如使用 OpenCV + FFmpeg:
    #   1. 读取视频帧
    #   2. 检测字幕/水印区域（auto 模式）
    #   3. 使用修复算法去除字幕/水印
    #   4. 输出处理后的视频
    #
    # 参考库：
    #   - opencv-python: 图像处理
    #   - paddleocr: 字幕检测
    #   - lama-cleaner: 图像修复
    # ============================================================
    
    # 占位：直接复制输入文件
    import shutil
    shutil.copy2(input_path, output_path)
    
    logger.info(f"视频处理完成: {output_path.name}")
    return output_path


def upload_video(file_path: Path, message_id: str) -> str:
    """
    上传处理后的视频
    
    TODO: 需要根据实际的存储配置实现上传逻辑
    可选方案：
    - 上传到 S3
    - 上传到 OSS
    - 返回本地文件路径（供后续处理）
    """
    logger.info(f"开始上传视频: {file_path.name}")
    
    # ============================================================
    # TODO: 实现实际的上传逻辑
    # 
    # 例如上传到 S3:
    #   import boto3
    #   s3 = boto3.client('s3')
    #   bucket = os.environ.get('AWS_S3_BUCKET')
    #   key = f"processed/{message_id}/{file_path.name}"
    #   s3.upload_file(str(file_path), bucket, key)
    #   return f"s3://{bucket}/{key}"
    # ============================================================
    
    # 占位：返回文件路径
    output_url = f"file://{file_path}"
    logger.info(f"视频上传完成: {output_url}")
    return output_url


def main():
    start_time = time.time()
    
    # 读取环境变量
    message_id = os.environ.get("TASK_MESSAGE_ID", "unknown")
    video_url = os.environ.get("TASK_VIDEO_URL", "")
    webhook_url = os.environ.get("TASK_WEBHOOK_URL", "")
    detect_type = os.environ.get("TASK_DETECT_TYPE", "auto")
    user_id = os.environ.get("TASK_USER_ID", "")
    target_regions_json = os.environ.get("TASK_TARGET_REGIONS", "")
    
    logger.info(f"===== 任务开始: {message_id} =====")
    logger.info(f"视频 URL: {video_url[:100]}...")
    logger.info(f"检测类型: {detect_type}")
    
    if not video_url:
        logger.error("TASK_VIDEO_URL 未设置")
        sys.exit(1)
    
    # 解析目标区域
    target_regions = None
    if target_regions_json:
        try:
            target_regions = json.loads(target_regions_json)
        except json.JSONDecodeError:
            logger.warning("目标区域 JSON 解析失败，忽略")
    
    try:
        # Step 1: 下载视频
        send_progress(webhook_url, message_id, "downloading", 0,
                     "正在下载视频...", user_id)
        input_path = download_video(video_url, message_id)
        send_progress(webhook_url, message_id, "downloading", 100,
                     "视频下载完成", user_id)
        
        # Step 2: 处理视频
        send_progress(webhook_url, message_id, "processing", 0,
                     "正在处理视频...", user_id)
        output_path = process_video(input_path, message_id, detect_type,
                                   target_regions)
        send_progress(webhook_url, message_id, "processing", 100,
                     "视频处理完成", user_id)
        
        # Step 3: 上传视频
        send_progress(webhook_url, message_id, "uploading", 0,
                     "正在上传视频...", user_id)
        output_url = upload_video(output_path, message_id)
        send_progress(webhook_url, message_id, "uploading", 100,
                     "视频上传完成", user_id)
        
        processing_time = time.time() - start_time
        
        # Step 4: 输出结果 JSON 到 stdout
        result = {
            "output_video_url": output_url,
            "processing_time": round(processing_time, 2),
        }
        
        # 这行 JSON 会被 Worker 编排器解析
        print(json.dumps(result))
        
        logger.info(f"===== 任务完成: {message_id} ({processing_time:.1f}s) =====")
        
    except Exception as e:
        logger.error(f"任务处理失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # 清理临时文件
        for dir_path in [INPUT_DIR, OUTPUT_DIR]:
            for f in dir_path.iterdir():
                try:
                    f.unlink()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
