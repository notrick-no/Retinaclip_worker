#!/usr/bin/env python3
"""
测试 RabbitMQ 连接与队列状态。

用法：
  python test_rabbitmq.py
  # 或指定 URL/队列
  RABBITMQ_URL=amqp://user:pass@host:5672/ RABBITMQ_QUEUE=media.uploaded python test_rabbitmq.py
"""

import os
import sys

# 从脚本所在目录加载 .env（若存在）
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(_env_path)
except ImportError:
    pass

try:
    import pika
except ImportError:
    print("请先安装: pip install pika")
    sys.exit(1)


def mask_url(url: str) -> str:
    """隐藏密码，便于日志输出。"""
    if "@" not in url:
        return url
    pre, rest = url.split("@", 1)
    if ":" in pre:
        user = pre.split(":")[0]
        return f"{user}:****@{rest}"
    return f"****@{rest}"


def main():
    url = os.environ.get("RABBITMQ_URL", "").strip()
    queue = os.environ.get("RABBITMQ_QUEUE", "media.uploaded")

    if not url:
        print("错误: 未设置 RABBITMQ_URL")
        print("示例: export RABBITMQ_URL='amqp://user:pass@host:5672/'")
        sys.exit(1)

    print(f"连接: {mask_url(url)}")
    print(f"队列: {queue}")
    print("---")

    try:
        params = pika.URLParameters(url)
        params.socket_timeout = 10
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        # passive=True：队列不存在会报错，存在则返回队列信息
        state = ch.queue_declare(queue=queue, passive=True)
        count = state.method.message_count
        conn.close()
        print("状态: 连接成功")
        print(f"队列消息数: {count}")
        sys.exit(0)
    except pika.exceptions.AMQPConnectionError as e:
        print("状态: 连接失败（网络/认证/地址）")
        print(f"错误: {e}")
        sys.exit(1)
    except pika.exceptions.ChannelClosedByBroker as e:
        # 例如 404 队列不存在
        print("状态: 连接成功，但队列不存在或不可访问")
        print(f"错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"状态: 异常 - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
