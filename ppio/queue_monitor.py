from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import pika

logger = logging.getLogger(__name__)


@dataclass
class QueueStats:
    message_count: int
    consumer_count: int


def _mask_url(rabbitmq_url: str) -> str:
    try:
        p = urlparse(rabbitmq_url)
        if p.password:
            netloc = f"{p.username}:***@{p.hostname or ''}"
            if p.port:
                netloc += f":{p.port}"
            return p._replace(netloc=netloc).geturl()
    except Exception:  # noqa: BLE001
        pass
    return rabbitmq_url


class QueueDepthMonitor:
    def __init__(self, rabbitmq_url: str, queue_name: str) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = queue_name
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel = None

    def connect(self) -> None:
        params = pika.URLParameters(self.rabbitmq_url)
        params.heartbeat = 600
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        logger.info(
            "RabbitMQ 已连接(调度观测): %s 队列=%s",
            _mask_url(self.rabbitmq_url),
            self.queue_name,
        )

    def close(self) -> None:
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:  # noqa: BLE001
            pass
        self._connection = None
        self._channel = None

    def passive_check(self) -> QueueStats:
        if not self._channel:
            self.connect()
        assert self._channel is not None
        try:
            r = self._channel.queue_declare(
                queue=self.queue_name, passive=True, durable=True
            )
        except Exception:
            self.close()
            self.connect()
            assert self._channel is not None
            r = self._channel.queue_declare(
                queue=self.queue_name, passive=True, durable=True
            )
        return QueueStats(
            message_count=int(r.method.message_count),
            consumer_count=int(r.method.consumer_count),
        )
