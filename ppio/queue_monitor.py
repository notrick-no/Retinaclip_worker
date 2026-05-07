from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, unquote, urlparse

import pika
import requests

logger = logging.getLogger(__name__)


@dataclass
class QueueStats:
    message_count: int
    consumer_count: int
    """RabbitMQ Management API 的 messages_unacknowledged；未启用 Management 时为 None。"""
    messages_unacknowledged: Optional[int] = None


def queue_has_pending_work(st: QueueStats) -> bool:
    """有待投递或在途未 ack 则视为仍有任务，不应进入空闲关机计时。"""
    if st.message_count > 0:
        return True
    if st.messages_unacknowledged is not None and st.messages_unacknowledged > 0:
        return True
    return False


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
    def __init__(
        self,
        rabbitmq_url: str,
        queue_name: str,
        *,
        management_url: Optional[str] = None,
        management_vhost: str = "/",
    ) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = queue_name
        self._management_url = (management_url or "").strip() or None
        self._management_vhost = management_vhost if management_vhost else "/"
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel = None
        self._mgmt_warned = False

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

    def _fetch_messages_unacknowledged(self) -> Optional[int]:
        if not self._management_url:
            return None
        v_enc = quote(self._management_vhost, safe="")
        q_enc = quote(self.queue_name, safe="")
        url = f"{self._management_url}/api/queues/{v_enc}/{q_enc}"
        auth = None
        try:
            p = urlparse(self._management_url)
            if p.username is not None or p.password is not None:
                auth = (unquote(p.username or ""), unquote(p.password or ""))
        except Exception:  # noqa: BLE001
            auth = None
        try:
            r = requests.get(url, auth=auth, timeout=10)
            r.raise_for_status()
            data = r.json()
            unack = int(data.get("messages_unacknowledged", 0))
            self._mgmt_warned = False
            return unack
        except Exception as e:  # noqa: BLE001
            if not self._mgmt_warned:
                self._mgmt_warned = True
                logger.warning(
                    "RABBITMQ_MANAGEMENT_URL 拉取未 ack 失败，将仅按 message_count 判断空闲: %s",
                    e,
                )
            else:
                logger.debug("Management 拉取未 ack: %s", e)
            return None

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
        message_count = int(r.method.message_count)
        # 只有当 ready=0 时才需要查 unack，用于防止「取走但未 ack 的在途任务」导致误停机。
        # ready>0 时本来就不会触发缩容，因此无需额外打 Management API，减少外部轮询压力。
        unack = self._fetch_messages_unacknowledged() if message_count == 0 else None
        return QueueStats(
            message_count=message_count,
            consumer_count=int(r.method.consumer_count),
            messages_unacknowledged=unack,
        )
