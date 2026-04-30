from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class PPIOSchedulerConfig:
    ppio_api_key: str
    ppio_base_url: str
    """按队列 create 时依次尝试；逗号分隔多个 productId，前一个失败（如库存不足）则试下一个。"""
    ppio_product_ids: List[str]
    """非空时为托管模式：依次对这些实例 start/stop，不 create/delete。多个 ID 用逗号分隔，有任务时按顺序 start 直到成功；空队列时全部 stop。"""
    managed_instance_ids: List[str]
    ppio_cluster_id: Optional[str]
    ppio_gpu_num: int
    ppio_rootfs_size: int
    ppio_image_url: str
    ppio_image_auth: Optional[str]
    ppio_image_auth_id: Optional[str]
    ppio_ports: str
    ppio_billing: str
    ppio_instance_name_prefix: str
    ppio_create_command: str
    ppio_create_entrypoint: Optional[str]
    ppio_env_extra: List[tuple[str, str]]
    ppio_kind: str
    ppio_month: int
    ppio_min_cuda: Optional[str]
    rabbitmq_url: str
    rabbitmq_queue: str
    poll_interval_ms: int
    # idle_close_ms=0 时，scheduler 用 idle_min_grace_ms 作为「ready 为空」后的最短等待再关机
    idle_close_ms: int
    idle_min_grace_ms: int
    idle_action: str
    log_tail_lines: int
    log_fetch_interval_s: int
    products_list_path: str
    """实例详情端点路径，覆盖默认猜测列表的首选；从 PPIO 文档 / 控制台 DevTools 抓出来后填入。"""
    instance_detail_path: Optional[str]
    """实例列表端点路径，同上"""
    instance_list_path: Optional[str]
    """为 true 时由队列深度自动起停；为 false 时仅 HTTP 管理接口/手动可起停（用于先调试 API）。"""
    queue_automation: bool
    admin_listen_host: str
    admin_listen_port: int
    """非空时启用 /api/... 管理端点，请求头需 Authorization: Bearer <token>"""
    admin_token: Optional[str]


def _b(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _i(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return int(str(raw).strip(), 10)


def _parse_csv_ids(raw: str) -> List[str]:
    """逗号/分号分隔的 ID 或 productId 列表，去空、去首尾空格。"""
    if not (raw or "").strip():
        return []
    out: List[str] = []
    for chunk in raw.replace(";", ",").split(","):
        s = chunk.strip()
        if s:
            out.append(s)
    return out


def _parse_env_list(raw: str) -> List[tuple[str, str]]:
    if not (raw or "").strip():
        return []
    out: List[tuple[str, str]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            out.append((k, v))
    return out


def _env_bool(name: str, default: bool) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    if v in ("", "default"):
        return default
    return v in ("1", "true", "yes", "on")


def _admin_listen_port() -> int:
    """Railway/Heroku 等会注入 PORT；显式 PPIO_ADMIN_LISTEN_PORT 优先。"""
    raw = os.environ.get("PPIO_ADMIN_LISTEN_PORT")
    if raw is not None and str(raw).strip() != "":
        return max(1, int(str(raw).strip(), 10))
    raw_port = os.environ.get("PORT")
    if raw_port is not None and str(raw_port).strip() != "":
        return max(1, int(str(raw_port).strip(), 10))
    return 9843


def _admin_listen_host() -> str:
    """有 PORT 或 Railway 环境时默认 0.0.0.0，否则本机 127.0.0.1。"""
    raw = os.environ.get("PPIO_ADMIN_LISTEN_HOST")
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip()
    if (os.environ.get("PORT") or "").strip() or (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip():
        return "0.0.0.0"
    return "127.0.0.1"


def load_config() -> PPIOSchedulerConfig:
    ppio_key = _b("PPIO_API_KEY") or _b("PPINFRA_API_KEY")
    if not ppio_key:
        raise ValueError("请设置 PPIO_API_KEY 或 PPINFRA_API_KEY")

    managed_ids = _parse_csv_ids(_b("PPIO_MANAGED_INSTANCE_ID"))
    product_ids = _parse_csv_ids(_b("PPIO_PRODUCT_ID"))
    if not managed_ids and not product_ids:
        raise ValueError(
            "请设置 PPIO_MANAGED_INSTANCE_ID（仅启停已有实例）或 PPIO_PRODUCT_ID（创建新实例）；"
            "多个备用值可用英文逗号分隔"
        )

    rmq = _b("RABBITMQ_URL")
    if not rmq:
        raise ValueError("请设置 RABBITMQ_URL（与 dev/message_queue_worker 一致）")

    create_cmd = _b("PPIO_CREATE_COMMAND")
    if not create_cmd:
        create_cmd = (
            '/bin/bash -lc "set -euo pipefail; cd /opt/retinaclip && python3 -u message_queue_worker.py"'
        )

    queue = (
        _b("RABBITMQ_QUEUE")
        or _b("RABBITMQ_MEDIA_UPLOAD")
        or _b("MEDIA_QUEUE")
        or "media.uploaded"
    )

    if managed_ids:
        default_idle = "stop"
    else:
        default_idle = "delete"
    raw_idle = (_b("PPIO_IDLE_ACTION") or default_idle).lower() or default_idle
    if managed_ids and raw_idle == "delete":
        raw_idle = "stop"

    admin_tok = _b("PPIO_ADMIN_TOKEN") or None

    return PPIOSchedulerConfig(
        ppio_api_key=ppio_key,
        ppio_base_url=_b("PPIO_BASE_URL", "https://api.ppinfra.com").rstrip("/"),
        ppio_product_ids=product_ids,
        managed_instance_ids=managed_ids,
        ppio_cluster_id=_b("PPIO_CLUSTER_ID") or None,
        ppio_gpu_num=_i("PPIO_GPU_NUM", 1),
        ppio_rootfs_size=_i("PPIO_ROOTFS_GB", 50),
        ppio_image_url=_b("PPIO_IMAGE_URL", "python:3.11-slim"),
        ppio_image_auth=_b("PPIO_IMAGE_AUTH") or None,
        ppio_image_auth_id=_b("PPIO_IMAGE_AUTH_ID") or None,
        ppio_ports=_b("PPIO_PORTS", ""),
        ppio_billing=_b("PPIO_BILLING", "onDemand"),
        ppio_instance_name_prefix=_b("PPIO_INSTANCE_NAME_PREFIX", "retinaclip-ppio-sched-"),
        ppio_create_command=create_cmd,
        ppio_create_entrypoint=_b("PPIO_CREATE_ENTRYPOINT") or None,
        ppio_env_extra=_parse_env_list(_b("PPIO_EXTRA_ENVS", "")),
        ppio_kind=_b("PPIO_INSTANCE_KIND", "gpu"),
        ppio_month=_i("PPIO_COMMIT_MONTHS", 0),
        ppio_min_cuda=_b("PPIO_MIN_CUDA") or None,
        rabbitmq_url=rmq,
        rabbitmq_queue=queue,
        poll_interval_ms=max(1000, _i("PPIO_SCHEDULER_POLL_INTERVAL_MS", 15000)),
        idle_close_ms=max(
            0,
            _i(
                "PPIO_IDLE_CLOSE_MS",
                # 默认 5min：RabbitMQ message_count 不含未 ack 的在途消息，取走后队列可先变空而任务仍在跑
                _i("PPIO_SCALE_DOWN_IDLE_MS", 300000),
            ),
        ),
        idle_min_grace_ms=max(0, _i("PPIO_IDLE_MIN_GRACE_MS", 120000)),
        idle_action=raw_idle,
        log_tail_lines=max(1, min(2000, _i("PPIO_LOG_TAIL", 200))),
        log_fetch_interval_s=max(5, _i("PPIO_LOG_FETCH_INTERVAL_S", 30)),
        products_list_path=_b("PPIO_PRODUCTS_API_PATH", "/gpu-instance/openapi/v1/products"),
        instance_detail_path=_b("PPIO_INSTANCE_DETAIL_API_PATH") or None,
        instance_list_path=_b("PPIO_INSTANCE_LIST_API_PATH") or None,
        queue_automation=_env_bool("PPIO_QUEUE_AUTOMATION", True),
        admin_listen_host=_admin_listen_host(),
        admin_listen_port=_admin_listen_port(),
        admin_token=admin_tok,
    )
