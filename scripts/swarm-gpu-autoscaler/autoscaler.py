#!/usr/bin/env python3
"""
Swarm GPU 节点按需启停「守门员」脚本

跑在 Manager 节点上（可放在容器内并挂载 /var/run/docker.sock），逻辑：
1. 监控 RabbitMQ 队列堆积量
2. 有任务时：scale 服务 > 0，若 GPU 实例已关机则调用阿里云 API 开机
3. 队列连续 X 分钟为空：将 GPU 节点设为 drain，scale 到 0，再调用 API 关机

环境变量见 config.py，本地测试可设 AUTOSCALE_DRY_RUN=true 避免真实启停实例。
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pika
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526 import models as ecs_models

from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_queue_size(rabbitmq_url: str, queue: str, timeout: int = 10) -> int:
    """获取 RabbitMQ 队列当前消息数（含 ready + unacked）。"""
    try:
        params = pika.URLParameters(rabbitmq_url)
        params.socket_timeout = timeout
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        state = ch.queue_declare(queue=queue, passive=True)
        size = state.method.message_count
        conn.close()
        return size
    except Exception as e:
        log.warning("获取队列长度失败: %s", e)
        return -1


def docker_cmd(cmd: List[str], dry_run: bool) -> tuple:
    """执行 docker 命令（通过 DOCKER_HOST 或默认 socket）。"""
    if dry_run:
        log.info("[DRY-RUN] docker %s", " ".join(cmd))
        return True, ""
    try:
        out = subprocess.run(
            ["docker"] + cmd,
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ},
        )
        if out.returncode != 0:
            return False, (out.stderr or out.stdout or "").strip()
        return True, (out.stdout or "").strip()
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except FileNotFoundError:
        return False, "docker not found"
    except Exception as e:
        return False, str(e)


def scale_service(stack_name: str, service_name: str, replicas: int, dry_run: bool) -> bool:
    """扩缩容 Swarm 服务。服务全名 = stack_name_service_name。"""
    full_name = f"{stack_name}_{service_name}"
    ok, err = docker_cmd(["service", "scale", f"{full_name}={replicas}"], dry_run)
    if not ok:
        log.error("scale 失败 %s=%s: %s", full_name, replicas, err)
        return False
    log.info("已执行 scale %s=%s", full_name, replicas)
    return True


def node_update_availability(node_id: str, availability: str, dry_run: bool) -> bool:
    """设置节点可用性：drain / active。"""
    ok, err = docker_cmd(["node", "update", "--availability", availability, node_id], dry_run)
    if not ok:
        log.error("node update --availability %s %s 失败: %s", availability, node_id, err)
        return False
    log.info("已执行 node update --availability %s %s", availability, node_id)
    return True


def create_ecs_client(access_key_id: str, access_key_secret: str, region_id: str) -> EcsClient:
    cfg = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        endpoint=f"ecs.{region_id}.aliyuncs.com",
        region_id=region_id,
    )
    return EcsClient(cfg)


def get_instance_status(client: EcsClient, instance_id: str, region_id: str) -> Optional[str]:
    """查询实例状态：Running / Stopped / Starting / Stopping。"""
    try:
        req = ecs_models.DescribeInstanceStatusRequest(
            region_id=region_id,
            instance_id=[instance_id],
        )
        resp = client.describe_instance_status(req)
        # SDK 返回 body.instance_statuses.instance_status (列表)
        inst_list = getattr(resp.body, "instance_statuses", None)
        status_list = (getattr(inst_list, "instance_status", None) if inst_list else None) or []
        if not status_list:
            return None
        return getattr(status_list[0], "status", None)
    except Exception as e:
        log.warning("DescribeInstanceStatus 失败: %s", e)
        return None


def start_instance(client: EcsClient, instance_id: str, dry_run: bool) -> bool:
    if dry_run:
        log.info("[DRY-RUN] 阿里云 StartInstance %s", instance_id)
        return True
    try:
        req = ecs_models.StartInstanceRequest(instance_id=instance_id)
        client.start_instance(req)
        log.info("已调用 StartInstance %s", instance_id)
        return True
    except Exception as e:
        log.error("StartInstance 失败: %s", e)
        return False


def stop_instance(
    client: EcsClient,
    instance_id: str,
    force_stop: bool = False,
    stopped_mode: str = "StopCharging",
    dry_run: bool = False,
) -> bool:
    """停止实例。StoppedMode: StopCharging 节省停机 | KeepCharging 普通停机。"""
    if dry_run:
        log.info("[DRY-RUN] 阿里云 StopInstance %s (StoppedMode=%s)", instance_id, stopped_mode)
        return True
    try:
        req = ecs_models.StopInstanceRequest(
            instance_id=instance_id,
            force_stop=force_stop,
            stopped_mode=stopped_mode,
        )
        client.stop_instance(req)
        log.info("已调用 StopInstance %s (StoppedMode=%s)", instance_id, stopped_mode)
        return True
    except Exception as e:
        log.error("StopInstance 失败: %s", e)
        return False


def wait_instance_running(
    client: EcsClient, instance_id: str, region_id: str, timeout_seconds: int = 300, poll_interval: int = 15
) -> bool:
    """等待实例变为 Running。"""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = get_instance_status(client, instance_id, region_id)
        if status == "Running":
            log.info("实例 %s 已 Running", instance_id)
            return True
        if status:
            log.info("实例 %s 当前状态: %s", instance_id, status)
        time.sleep(poll_interval)
    log.warning("等待实例 Running 超时 %ss", timeout_seconds)
    return False


def run_loop(cfg: dict):
    rabbitmq = cfg["rabbitmq"]
    swarm = cfg["swarm"]
    aliyun = cfg["aliyun"]
    scale_cfg = cfg["scale"]
    dry_run = cfg["dry_run"]

    gpu_node_id = swarm["gpu_node_id"]
    gpu_instance_id = aliyun["gpu_instance_id"]
    if not gpu_node_id or not gpu_instance_id:
        log.warning("未配置 SWARM_GPU_NODE_ID 或 SWARM_GPU_INSTANCE_ID，仅做 scale，不启停 ECS")

    ecs_client = None
    if aliyun["access_key_id"] and aliyun["access_key_secret"]:
        ecs_client = create_ecs_client(
            aliyun["access_key_id"],
            aliyun["access_key_secret"],
            aliyun["region_id"],
        )

    idle_since: Optional[datetime] = None
    check_interval = scale_cfg["check_interval_seconds"]
    single_run = cfg.get("single_run", False)
    idle_minutes = scale_cfg["idle_minutes_before_shutdown"]
    scale_up_threshold = scale_cfg["queue_scale_up_threshold"]
    scale_down_threshold = scale_cfg["queue_scale_down_threshold"]
    target_replicas = scale_cfg["target_replicas_when_busy"]

    while True:
        try:
            q_size = get_queue_size(rabbitmq["url"], rabbitmq["queue"])
            if q_size < 0:
                time.sleep(check_interval)
                continue

            now = datetime.utcnow()

            if q_size >= scale_up_threshold:
                idle_since = None
                # 有任务：先尝试开机，再恢复节点 active，再 scale
                if ecs_client and gpu_instance_id:
                    status = get_instance_status(ecs_client, gpu_instance_id, aliyun["region_id"])
                    if status == "Stopped":
                        start_instance(ecs_client, gpu_instance_id, dry_run)
                        if not dry_run:
                            wait_instance_running(
                                ecs_client,
                                gpu_instance_id,
                                aliyun["region_id"],
                                timeout_seconds=300,
                            )
                    if gpu_node_id:
                        node_update_availability(gpu_node_id, "active", dry_run)
                scale_service(swarm["stack_name"], swarm["service_name"], target_replicas, dry_run)

            elif q_size <= scale_down_threshold:
                if idle_since is None:
                    idle_since = now
                elif (now - idle_since) >= timedelta(minutes=idle_minutes):
                    # 连续空闲超过 X 分钟：drain -> scale 0 -> 关机
                    if gpu_node_id:
                        node_update_availability(gpu_node_id, "drain", dry_run)
                        time.sleep(5)
                    scale_service(swarm["stack_name"], swarm["service_name"], 0, dry_run)
                    if ecs_client and gpu_instance_id:
                        time.sleep(10)
                        stop_instance(
                            ecs_client,
                            gpu_instance_id,
                            force_stop=False,
                            stopped_mode="StopCharging",
                            dry_run=dry_run,
                        )
                    idle_since = None
            else:
                idle_since = None

        except KeyboardInterrupt:
            log.info("收到中断，退出")
            break
        except Exception as e:
            log.exception("本轮异常: %s", e)

        if single_run:
            break
        time.sleep(check_interval)


def main():
    cfg = load_config()
    if not cfg["rabbitmq"]["url"]:
        log.error("请设置 RABBITMQ_URL")
        sys.exit(1)
    if cfg["dry_run"]:
        log.info("AUTOSCALE_DRY_RUN=true，仅打印不执行启停与 scale")
    run_loop(cfg)


if __name__ == "__main__":
    main()
