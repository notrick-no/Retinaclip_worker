# scripts/swarm-gpu-autoscaler/config.py
"""从环境变量加载配置，便于本地测试与云端一致。"""

import os


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def env_int(key: str, default: int) -> int:
    v = os.environ.get(key)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def load_config():
    return {
        "rabbitmq": {
            "url": env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
            "queue": env("RABBITMQ_QUEUE", "media.uploaded"),
        },
        "swarm": {
            "stack_name": env("SWARM_STACK_NAME", "retinaclip"),
            "service_name": env("SWARM_SERVICE_NAME", "gpu-worker"),
            "gpu_node_id": env("SWARM_GPU_NODE_ID", ""),  # docker node ls 里的 NODE ID 或 Hostname
        },
        "aliyun": {
            "access_key_id": env("ALIBABA_CLOUD_ACCESS_KEY_ID", ""),
            "access_key_secret": env("ALIBABA_CLOUD_ACCESS_KEY_SECRET", ""),
            "region_id": env("ALIYUN_ECS_REGION", "cn-shanghai"),
            "gpu_instance_id": env("SWARM_GPU_INSTANCE_ID", ""),  # 要启停的 ECS 实例 ID，如 i-xxx
        },
        "scale": {
            "queue_scale_up_threshold": env_int("AUTOSCALE_QUEUE_SCALE_UP_THRESHOLD", 1),
            "queue_scale_down_threshold": env_int("AUTOSCALE_QUEUE_SCALE_DOWN_THRESHOLD", 0),
            "target_replicas_when_busy": env_int("AUTOSCALE_TARGET_REPLICAS", 1),
            "idle_minutes_before_shutdown": env_int("AUTOSCALE_IDLE_MINUTES_BEFORE_SHUTDOWN", 15),
            "check_interval_seconds": env_int("AUTOSCALE_CHECK_INTERVAL_SECONDS", 30),
        },
        "dry_run": env_bool("AUTOSCALE_DRY_RUN", False),
        "single_run": env_bool("AUTOSCALE_SINGLE_RUN", False),
    }


def env_bool(key: str, default: bool) -> bool:
    v = os.environ.get(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")
