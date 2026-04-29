from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import requests

from ppio.client import PPIOClient
from ppio.config import PPIOSchedulerConfig, load_config
from ppio.queue_monitor import QueueDepthMonitor

logger = logging.getLogger(__name__)


@dataclass
class ManagedInstance:
    instance_id: str
    name: str
    created_at: float = field(default_factory=time.time)
    last_log_at: float = 0.0


class PPIOScheduler:
    """
    模式 A：PPIO_MANAGED_INSTANCE_ID 有值 — 仅对已有实例 start/stop，不创建/删除。
    模式 B：无该变量 — 与旧版一致，按队列 create + delete/stop。
    """

    def __init__(self, cfg: PPIOSchedulerConfig) -> None:
        self.cfg = cfg
        self.client = PPIOClient(
            cfg.ppio_base_url,
            cfg.ppio_api_key,
            instance_list_path=cfg.instance_list_path,
            instance_detail_path=cfg.instance_detail_path,
            cluster_id=cfg.ppio_cluster_id,
        )
        self.queue = QueueDepthMonitor(cfg.rabbitmq_url, cfg.rabbitmq_queue)
        self.managed: Dict[str, ManagedInstance] = {}
        # last_non_empty_at：最后一次观测到 message_count>0 的时间；_ready_empty_since：首次观测到 ready=0 的时间
        self.last_non_empty_at = 0.0
        self._ready_empty_since: Optional[float] = None
        self._shutdown = threading.Event()
        self._counter = 0
        self._op_lock = threading.RLock()
        self._last_error: Optional[str] = None
        self._last_error_at: Optional[float] = None
        self._last_queue_stats: Optional[Dict[str, int]] = None
        if cfg.managed_instance_id:
            mid = cfg.managed_instance_id
            self.managed[mid] = ManagedInstance(instance_id=mid, name="(managed)")

    def _record_error(self, msg: str) -> None:
        self._last_error = (msg or "")[:4000]
        self._last_error_at = time.time()
        logger.error("PPIO: %s", msg)

    def _clear_error(self) -> None:
        self._last_error = None
        self._last_error_at = None

    def _queue_empty_long_enough(self) -> bool:
        if self._ready_empty_since is None:
            return False
        need = self.cfg.idle_close_ms
        if need <= 0:
            need = self.cfg.idle_min_grace_ms
        return (time.time() - self._ready_empty_since) * 1000 >= need

    def _is_managed_mode(self) -> bool:
        return bool(self.cfg.managed_instance_id)

    def _merge_remote_instances(self) -> None:
        if self._is_managed_mode():
            return
        try:
            rows = self.client.list_instances(
                name_prefix=self.cfg.ppio_instance_name_prefix
            )
        except Exception as e:  # noqa: BLE001
            self._record_error(f"list_instances: {e}")
            return
        seen: Set[str] = set()
        for row in rows:
            iid = str(row.get("id") or row.get("instanceId") or "")
            name = str(row.get("name") or "")
            if not iid:
                continue
            st = str(row.get("state") or row.get("status") or "").lower()
            if st in ("removed", "removing", "to_remove"):
                continue
            seen.add(iid)
            if iid not in self.managed:
                self.managed[iid] = ManagedInstance(instance_id=iid, name=name)
        dead = [k for k in self.managed if k not in seen]
        for k in dead:
            del self.managed[k]

    def _build_envs(self) -> List[dict[str, str]]:
        core: List[tuple[str, str]] = [
            ("RABBITMQ_URL", self.cfg.rabbitmq_url),
            ("RABBITMQ_MEDIA_UPLOAD", self.cfg.rabbitmq_queue),
            ("MEDIA_QUEUE", self.cfg.rabbitmq_queue),
        ]
        for k in ("BACKEND_URL", "WEBHOOK_SECRET"):
            v = os.environ.get(k)
            if v:
                core.append((k, v))
        merged = list(core) + list(self.cfg.ppio_env_extra)
        out: List[dict[str, str]] = []
        for k, v in merged:
            if not k:
                continue
            out.append({"key": k, "value": str(v)})
        return out[:100]

    def _create_one(self) -> Optional[str]:
        if not (self.cfg.ppio_product_id or "").strip():
            self._record_error("创建实例需要 PPIO_PRODUCT_ID")
            return None
        self._counter += 1
        name = f"{self.cfg.ppio_instance_name_prefix}{int(time.time())}-{self._counter}-{uuid.uuid4().hex[:6]}"
        body: Dict[str, Any] = {
            "name": name,
            "productId": self.cfg.ppio_product_id,
            "gpuNum": self.cfg.ppio_gpu_num,
            "rootfsSize": self.cfg.ppio_rootfs_size,
            "imageUrl": self.cfg.ppio_image_url,
            "kind": self.cfg.ppio_kind,
            "month": self.cfg.ppio_month,
            "command": self.cfg.ppio_create_command,
            "envs": self._build_envs(),
            "billingMethod": self.cfg.ppio_billing,
        }
        if self.cfg.ppio_create_entrypoint:
            body["entrypoint"] = self.cfg.ppio_create_entrypoint
        if self.cfg.ppio_cluster_id:
            body["clusterId"] = self.cfg.ppio_cluster_id
        if self.cfg.ppio_image_auth:
            body["imageAuth"] = self.cfg.ppio_image_auth
        if self.cfg.ppio_image_auth_id:
            body["imageAuthId"] = self.cfg.ppio_image_auth_id
        if self.cfg.ppio_ports:
            body["ports"] = self.cfg.ppio_ports
        if self.cfg.ppio_min_cuda:
            body["minCuda"] = self.cfg.ppio_min_cuda

        logger.info("创建 PPIO 实例: name=%s product=%s", name, self.cfg.ppio_product_id)
        res = self.client.create_gpu_instance(body)
        iid = str(
            res.get("id")
            or res.get("instanceId")
            or res.get("InstanceId")
            or (res.get("data") or {}).get("id")
            or ""
        )
        if not iid:
            self._record_error(f"创建实例后未得到 ID: {res!r}")
            return None
        self.managed[iid] = ManagedInstance(instance_id=iid, name=name)
        self._clear_error()
        return iid

    # 表示「实例已在目标状态、本次操作可忽略」的关键词
    _START_IDEMPOTENT_HINTS = (
        "running", "已运行", "already", "无需", "状态",
        "invalid state change",  # PPIO: VALIDATOR_PARAM, 通常说明已在 running/starting
    )
    _STOP_IDEMPOTENT_HINTS = (
        "exited", "已停止", "停止", "stopped", "not running",
        "invalid state change",
    )

    def _safe_start(self, iid: str) -> bool:
        try:
            self.client.start_instance(iid)
            self._clear_error()
            return True
        except requests.HTTPError as e:
            body = ""
            if e.response is not None:
                body = (e.response.text or "")[:2000]
            if e.response is not None and 400 <= e.response.status_code < 500:
                if any(x in body.lower() for x in self._START_IDEMPOTENT_HINTS):
                    logger.info("start_instance(%s) 可忽略: %s", iid, body[:200])
                    self._clear_error()
                    return True
            self._record_error(f"start_instance {iid}: {e} {body}")
            return False
        except Exception as e:  # noqa: BLE001
            self._record_error(f"start_instance {iid}: {e}")
            return False

    def _safe_stop(self, iid: str) -> bool:
        try:
            self.client.stop_instance(iid)
            self._clear_error()
            return True
        except requests.HTTPError as e:
            body = ""
            if e.response is not None:
                body = (e.response.text or "")[:2000]
            if e.response is not None and 400 <= e.response.status_code < 500:
                if any(x in body.lower() for x in self._STOP_IDEMPOTENT_HINTS):
                    logger.info("stop_instance(%s) 可忽略: %s", iid, body[:200])
                    self._clear_error()
                    return True
            self._record_error(f"stop_instance {iid}: {e} {body}")
            return False
        except Exception as e:  # noqa: BLE001
            self._record_error(f"stop_instance {iid}: {e}")
            return False

    def _release_excess_instances(self) -> None:
        if len(self.managed) <= 1:
            return
        ordered = sorted(
            self.managed.items(),
            key=lambda kv: kv[1].created_at,
        )
        for iid, _ in ordered[:-1]:
            self._release_instance(iid, reason="excess")
            if len(self.managed) <= 1:
                break

    def _release_instance(
        self, instance_id: str, *, reason: str = "idle"
    ) -> None:
        try:
            if self._is_managed_mode():
                self._safe_stop(instance_id)
            elif self.cfg.idle_action == "stop":
                self._safe_stop(instance_id)
            else:
                self.client.delete_instance(instance_id)
                self._clear_error()
                logger.info("已删除实例 %s (%s)", instance_id, reason)
        except Exception as e:  # noqa: BLE001
            self._record_error(f"释放实例 {instance_id}: {e}")
        self.managed.pop(instance_id, None)

    def _emit_logs(self) -> None:
        now = time.time()
        for mid in list(self.managed.values()):
            if now - mid.last_log_at < self.cfg.log_fetch_interval_s:
                continue
            mid.last_log_at = now
            try:
                d = self.client.get_instance(mid.instance_id)
            except Exception as e:  # noqa: BLE001
                logger.warning("get_instance(日志) %s: %s", mid.instance_id, e)
                continue
            sys_log = d.get("syslogUrl") or d.get("sysLogUrl") or d.get("sys")
            inst_log = d.get("logUrl") or d.get("instanceLogUrl")
            for label, u in (("sys", sys_log), ("app", inst_log)):
                if not u:
                    continue
                text = PPIOClient.fetch_log_url(
                    str(u), tail=self.cfg.log_tail_lines
                )
                if text:
                    logger.info(
                        "实例 %s 日志(%s) tail=%s:\n%s",
                        mid.instance_id,
                        label,
                        self.cfg.log_tail_lines,
                        text[:8000],
                    )
            st = d.get("state") or d.get("status")
            if st:
                logger.info("实例 %s 状态: %s", mid.instance_id, st)

    def one_poll(self) -> None:
        st = self.queue.passive_check()
        self._last_queue_stats = {
            "message_count": st.message_count,
            "consumer_count": st.consumer_count,
        }
        if st.message_count > 0:
            self.last_non_empty_at = time.time()
            self._ready_empty_since = None
        else:
            if self._ready_empty_since is None:
                self._ready_empty_since = time.time()

        with self._op_lock:
            if not self.cfg.queue_automation:
                if self.managed:
                    self._emit_logs()
                return

            if self._is_managed_mode():
                iid = self.cfg.managed_instance_id
                assert iid is not None
                if st.message_count > 0:
                    self._safe_start(iid)
                elif self._queue_empty_long_enough():
                    self._safe_stop(iid)
            else:
                self._merge_remote_instances()
                if st.message_count > 0:
                    if len(self.managed) == 0:
                        try:
                            self._create_one()
                        except Exception as e:  # noqa: BLE001
                            self._record_error(f"有任务时创建算力: {e}")
                    else:
                        self._release_excess_instances()
                elif self.managed and self._queue_empty_long_enough():
                    for xid in list(self.managed.keys()):
                        self._release_instance(xid)

            if self.managed:
                self._emit_logs()

        logger.info(
            "队列: messages=%s consumers=%s 实例数=%s managed=%s automation=%s",
            st.message_count,
            st.consumer_count,
            len(self.managed),
            bool(self.cfg.managed_instance_id),
            self.cfg.queue_automation,
        )

    def ppio_instance_id_for_api(self) -> str:
        if self.cfg.managed_instance_id:
            return self.cfg.managed_instance_id
        if self.managed:
            return next(iter(self.managed.keys()))
        return ""

    def get_snapshot(self) -> Dict[str, Any]:
        q: Dict[str, Any] = {"message_count": None, "consumer_count": None}
        if self._last_queue_stats is not None:
            q = dict(self._last_queue_stats)
        ppio_detail: Any = None
        iid = self.ppio_instance_id_for_api() or self.cfg.managed_instance_id
        idle_info: Dict[str, Any]
        with self._op_lock:
            if iid:
                try:
                    ppio_detail = self.client.get_instance(iid)
                except Exception as e:  # noqa: BLE001
                    ppio_detail = {"error": str(e)}
            eff = self.cfg.idle_close_ms
            if eff <= 0:
                eff = self.cfg.idle_min_grace_ms
            idle_info = {
                "idle_close_ms": self.cfg.idle_close_ms,
                "idle_min_grace_ms": self.cfg.idle_min_grace_ms,
                "effective_idle_close_ms": eff,
                "ready_empty_since": self._ready_empty_since,
            }
            if self._ready_empty_since is not None:
                idle_info["ready_empty_for_ms"] = int(
                    (time.time() - self._ready_empty_since) * 1000
                )
        err: Optional[Dict[str, Any]] = None
        if self._last_error:
            err = {
                "message": self._last_error,
                "at": self._last_error_at,
            }
        return {
            "mode": "managed_start_stop" if self._is_managed_mode() else "create_release",
            "instance_id": iid or None,
            "queue": q,
            "idle": idle_info,
            "ppio": ppio_detail,
            "last_error": err,
            "queue_automation": self.cfg.queue_automation,
        }

    def api_start(self) -> Dict[str, Any]:
        with self._op_lock:
            if self._is_managed_mode():
                iid = self.cfg.managed_instance_id
                if not iid:
                    return {"ok": False, "error": "无实例 ID"}
                self._safe_start(iid)
                return {
                    "ok": not self._last_error,
                    "instance_id": iid,
                    "last_error": self._last_error,
                }
            if not (self.cfg.ppio_product_id or "").strip():
                return {
                    "ok": False,
                    "error": "无 PPIO_MANAGED_INSTANCE_ID 时须配置 PPIO_PRODUCT_ID",
                }
            if len(self.managed) == 0:
                self._create_one()
            else:
                for xid in list(self.managed.keys()):
                    self._safe_start(xid)
            return {
                "ok": not self._last_error,
                "instance_id": self.ppio_instance_id_for_api() or None,
                "last_error": self._last_error,
            }

    def api_stop(self) -> Dict[str, Any]:
        with self._op_lock:
            if self._is_managed_mode():
                iid = self.cfg.managed_instance_id
                if not iid:
                    return {"ok": False, "error": "无实例 ID"}
                self._safe_stop(iid)
                return {
                    "ok": not self._last_error,
                    "instance_id": iid,
                    "last_error": self._last_error,
                }
            for xid in list(self.managed.keys()):
                self._safe_stop(xid)
            return {"ok": not self._last_error, "last_error": self._last_error}

    def run_loop(self) -> None:
        self.queue.connect()
        while not self._shutdown.is_set():
            try:
                self.one_poll()
            except Exception as e:  # noqa: BLE001
                with self._op_lock:
                    self._record_error(f"one_poll: {e}")
                logger.exception("调度轮询异常: %s", e)
            self._shutdown.wait(self.cfg.poll_interval_ms / 1000.0)

    def stop(self) -> None:
        self._shutdown.set()
        self.queue.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    for noisy in ("pika", "urllib3", "requests", "urllib3.connectionpool"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    cfg = load_config()
    sch = PPIOScheduler(cfg)
    if cfg.admin_token:
        from ppio.admin_api import run_admin_thread

        run_admin_thread(
            sch,
            host=cfg.admin_listen_host,
            port=cfg.admin_listen_port,
            token=cfg.admin_token,
        )
        logging.getLogger(__name__).info(
            "管理 API 已监听 %s:%s（需 Authorization: Bearer）",
            cfg.admin_listen_host,
            cfg.admin_listen_port,
        )
    elif (os.environ.get("PORT") or "").strip():
        from ppio.railway_health import run_health_server_thread

        run_health_server_thread(
            host=cfg.admin_listen_host,
            port=cfg.admin_listen_port,
        )

    def handle_sig(*_a: Any) -> None:  # noqa: ANN401
        sch.stop()

    try:
        import signal

        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)
    except Exception:  # noqa: BLE001
        pass
    try:
        sch.run_loop()
    except KeyboardInterrupt:
        sch.stop()


if __name__ == "__main__":
    main()
