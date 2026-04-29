from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


def _unwrap_payload(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    if "data" in data and len(data) == 1 + int("code" in data) + int("message" in data):
        return data.get("data")
    if "data" in data:
        return data.get("data")
    return data


class PPIOClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        instance_list_path: Optional[str] = None,
        instance_detail_path: Optional[str] = None,
        cluster_id: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._instance_list_path = (instance_list_path or "").strip() or None
        self._instance_detail_path = (instance_detail_path or "").strip() or None
        self._cluster_id = (cluster_id or "").strip() or None
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self._session.trust_env = True

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
        log_errors: bool = True,
    ) -> Any:
        url = self._url(path)
        r = self._session.request(
            method,
            url,
            params=params,
            json=json_body,
            timeout=60,
        )
        if r.status_code >= 400:
            if log_errors:
                logger.error("PPIO %s %s 失败: %s %s", method, path, r.status_code, r.text[:2000])
            r.raise_for_status()
        if not r.content:
            return None
        try:
            return r.json()
        except json.JSONDecodeError:
            return {"raw": r.text}

    def list_gpu_products(
        self, path: str, cluster_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if cluster_id:
            params["clusterId"] = cluster_id
        paths = [path]
        if path not in ("/gpu-instance/openapi/v1/products",):
            paths.append("/gpu-instance/openapi/v1/products")
        last = None
        for p in paths:
            try:
                data = self._request("GET", p, params=params)
                un = _unwrap_payload(data)
                if un is None:
                    continue
                if isinstance(un, list):
                    return [x for x in un if isinstance(x, dict)]
                if isinstance(un, dict) and "list" in un and isinstance(un["list"], list):
                    return [x for x in un["list"] if isinstance(x, dict)]
                if isinstance(un, dict):
                    for k in ("items", "products", "data"):
                        v = un.get(k)
                        if isinstance(v, list) and v:
                            return [x for x in v if isinstance(x, dict)]
            except Exception as e:  # noqa: BLE001
                last = e
                logger.debug("list_gpu_products 尝试 %s: %s", p, e)
        if last:
            raise last
        return []

    @staticmethod
    def _rows_from_list_payload(un: Any) -> List[Dict[str, Any]]:
        if isinstance(un, list):
            return [x for x in un if isinstance(x, dict)]
        if not isinstance(un, dict):
            return []
        for k in ("list", "items", "instances", "instance", "records"):
            v = un.get(k)
            if isinstance(v, list) and v:
                return [x for x in v if isinstance(x, dict)]
            if k == "instance" and isinstance(v, dict) and v:
                return [v]
        inner = un.get("data")
        if isinstance(inner, dict):
            for k in ("list", "items", "instances", "instance"):
                v = inner.get(k)
                if isinstance(v, list) and v:
                    return [x for x in v if isinstance(x, dict)]
                if k == "instance" and isinstance(v, dict) and v:
                    return [v]
        return []

    def list_instances(
        self,
        *,
        name_prefix: Optional[str] = None,
        page: int = 0,
        page_size: int = 50,
    ) -> List[Dict[str, Any]]:
        # 官方文档：GET .../gpu/instances ，参数 pageNum / pageSize；旧路径 .../gpu/instance/list 易 404
        if self._instance_list_path:
            paths = [self._instance_list_path]
        else:
            # 文档示例：GET .../gpu/instances （注意路径中含 gpu）；旧版 .../gpu/instance/list 易 404
            paths = [
                "/gpu-instance/openapi/v1/gpu/instances",
                "/gpu-instance/openapi/v1/instances",
                "/gpu-instance/openapi/v1/gpu/instance/list",
            ]
        param_variants: List[Dict[str, Any]] = [
            {"pageNum": page, "pageSize": page_size},
            {"page": page, "pageSize": page_size},
        ]
        last_err: Optional[Exception] = None
        for path in paths:
            for base in param_variants:
                params = dict(base)
                if self._cluster_id:
                    params["clusterId"] = self._cluster_id
                if name_prefix:
                    params["name"] = name_prefix
                try:
                    data = self._request("GET", path, params=params, log_errors=False)
                    un = _unwrap_payload(data)
                    items = self._rows_from_list_payload(un)
                    if name_prefix:
                        items = [
                            x
                            for x in items
                            if str(x.get("name") or "").startswith(name_prefix)
                        ]
                    return items
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    logger.debug("list_instances 尝试 %s %s: %s", path, base, e)
        if last_err:
            logger.error(
                "PPIO list_instances 全部路径失败: %s",
                last_err,
            )
        raise last_err or RuntimeError("list_instances: unknown error")

    def get_instance(self, instance_id: str) -> Dict[str, Any]:
        # PPIO API 不同版本/区域的实例详情端点路径不一致，依次尝试已知变体
        attempts: list[tuple[str, str, Optional[Dict[str, Any]], Any]] = []
        if self._instance_detail_path:
            p = self._instance_detail_path
            if "{instanceId}" in p or "{id}" in p:
                expanded = p.replace("{instanceId}", instance_id).replace("{id}", instance_id)
                attempts.append(("GET", expanded, None, None))
            else:
                attempts.append(("GET", p, {"instanceId": instance_id}, None))
        attempts.extend(
            [
                # 文档：GET /gpu-instance/openapi/v1/gpu/instance?instanceId=
                ("GET", "/gpu-instance/openapi/v1/gpu/instance", {"instanceId": instance_id}, None),
                ("GET", f"/gpu-instance/openapi/v1/gpu/instances/{instance_id}", None, None),
                ("GET", f"/gpu-instance/openapi/v1/instances/{instance_id}", None, None),
                ("GET", "/gpu-instance/openapi/v1/gpu/instance/detail", {"instanceId": instance_id}, None),
                ("POST", "/gpu-instance/openapi/v1/gpu/instance/detail", None, {"instanceId": instance_id}),
                ("GET", "/gpu-instance/openapi/v1/gpu/instance/info", {"instanceId": instance_id}, None),
                ("POST", "/gpu-instance/openapi/v1/gpu/instance/info", None, {"instanceId": instance_id}),
                ("GET", "/gpu-instance/openapi/v1/gpu/instance/get", {"instanceId": instance_id}, None),
                ("GET", f"/gpu-instance/openapi/v1/gpu/instance/{instance_id}", None, None),
            ]
        )
        last_err: Optional[Exception] = None
        for method, path, params, json_body in attempts:
            try:
                if method == "GET":
                    data = self._request("GET", path, params=params, log_errors=False)
                else:
                    data = self._request("POST", path, json_body=json_body, log_errors=False)
                un = _unwrap_payload(data)
                if isinstance(un, dict) and un:
                    return un
            except Exception as e:  # noqa: BLE001
                last_err = e
                logger.debug("get_instance 尝试 %s %s: %s", method, path, e)

        # 兜底：用 list_instances 拉列表后过滤本实例
        try:
            items = self.list_instances(page=0, page_size=200)
            for it in items:
                iid = str(it.get("id") or it.get("instanceId") or "")
                if iid == instance_id:
                    return it
        except Exception as e:  # noqa: BLE001
            last_err = last_err or e
            logger.debug("get_instance 兜底 list_instances 也失败: %s", e)

        raise RuntimeError(
            f"无法查询实例详情: {instance_id}" + (f" ({last_err})" if last_err else "")
        ) from last_err

    def create_gpu_instance(self, body: Dict[str, Any]) -> Dict[str, Any]:
        data = self._request(
            "POST",
            "/gpu-instance/openapi/v1/gpu/instance/create",
            json_body=body,
        )
        un = _unwrap_payload(data)
        if isinstance(un, dict):
            return un
        return {"raw": un}

    def start_instance(self, instance_id: str) -> None:
        self._request(
            "POST",
            "/gpu-instance/openapi/v1/gpu/instance/start",
            json_body={"instanceId": instance_id},
        )

    def stop_instance(self, instance_id: str) -> None:
        self._request(
            "POST",
            "/gpu-instance/openapi/v1/gpu/instance/stop",
            json_body={"instanceId": instance_id},
        )

    def delete_instance(self, instance_id: str) -> None:
        self._request(
            "POST",
            "/gpu-instance/openapi/v1/gpu/instance/delete",
            json_body={"instanceId": instance_id},
        )

    @staticmethod
    def fetch_log_url(url: str, *, tail: int = 200) -> str:
        if not url or not str(url).startswith("http"):
            return ""
        try:
            sep = "&" if "?" in url else "?"
            u = f"{url}{sep}follow=0&sse=0&tail={tail}"
            r = requests.get(u, timeout=30, stream=False)
            r.raise_for_status()
            return (r.text or "")[:500_000]
        except Exception as e:  # noqa: BLE001
            logger.warning("拉取日志 URL 失败: %s", e)
            return ""
