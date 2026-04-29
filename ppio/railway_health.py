"""未启用 Admin API 时，为 Railway 等平台提供仅 GET /health 的最小 HTTP 监听（需监听 PORT）。"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Type

logger = logging.getLogger(__name__)


def _handler() -> Type[BaseHTTPRequestHandler]:
    class HealthHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            logger.info("%s - %s", self.address_string(), fmt % args)

        def do_GET(self) -> None:  # noqa: N802
            path = (self.path or "").split("?", 1)[0]
            if path in ("/", "/health"):
                body = json.dumps({"ok": True, "role": "ppio-scheduler"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

    return HealthHandler


def run_health_server_thread(*, host: str, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, int(port)), _handler())
    t = threading.Thread(
        target=server.serve_forever,
        name="ppio-railway-health",
        daemon=True,
    )
    t.start()
    logger.info("平台健康检查已监听 %s:%s（GET / 与 /health）", host, port)
    return server
