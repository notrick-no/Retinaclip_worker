from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Type
from urllib.parse import urlparse

from ppio.scheduler import PPIOScheduler

logger = logging.getLogger(__name__)


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


def make_handler(
    sch: PPIOScheduler, token: str
) -> Type[BaseHTTPRequestHandler]:
    class AdminHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            logger.info("%s - %s", self.address_string(), fmt % args)

        def _unauthorized(self) -> None:
            self.send_response(401)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(_json_bytes({"error": "unauthorized"}))

        def _ok(self, code: int, body: Any) -> None:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(_json_bytes(body))

        def _read_json(self) -> Any:
            n = int(self.headers.get("Content-Length", "0") or "0")
            if n <= 0:
                return None
            raw = self.rfile.read(n)
            if not raw:
                return None
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return None

        def _authed(self) -> bool:
            h = self.headers.get("Authorization") or ""
            if h.startswith("Bearer "):
                got = h[7:].strip()
            else:
                got = (self.headers.get("X-Admin-Token") or "").strip()
            return bool(token) and got == token

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Admin-Token")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in ("/health", "/api/v1/health"):
                self._ok(200, {"ok": True, "role": "ppio-scheduler-admin"})
                return
            if path not in ("/api/v1/status",):
                self._ok(404, {"error": "not_found", "path": path})
                return
            if not self._authed():
                self._unauthorized()
                return
            try:
                self._ok(200, sch.get_snapshot())
            except Exception as e:  # noqa: BLE001
                self._ok(500, {"error": str(e)})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if not self._authed():
                self._unauthorized()
                return
            _ = self._read_json()
            try:
                if path == "/api/v1/instance/start":
                    self._ok(200, sch.api_start())
                elif path == "/api/v1/instance/stop":
                    self._ok(200, sch.api_stop())
                else:
                    self._ok(404, {"error": "not_found", "path": path})
            except Exception as e:  # noqa: BLE001
                self._ok(500, {"error": str(e)})

    return AdminHandler


def run_admin_thread(
    sch: PPIOScheduler, *, host: str, port: int, token: str
) -> ThreadingHTTPServer:
    handler = make_handler(sch, token)
    server = ThreadingHTTPServer((host, int(port)), handler)
    t = threading.Thread(
        target=server.serve_forever,
        name="ppio-admin-api",
        daemon=True,
    )
    t.start()
    return server
