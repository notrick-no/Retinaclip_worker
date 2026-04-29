"""python -m ppio 启动调度器；python -m ppio list-products 列出 GPU 产品。"""

from __future__ import annotations

import json
import logging
import sys

from ppio.client import PPIOClient
from ppio.scheduler import main as scheduler_main


def _list_products() -> None:
    import os

    logging.basicConfig(level=logging.INFO)
    key = (os.environ.get("PPIO_API_KEY") or os.environ.get("PPINFRA_API_KEY") or "").strip()
    if not key:
        print("需要环境变量 PPIO_API_KEY 或 PPINFRA_API_KEY", file=sys.stderr)
        sys.exit(1)
    base = (os.environ.get("PPIO_BASE_URL") or "https://api.ppinfra.com").rstrip("/")
    path = (os.environ.get("PPIO_PRODUCTS_API_PATH") or "/gpu-instance/openapi/v1/products").strip()
    cluster = (os.environ.get("PPIO_CLUSTER_ID") or "").strip() or None
    c = PPIOClient(base, key)
    rows = c.list_gpu_products(path, cluster)
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in ("list-products", "products"):
        _list_products()
        return
    scheduler_main()


if __name__ == "__main__":
    main()
