# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repo shape (two parallel worker stacks)

This repo contains two independent scheduler/worker implementations that share the same RabbitMQ queue contract (`media.uploaded`) and webhook contract (HMAC-signed POSTs to `BACKEND_URL`). They are not layered — pick one based on the cloud provider:

- **`aliyun/`** — TypeScript. Aliyun ECS orchestrator. Entry: `aliyun/index.ts`. The root `index.ts` is a one-line re-export. Run with `npm start` / `tsx index.ts`.
- **`ppio/`** — Python package (`python -m ppio`). PPIO (派欧云) GPU instance scheduler. Talks to PPIO REST API at `https://api.ppinfra.com`.
- **`dev/message_queue_worker.py`** — the actual video-processing consumer that runs *inside* a PPIO instance (or locally for development). Mirrors the contract that `worker_simulation/index.ts` (a TS passthrough simulator) implements.

The TS root and `aliyun/` use `amqplib`; the Python side uses `pika`. Both passively `queue_declare(passive=True)` for depth checks — they do not consume from each other.

## Architecture: aliyun/

`aliyun/index.ts` boots one of two modes based on `WORKER_ECS_POOL_ENABLED`:

- **Pool mode** (`config.ecs.poolEnabled = true`): `PoolQueueScheduler` *observes* queue depth without consuming. When depth > 0 it starts Stopped pool ECS instances; the messages are consumed by workers running *inside* those ECS instances (each instance has the message-queue worker + webhook sender baked into its image / cloud-init).
- **Non-pool mode**: `QueueConsumer` consumes messages directly. For each message, `ECSOrchestrator` provisions a fresh ECS instance, hands it the task via cloud-init, waits for the webhook callback, then releases the instance.

`ECSOrchestrator.cleanupZombieInstances()` runs on `healthCheck.zombieCheckInterval`. Shutdown is two-phase: stop consuming → drain in-flight tasks within `shutdown.graceMs` → force-exit. Sending a second SIGINT/SIGTERM during shutdown forces an immediate exit.

**Note on current branch state**: many `aliyun/` TypeScript modules referenced by `aliyun/index.ts` (`./config`, `./ecs-orchestrator`, `./queue-consumer`, `./pool-queue-scheduler`, `./logger`) are *deleted* in this slimmed working tree (`git status` shows them as `D`). `npm start` will fail until those files are restored from a sibling branch or recreated. The `ppio/` stack is fully self-contained and runnable.

## Architecture: ppio/

`ppio.scheduler.PPIOScheduler` runs a polling loop (`PPIO_SCHEDULER_POLL_INTERVAL_MS`, default 15s) over `QueueDepthMonitor.passive_check()`. Two operating modes, selected by env at config-load time (`ppio/config.py:load_config`):

- **Managed mode** (`PPIO_MANAGED_INSTANCE_ID` set): only `start_instance` / `stop_instance` are ever called on the one fixed instance. `idle_action` is forced to `stop` (never `delete`).
- **Create-release mode** (`PPIO_PRODUCT_ID` set instead): on non-empty queue and 0 managed instances, `_create_one()` provisions a new GPU instance with `_build_envs()` injecting `RABBITMQ_URL`, `MEDIA_QUEUE`, `BACKEND_URL`, `WEBHOOK_SECRET`, plus `PPIO_EXTRA_ENVS`. On idle (empty queue for `PPIO_IDLE_CLOSE_MS`), instances are stopped or deleted depending on `PPIO_IDLE_ACTION`.

If `PPIO_QUEUE_AUTOMATION=false`, the loop only emits logs — start/stop only happen via the admin HTTP API. This is the recommended setup for first-time API debugging.

**Admin API** (`ppio/admin_api.py`): enabled when `PPIO_ADMIN_TOKEN` is set. Listens on `PPIO_ADMIN_LISTEN_HOST:PPIO_ADMIN_LISTEN_PORT` (default `127.0.0.1:9843`). Endpoints:
- `GET  /health` (no auth)
- `GET  /api/v1/status` → returns `get_snapshot()`: mode, instance_id, queue stats, full ppio instance detail, last_error
- `POST /api/v1/instance/start` → `api_start()`
- `POST /api/v1/instance/stop`  → `api_stop()`

Auth is `Authorization: Bearer <token>` or `X-Admin-Token: <token>`. CORS is open (`*`) for OPTIONS.

`get_snapshot()` calls `client.get_instance(iid)` synchronously inside `_op_lock`, so a slow PPIO API will block other admin requests — this is intentional to serialize start/stop/status against the polling loop.

`PPIOClient.get_instance` tries GET-with-params first, then POST-with-body — PPIO's API is inconsistent across endpoints. `start/stop` are wrapped in `_safe_start`/`_safe_stop` which swallow 4xx errors when the body matches phrases like `running` / `已运行` / `stopped` / `已停止` (idempotency).

## Commands

### TypeScript (root / aliyun/)

```bash
npm install                  # install deps
npm start                    # = tsx index.ts → aliyun/index.ts (needs aliyun/* siblings present)
npm run worker               # alias of start
npm run pool                 # tsx scripts/test-ecs-pool.ts (script may be missing on this branch)
npm test                     # vitest run (most test files are deleted on this branch)
npm run test:watch
npm run test:smoke           # verify-config + test-rabbitmq + test-ecs (scripts may be missing)
```

### PPIO scheduler (Python)

All PPIO scripts go through `scripts/lib/common.sh`, which `cd`s to repo root, sources `.env` (if present) with `set -a`, sets `PYTHONPATH=$REPO_ROOT`, and prefers `python` (current conda env) over `python3`.

```bash
bash scripts/ppio-install.sh          # pip install -r ppio/requirements.txt
bash scripts/check-ppio-env.sh        # validate required envs locally (no network)
bash scripts/ppio-list-products.sh    # python -m ppio list-products → JSON of GPU products
bash scripts/ppio-scheduler.sh        # python -m ppio → main scheduler loop
bash scripts/dev-install-mq-deps.sh   # pip install -r dev/requirements.txt
bash scripts/dev-message-queue-worker.sh   # python dev/message_queue_worker.py (local consumer)

# npm aliases (after npm install):
npm run ppio:install / ppio:check-env / ppio:products / ppio:scheduler
npm run dev:mq-install / dev:mq-worker
```

```bash
# Single-shot product listing without the script:
python -m ppio list-products
```

There is no Python test suite — vitest is TS-only and most `tests/*.ts` are deleted on this branch.

## Env / config

`env_example` is the source of truth for all env keys (Chinese comments). Copy to `.env`; the bash scripts auto-source it. Required minimums:

- **Both stacks**: `RABBITMQ_URL`, `RABBITMQ_QUEUE` (defaults to `media.uploaded`; PPIO also reads `RABBITMQ_MEDIA_UPLOAD` / `MEDIA_QUEUE` as fallbacks), `BACKEND_URL`, `WEBHOOK_SECRET`.
- **PPIO**: `PPIO_API_KEY` (or `PPINFRA_API_KEY`), and *one of* `PPIO_MANAGED_INSTANCE_ID` (managed mode) or `PPIO_PRODUCT_ID` (create mode). `load_config` raises `ValueError` if neither is set.
- **Admin API**: `PPIO_ADMIN_TOKEN` to enable; otherwise scheduler runs headless.

`PPIO_IDLE_ACTION=delete` is silently rewritten to `stop` in managed mode (you cannot delete a managed instance through the scheduler).

Hard-coded defaults to be aware of:
- `dev/message_queue_worker.py` has a hard-coded `RABBITMQ_URL` and `WEBHOOK_SECRET` at module top — these override anything in `.env` for that script. Edit them before running locally, or refactor to read env first.
- Default container entrypoint when `PPIO_CREATE_COMMAND` is unset: `cd /opt/retinaclip && python3 -u message_queue_worker.py`. This implies the GPU image must already contain the worker code at `/opt/retinaclip/`.

## Webhook contract (shared by both stacks)

Workers POST to `BACKEND_URL` with HMAC-SHA256 signature of the raw body keyed by `WEBHOOK_SECRET`. Payload includes `job_id` / `queue_job_id` / `queueJobId` (all set to the same value for compatibility), `user_id`, `status` (`COMPLETED` | `FAILED`), `event_type` (`completed` | `failed`), and either `output_video_url` or `error_message`. The triple `job_id` aliasing exists because different consumers (Prisma vs. older TS) read different field names — keep all three when modifying the payload builder.
