# 测试与校验说明

在项目根目录执行（需已 `npm install`）。**依赖 `.env`** 的脚本会读取与正式 Worker 相同的配置。

---

## 一、日常运行（非测试）

| 命令 | 作用 |
|------|------|
| `npm start` | 启动 RetinaClip Worker（等同 `npm run worker`、`npx tsx index.ts`）。连接 RabbitMQ 并消费队列，按任务创建/复用 ECS。 |
| `npm run worker` | 与 `npm start` 相同。 |

---

## 二、单元测试（不连阿里云 / 不连真实 MQ）

| 命令 | 作用 |
|------|------|
| `npm test` | 运行 **Vitest**，覆盖 `config`、`task-routing`、`queue-consumer`、`webhook-sender` 等（使用 mock，**不要求**完整 `.env`）。 |
| `npm run test:unit` | 与 `npm test` 相同。 |
| `npm run test:watch` | Vitest 监听模式，改代码自动重跑。 |

**内容概要**：校验配置解析规则、任务镜像路由逻辑、队列消费与 webhook 拼装等。

---

## 三、环境与连通性（建议上云或本机有 `.env` 后使用）

| 命令 | 作用 |
|------|------|
| `npm run test:config` | 执行 `scripts/verify-config.ts`：调用 `loadConfig()`，打印 **打码后的** ECS/RabbitMQ/池/镜像路由摘要。**不发起网络请求**。用于确认 `.env` 必填项、JSON 映射无语法错误。 |
| `npm run test:rabbitmq` | 执行 `scripts/test-rabbitmq.ts`：用 `RABBITMQ_URL` 建立连接，对 `RABBITMQ_QUEUE` 执行 `assertQueue`。**只测 MQ**，不消费消息。 |
| `npm run test:ecs` | 执行 `scripts/test-ecs-api.ts`：调用 `DescribeRegions`、`DescribeImages`（校验 `ALIYUN_ECS_IMAGE_ID`）。**只读**，不创建实例。 |
| `npm run pool` | 执行 `scripts/test-ecs-pool.ts`：列出带池标签的实例、与编排器一致的「可复用 Stopped」数量、`ephemeral` 实例概况。 |

**`npm run pool` 带 profile 参数**（仅在 `.env` 中 `WORKER_ECS_POOL_PROFILE_FILTER_ENABLED=true` 时生效，与编排器一致；默认 false 时不按 profile 筛池）：

```bash
npm run pool -- subtitle
```

---

## 四、组合命令

| 命令 | 作用 |
|------|------|
| `npm run test:smoke` | 依次：`test:config` → `test:rabbitmq` → `test:ecs`。快速确认「配置 + MQ + ECS API」是否正常。 |
| `npm run test:all` | 依次：`test:config` → `test:unit` → `test:smoke` → `pool`。本地/CI 全量自检（**最后一步会查 ECS 实例列表**，需有效 AK 与网络）。 |

## 五、池机手动测试脚本

- NAS 挂载 + 私有 Docker 仓库拉取/启动测试（只用于宿主机排障）：
  - `./scripts/test-nas-docker.sh`
  - 若你希望挂载点域名不要依赖 `hostname -f`，可先导出 `WORKER_NAS_MOUNT_DOMAIN` 再运行。

> 说明：该脚本不会触碰 ECS/池，仅在你运行它的宿主机上完成 NAS 挂载、`docker pull` 和 `docker run`。因此也可以在“调度器服务器”上使用来验证 registry/NAS/容器启动是否正常。

---

## 六、调度器侧端到端测试（推荐：不手动登录池机）

该脚本会在调度器上直接调用 `ECSOrchestrator.runTask()`：
- 调度器分配任务到池机（优先复用池）
- 池机内执行：NAS 自动挂载 `/mnt`、`docker pull`、`docker run`、写入结果文件
- 调度器侧拿到 `TaskResult` 并打印成功/失败信息（失败时会给出错误原因）

脚本：
- `scripts/test-scheduler-run-pool-task.ts`

用法（需要你提供“可访问的视频地址 + webhook 地址 + 目标镜像”）：
```bash
npx tsx scripts/test-scheduler-run-pool-task.ts \
  --videoDownloadUrl http://... \
  --webhookUrl http://... \
  --processingImage 172.16.0.70:5000/quzimu-app:v3
```

可选指定 poolProfile（仅当你启用了 profile 筛池时才可能生效）：
```bash
npx tsx scripts/test-scheduler-run-pool-task.ts \
  --videoDownloadUrl http://... \
  --webhookUrl http://... \
  --processingImage 172.16.0.70:5000/quzimu-app:v3 \
  --poolProfile subtitle
```

---

## 七、Shell 快捷脚本 `scripts/rc.sh`

适合不想敲 `npm run` 时（需可执行：`chmod +x scripts/rc.sh`）。

| 命令 | 等价 |
|------|------|
| `./scripts/rc.sh` | 打印帮助 |
| `./scripts/rc.sh s` | `npm start` |
| `./scripts/rc.sh p` | `npm run pool` |
| `./scripts/rc.sh p <profile>` | `npm run pool -- <profile>` |
| `./scripts/rc.sh t` | `npm test` |
| `./scripts/rc.sh c` | `npm run test:config` |
| `./scripts/rc.sh r` | `npm run test:rabbitmq` |
| `./scripts/rc.sh e` | `npm run test:ecs` |
| `./scripts/rc.sh m` | `npm run test:smoke` |

---

## 六、推荐自测顺序（新环境）

1. `npm run test:config`  
2. `npm run test:rabbitmq`  
3. `npm run test:ecs`  
4. `npm run pool`（若使用实例池）  
5. `npm test`  
6. `npm start` 发真实队列消息做端到端验证  

更多环境变量说明见根目录 `env_example`。
