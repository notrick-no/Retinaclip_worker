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

---

## 五、Shell 快捷脚本 `scripts/rc.sh`

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
