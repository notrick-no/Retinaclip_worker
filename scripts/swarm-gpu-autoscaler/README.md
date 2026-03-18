# Swarm GPU 节点按需启停（守门员）

在 Docker Swarm 架构下，Manager 常开，GPU 节点根据 RabbitMQ 队列堆积量自动开机/关机，节省成本。

## 原理

1. **监控队列**：定时查询 `media.uploaded` 队列消息数。
2. **有任务**：`docker service scale <stack>_gpu-worker=N`，若 GPU 实例为 Stopped 则调用阿里云 StartInstance，并等待 Running 后把节点设为 active。
3. **连续空闲 X 分钟**：将 GPU 节点 `docker node update --availability drain`，scale 到 0，再调用 StopInstance（StopCharging 节省停机）。

## 本地测试

1. 安装依赖（建议用 venv）：
   ```bash
   cd scripts/swarm-gpu-autoscaler
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. 配置环境变量（可复制 `.env.example` 为 `.env` 后修改）：
   - `RABBITMQ_URL`、`RABBITMQ_QUEUE` 必填（本地可起一个 RabbitMQ 或填已有地址）。
   - 本地测试建议设置 `AUTOSCALE_DRY_RUN=true`，不会真正执行 scale 和阿里云启停。

3. 运行：
   ```bash
   export AUTOSCALE_DRY_RUN=true
   python autoscaler.py
   ```
   观察日志：会按队列长度与阈值打印将要执行的操作（不实际执行）。

4. 若本机有 Docker 且已加入 Swarm，可去掉 `AUTOSCALE_DRY_RUN` 做真实 scale 测试（不填 `SWARM_GPU_INSTANCE_ID` 时不会调阿里云，只做 scale）。

## 云端部署

- **方式一（推荐）**：在 Manager 节点用 crontab 每 5 分钟跑一次脚本。设置 `AUTOSCALE_SINGLE_RUN=true` 时脚本执行一轮后退出，由 cron 负责周期调度，例如：
  ```cron
  */5 * * * * cd /path/to/scripts/swarm-gpu-autoscaler && .venv/bin/python autoscaler.py
  ```
- **方式二**：在 Manager 上跑一个常驻容器，挂载 `DOCKER_HOST` 或 `/var/run/docker.sock`，环境变量注入 AccessKey、实例 ID、队列地址等。

### 为什么推荐方案一

| 维度 | 方案一（crontab） | 方案二（常驻容器） |
|------|------------------|---------------------|
| **复杂度** | 无需多一个容器、不用挂载 docker.sock，系统自带 cron 即可 | 要维护镜像、挂载、健康检查，部署更重 |
| **故障恢复** | 某次执行挂了，下一轮 5 分钟会自动再跑；无单点常驻进程 | 进程崩了需要进程管理器（如 systemd/supervisor）或编排重启 |
| **资源占用** | 每 5 分钟跑几十秒，平时不占内存/CPU | 常驻进程一直占内存，并可能积累连接/句柄 |
| **适用场景** | 扩缩按「分钟级」响应即可（GPU 冷启也要 1–3 分钟） | 若未来需要「秒级」反应可再考虑 |

总结：GPU 节点冷启动本身要 1–3 分钟，5 分钟轮询足够用，方案一更简单、更稳、也更省资源，因此推荐。若你希望和 Swarm 一起用 Docker 统一部署，再选方案二。

## 配置说明

| 变量 | 说明 |
|------|------|
| `RABBITMQ_URL` | RabbitMQ 连接 URL |
| `RABBITMQ_QUEUE` | 队列名，默认 `media.uploaded` |
| `SWARM_STACK_NAME` | stack 名称，与 `docker stack deploy -c docker-compose.swarm.yml <name>` 一致 |
| `SWARM_SERVICE_NAME` | 服务名，与 compose 里 `gpu-worker` 对应 |
| `SWARM_GPU_NODE_ID` | GPU 节点在 Swarm 中的 ID 或 Hostname，用于 drain/active |
| `SWARM_GPU_INSTANCE_ID` | 阿里云 ECS 实例 ID（如 `i-xxx`） |
| `AUTOSCALE_QUEUE_SCALE_UP_THRESHOLD` | 队列消息数 ≥ 此值则扩容 |
| `AUTOSCALE_IDLE_MINUTES_BEFORE_SHUTDOWN` | 队列空多少分钟后关机 |
| `AUTOSCALE_DRY_RUN` | 为 true 时只打日志不执行 |

## 与栈部署对应关系

- 部署栈：`docker stack deploy -c docker-compose.swarm.yml retinaclip`
- 服务全名：`retinaclip_gpu-worker`
- GPU 节点需先打标签：`docker node update --label-add type=gpu <NODE_ID>`
