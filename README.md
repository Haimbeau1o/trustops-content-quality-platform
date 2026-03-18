# TrustOps Content Quality Platform

面向内容质量与数据服务场景的后端平台型项目骨架。项目主线是服务工程与数据治理，AI 只作为增强模块，不承担主判定链路。

## 项目定位

这个仓库用于展示「后端平台工程能力 + 内容质量业务抽象 + AI 工程化增强」的组合能力，目标是对齐字节跳动内容质量与数据服务相关 JD。

核心定位：
- 后端主链路：事件接入、规则判定、案例流转、数据服务、异步任务
- AI 增强链路：案例摘要、规则解释、相似案例召回（可回退）
- 面向线上：可靠性、安全性、性能和可观测性优先

## JD 来源说明

本仓库内容来自 `jd-to-offer` 工作空间中该岗位 case 的结构化产物，并做了项目化改写：

- 来源工作区：`/Volumes/passport/简历/滴滴/.worktrees/trustops-expansion`
- 原始 JD：`examples/bytedance_2026_content_quality_ai_tool_jd.md`
- 参考 case：
  - `cases/bytedance-content-quality-2026/02_knowledge_system.md`
  - `cases/bytedance-content-quality-2026/04_project_blueprint.md`
  - `cases/bytedance-content-quality-2026/05_interview_assets.md`

完整来源与映射见 [docs/jd-source.md](docs/jd-source.md)。

## 关键技能点与知识点

项目聚焦以下能力：
- 后端服务工程：分层架构、模块边界、幂等重试、上线交付
- 数据基础设施：MySQL/Redis/MQ 的组合设计与一致性治理
- 可靠性与安全性：限流、鉴权、审计日志、可追踪链路
- 业务抽象：把内容质量流程建模为 `Event / Rule / Case / Evidence`
- AI 应用工程化：结构化输出、失败回退、增强不替代主链路

详细知识清单见 [docs/knowledge-points.md](docs/knowledge-points.md)。

## 架构摘要

建议采用 `Go + Hertz` 作为主服务，`Python + FastAPI` 作为 AI Copilot sidecar：

1. Ingress Gateway 接收内容事件并做鉴权、限流、校验
2. Evaluation Engine 进行规则判定与证据聚合
3. Case Workflow 管理案例状态机和运营动作日志
4. Data Service Layer 提供事件/案例/指标查询接口
5. Async Worker 承担回放、重试、补偿和批处理
6. AI Copilot 提供摘要/解释/召回，失败时回退到主链路

详细蓝图见 [docs/project-blueprint.md](docs/project-blueprint.md)。

## 模块拆分

- `services/gateway-go`：Go 主服务入口与业务 API
- `services/worker`：异步任务消费、重试与补偿
- `services/ai-copilot`：Python 增强服务（摘要、解释、召回）
- `infra`：部署、监控、消息与存储相关基础设施配置
- `scripts`：本地开发、回放、校验、压测脚本
- `docs`：JD 来源、知识体系、蓝图与面试素材

## 里程碑路线图（建议）

1. Week 1：领域模型与事件 schema，搭建服务骨架
2. Week 2：规则判定 + 案例流转主链路打通
3. Week 3：接入 Redis/MQ，完善异步与缓存一致性
4. Week 4：接入 AI Copilot、加指标埋点与误判复盘

## 目录结构

```text
trustops-content-quality-platform/
├── README.md
├── .gitignore
├── docs/
│   ├── jd-source.md
│   ├── knowledge-points.md
│   ├── project-blueprint.md
│   └── interview-assets.md
├── services/
│   ├── gateway-go/
│   ├── ai-copilot/
│   └── worker/
├── infra/
└── scripts/
```

## Phase 3 运行形态（runtime reliability）

Phase 3 在 phase-2 运行骨架上补齐了可靠性主链路能力：
- 配置：`services/gateway-go/internal/config` 从环境变量加载，带默认值
- 存储：`internal/storage` 支持 `MySQL(持久化) + Redis(查询缓存)`，Redis 异常时降级为 MySQL-only
- 幂等：事件接入按 `event_id` 做请求幂等，重复请求返回同一 `case_id`
- 事务：单次 ingest 在一个事务内写入 `content_cases + idempotency + outbox + audit_log`
- 消息：网关后台 outbox relay 异步发布到 RabbitMQ，失败按重试阈值进入 dead-letter
- 异步：`services/worker` 消费队列并记录处理日志
- AI：`services/ai-copilot` 继续保持增强链路，不替代主判定
- 运维：新增 `GET /api/v1/ops/metrics`，输出 ingest/outbox/audit 指标汇总
- 编排：`docker-compose.yml` 为 MySQL / Redis / RabbitMQ 增加健康检查，避免启动窗口进入“假成功”链路

### 1) 环境变量

项目根目录提供了 `.env.example`，可复制为 `.env` 后按需覆盖：

```bash
cp .env.example .env
```

默认值已经适配 Docker Compose，本地不改也可直接启动。

### 2) 一键启动（推荐）

```bash
docker compose up --build
```

运行拓扑：
- `gateway`：`8080`
- `copilot`：`8090`
- `worker`：后台消费 RabbitMQ，无对外端口
- `mysql`：`3306`（自动执行 `infra/mysql/init/001_init.sql`）
- `redis`：`6379`
- `rabbitmq`：`5672` / `15672`

### 3) API 保持不变

Gateway：
- `GET /healthz`
- `POST /api/v1/content/events/ingest`
- `GET /api/v1/content/cases/:case_id`
- `GET /api/v1/ops/metrics`

Copilot：
- `GET /healthz`
- `POST /copilot/content/summary`

### 4) 样例请求

```bash
./scripts/sample_requests.sh
```

该脚本会按顺序执行：健康检查、事件接入、案例查询、copilot 摘要。

### 5) 本地测试

```bash
cd services/gateway-go && go test ./...
cd services/ai-copilot && python3 -m pytest -q
cd services/worker && go test ./...
```
