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

## 本地运行（第一版骨架）

### 1) Go gateway 单独运行

```bash
cd services/gateway-go
go mod tidy
go run ./cmd/server
```

可用接口：
- `GET http://127.0.0.1:8080/healthz`
- `POST http://127.0.0.1:8080/api/v1/content/events/ingest`
- `GET http://127.0.0.1:8080/api/v1/content/cases/:case_id`

### 2) Python copilot 单独运行

```bash
cd services/ai-copilot
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8090
```

可用接口：
- `GET http://127.0.0.1:8090/healthz`
- `POST http://127.0.0.1:8090/copilot/content/summary`

### 3) 使用 Docker Compose 一键启动基础依赖和服务

```bash
docker compose up --build
```

启动后默认端口：
- Gateway: `8080`
- Copilot: `8090`
- MySQL: `3306`
- Redis: `6379`
- RabbitMQ: `5672` / `15672`

### 4) 运行 copilot 测试

```bash
cd services/ai-copilot
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python3 -m pytest -q
```
