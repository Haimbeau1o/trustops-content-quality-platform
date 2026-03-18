# Project Blueprint

## 项目名称

TrustOps Platform - Content Quality & Data Services

## 项目摘要

面向内容质量与数据服务的信任治理平台，覆盖事件接入、规则判定、案例流转、数据服务和 AI Copilot 增强。

## 技术栈建议

- 主链路：Go + Hertz
- AI 增强：Python + FastAPI
- 数据层：MySQL + Redis + RabbitMQ
- 可观测性：Prometheus / Grafana

## 核心模块

### Ingress Gateway

- 鉴权、限流、租户隔离、请求校验

### Evaluation Engine

- 规则判定、质量打分、证据聚合

### Case Workflow

- 质检案例创建、流转、操作日志

### Data Service Layer

- 事件查询、案例检索、指标聚合接口

### Async Worker

- 批量回放、重试、补偿任务

### AI Copilot

- 案例摘要、规则解释、相似案例召回
- 仅增强，失败可回退，不接管主判定

## 里程碑

1. Week 1：定义领域模型和服务骨架
2. Week 2：完成规则判定与案例流转主链路
3. Week 3：接入 Redis/MQ，完善异步和缓存
4. Week 4：接入 AI Copilot、指标埋点和误判复盘

## 指标建议

- Event ingest latency
- Rule evaluation latency
- Case creation success rate
- Evidence completeness
- Misclassification replay turnaround time
- Copilot grounding coverage

## Demo 场景

- 内容发布事件命中规则后自动生成质检案例
- 运营查看案例摘要、证据链和规则解释
- 回放误判案例并定位高误杀规则
- 通过数据服务接口查询事件与案例指标

