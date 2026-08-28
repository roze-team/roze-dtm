# Roze DTM 功能路线

本项目参考 `dtm-labs/dtm` 的功能覆盖面，但实现必须遵循 Roze 的 Rust 原生 HTTP、配置、日志、指标、生命周期和治理契约。功能语义可以对齐，代码不直接移植。

## 当前能力

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| TCC | 已支持 | Try/Confirm/Cancel、空回滚、防悬挂、幂等屏障与失败恢复 |
| Saga | 已支持核心 | 顺序执行、依赖 DAG 分层并发、逆依赖并发补偿、重试与超时恢复；真实并发故障注入待补 |
| 存储 | 已支持核心 | Memory、SQLite、PostgreSQL、MySQL、Redis standalone/Cluster；关系型后端与 Redis standalone/三节点 Cluster 往返已通过 CI，切换故障证据待补 |
| 高可用恢复 | 已支持 | 持久化租约保证同一时刻只有一个恢复 worker 推进事务 |
| 控制面 | 已支持 | Roze HTTP、数字响应信封、鉴权、健康检查、HTTP/RPC 与低基数 DTM 业务指标、审计事件 |
| 重试告警 | 已支持核心 | dtm-labs AlertWebHook 负载、顺序/并发失败阈值、超时与脱敏；真实 HTTPS 故障矩阵待补 |
| DTM HTTP/JSON-RPC/gRPC 兼容 | 已支持核心 | 9 个 gRPC 方法、逐事务 timeout/retry/Header 及 WaitResult 异步调度；真实 Rust gRPC 客户端已通过，本地固定上游跨语言互操作待补 |
| XA | 已支持核心 | 协调生命周期、动态分支、可信 phase-2 参数、MySQL/PostgreSQL Rust 资源管理器、启发式决策与 prepared 对账已通过真实数据库 CI；崩溃恢复矩阵待补 |
| 二阶段消息 | 已支持核心 | Prepared/Dispatch/Abort、顺序/并发投递、部分失败精确重试、持久化延迟投递、上游顶层 `concurrent` 与 `custom_data.delay`，并复用 `roze-transaction` Outbox/Inbox 与 `roze-mq`；真实重启跨投递点验证待补 |
| Topic/KV | 已支持核心 | 通用版本化 KV、订阅增删查、HTTP 兼容端点与 `topic://` 消息分支展开 |
| Workflow DSL | 已支持核心 | 静态依赖图、恢复和逆序补偿；callback Workflow 支持复合进度、终态及 HTTP/JSON-RPC/gRPC QueryPrepared 主动恢复，互操作待验证 |
| Redis 存储 | 已支持核心 | Roze topology、revision CAS/屏障、服务端时间 + epoch 租约、恢复写入 fencing、KV/topic 已通过 standalone/三节点 Cluster CI；MOVED/ASK、切换和分区故障证据待补 |
| SDK | 已支持 Web 范围 | 已提供 Rust HTTP/gRPC client，以及 TypeScript/JavaScript `/v1` 与 dtm-labs HTTP/JSON-RPC 兼容客户端 |
| OpenAPI | 已支持 | OpenAPI 3.1 覆盖全部 54 条 HTTP 路径，支持自由 JSON payload、Bearer 安全声明、确定性生成和 Router 覆盖校验 |
| Dashboard | 已支持核心 | Roze Admin 风格脱敏快照、服务端动作声明、二次确认管理操作、独立页面和有界审计时间线已通过真实服务 API smoke；Roze Admin 内嵌模块和暗色浏览器端到端待补 |

## 实施顺序

1. 加固现有 TCC/Saga：并发 Saga 已接入，继续补数据库故障测试、恢复 soak 与指标告警。
2. 接入 Roze Outbox/Inbox 和二阶段消息，形成数据库写入到可靠事件发布闭环。
3. 提供 Workflow DSL 与 Rust SDK，保持控制面协议可版本化。
4. 在已通过的 XA 真实数据库矩阵上补齐崩溃点恢复和资源侧启发式决策审计证据。
5. 在已通过的 Redis standalone/Cluster 往返矩阵上补齐 fencing/切换故障证据、Dashboard 暗色浏览器端到端与 Roze Admin 内嵌模块。

任何标记为“已支持”的生产能力都必须同时具备契约文档、自动测试和可复现的运行入口。
