# 迁移收口审计

本文件记录 `roze-dtm` 在禁止 Rust 编译期间能够证明的迁移边界。它不把源码存在、静态合同或短时 smoke 解释为运行正确性或生产稳定性。

## 审计基线

- 原 Roze 基线：`roze-team/roze` 提交 `217274a134068f174cbe4a266a011bf719e15d0d` 中的 `crates/roze-dtm` 与 `apps/roze-dtm`。其核心范围是 TCC、Saga、内存/SQLite 存储、分支屏障、恢复租约和 Roze HTTP 控制面。
- dtm-labs 协议基线：`dtm-labs/dtm` 提交 `18146ee53bafbf094b1a5f12ca7e8a29bdb57edd`。兼容范围固定为 20 条 `/api/dtmsvr/**`/指标路由、1 条 JSON-RPC 路由、5 个 JSON-RPC 方法和 9 个 gRPC 方法及其字段号。
- Roze 生产基线：原生 `roze-http`/`roze-rpc`、数字响应、类型化配置、生产校验、固定脱敏、结构化日志/审计、健康与生命周期、低基数指标、持久化恢复租约和证据保守判定。

## 静态覆盖判定

| 范围 | 当前源码与合同证据 | 静态判定 | 尚需运行证据 |
| --- | --- | --- | --- |
| 原 Roze 能力 | `src/lib.rs` 保留 TCC、Saga、屏障、恢复与 Memory/SQLite，并将原状态模型向后兼容扩展 | 已覆盖且为超集 | Cargo 测试、原数据升级读取与真实重启 |
| 事务模式 | TCC、顺序/并发 DAG Saga、二阶段 Message、静态/回调 Workflow、XA 协调与 MySQL/PostgreSQL 资源管理器 | 源码与合同已覆盖 | 跨进程分支调用、崩溃点和真实数据库验证 |
| HTTP/JSON-RPC | 21 条兼容路由、5 个 JSON-RPC 方法、上游 `dtm_result`、请求选项、管理/KV/topic 与线级查询 DTO | 协议静态收口 | 固定上游 Go/Node 客户端互操作 |
| gRPC | `proto/dtmgimp.proto` 的 9 个 RPC、消息和字段号与固定上游一致，服务端与 Rust 客户端均有对应入口 | 协议静态收口 | protobuf 生成、编译和真实 gRPC 互操作 |
| 存储 | Memory、SQLite、PostgreSQL、MySQL、Redis standalone/Cluster；事务、动态分支、Workflow、KV/topic、屏障、revision CAS、恢复 lease/fence | 源码覆盖核心语义 | 五后端矩阵、Redis Cluster/MOVED/ASK、断连与 fencing 故障注入 |
| 客户端 | Rust HTTP/gRPC 与 TypeScript/JavaScript 原生和兼容客户端；XA、callback Workflow、topic/KV 与管理操作 | 源码与类型合同已覆盖 | 编译、类型检查及跨语言端到端 |
| 管理 | Roze Admin 风格 Dashboard、脱敏快照、XA 对账、服务端动作声明、二次确认和有界审计时间线 | 静态页面和合同已覆盖 | 真实 Bearer、状态变更、暗色模式和浏览器端到端 |
| 治理与运维 | 类型化生产配置、Bearer、URL allowlist、请求上限、无重定向、Roze 生命周期/健康、审计、告警、HTTP/RPC 指标及事件驱动、固定内存、低基数的 DTM 转换/分支/重试指标 | 源码与静态门禁已覆盖 | 运行时指标变化、告警接收端、依赖降级与长时间资源趋势 |
| 许可证 | MIT 项目许可证与 `THIRD_PARTY_NOTICES.md` 中固定上游 BSD 3-Clause 全文及归属 | 已覆盖 | 发布包中复核 notices |

## Roze 原生替代，不作为未迁移缺口

- 不复制 Go 协调器、Go 插件驱动或旧 Vue Admin 源码；分别使用 Roze Rust 状态机、Roze HTTP/RPC/配置边界和独立 Roze Admin 风格 Dashboard。
- 不追求上游全部语言 SDK；本项目交付 Roze Rust 客户端和浏览器/Node.js TypeScript、JavaScript 客户端，兼容协议允许其他上游 SDK 接入。
- 不复制 BoltDB 与 SQL Server 的具体适配器。SQLite 承担嵌入式开发/单实例持久化，PostgreSQL/MySQL/Redis 承担生产持久化；这是后端组合差异，不改变固定 HTTP/JSON-RPC/gRPC 协议和五种事务语义。
- 上游数据库自增 `id` 不是事务身份；兼容 DTO 返回文档化的 `id: 0`，所有业务和管理操作以 `gid` 为唯一标识。
- 上游 Redis/BoltDB 的 `DataExpire`/`FinishedDataExpire` 是存储保留策略，不是事务协议。当前项目未声明自动删除事务；生产环境应在取得真实恢复/审计保留要求后设计可审计清理策略，不能在迁移中静默删除协调记录。

## 当前唯一剩余门槛

静态迁移收口不等于完整目标已经通过验收。以下事项都需要解除编译禁令、启动真实依赖或运行较长时间，当前不能用更多源码声明替代：

1. `cargo fmt --check`、`cargo check`、Clippy 和 workspace/目标测试，包含 proto 生成及所有源码测试。
2. 固定 dtm-labs Go 客户端与 Rust/TypeScript/JavaScript 客户端的 HTTP、JSON-RPC、gRPC 五模式互操作矩阵。
3. SQLite、PostgreSQL、MySQL、Redis standalone/Cluster 的真实持久化、并发注册、租约、revision CAS、屏障、Workflow、KV/topic 和升级读取。
4. XA 在 MySQL/PostgreSQL 的 Prepare/Commit/Rollback、prepared 对账、启发式处置及各崩溃点恢复。
5. 分支超时、409/425、JSON-RPC/gRPC callback、重定向拒绝、告警失败、网络分区、进程重启和多 worker 接管故障注入。
6. 真实服务上的 Dashboard 鉴权/管理动作/审计、指标数值变化、生产只读 smoke，以及 24h/72h soak 和证据晋级。

在这些运行门槛完成前，`docs/roadmap.md` 和 `docs/production-validation.md` 中相应能力保持“已支持核心/部分支持”或 `inconclusive`，不得标记为生产稳定。
