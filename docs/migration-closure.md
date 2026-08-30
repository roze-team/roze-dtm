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
| HTTP/JSON-RPC | 21 条兼容路由、5 个 JSON-RPC 方法、上游 `dtm_result`、请求选项、管理/KV/topic 与线级查询 DTO | 协议静态收口；固定上游 Go 客户端 TCC 成功/失败回滚、Saga/Message、真实 PostgreSQL XA 提交/回滚与仓库 JS/TS SDK 已纳入真实服务门禁 | 固定上游 Node 客户端及更广超时矩阵；固定 Go `dtmcli` 无 Workflow helper |
| gRPC | `proto/dtmgimp.proto` 的 9 个 RPC、消息和字段号与固定上游一致，服务端与 Rust 客户端均有对应入口 | 协议静态收口 | protobuf 生成、编译和真实 gRPC 互操作 |
| 存储 | Memory、SQLite、PostgreSQL、MySQL、Redis standalone/Cluster；事务、动态分支、Workflow、KV/topic、屏障、revision CAS、恢复 lease/fence，以及 DataExpire/FinishedDataExpire 有界 CAS 清理 | 源码覆盖核心语义；Redis MOVED/ASK、主从切换和旧主重启已通过 CI | 五后端升级读取、网络分区与 fencing 故障注入 |
| 客户端 | Rust HTTP/gRPC 与 TypeScript/JavaScript 原生和兼容客户端；XA、callback Workflow、topic/KV 与管理操作 | 源码、严格 `tsc` 类型检查、JS/TS 运行时、固定上游 Go 客户端及 Rust gRPC 已通过跨进程真实服务验收 | 固定上游 Node 客户端及 TLS 互操作 |
| 管理 | Roze Admin 风格 Dashboard、脱敏快照、XA 对账、服务端动作声明、二次确认和有界审计时间线 | 静态页面和合同已覆盖 | 真实 Bearer、状态变更、暗色模式和浏览器端到端 |
| 治理与运维 | 类型化生产配置、Bearer、URL allowlist、请求上限、无重定向、Roze 生命周期/健康、审计、告警、HTTP/RPC 指标及事件驱动、固定内存、低基数的 DTM 转换/分支/重试指标 | 源码与静态门禁已覆盖 | 运行时指标变化、告警接收端、依赖降级与长时间资源趋势 |
| 许可证 | MIT 项目许可证与 `THIRD_PARTY_NOTICES.md` 中固定上游 BSD 3-Clause 全文及归属 | 已覆盖 | 发布包中复核 notices |

## Roze 原生替代，不作为未迁移缺口

- 不复制 Go 协调器、Go 插件驱动或旧 Vue Admin 源码；分别使用 Roze Rust 状态机、Roze HTTP/RPC/配置边界和独立 Roze Admin 风格 Dashboard。
- 不追求上游全部语言 SDK；本项目交付 Roze Rust 客户端和浏览器/Node.js TypeScript、JavaScript 客户端，兼容协议允许其他上游 SDK 接入。
- 不复制 BoltDB 与 SQL Server 的具体适配器。SQLite 承担嵌入式开发/单实例持久化，PostgreSQL/MySQL/Redis 承担生产持久化；这是后端组合差异，不改变固定 HTTP/JSON-RPC/gRPC 协议和五种事务语义。
- 上游数据库自增 `id` 不是事务身份；兼容 DTO 返回文档化的 `id: 0`，所有业务和管理操作以 `gid` 为唯一标识。
- 上游 Redis/BoltDB 的 `DataExpire`/`FinishedDataExpire` 已迁移为五种存储统一的显式保留策略。默认值保持 7 天/1 天，清理批次有界，使用 compare-and-delete 避免与恢复并发误删，并发冲突、删除数量和失败均进入低基数指标/审计。部署方必须根据真实恢复、对账和审计要求覆盖默认值；持久审计 sink 不随协调记录清理。

## 编译禁令解除后的验收进展

- `cargo fmt --all -- --check`、`cargo check --workspace --all-targets`、Clippy `-D warnings` 和 workspace 全目标测试已经通过，包含 proto 生成、Workflow DAG 与 XA 源码测试。
- 使用 `service/config.sqlite.smoke.yaml` 启动的真实 Roze 服务已经通过 HTTP 五事务模式、JSON-RPC Saga、Rust gRPC 客户端、受保护 Dashboard/管理动作、指标检查和可恢复 Message 503 分支故障。
- `tests/xa_backends.rs` 与 CI 真实依赖矩阵已覆盖并通过 PostgreSQL/MySQL XA Commit/Rollback/屏障/对账、manager 重建后 prepared 恢复、unknown XID 幂等、Redis standalone 和三主三从 Redis Cluster；MySQL 验收还固定了 `XA_RECOVER_ADMIN` 最小恢复权限。revision `20a46b92e70e94c17b0aaa9eb39e05ce88d4024e` 的 run `33239791535` 还通过了 Redis MOVED/ASK、槽 owner 停机、副本提升、切换后往返与旧主重启。
- revision `1705e1e1519b8bd47d1381ca0811d062b6a91093` 的 CI run `33246708377` 已通过严格 TypeScript、真实 SQLite HTTP/JSON-RPC/gRPC、仓库 JS/TS SDK、固定 dtm-labs Go TCC 成功/失败回滚、Saga/Message、callback 错误码与终态竞争，并通过上述 PostgreSQL/MySQL、Redis standalone/Cluster、ASK/MOVED、主从切换和旧主重启真实依赖矩阵。
- revision `244f02beb053a4b0a141ad2e43c39b4ecf98462a` 的 CI run `33284169721` 已使用文件 SQLite 在延迟 Message 到期前强制终止第一个 worker，跨过投递点和旧恢复租约到期后由不同 worker 恢复到 `Succeeded`，并验证分支仅调用一次；同一运行再次通过完整跨语言协议与真实数据库/Redis 故障矩阵。
- revision `caa11df8e5f546119f902cd8835a02987885196c` 的 CI run `33243037990` 进一步通过 gRPC callback `FAILED_PRECONDITION`/`ABORTED`、metadata、二进制 payload、逐调用 deadline、恢复重试和全局超时终态，并再次通过全部真实依赖矩阵。
- revision `f29835e1e878d3600ec3db352aa5b5b56eae0d21` 的 CI run `33245231136` 已使用运行时临时私有 CA 与带 `localhost` SAN 的服务证书，通过严格 `grpcs://` 证书链/主机名校验重跑上述 gRPC callback 矩阵，并再次通过全部真实依赖与 Redis Cluster 故障矩阵。

## 剩余运行门槛

以下事项不能用更多源码声明或一次短时 smoke 替代：

1. 在已通过的固定 dtm-labs Go TCC 成功/失败回滚、Saga/Message、Rust gRPC、JavaScript/TypeScript SDK 严格类型检查与运行时、HTTP/JSON-RPC/gRPC callback 错误码、可信 `grpcs://` TLS、gRPC deadline/全局超时和终态竞争基础上，等待新增真实 PostgreSQL XA 提交/回滚门禁通过，并继续补固定上游 Node 客户端与更广的超时组合。固定 Go `dtmcli` 不提供 Workflow helper，不再将其列为可执行矩阵项。
2. 在已通过的 PostgreSQL/MySQL/Redis CI 矩阵之上，继续覆盖 Redis 网络分区、关系型存储升级读取和多 worker 接管。
3. XA 在 Prepare 前后、分支注册后、全局决策后发生数据库容器/业务进程硬崩溃或网络超时的恢复。
4. 真实 HTTPS 告警接收端、重定向拒绝、进程重启、持久审计 sink 和 Dashboard 暗色模式端到端。
5. 绑定确切 Git revision 的 24h/72h soak、资源趋势、错误预算和故障时间线证据。

因此当前判定为“源码与静态迁移已收口，完整本地短时验收与真实依赖 CI 已通过；生产稳定性仍为 `inconclusive`”。在剩余运行门槛完成前，`docs/roadmap.md` 和 `docs/production-validation.md` 不得标记为生产稳定。
