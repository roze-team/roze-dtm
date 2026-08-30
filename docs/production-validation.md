# 生产验证状态

本文件只记录可复现入口和已经获得的证据。源码、静态检查或短时 smoke 不等价于生产稳定性证明。

## Redis 存储

Redis 后端需要分别验证 standalone 与 Cluster。测试不得输出连接 URL、用户名或密码。

```bash
ROZE_TEST_REDIS_URL=redis://127.0.0.1:6379 \
bash scripts/redis-integration.sh
```

```bash
ROZE_TEST_REDIS_CLUSTER_URLS=redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002 \
bash scripts/redis-integration.sh
```

最低验证范围：

- 事务插入、读取、状态更新和重复 GID 拒绝。
- 动态分支并发注册、Workflow 进度/终态及 callback 恢复 CAS。
- TCC 空回滚、防悬挂和重复屏障。
- KV 版本竞争、topic 订阅并发更新。
- 建连和命令超过 `redis_operation_timeout_ms` 时有界失败，恢复 worker 可在后续周期重试。
- 两个 worker 的租约竞争、续租、过期接管和 Redis 服务端时间语义。
- 同一 owner 过期后重新获取产生新 epoch，旧 epoch 的事务 CAS、Workflow 更新、屏障创建和屏障释放全部失败闭合。
- 普通控制面写入与恢复写入并发时，stale transaction revision 被拒绝且动态分支不会被静默覆盖。
- Cluster MOVED/ASK、节点重启、主从切换、断连恢复及同槽脚本行为。

当前状态：服务端时间 + epoch 租约、恢复 fenced store、事务 revision/payload CAS、Workflow/屏障 fenced Lua、事务级屏障清理索引和 ignored integration tests 已加入源码；workspace 编译、单元测试和 Clippy 已在本地通过。revision `20a46b92e70e94c17b0aaa9eb39e05ce88d4024e` 的 CI run `33239791535` 在三主三从拓扑中通过：使用迁移槽上的新 key 触发 ASK，使用同一已建 Cluster 连接在槽所有权改变后触发 MOVED，再停止实际槽 owner、等待副本提升、执行事务往返、重启旧主节点并再次执行往返。网络分区和长时间运行仍为 `inconclusive`。

## 协议互操作

HTTP、JSON-RPC 和 gRPC 兼容协议需要使用固定上游客户端版本执行 TCC、Saga、XA、Message、Workflow 和 callback Workflow 端到端矩阵。callback 还必须覆盖 HTTP `409/425`、JSON-RPC `-32901/-32902`、gRPC `ABORTED/FAILED_PRECONDITION`、TLS、超时和客户端在 callback 内提交终态的并发竞争。

当前状态：protobuf、OpenAPI 3.1、Rust/TypeScript/JavaScript 客户端合同和源码测试已存在；`python scripts/validate-compatibility.py` 静态固定 21 条兼容 HTTP/JSON-RPC 路由、9 个 gRPC 方法、proto 字段号、SDK 入口、上游查询 DTO 与 BSD 归属。`scripts/ci-protocol-integration.sh` 启动真实 SQLite 服务后执行原始 HTTP 五种事务、JSON-RPC Saga、受保护管理接口、Dashboard/指标、Rust gRPC smoke、仓库 JavaScript/TypeScript 原生及兼容 SDK，并运行固定 `dtm-labs/dtm@18146ee53bafbf094b1a5f12ca7e8a29bdb57edd` 官方 Go 客户端的 TCC 成功/失败回滚、Saga、Message，以及真实 PostgreSQL prepared transaction XA 提交/回滚。该固定 Go `dtmcli` 不提供 Workflow helper，Workflow 因此由仓库 JavaScript/TypeScript SDK、原始协议与 Rust 门禁覆盖。该门禁还实际覆盖可恢复 Message 503、HTTP callback `425`→`409`、JSON-RPC callback `-32902`→`-32901`、使用临时私有 CA 且严格校验 `localhost` SAN 的 `grpcs://` callback、gRPC `FAILED_PRECONDITION`→`ABORTED`、metadata/二进制 payload、逐调用 deadline、全局超时终态，以及 callback 内提交终态与恢复写入的竞争。revision `1705e1e1519b8bd47d1381ca0811d062b6a91093` 的 CI run `33246708377` 已通过此前完整跨语言门禁及 PostgreSQL/MySQL、Redis standalone/Cluster、ASK/MOVED、主从切换和旧主重启故障矩阵；本轮新增官方 Go XA 证据需等待当前提交 CI，固定上游 Node 客户端及更广的超时组合仍为 `inconclusive`。

并发 Saga 的源码合同覆盖依赖 DAG 校验、同层并发、后继等待和逆依赖补偿；HTTP 兼容层映射上游 `custom_data.concurrent/orders`。并发 Message 的源码合同覆盖同时投递、部分失败保留成功结果、仅重试失败分支，以及与延迟投递组合时到期前零调用；HTTP/JSON-RPC 兼容层映射上游请求顶层 `concurrent`。相关 Rust 测试已通过，单分支 503 后恢复重试已在真实服务 smoke 中通过；多进程并发、网络分区和重启故障注入仍为 `inconclusive`。

延迟 Message 的源码合同覆盖提交决策先持久化、到期前零分支调用、绝对恢复时间和到期后正常投递；HTTP/JSON-RPC/gRPC 兼容请求共享 `custom_data.delay` 秒到毫秒映射。Rust 测试已通过；`scripts/message-restart-integration.mjs` 进一步使用文件 SQLite 创建延迟 Message，在到期前强制终止协调器，跨过投递点与旧租约到期后使用不同 worker id 重启，验证恢复到 `Succeeded`、仅一次分支调用和稳定终态。revision `244f02beb053a4b0a141ad2e43c39b4ecf98462a` 的 CI run `33284169721` 已通过该门禁及完整跨语言/真实依赖矩阵；生产 PostgreSQL/Redis 多实例重启、网络分区与长时间负载仍为 `inconclusive`。

分支重试告警源码合同覆盖顺序/并发失败路径、阈值、超时、URL 查询串移除、配置 Debug 脱敏和告警失败不干扰事务恢复，相关 Rust 测试已通过。生产验收仍需使用真实 HTTPS 接收端验证阈值前零调用、阈值后重复通知、非 2xx、超时、断连、服务重启与 payload/log/Dashboard 泄漏检查，状态为 `inconclusive`。

`scripts/production-http-contract-smoke.mjs` 可对已运行的生产候选执行 12 项只读检查：三类探针、指标、OpenAPI、未授权拒绝、授权统计、Dashboard 脱敏、XA 对账、部署修订号、dtm-labs HTTP 和 JSON-RPC。运行必须绑定完整 Git revision 和显式部署拓扑；服务端 `/api/dtmsvr/version` 返回的 `release_revision` 必须与 `ROZE_DTM_EXPECTED_REVISION` 完全一致。脚本会将 OpenAPI/指标快照及检查结果写入证据目录。`scripts/validate-production-evidence.py` 独立校验时间范围、拓扑、判定一致性、检查项唯一性、相对工件路径、字节数与 SHA-256；通过烟测仍只证明该次短时 HTTP 合同，不替代数据库故障注入或 24h/72h soak。

`scripts/production-soak.mjs` 周期执行上述合同检查并保存每次指标/OpenAPI 快照、报告路径和 SHA-256，最终生成 `soak-report.json`。运行时沿用 HTTP smoke 的必填变量，并额外设置 profile；故障时间线可以通过 `ROZE_DTM_FAULT_TIMELINE_JSON` 指向 JSON 数组。示例：

```bash
ROZE_DTM_SOAK_PROFILE=24h \
ROZE_DTM_SOAK_INTERVAL_SECONDS=300 \
ROZE_DTM_EVIDENCE_DIR=/secure/evidence/roze-dtm-24h \
node scripts/production-soak.mjs
python scripts/validate-soak-evidence.py /secure/evidence/roze-dtm-24h/soak-report.json
```

`smoke` profile 默认只运行 60 秒并标记为 `harness_only`；它不能获得 24h/72h 资格。校验器要求长稳报告的实际单调时钟持续时间达到 86400/259200 秒，逐个复验子报告及其工件哈希，并拒绝中断、缺失样本、revision 不一致或超出错误预算的报告。

指标 smoke 还要求 `roze_dtm_metrics_registry_available 1` 存在。生产验收必须在创建、推进、失败、重试和终态后核对 `roze_dtm_transaction_transitions_total`、`roze_dtm_branch_state_observations_total` 与 `roze_dtm_retry_scheduled_observations_total` 的单调变化；将测试事务时间推进到配置保留边界后，还要核对 `roze_dtm_retention_deleted_total` / `roze_dtm_retention_conflicts_total`、协调记录与屏障的同步删除，以及扫描后并发更新不会被删除。确认抓取不触发存储全表扫描、指标中不存在 GID、branch id、URL、payload、Header、控制令牌或错误正文；静态源码测试不能替代该运行验证。

## XA Resource Manager

MySQL 验证必须启用 InnoDB，并为执行恢复扫描的受限资源管理账户授予全局动态权限 `XA_RECOVER_ADMIN`；该账户不应获得 root 或无关的全局写权限。测试要覆盖 XA START 之后、分支注册之后、XA PREPARE 之后和全局决策之后的进程中断；PostgreSQL 必须配置非零 `max_prepared_transactions` 并覆盖相同崩溃点。两种数据库都要验证 `roze_xa_barriers` 原子去重、重复 Commit/Rollback、`recover_prepared` 对账、注册失败回滚、网络超时重试、未知 XID，以及协调器追加的 `gid/trans_type/branch_id/op` 不可被 phase-2 URL 原查询参数覆盖。人工 Commit/Rollback 必须验证 `roze_xa_decisions` 的 intent-first 写入、decision id 幂等与冲突拒绝、失败后重试、终态复用、`reconcile` 双向差集，以及原因不进入日志、指标或 Dashboard。

当前状态：MySQL/PostgreSQL 资源管理器、固定 XID 校验、屏障与启发式决策 DDL、intent-first 决策持久化、prepared transaction 双向对账和 phase-2 参数覆盖已加入，并通过本地编译、单元测试与 Clippy。`tests/xa_backends.rs` 在 Prepare 后丢弃原资源管理器对象，从同一数据库池重建 manager，再执行 prepared 扫描和 Commit；Commit 后直接重放 phase-2，以数据库 unknown XID 响应验证 `AlreadyResolved`。两种数据库的 Prepare、Commit、Rollback、屏障去重、启发式 decision id 幂等与对账，以及 PostgreSQL prepared transactions 和 MySQL `XA_RECOVER_ADMIN` 最小权限拓扑均由 CI 执行。数据库容器/业务进程硬崩溃点和网络超时仍为 `inconclusive`。

## Dashboard

`GET /dashboard` 仅提供静态页面，`GET /v1/dashboard` 和所有管理动作必须验证 Bearer 控制令牌。生产验收需要覆盖未授权拒绝、筛选与分页边界、空数据、各事务状态、终态无动作、待恢复事务只显示服务端允许动作、二次确认、执行中锁定、审计历史容量/顺序、窄屏和系统暗色模式，并检查浏览器网络响应中不存在分支 URL、payload、Header、metadata、Workflow 数据、依赖错误或错误正文。控制令牌不得进入 URL、localStorage、sessionStorage、日志或表格 DOM；进程重启后审计时间线清空，不得将它误作持久审计证据。

不启动服务的合同检查使用 `python scripts/validate-dashboard.py` 与 `python scripts/validate-openapi.py`；前者验证静态页面唯一元素 id、无第三方资源、CSP、令牌不落浏览器存储及管理动作接线，后者递归验证全部 OpenAPI schema 引用并固定 Dashboard 行级动作枚举。它们不能替代真实 Bearer、状态变更和审计端到端验证。

当前状态：Roze Admin 风格页面、脱敏快照、服务端动作声明、`reset-retry`/`force-stop` 二次确认、XA 人工对账指标、有界审计时间线和 `/v1/xa/reconciliation` 源码测试已加入。真实 SQLite Roze 服务已验证未授权拒绝、受保护 Dashboard 快照、force-stop、审计事件、指标存在及令牌不泄漏；此前还完成桌面/390px 窄屏静态浏览器渲染。暗色模式的真实服务浏览器端到端、持久审计 sink 和服务重启后的行为仍为 `inconclusive`。

## 生产证据要求

生产证据必须绑定确切 Git revision，记录依赖拓扑、命令、持续时间、工作负载、错误预算、故障注入时间线、资源趋势和最终判定。HTTP 合同证据还必须通过独立工件哈希校验。未执行、被中断或缺少工件的运行一律标记为 `inconclusive`，不得根据源码检查补写通过结论。
