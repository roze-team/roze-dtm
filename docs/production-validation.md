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

当前状态：服务端时间 + epoch 租约、恢复 fenced store、事务 revision/payload CAS、Workflow/屏障 fenced Lua 和 ignored integration tests 已加入源码。受当前禁编译要求约束，本提交没有执行 Cargo 测试，也没有形成真实 Redis、主从切换或长时间运行证据，因此 Redis 后端仍不能据此宣称完成多节点生产验证，状态保持 `inconclusive`。

## 协议互操作

HTTP、JSON-RPC 和 gRPC 兼容协议需要使用固定上游客户端版本执行 TCC、Saga、XA、Message、Workflow 和 callback Workflow 端到端矩阵。callback 还必须覆盖 HTTP `409/425`、JSON-RPC `-32901/-32902`、gRPC `ABORTED/FAILED_PRECONDITION`、TLS、超时和客户端在 callback 内提交终态的并发竞争。

当前状态：protobuf、OpenAPI 3.1、Rust/TypeScript/JavaScript 客户端合同和源码测试已存在；OpenAPI 已通过 Router 全覆盖检查及标准规范校验。真实 Go/Rust/TypeScript/JavaScript 跨语言互操作仍未执行。

`scripts/production-http-contract-smoke.mjs` 可对已运行的生产候选执行 12 项只读检查：三类探针、指标、OpenAPI、未授权拒绝、授权统计、Dashboard 脱敏、XA 对账、部署修订号、dtm-labs HTTP 和 JSON-RPC。运行必须绑定完整 Git revision 和显式部署拓扑；服务端 `/api/dtmsvr/version` 返回的 `release_revision` 必须与 `ROZE_DTM_EXPECTED_REVISION` 完全一致。脚本会将 OpenAPI/指标快照及检查结果写入证据目录。`scripts/validate-production-evidence.py` 独立校验时间范围、拓扑、判定一致性、检查项唯一性、相对工件路径、字节数与 SHA-256；通过烟测仍只证明该次短时 HTTP 合同，不替代数据库故障注入或 24h/72h soak。

## XA Resource Manager

MySQL 验证必须启用 InnoDB，并覆盖 XA START 之后、分支注册之后、XA PREPARE 之后和全局决策之后的进程中断；PostgreSQL 必须配置非零 `max_prepared_transactions` 并覆盖相同崩溃点。两种数据库都要验证 `roze_xa_barriers` 原子去重、重复 Commit/Rollback、`recover_prepared` 对账、注册失败回滚、网络超时重试、未知 XID，以及协调器追加的 `gid/trans_type/branch_id/op` 不可被 phase-2 URL 原查询参数覆盖。人工 Commit/Rollback 必须验证 `roze_xa_decisions` 的 intent-first 写入、decision id 幂等与冲突拒绝、失败后重试、终态复用、`reconcile` 双向差集，以及原因不进入日志、指标或 Dashboard。

当前状态：MySQL/PostgreSQL 资源管理器、固定 XID 校验、屏障与启发式决策 DDL、intent-first 决策持久化、prepared transaction 双向对账、phase-2 参数覆盖和源码测试已加入。受当前禁编译要求约束，尚未执行编译、真实数据库或崩溃注入验证，因此状态为 `inconclusive`。

## Dashboard

`GET /dashboard` 仅提供静态页面，`GET /v1/dashboard` 必须验证 Bearer 控制令牌。生产验收需要覆盖未授权拒绝、筛选与分页边界、空数据、各事务状态、审计历史容量/顺序、窄屏和系统暗色模式，并检查浏览器网络响应中不存在分支 URL、payload、Header、metadata、Workflow 数据、依赖错误或错误正文。控制令牌不得进入 URL、localStorage、sessionStorage、日志或表格 DOM；进程重启后审计时间线清空，不得将它误作持久审计证据。

当前状态：Roze Admin 风格页面、脱敏快照、XA 人工对账指标、有界审计时间线和 `/v1/xa/reconciliation` 源码测试已加入；已用本地静态 HTTP 服务完成桌面/390px 窄屏浏览器渲染、连接失败反馈和控制台错误检查。受当前禁编译要求约束，尚未执行真实 Roze 服务、受保护 Dashboard API、暗色模式或端到端验证，因此状态保持 `inconclusive`。

## 生产证据要求

生产证据必须绑定确切 Git revision，记录依赖拓扑、命令、持续时间、工作负载、错误预算、故障注入时间线、资源趋势和最终判定。HTTP 合同证据还必须通过独立工件哈希校验。未执行、被中断或缺少工件的运行一律标记为 `inconclusive`，不得根据源码检查补写通过结论。
