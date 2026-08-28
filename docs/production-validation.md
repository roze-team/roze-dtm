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

当前状态：protobuf、OpenAPI 3.1、Rust/TypeScript/JavaScript 客户端合同和源码测试已存在；`python scripts/validate-compatibility.py` 静态固定 21 条兼容 HTTP/JSON-RPC 路由、9 个 gRPC 方法、proto 字段号、SDK 入口、上游查询 DTO 与 BSD 归属，OpenAPI 也已通过 Router 全覆盖检查及标准规范校验。该门禁不启动服务，不能证明序列化运行结果或真实客户端行为；Go/Rust/TypeScript/JavaScript 跨语言互操作仍未执行。

并发 Saga 的源码合同覆盖依赖 DAG 校验、同层并发、后继等待和逆依赖补偿；HTTP 兼容层映射上游 `custom_data.concurrent/orders`。并发 Message 的源码合同覆盖同时投递、部分失败保留成功结果、仅重试失败分支，以及与延迟投递组合时到期前零调用；HTTP/JSON-RPC 兼容层映射上游请求顶层 `concurrent`。受当前禁编译要求约束，尚未执行这些 Rust 测试及真实分支故障注入，因此新增能力的运行证据仍为 `inconclusive`。

延迟 Message 的源码合同覆盖提交决策先持久化、到期前零分支调用、绝对恢复时间和到期后正常投递；HTTP/JSON-RPC/gRPC 兼容请求共享 `custom_data.delay` 秒到毫秒映射。受当前禁编译要求约束，Rust 测试及真实重启跨越投递点的验证尚未执行，状态为 `inconclusive`。

分支重试告警源码合同覆盖顺序/并发失败路径、阈值、超时、URL 查询串移除、配置 Debug 脱敏和告警失败不干扰事务恢复。生产验收仍需使用真实 HTTPS 接收端验证阈值前零调用、阈值后重复通知、非 2xx、超时、断连、服务重启与 payload/log/Dashboard 泄漏检查；当前禁编译窗口内未执行，状态为 `inconclusive`。

`scripts/production-http-contract-smoke.mjs` 可对已运行的生产候选执行 12 项只读检查：三类探针、指标、OpenAPI、未授权拒绝、授权统计、Dashboard 脱敏、XA 对账、部署修订号、dtm-labs HTTP 和 JSON-RPC。运行必须绑定完整 Git revision 和显式部署拓扑；服务端 `/api/dtmsvr/version` 返回的 `release_revision` 必须与 `ROZE_DTM_EXPECTED_REVISION` 完全一致。脚本会将 OpenAPI/指标快照及检查结果写入证据目录。`scripts/validate-production-evidence.py` 独立校验时间范围、拓扑、判定一致性、检查项唯一性、相对工件路径、字节数与 SHA-256；通过烟测仍只证明该次短时 HTTP 合同，不替代数据库故障注入或 24h/72h soak。

## XA Resource Manager

MySQL 验证必须启用 InnoDB，并覆盖 XA START 之后、分支注册之后、XA PREPARE 之后和全局决策之后的进程中断；PostgreSQL 必须配置非零 `max_prepared_transactions` 并覆盖相同崩溃点。两种数据库都要验证 `roze_xa_barriers` 原子去重、重复 Commit/Rollback、`recover_prepared` 对账、注册失败回滚、网络超时重试、未知 XID，以及协调器追加的 `gid/trans_type/branch_id/op` 不可被 phase-2 URL 原查询参数覆盖。人工 Commit/Rollback 必须验证 `roze_xa_decisions` 的 intent-first 写入、decision id 幂等与冲突拒绝、失败后重试、终态复用、`reconcile` 双向差集，以及原因不进入日志、指标或 Dashboard。

当前状态：MySQL/PostgreSQL 资源管理器、固定 XID 校验、屏障与启发式决策 DDL、intent-first 决策持久化、prepared transaction 双向对账、phase-2 参数覆盖和源码测试已加入。受当前禁编译要求约束，尚未执行编译、真实数据库或崩溃注入验证，因此状态为 `inconclusive`。

## Dashboard

`GET /dashboard` 仅提供静态页面，`GET /v1/dashboard` 和所有管理动作必须验证 Bearer 控制令牌。生产验收需要覆盖未授权拒绝、筛选与分页边界、空数据、各事务状态、终态无动作、待恢复事务只显示服务端允许动作、二次确认、执行中锁定、审计历史容量/顺序、窄屏和系统暗色模式，并检查浏览器网络响应中不存在分支 URL、payload、Header、metadata、Workflow 数据、依赖错误或错误正文。控制令牌不得进入 URL、localStorage、sessionStorage、日志或表格 DOM；进程重启后审计时间线清空，不得将它误作持久审计证据。

不启动服务的合同检查使用 `python scripts/validate-dashboard.py` 与 `python scripts/validate-openapi.py`；前者验证静态页面唯一元素 id、无第三方资源、CSP、令牌不落浏览器存储及管理动作接线，后者递归验证全部 OpenAPI schema 引用并固定 Dashboard 行级动作枚举。它们不能替代真实 Bearer、状态变更和审计端到端验证。

当前状态：Roze Admin 风格页面、脱敏快照、服务端动作声明、`reset-retry`/`force-stop` 二次确认、XA 人工对账指标、有界审计时间线和 `/v1/xa/reconciliation` 源码测试已加入；此前已用本地静态 HTTP 服务完成桌面/390px 窄屏浏览器渲染、连接失败反馈和控制台错误检查。受当前禁编译要求约束，本轮尚未执行真实 Roze 服务、受保护 Dashboard API、管理动作、暗色模式或端到端验证，因此状态保持 `inconclusive`。

## 生产证据要求

生产证据必须绑定确切 Git revision，记录依赖拓扑、命令、持续时间、工作负载、错误预算、故障注入时间线、资源趋势和最终判定。HTTP 合同证据还必须通过独立工件哈希校验。未执行、被中断或缺少工件的运行一律标记为 `inconclusive`，不得根据源码检查补写通过结论。
