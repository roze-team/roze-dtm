# Roze DTM

Roze 的独立分布式事务协调器，提供 TCC、Saga、Workflow、二阶段消息和 XA。代码从 `roze-team/roze` 的 `217274a134068f174cbe4a266a011bf719e15d0d` 提取，保留原有状态机、持久化、分支屏障、恢复租约和原生 Roze HTTP 控制面，并按固定 dtm-labs 协议基线扩展兼容控制面、gRPC、五种存储与事务保留策略。

## 项目结构

- `src/lib.rs`：DTM 核心库，包含 TCC、顺序/并发 DAG Saga、Workflow、二阶段消息与 XA 状态机，以及内存、SQLite、PostgreSQL、MySQL、Redis 存储、HTTP 分支调用、恢复逻辑和 DataExpire/FinishedDataExpire 有界 CAS 清理。
- `src/xa.rs`：MySQL/PostgreSQL XA 业务资源管理器、屏障、prepared transaction 扫描与幂等 phase-2。
- `service/`：独立控制面服务。
- `service/static/openapi.json`：覆盖原生、兼容、管理与运维端点的 OpenAPI 3.1 合同。
- `proto/dtmgimp.proto`：与 dtm-labs/dtm 保持字段号兼容的 gRPC 协议合同和生成边界。
- `docs/dtm-grpc.md`：Roze gRPC 生命周期、鉴权、健康检查和客户端契约。
- `service/config.yaml`：开发环境示例配置，也是服务的默认配置。
- `docs/dtm.md`：API、部署与安全契约。
- `docs/roadmap.md`：参考 dtm-labs/dtm 的能力矩阵与 Roze 实施顺序。
- `sdk/`：原生 `/v1` 控制面的 TypeScript/JavaScript Web 客户端与使用说明。
- `docs/dtm-compatibility.md`：dtm-labs/dtm HTTP 兼容端点、调用顺序和差异。
- `docs/production-validation.md`：真实依赖、互操作、故障注入和生产证据状态。
- `docs/migration-closure.md`：原 Roze、固定 dtm-labs 协议与 Roze 生产要求的逐项静态收口审计。

## 验证

```bash
cargo test --workspace
```

本地完整协议烟测使用独立 SQLite 内存配置启动真实 HTTP/gRPC 服务，并运行跨进程分支 mock：

```bash
ROZE_CONFIG_PATH=service/config.sqlite.smoke.yaml \
ROZE_DTM_CONTROL_TOKEN='roze-dtm-smoke-token-32-bytes!!' \
ROZE_DTM_RELEASE_REVISION=$(git rev-parse HEAD) \
cargo run -p roze-dtm-service

ROZE_DTM_CONTROL_TOKEN='roze-dtm-smoke-token-32-bytes!!' \
node scripts/local-protocol-integration.mjs

ROZE_DTM_CONTROL_TOKEN='roze-dtm-smoke-token-32-bytes!!' \
ROZE_DTM_EXPECTED_REVISION=$(git rev-parse HEAD) \
node scripts/sdk-protocol-integration.mjs

ROZE_DTM_CONTROL_TOKEN='roze-dtm-smoke-token-32-bytes!!' \
ROZE_DTM_EXPECTED_REVISION=$(git rev-parse HEAD) \
node --experimental-transform-types scripts/sdk-typescript-integration.ts

ROZE_DTM_GRPC_ENDPOINT=http://127.0.0.1:36791 \
ROZE_DTM_CONTROL_TOKEN='roze-dtm-smoke-token-32-bytes!!' \
cargo run --example grpc_smoke
```

协议脚本覆盖 HTTP 的 TCC、Saga、Workflow、Message、XA，JSON-RPC Saga，受保护的管理查询/force-stop、Dashboard 审计、低基数指标，以及一次可恢复的 Message 503 分支失败。SDK 门禁还通过仓库 JavaScript/TypeScript 客户端执行五种原生事务、DTM 兼容调用、callback 错误码与终态竞争，并通过固定提交 `18146ee53bafbf094b1a5f12ca7e8a29bdb57edd` 的 dtm-labs Go 官方客户端执行 TCC 成功/失败回滚、Saga 和 Message。真实 gRPC callback 门禁会临时生成私有 CA 与带 `localhost` SAN 的服务证书，通过严格校验的 `grpcs://` 验证 `FAILED_PRECONDITION`→重试→`ABORTED`、metadata、二进制 payload、逐调用 deadline 和全局超时终态；私钥在脚本退出时删除且不入库。`bash scripts/ci-protocol-integration.sh` 可启动服务并串行执行全部 HTTP、JSON-RPC、gRPC 和跨语言验收；`node scripts/message-restart-integration.mjs` 使用文件 SQLite 持久化延迟 Message，强制终止第一个 worker，跨过投递点和旧租约到期后以新 worker 恢复，并断言分支只投递一次。它们仅供本机/CI 验收，示例令牌和 SQLite 配置不得用于部署。

不启动 Rust 编译器时，可独立重建并核对 HTTP 合同：

```bash
python scripts/generate-openapi.py
python scripts/validate-openapi.py
python scripts/validate-compatibility.py
python scripts/validate-migration-closure.py
python scripts/validate-dashboard.py
```

对已经部署的生产候选服务执行只读合同烟测时，必须显式提供完整 Git revision、控制令牌、部署拓扑和证据目录。部署时的 `ROZE_DTM_RELEASE_REVISION` 与烟测的 `ROZE_DTM_EXPECTED_REVISION` 必须是同一个 40 位 Git revision；烟测会读取服务版本端点并拒绝修订号不一致的候选：

```bash
ROZE_DTM_BASE_URL=https://dtm.example.com \
ROZE_DTM_CONTROL_TOKEN='env-secret' \
ROZE_DTM_EXPECTED_REVISION=0123456789abcdef0123456789abcdef01234567 \
ROZE_DTM_EVIDENCE_DIR=target/dtm-http-evidence \
ROZE_DTM_TOPOLOGY_JSON='{"store":"postgres","replica_count":2,"dependencies":["postgres"]}' \
node scripts/production-http-contract-smoke.mjs
python scripts/validate-production-evidence.py target/dtm-http-evidence/http-contract-report.json
```

该烟测不会创建事务，只读取探针、指标、OpenAPI、统计、Dashboard、XA 对账和部署修订号，并调用无持久副作用的 HTTP/JSON-RPC GID 生成。报告不记录令牌；OpenAPI 和指标快照使用 SHA-256 绑定，任何缺失或篡改都会被独立校验器拒绝。

设置以下变量后，工作区测试还会执行真实关系型数据库契约测试：

```bash
ROZE_DTM_TEST_POSTGRES_URL=postgres://user:password@localhost/roze_dtm
ROZE_DTM_TEST_MYSQL_URL=mysql://user:password@localhost/roze_dtm
cargo test --workspace
```

Redis 后端的真实依赖测试默认忽略；可通过 standalone Redis 显式运行：

```bash
ROZE_TEST_REDIS_URL=redis://127.0.0.1:6379 \
cargo test redis_store_round_trip_against_real_service -- --ignored --nocapture
```

Redis Cluster 使用逗号分隔的种子节点执行对应 ignored test：

```bash
ROZE_TEST_REDIS_CLUSTER_URLS=redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002 \
cargo test redis_cluster_store_round_trip_against_real_service -- --ignored --nocapture
```

CI 会启动 PostgreSQL、MySQL、Redis standalone 和三节点 Redis Cluster，启用 PostgreSQL prepared transactions、为受限 MySQL 测试用户授予 `XA_RECOVER_ADMIN`，并强制执行关系型存储、真实 XA Commit/Rollback/屏障/对账及两种 Redis 集成测试。生产 MySQL 的 DTM 资源管理账户同样需要该动态权限才能跨连接执行 prepared XID 对账，但不需要 root 或其他全局写权限。

## 运行

开发环境可直接使用仓库内配置：

```bash
cargo run -p roze-dtm-service
```

也可以通过 `ROZE_CONFIG_PATH` 指定配置文件。生产环境必须使用持久化存储、独立控制令牌、唯一 worker id，并限制允许调用的分支来源；完整要求见 [DTM 服务契约](docs/dtm.md)。

## 容器运行

仓库提供 PostgreSQL 生产拓扑示例：

```bash
ROZE_DTM_RELEASE_REVISION=$(git rev-parse HEAD) docker compose up --build
```

Redis standalone 拓扑示例使用独立 Compose 文件：

```bash
ROZE_DTM_RELEASE_REVISION=$(git rev-parse HEAD) docker compose -f compose.redis.yaml up --build
```

服务默认监听 HTTP `http://127.0.0.1:8090` 和 gRPC `127.0.0.1:36790`。Compose 中的令牌和数据库密码仅用于本地演示，部署前必须替换；Compose 还会强制要求 `ROZE_DTM_RELEASE_REVISION`。生产配置模板位于 `service/config.production.yaml`。

## 管理 Dashboard

浏览器访问 `http://127.0.0.1:8090/dashboard` 可打开事务管理 Dashboard。页面视觉与交互参考 Roze Admin 的 Workspace/Resource Page：Admin 侧栏、工作区指标卡、紧凑筛选区、状态标签、事务进度、XA 人工对账计数、分页表格、审计时间线，以及由服务端按事务状态声明的管理动作。页面通过 `GET /v1/dashboard` 获取脱敏快照，并可在二次确认后调用 `reset-retry` 或 `force-stop`；所有数据和动作都必须携带控制令牌。令牌只保存在当前页面内存，不写入 URL 或浏览器存储。`GET /v1/xa/reconciliation` 提供等待全局决策、phase-2 进行中和需要人工对账的 XA 安全摘要。

`GET /openapi.json` 公开完整 OpenAPI 3.1 合同。该文件由 `scripts/generate-openapi.py` 确定性生成，`scripts/validate-openapi.py` 会与当前 Router 做逐路径覆盖检查并递归验证 schema 引用及 operationId 唯一性。`scripts/validate-compatibility.py` 固定检查 dtm-labs 的 21 条 HTTP/JSON-RPC 路由、9 个 gRPC 方法、proto 字段号、SDK 入口、兼容 DTO 和 BSD 归属；`scripts/validate-migration-closure.py` 固定五种事务、五种存储、客户端、治理与收口文档证据。`scripts/validate-dashboard.py` 独立检查 Dashboard 元素 id、无外部资源、CSP、令牌不落浏览器存储，以及受控管理动作接线。

`GET /metrics` 与兼容别名 `/api/metrics` 在 Roze HTTP/RPC 指标之外公开事件驱动、固定内存、低基数的 DTM 指标：registry 可用性、按事务类型/状态/操作聚合的 `roze_dtm_transaction_transitions_total`、按类型/状态聚合的 `roze_dtm_branch_state_observations_total`，以及按类型聚合的 `roze_dtm_retry_scheduled_observations_total`。指标抓取不会扫描事务存储；标签不包含 GID、branch id、URL、payload、Header 或错误正文。

Dashboard 数据不包含分支 URL、请求载荷、Header、metadata、Workflow 二进制数据或依赖错误，只返回 GID、类型、状态、分支计数、尝试次数、时间字段和服务端计算的允许动作。终态事务不暴露管理动作；`reset-retry` 仅在存在失败/运行/补偿分支或 callback Workflow 时显示，业务延迟尚未到期的 Message 不会错误显示“立即重试”；`force-stop` 仅对非终态事务显示并带不可自动撤销警告。审计时间线是容量 200、最新优先的进程内环形历史，每次快照最多返回 50 条脱敏控制事件；它不替代持久化审计 sink，服务重启后会清空。`/dashboard` 只提供静态页面壳，受保护的数据与变更接口仍应仅暴露在管理网络或服务网格内。

## 存储后端

`application.dtm.store.kind` 支持：

- `memory`：仅限开发和测试。
- `sqlite`：单实例持久化。
- `postgres`：推荐的生产后端，支持多实例恢复租约。
- `mysql`：生产后端，支持多实例恢复租约。
- `redis`：复用 Roze standalone/Cluster 客户端，提供 revision + payload CAS、原子屏障、版本化 KV，以及基于 Redis 服务端时间和单调 epoch 的恢复租约写入 fencing。

所有关系型后端会在启动时幂等创建事务、分支屏障和恢复租约表。连接由 Roze `roze-sqlx` 管理，可通过 `max_connections` 设置连接池上限。Redis 配置使用 `redis_url` 或 `redis_cluster_urls`，并要求安全的 `redis_namespace`；`redis_operation_timeout_ms`（默认 5000）同时限制建连和每次命令。所有数据 key 共享显式 Cluster hash tag；普通 CAS/屏障脚本访问单 key，恢复写入脚本在同一槽内原子访问事务或屏障 Hash 与租约 Hash。

动态分支注册由存储层原子执行：内存后端使用写锁，PostgreSQL/MySQL 使用行锁，SQLite 和 Redis 使用带冲突重试的比较更新，避免多实例并发注册互相覆盖。事务载荷包含向后兼容的单调 `revision`；旧记录缺失该字段时从 0 开始，后续成功变更递增。Redis 普通状态推进拒绝 stale revision，恢复推进还要求 owner、epoch 和 Redis 服务端过期时间同时匹配。五种后端也提供版本化通用 KV 和 topic 订阅；Message 的 `topic://name` 分支会在提交时展开为订阅 URL 快照。

Saga 默认保持声明顺序；设置 `options.concurrent: true` 后，所有前置依赖已成功的分支按层并发执行。失败时仅补偿已经成功的分支，并按依赖反向分层并发补偿。未知、重复、自依赖或成环依赖会在持久化前拒绝。Message 设置同一选项后会并发投递所有未成功分支，单个失败不回滚其他成功结果，恢复时仅重试失败分支。dtm-labs HTTP/JSON-RPC 兼容入口接受 Message 请求顶层 `concurrent`，Saga 同时解析 `custom_data` 中的 `concurrent` 与零基 `orders`。

Message 支持原生 `options.delay_millis` 延迟投递；显式 Dispatch/Submit 决策会先持久化为 `Succeeding`，到达“创建时间 + 延迟”前不会调用分支，恢复 worker 按该时间唤醒。dtm-labs 兼容入口解析 `custom_data: {"delay": 10}`，其中上游单位为秒。

服务可通过 `application.dtm.alert_webhook_url` 启用 dtm-labs `AlertWebHook` 兼容告警。分支连续失败达到 `alert_retry_limit` 后会发送有界 JSON；分支 URL 查询串会被移除，Webhook URL 和依赖错误不会进入日志或 Dashboard，告警端故障也不会改变事务恢复结果。

## XA Resource Manager

`roze_dtm::xa` 提供 MySQL 与 PostgreSQL 的 Rust 原生 XA 资源管理器。它在同一物理数据库连接中依次执行 XA/本地事务启动、`roze_xa_barriers` 幂等屏障、业务闭包、DTM 分支注册与 Prepare；注册或业务执行失败时失败闭合并回滚。二阶段接口提供 Commit/Rollback、重复 phase-2 的 `AlreadyResolved` 结果以及 prepared transaction 恢复扫描。`DtmHttpClient::xa_global_transaction` 对全局 Prepare、业务闭包和最终 Commit/Rollback 决策进行封装。

应用必须先显式执行 `install_barrier_schema`，或将导出的屏障与 `roze_xa_decisions` DDL 纳入受控迁移。`resolve_heuristically` 会先幂等持久化有界 decision id、Commit/Rollback 决策和人工原因，再执行 phase-2 并记录 `applied`、`already_resolved` 或 `failed`；`reconcile` 对照数据库 prepared XID 与待处理决策，列出无决策 prepared 资源和找不到 prepared 资源的决策。人工原因不会由库写入日志或 Dashboard。PostgreSQL 还必须配置非零 `max_prepared_transactions`。协调器调用 phase-2 URL 时会保留业务查询参数，并覆盖追加可信的 `gid`、`trans_type=xa`、`branch_id` 和 `op`。

## Rust 客户端

核心 crate 提供 `roze_dtm::client::DtmHttpClient` 和 `roze_dtm::grpc_client::DtmGrpcClient`。HTTP 客户端支持提交五类事务、XA Prepare/Commit/Rollback 与资源分支注册、逐事务 timeout/retry/Header、callback Workflow、状态转换、事务查询、topic/KV 和兼容 GID；gRPC 客户端覆盖 `dtmgimp.Dtm` 全部方法、提供二进制 Workflow 进度助手并传播 Roze Context。两种客户端均提供 named callback Workflow 助手，按上游 `{name,data}` 合同编码任意二进制数据。`sdk/` 另提供浏览器及 Node.js 18+ 可用的 TypeScript/JavaScript `/v1` 客户端，以及覆盖 dtm-labs HTTP/JSON-RPC 协议的兼容客户端。恢复 worker 可主动查询 HTTP、JSON-RPC 或 gRPC `QueryPrepared` callback，并持久化有上限的重试调度。生产环境应配置 Bearer token 和 `allowed_branch_origins`。

## 上游同步

服务依赖的 Roze crates 固定到迁移时的上游提交。同步新版时，应一起审查核心库、服务、配置和契约，并更新 `Cargo.toml` 中所有 Roze 依赖的 `rev`。
