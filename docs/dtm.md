# Roze DTM 服务契约

Roze DTM 是内置的分布式事务协调服务。默认模式为 TCC，Saga 用于长流程和最终一致场景。协调器持久化全局事务和分支状态，并通过分支屏障保证重复确认、取消和补偿不会重复执行。

## 部署配置

生产环境必须使用持久化存储、独立控制令牌和部署实例唯一的 worker id：

```yaml
rest:
  addr: 0.0.0.0:8090
  register: false

application:
  dtm:
    control_token: env://ROZE_DTM_CONTROL_TOKEN
    recover_interval_ms: 1000
    recovery_lease_ttl_ms: 5000
    worker_id: env://ROZE_DTM_WORKER_ID
    allowed_branch_origins:
      - http://inventory:8080
      - http://account:8080
    store:
      kind: postgres
      database_url: env://ROZE_DTM_DATABASE_URL
      max_connections: 20
    max_attempts: 5
    retry_backoff_ms: 1000
    max_retry_backoff_ms: 30000
    branch_call_timeout_ms: 5000
    transaction_timeout_ms: 60000
```

`allowed_branch_origins` 是所有环境的必填项，只接受精确的 HTTP(S) Origin，例如
`http://inventory:8080`。不得填写路径、通配符或用户凭据。提交事务时会校验
Action、Confirm、Cancel、Compensate 和 callback `QueryPrepared` URL；实际调用会再次
校验，并禁止 HTTP 重定向，避免通过分支地址或重定向访问未授权的内部端点。gRPC
callback 使用对应的 `http://` 或 `https://` origin 加入同一白名单。

生产环境禁止 `store.kind: memory`。持久化后端支持 `sqlite`、`postgres`、`mysql` 和 `redis`；数据库 URL scheme 必须与 kind 一致，`max_connections` 范围为 1–1000。Redis standalone 使用 `redis_url`，Cluster 使用一个或多个 `redis_cluster_urls`，两者仅接受 `redis://` 或 `rediss://`；Cluster 配置优先。`redis_namespace` 只允许 1–64 个 ASCII 字母、数字、`-`、`_`，用于构造显式 hash tag，禁止部署间共享命名空间。`redis_operation_timeout_ms` 范围为 1–120000，默认 5000，同时限制建连和每次 Redis 命令。`control_token` 至少 32 字节；`worker_id` 必须在同一部署中唯一。恢复租约时长至少是恢复周期的两倍。配置文件只保存 `env://` 引用，不保存明文密钥。

Redis 生产配置示例见 `service/config.redis.production.yaml`。Redis 后端将事务、KV、屏障和租约分为四个 Hash，全部 key 共享显式 Cluster hash tag。租约脚本使用 Redis 服务端时间；首次获取或过期后重新获取会递增 epoch，同一 owner 在有效期内续租复用 epoch。恢复 worker 通过 fenced store 推进，事务 revision/payload CAS、Workflow 变更、屏障创建和释放均在同一 Lua 调用中校验 owner、epoch 与过期时间。事务扫描使用有界分批 `HSCAN`。当前仍需在禁编译窗口结束后执行真实 standalone/Cluster、过期接管和长耗时恢复故障测试，才能声明完整的多节点故障隔离证据。

除健康、启动、就绪和指标接口外，所有 `/v1/**` 请求必须携带：

```http
Authorization: Bearer <ROZE_DTM_CONTROL_TOKEN>
```

建议仅在服务网格或管理网络内暴露 DTM，并在入口层同时启用 TLS、访问控制和限流。

## HTTP API

- `POST /v1/tcc`：提交 TCC 事务。
- `POST /v1/tcc/{gid}/prepare`：执行 Try 分支。
- `POST /v1/tcc/{gid}/confirm`：执行 Confirm 分支。
- `POST /v1/tcc/{gid}/cancel`：执行 Cancel 分支。
- `POST /v1/saga`：提交 Saga 事务。
- `POST /v1/saga/{gid}/start`：执行 Saga 正向分支。
- `POST /v1/saga/{gid}/abort`：执行反向补偿。
- `POST /v1/workflows`、`/v1/workflows/{gid}/start|abort`：提交、执行或补偿 Workflow。
- `POST /v1/messages`、`/v1/messages/{gid}/prepare|dispatch|abort`：二阶段消息生命周期。
- `POST /v1/xa`、`/v1/xa/{gid}/prepare|commit|rollback`：XA 协调生命周期。
- `GET /v1/xa/reconciliation`：返回脱敏的 XA 等待决策、phase-2 进行中和人工对账清单。
- `GET /v1/transactions`：过滤并分页查询事务。
- `GET /v1/transactions/{gid}`：查询单个事务。
- `POST /v1/transactions/{gid}/recover`：强制推进一个可安全重放的状态。
- `POST /v1/transactions/{gid}/force-stop`：停止非终态事务的自动处理。
- `POST /v1/transactions/{gid}/reset-retry`：立即重新调度失败或补偿中的分支。
- `POST /v1/recover`：触发一次全局恢复扫描。
- `GET /v1/stats`：按类型和状态统计事务。
- `GET /v1/dashboard`：返回只读、分页且脱敏的 Dashboard 快照。
- `GET /dashboard`：返回参考 Roze Admin Workspace/Resource Page 视觉的静态管理页面；不包含受保护数据。
- `GET /healthz`、`GET /startupz`、`GET /readyz`：运行状态探针。
- `GET /metrics`：Prometheus 指标。

所有业务响应使用 Roze 数字信封：成功 `code: 0`，错误 code 与 HTTP 状态一致。查询参数支持 `gid`、`kind`、`status`、`offset`、`limit`；默认 limit 为 50，最大为 200。

Dashboard API 使用同一组过滤和分页边界，但只返回管理摘要字段。它明确排除分支 Action/Confirm/Cancel/Compensate URL、payload、Header、metadata、Workflow progress data、回滚原因和依赖错误。响应还包含最近 50 条控制审计事件，底层进程内历史固定容量为 200；事件仅含 sequence、时间、稳定事件名、结果、可选 GID 和事务状态，不含 token、错误正文或业务数据。该环形历史用于即时运维态势，不能替代非丢弃持久审计 sink。页面不会持久化控制令牌，也不会加载第三方脚本、字体或图片；部署方仍需在入口层为 `/dashboard` 与 `/v1/dashboard` 配置 TLS、访问来源限制和常规安全响应头。

## TCC 请求

```json
{
  "gid": "order-1001",
  "timeout_millis": 60000,
  "metadata": { "tenant": "tenant-1" },
  "options": {
    "request_timeout_millis": 3000,
    "retry_interval_millis": 1000,
    "retry_limit": 3,
    "branch_headers": { "x-tenant": "tenant-1" }
  },
  "branches": [
    {
      "id": "inventory",
      "kind": "TccTry",
      "action": "http://inventory/try",
      "confirm": "http://inventory/confirm",
      "cancel": "http://inventory/cancel",
      "payload": { "sku": "A", "count": 1 }
    }
  ]
}
```

`kind` 可省略；TCC 端点只接受 `TccTry` 分支。每个分支必须提供合法的 HTTP(S) Try、Confirm 和 Cancel 地址。

原生 HTTP API 的逐事务选项使用毫秒。`request_timeout_millis` 覆盖该事务的单次 HTTP 分支调用超时；`retry_interval_millis` 是有上限指数退避的初始间隔；`retry_limit` 表示额外重试次数，Saga 在耗尽后才进入补偿，TCC Try 使用同一总尝试次数边界；`branch_headers` 会在 URL 白名单再次校验后发送给分支。Header 最多 32 个，名称和值分别不超过 64 和 1024 字节，并必须是合法 HTTP Header。

## Saga 请求

```json
{
  "gid": "transfer-1001",
  "branches": [
    {
      "id": "out",
      "kind": "SagaAction",
      "action": "http://account/trans-out",
      "compensate": "http://account/trans-out-compensate",
      "payload": { "amount": 30 }
    }
  ]
}
```

Saga 端点只接受 `SagaAction` 分支，并要求补偿地址。

## XA 资源管理器

`roze_dtm::xa::{MySqlXaResourceManager, PostgresXaResourceManager}` 面向业务数据库提供本地 XA 边界。全局事务先通过 `DtmHttpClient::prepare_xa` 创建；每个业务分支调用 `prepare_branch`，在一个独占连接内执行屏障、业务 SQL、`registerXaBranch` 和 Prepare；全局业务成功后调用 `commit_xa`，失败则调用 `rollback_xa`。业务 phase-2 路由根据协调器追加的 `gid`、`branch_id` 和 `op` 构造 `XaBranchDescriptor`，再调用资源管理器的 `resolve`。

MySQL 使用 `XA START/END/PREPARE/COMMIT/ROLLBACK`，PostgreSQL 使用 `BEGIN/PREPARE TRANSACTION/COMMIT PREPARED/ROLLBACK PREPARED`。XID 只允许有界安全 ASCII，避免不能绑定参数的 XA 控制语句发生 SQL 注入；MySQL 的组合资源 ID 额外限制为 64 字节。`recover_prepared` 返回当前数据库仍处于 prepared 状态的资源 ID；重复 Commit/Rollback 映射为 `AlreadyResolved`。

人工启发式处置必须通过资源管理器的 `resolve_heuristically`，使用 1–64 字节安全 decision id 和 1–512 字节原因。资源管理器先在业务数据库的 `roze_xa_decisions` 中幂等写入 `requested`，再执行 Commit/Rollback，最后落为 `applied`、`already_resolved` 或 `failed`；同一 decision id 的不同资源、决策或原因会被拒绝。`reconcile` 将该记录与数据库真实 prepared XID 比较。原因属于受保护运维数据，不进入日志、指标或 Dashboard。`roze_xa_barriers` 与 `roze_xa_decisions` 必须通过应用迁移或显式 schema 安装创建，PostgreSQL 必须配置非零 `max_prepared_transactions`。

## 输入边界

- gid 和分支 id 为 1–128 字节；同一事务内分支 id 不可重复。
- 每个事务包含 1–100 个分支。
- 分支 URL 最长 2048 字节，只接受 `http://` 或 `https://`。
- metadata 最多 32 项；键最长 64 字节，值最长 256 字节。
- `timeout_millis` 范围为 1 秒至 24 小时。
- 逐事务重试和请求超时范围为 1 毫秒至 24 小时，重试次数不超过 10000。
- 原生 JSON 提取器默认限制请求体为 2 MiB。

## 状态恢复与屏障

恢复 worker 随 HTTP 服务一起由 `ServiceGroup` 管理，统一响应关闭信号。每轮恢复先竞争持久化租约，同一时刻只有一个实例推进事务。安全恢复路径为：

- TCC：`Submitted -> Prepared` 后等待显式 Submit；提交决定先持久化为 `Succeeding`，再推进 Confirm 至 `Succeeded`，超时/Aborting 后 Cancel。
- XA 与二阶段消息同样不会由恢复 worker 将 `Prepared` 猜测为提交决定；只有显式 Submit/Commit 持久化 `Succeeding` 后才执行 phase-2。
- Saga：`Submitted -> Succeeded`，或超时/Aborting 后补偿。
- 终态事务原样返回。
- `Trying`、`Succeeding` 等无法安全整体重放的状态拒绝手工强推，避免重复调用分支。

分支屏障以 `gid + branch_id + operation` 去重；Cancel 早于 Try 时按空回滚处理。失败分支记录下次重试时间并使用有上限的指数退避。

## 日志与审计

提交、状态推进、手工恢复和自动恢复均使用稳定事件名。控制面成功或失败事件通过 `roze.audit` 目标输出；配置独立审计 sink 时写入非丢弃 JSONL 文件，否则进入普通日志 sink。Dashboard 同时维护有界、重启即失的脱敏事件视图，便于即时排障但不作为合规审计记录。日志和 Dashboard 均不得包含控制令牌、请求载荷、分支响应体或原始依赖错误。

## 当前边界

已支持内存、SQLite、PostgreSQL、MySQL 与 Redis 存储，Saga、TCC、静态及 callback Workflow、二阶段消息和 XA 协调状态机，MySQL/PostgreSQL XA 资源管理器、资源侧启发式决策持久化与 prepared 对账，HTTP 分支调用、超时、重试、分支屏障、持久化恢复租约、Redis 恢复写入 fencing、版本化 KV、topic 订阅、自动恢复 worker、callback Workflow 的 HTTP/JSON-RPC/gRPC QueryPrepared 主动恢复、原生 Roze HTTP/gRPC 控制面、Roze Admin 风格脱敏 Dashboard、有界审计时间线和审计事件，以及 TypeScript/JavaScript 原生与 dtm-labs HTTP/JSON-RPC 兼容 SDK。生成式 OpenAPI 与 Roze Admin 内嵌模块仍属于后续扩展；XA、Redis、gRPC 适配器及 callback 恢复仍需完成编译、真实依赖、跨语言互操作和故障注入验证。
