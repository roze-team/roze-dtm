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
Action、Confirm、Cancel 和 Compensate URL；实际分支调用会再次校验，并禁止
HTTP 重定向，避免通过分支地址或重定向访问未授权的内部端点。

生产环境禁止 `store.kind: memory`。持久化后端支持 `sqlite`、`postgres` 和 `mysql`；数据库 URL scheme 必须与 kind 一致，`max_connections` 范围为 1–1000。`control_token` 至少 32 字节；`worker_id` 必须在同一部署中唯一。恢复租约时长至少是恢复周期的两倍。配置文件只保存 `env://` 引用，不保存明文密钥。

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
- `GET /v1/transactions`：过滤并分页查询事务。
- `GET /v1/transactions/{gid}`：查询单个事务。
- `POST /v1/transactions/{gid}/recover`：强制推进一个可安全重放的状态。
- `POST /v1/transactions/{gid}/force-stop`：停止非终态事务的自动处理。
- `POST /v1/transactions/{gid}/reset-retry`：立即重新调度失败或补偿中的分支。
- `POST /v1/recover`：触发一次全局恢复扫描。
- `GET /v1/stats`：按类型和状态统计事务。
- `GET /healthz`、`GET /startupz`、`GET /readyz`：运行状态探针。
- `GET /metrics`：Prometheus 指标。

所有业务响应使用 Roze 数字信封：成功 `code: 0`，错误 code 与 HTTP 状态一致。查询参数支持 `gid`、`kind`、`status`、`offset`、`limit`；默认 limit 为 50，最大为 200。

## TCC 请求

```json
{
  "gid": "order-1001",
  "timeout_millis": 60000,
  "metadata": { "tenant": "tenant-1" },
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

## 输入边界

- gid 和分支 id 为 1–128 字节；同一事务内分支 id 不可重复。
- 每个事务包含 1–100 个分支。
- 分支 URL 最长 2048 字节，只接受 `http://` 或 `https://`。
- metadata 最多 32 项；键最长 64 字节，值最长 256 字节。
- `timeout_millis` 范围为 1 秒至 24 小时。
- 原生 JSON 提取器默认限制请求体为 2 MiB。

## 状态恢复与屏障

恢复 worker 随 HTTP 服务一起由 `ServiceGroup` 管理，统一响应关闭信号。每轮恢复先竞争持久化租约，同一时刻只有一个实例推进事务。安全恢复路径为：

- TCC：`Submitted -> Prepared -> Succeeded`，或超时/Aborting 后 Cancel。
- Saga：`Submitted -> Succeeded`，或超时/Aborting 后补偿。
- 终态事务原样返回。
- `Trying`、`Succeeding` 等无法安全整体重放的状态拒绝手工强推，避免重复调用分支。

分支屏障以 `gid + branch_id + operation` 去重；Cancel 早于 Try 时按空回滚处理。失败分支记录下次重试时间并使用有上限的指数退避。

## 日志与审计

提交、状态推进、手工恢复和自动恢复均使用稳定事件名。控制面成功或失败事件通过 `roze.audit` 目标输出；配置独立审计 sink 时写入非丢弃 JSONL 文件，否则进入普通日志 sink。日志不得包含控制令牌、请求载荷、分支响应体或原始依赖错误。

## 当前边界

已支持内存、SQLite、PostgreSQL 与 MySQL 存储、TCC/Saga 状态机、HTTP 分支调用、超时、重试、分支屏障、关系型数据库恢复租约、自动恢复 worker、原生 Roze HTTP 控制面和审计事件。Redis 后端、XA、二阶段消息、Workflow DSL 和管理 Dashboard 仍属于后续扩展。
