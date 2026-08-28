# dtm-labs/dtm HTTP 兼容层

兼容基线为 `dtm-labs/dtm` 提交 `18146ee53bafbf094b1a5f12ca7e8a29bdb57edd`。兼容接口位于 `/api/dtmsvr/**`，与 Roze 原生 `/v1/**` 控制面并存；两者共享事务状态、持久化、屏障、恢复和审计。

## 已实现端点

| DTM 端点 | Roze 行为 |
| --- | --- |
| `GET /api/dtmsvr/version` | 返回服务版本 |
| `GET /api/dtmsvr/newGid` | 生成进程内单调唯一 GID |
| `POST /api/dtmsvr/prepare` | 创建 Prepared TCC/XA/Message 事务 |
| `POST /api/dtmsvr/submit` | 提交并推进 Saga/TCC/XA/Message/Workflow |
| `POST /api/dtmsvr/abort` | 取消或补偿事务 |
| `POST /api/dtmsvr/registerBranch` | 注册 TCC/XA 二阶段分支 |
| `POST /api/dtmsvr/registerTccBranch` | `registerBranch` 别名 |
| `POST /api/dtmsvr/registerXaBranch` | `registerBranch` 别名 |
| `POST /api/dtmsvr/prepareWorkflow` | 幂等创建 Prepared Workflow 并返回 transaction/progresses |
| `POST /api/dtmsvr/forceStop` | 将非终态事务永久标记为 Failed |
| `POST /api/dtmsvr/resetNextCronTime` | 让一个事务立即进入恢复调度 |
| `GET /api/dtmsvr/resetCronTime` | 批量重置非终态事务的恢复时间 |
| `GET /api/dtmsvr/query` | 查询全局事务与分支 |
| `GET /api/dtmsvr/all` | 按 GID、类型、状态分页查询 |
| `GET /api/dtmsvr/subscribe` | 创建 topic 或添加 URL 订阅者，使用版本化 CAS 更新 |
| `GET /api/dtmsvr/unsubscribe` | 从 topic 删除 URL 订阅者 |
| `DELETE /api/dtmsvr/topic/{topic_name}` | 删除 topic |
| `GET /api/dtmsvr/scanKV` | 按分类分页扫描通用 KV |
| `GET /api/dtmsvr/queryKV` | 按分类和 key 查询通用 KV |
| `GET /api/metrics` | 上游路径兼容的 Prometheus 文本指标别名 |
| `POST /api/json-rpc` | JSON-RPC 2.0：newGid、prepare、submit、abort、registerBranch |

兼容响应保留 `dtm_result: SUCCESS/FAILURE`。启用 `control_token` 时，兼容端点和 `/v1/**` 一样要求 Bearer token。

## TCC 调用顺序

1. 客户端调用 `prepare` 创建空的 Prepared TCC。
2. 客户端执行 Try，并调用 `registerBranch` 注册 Confirm/Cancel。
3. `submit` 执行 Confirm；业务失败时 `abort` 执行 Cancel。

注册的 TCC 分支会标记 Try 已完成，Roze 不会重复执行旧客户端已经执行的 Try。

## 有意差异

- Roze 不接受未列入 `allowed_branch_origins` 的分支 URL，也不跟随重定向。
- 所有控制操作都有输入上限、统一审计和恢复租约。
- `proto/dtmgimp.proto` 固定与上游兼容的 gRPC service、message 和字段号；Roze 服务端和 Rust 客户端覆盖全部 9 个 RPC，并共享 HTTP 控制面的存储、鉴权与生命周期。发布前仍需完成禁编译窗口之后的互操作测试。
- gRPC 事务扩展字段会持久化；逐事务重试间隔、请求超时、Saga 重试上限和分支 Header 已接入核心执行器。`WaitResult=false` 的 Submit/Abort 由带租约的恢复 worker 异步推进，`WaitResult=true` 同步等待。callback Workflow 已支持复合进度键、二进制结果和 `ReqExtra` 终态；恢复 worker 主动调用 `QueryPrepared` callback 尚未完成，因此故障恢复仍非完全等价。
- Message 分支支持 `topic://name`，提交时从持久化订阅快照展开为一个或多个 HTTP 分支；订阅变化不会改写已经提交的事务。
- JSON-RPC 始终返回 HTTP 200，并通过标准 `error.code` 表示协议或操作失败；语法错误返回 `-32700`，无效请求返回 `-32600`。
- `forceStop` 是不可自动恢复的管理操作，只应在确认人工介入后使用。
- XA phase-2 URL 参数和资源管理器本地 SQL 助手尚需客户端侧适配验证。

第三方归属见 `THIRD_PARTY_NOTICES.md`。

## 并发注册语义

`registerBranch`、`registerTccBranch`、`registerXaBranch` 和 JSON-RPC `registerBranch` 共用存储层原子注册操作。相同 branch id 与相同定义重复提交时幂等成功；相同 id 但定义不同会拒绝。PostgreSQL/MySQL 在事务内锁定全局事务行，SQLite 使用 payload 比较更新与有限重试，避免并发请求覆盖已经落库的分支。
