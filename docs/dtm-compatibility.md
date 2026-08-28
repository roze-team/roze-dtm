# dtm-labs/dtm HTTP 兼容层

兼容基线为 `dtm-labs/dtm` 提交 `18146ee53bafbf094b1a5f12ca7e8a29bdb57edd`。兼容接口位于 `/api/dtmsvr/**`，与 Roze 原生 `/v1/**` 控制面并存；两者共享事务状态、持久化、屏障、恢复和审计。

## 已实现端点

| DTM 端点 | Roze 行为 |
| --- | --- |
| `GET /api/dtmsvr/version` | 返回包版本和配置的 40 位部署 Git revision；开发环境未配置时 revision 为 `null` |
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
| `GET /api/dtmsvr/resetCronTime` | 按 `timeout` 秒阈值批量重置未来恢复时间，并返回准确的 `has_remaining` |
| `GET /api/dtmsvr/query` | 查询全局事务与分支 |
| `GET /api/dtmsvr/all` | 按 GID、类型、状态及创建时间范围分页查询；`position`/`next_position` 使用字符串游标 |
| `GET /api/dtmsvr/subscribe` | 创建 topic 或添加 URL 订阅者，使用版本化 CAS 更新 |
| `GET /api/dtmsvr/unsubscribe` | 从 topic 删除 URL 订阅者 |
| `DELETE /api/dtmsvr/topic/{topic_name}` | 删除 topic |
| `GET /api/dtmsvr/scanKV` | 按分类分页扫描通用 KV |
| `GET /api/dtmsvr/queryKV` | 按分类和 key 查询通用 KV |
| `GET /api/metrics` | 上游路径兼容的 Prometheus 文本指标别名 |
| `POST /api/json-rpc` | JSON-RPC 2.0：newGid、prepare、submit、abort、registerBranch |

兼容响应保留 `dtm_result: SUCCESS/FAILURE`。启用 `control_token` 时，兼容端点和 `/v1/**` 一样要求 Bearer token。

`all` 接受上游字段 `createTimeStart`、`createTimeEnd`（Unix 毫秒时间戳，边界包含），并以创建时间倒序、GID 倒序稳定分页。`position` 是上一页末项的唯一 GID，不应解析或自行构造；空字符串表示首页或没有下一页，无效游标及倒置时间范围会返回 `FAILURE`。`resetCronTime` 的 `timeout` 单位为秒，缺省值为 105 秒；它只重置恢复时间晚于“当前时间 + timeout”的事务，批次之外仍有匹配项时 `has_remaining=true`。

并发 Saga 兼容上游 `custom_data`：`{"concurrent":true,"orders":{"2":[0,1]}}` 表示零基分支 2 必须在分支 0 和 1 成功后执行。Roze 将其转换为持久化分支 id 依赖图，按依赖就绪层并发执行，并按逆依赖层补偿。分支和依赖索引必须在范围内，且依赖必须位于当前分支之前；非法或成环图会失败闭合。

并发 Message 兼容上游事务请求的顶层 `concurrent: true` 字段。Roze 在投递前统一持久化分支执行状态，并发调用全部未成功分支；部分失败时保留已成功分支的屏障和状态，后续恢复只重试失败分支。上游固定版本的 gRPC `DtmTransOptions` 不包含该字段，4 号字段是已废弃的透传 Header，因此 Roze 保留 `reserved 4`，不创建非上游兼容的 gRPC 扩展。

延迟 Message 兼容上游 `custom_data`：`{"delay":10}` 表示从事务创建时间起延迟 10 秒投递。Roze 在 Submit 时先持久化明确的 `Succeeding` 决策，到期前不调用分支；恢复 worker 使用持久化创建时间计算投递点。原生 `/v1/messages` 使用毫秒字段 `options.delay_millis`。

## TCC 调用顺序

1. 客户端调用 `prepare` 创建空的 Prepared TCC。
2. 客户端执行 Try，并调用 `registerBranch` 注册 Confirm/Cancel。
3. `submit` 执行 Confirm；业务失败时 `abort` 执行 Cancel。

注册的 TCC 分支会标记 Try 已完成，Roze 不会重复执行旧客户端已经执行的 Try。

## 有意差异

- Roze 不接受未列入 `allowed_branch_origins` 的分支 URL，也不跟随重定向。
- 所有控制操作都有输入上限、统一审计和恢复租约。
- `proto/dtmgimp.proto` 固定与上游兼容的 gRPC service、message 和字段号；Roze 服务端和 Rust 客户端覆盖全部 9 个 RPC，并共享 HTTP 控制面的存储、鉴权与生命周期。发布前仍需完成禁编译窗口之后的互操作测试。
- gRPC 事务扩展字段会持久化；逐事务重试间隔、请求超时、Saga 重试上限和分支 Header 已接入核心执行器。`WaitResult=false` 的 Submit/Abort 会先持久化明确的 `Succeeding`/`Aborting` 决策，再由带租约的恢复 worker 异步推进；`Prepared` 不会被后台线程猜测为提交，`WaitResult=true` 同步等待。callback Workflow 已支持复合进度键、二进制结果、`ReqExtra` 终态和恢复 worker 主动调用 HTTP/JSON-RPC/gRPC `QueryPrepared`；重试调度在五种存储中原子持久化。真实跨语言互操作和故障注入仍需在禁编译窗口后验证。
- Message 分支支持 `topic://name`，提交时从持久化订阅快照展开为一个或多个 HTTP 分支；订阅变化不会改写已经提交的事务。兼容层同时支持上游秒单位 `custom_data.delay`，Web SDK 提供 `messageDelayCustomData` 避免手写单位换算。
- 上游 `AlertWebHook` 对应 `application.dtm.alert_webhook_url`。达到 `alert_retry_limit` 后发送 `gid/status/branch/error/retry_count`；Roze 会移除 `branch` 查询串并禁止在日志、指标和 Dashboard 暴露 Webhook URL 或原始依赖错误。告警投递失败不影响事务状态机。
- Redis 存储复用 Roze standalone/Cluster 拓扑，事务使用单调 revision 与 payload CAS；恢复租约使用服务端时间和单调 epoch，恢复事务、Workflow 与屏障写入在同槽 Lua 中原子校验 owner/epoch/expiry。建连与命令统一受可配置超时限制；真实 Cluster、断连恢复、过期接管和长耗时 fencing 证据尚待补齐。
- JSON-RPC 始终返回 HTTP 200，并通过标准 `error.code` 表示协议或操作失败；语法错误返回 `-32700`，无效请求返回 `-32600`。
- `forceStop` 是不可自动恢复的管理操作，只应在确认人工介入后使用。
- XA phase-2 调用会保留业务查询参数，并覆盖追加可信的 `gid`、`trans_type=xa`、`branch_id` 与 `op`。Rust 客户端提供 XA Prepare/Commit/Rollback、分支注册，以及 MySQL/PostgreSQL 同连接本地 SQL、屏障、Prepare、幂等 phase-2、intent-first 启发式决策记录和 prepared transaction 双向对账；仍需使用固定上游客户端和真实数据库完成互操作、崩溃恢复及启发式决策验证。

第三方归属见 `THIRD_PARTY_NOTICES.md`。

## 并发注册语义

`registerBranch`、`registerTccBranch`、`registerXaBranch` 和 JSON-RPC `registerBranch` 共用存储层原子注册操作。相同 branch id 与相同定义重复提交时幂等成功；相同 id 但定义不同会拒绝。PostgreSQL/MySQL 在事务内锁定全局事务行，SQLite 使用 payload 比较更新与有限重试，避免并发请求覆盖已经落库的分支。
