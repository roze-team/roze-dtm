# DTM gRPC 兼容服务

gRPC 线协议以 `proto/dtmgimp.proto` 为唯一合同，package 与 service 保持为 `dtmgimp.Dtm`。服务端与 HTTP 控制面运行在同一进程、共享同一个 `Dtm`、存储连接池、分支 URL 策略、控制令牌、恢复 worker 和 `ServiceGroup` 关闭信号。

## 方法

- `NewGid`
- `Submit`
- `Prepare`
- `Abort`
- `RegisterBranch`
- `PrepareWorkflow`
- `Subscribe`
- `Unsubscribe`
- `DeleteTopic`

服务同时注册标准 `grpc.health.v1.Health`。启动后健康状态由 Roze `HealthRegistry` 驱动；存储探测失败或服务进入 draining 时报告 `NOT_SERVING`。

## 鉴权与上下文

配置 `application.dtm.control_token` 后，每个 DTM gRPC 方法都要求 metadata：

```text
authorization: Bearer <token>
```

请求 ID、trace ID、locale、timeout 和 retry budget 使用 `roze-rpc` metadata 合同恢复，并在成功响应或错误状态中回传。对外错误通过 `RozeError` 和 `status_from_error` 转换，不暴露数据库、分支调用或解析器内部错误。

## 配置

```yaml
rpc:
  addr: 0.0.0.0:36790
```

未配置 `rpc` 时只启动 HTTP 控制面。生产镜像和 Compose 示例同时暴露 `8090` 与 `36790`。

## 客户端

`roze_dtm::grpc_client::DtmGrpcClient` 包装生成的 tonic client，接收 `roze_context::Context`，通过 Roze RPC 客户端边界自动设置标准 gRPC timeout 并传播 Roze metadata。原始 protobuf DTO 和生成 client 也可从 `roze_dtm::pb::dtmgimp` 访问。

普通事务的二进制 payload 会优先按 JSON 解码；无法解码时保留为 JSON 字节数组。当前 HTTP 分支调用器发送 JSON，因此需要原始 protobuf 二进制业务载荷的普通分支应在业务适配层显式编码。callback Workflow 进度使用独立的 `WorkflowProgress`，gRPC `BusiPayload/BinData` 会原样往返，不经过 JSON 转换。

`CustomedData`、`QueryPrepared` 和 `ReqExtra` 会保存在事务 metadata 中，避免跨协议转换时丢失。`RetryInterval`、`RequestTimeout` 按上游合同以秒接收并转换为毫秒，`RetryLimit` 和 `BranchHeaders` 进入持久化 `TransactionOptions`：HTTP 分支调用会应用逐事务 timeout 和 Header，失败恢复使用逐事务初始退避，Saga 在重试次数耗尽后进入补偿。`WaitResult=false` 时，Submit/Abort 在完成状态校验和持久化调度后立即返回，实际分支调用由带租约的恢复 worker 推进；`WaitResult=true` 保持同步等待。Prepare 始终同步完成持久化阶段。

## Callback Workflow

`PrepareWorkflow` 创建或查询 Prepared Workflow，并返回已经持久化的进度。`RegisterBranch` 在 Workflow 模式下以 `(gid, branch_id, op)` 为复合身份键保存最终 `succeed/failed` 状态和原始二进制结果；完全相同的重复写入幂等成功，冲突写入被拒绝。Workflow `Submit` 读取 `ReqExtra.status`、`rollback_reason` 和 base64 `result`，以存储后端原子操作写入全局 `succeed/failed` 终态。内存使用写锁，SQLite 使用有限 CAS，PostgreSQL/MySQL 使用 `FOR UPDATE` 行锁。

HTTP 客户端进度数据受 JSON 字符串限制，必须是 UTF-8；任意二进制进度应使用 gRPC 客户端。每项数据和所有进度总量上限均为 2 MiB，最多 1000 个复合进度。callback Workflow 不继承服务端默认事务超时；调用方显式设置 `TimeoutToFail` 时，超时会稳定落为 `failed`。恢复 worker 主动调用 `QueryPrepared` 的 HTTP/gRPC callback 仍待补齐，因此当前 callback Workflow 需要客户端重入驱动恢复，尚不能视为完整的上游故障恢复等价。
