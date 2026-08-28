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

二进制 payload 会优先按 JSON 解码；无法解码时保留为 JSON 字节数组。当前 HTTP 分支调用器发送 JSON，因此需要原始 protobuf 二进制业务载荷的调用方应在业务适配层显式编码。

`CustomedData`、`QueryPrepared`、`ReqExtra`、`BranchHeaders` 以及非零重试/请求选项会保存在事务 metadata 中，避免跨协议转换时丢失。当前核心执行器仍使用服务级重试策略，尚未把逐事务 `RetryInterval`、`RetryLimit`、`RequestTimeout` 和自定义分支 Header 应用到实际分支调用；回调式 Workflow 的动态进度注册也仍在路线图中。
