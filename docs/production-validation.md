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
- Cluster MOVED/ASK、节点重启、主从切换、断连恢复及同槽脚本行为。

当前状态：测试入口和 ignored integration tests 已加入，但受当前禁编译要求约束，本提交没有执行 Cargo 测试，也没有形成真实 Redis 或长时间运行证据。写入 fencing 尚未完成，因此 Redis 后端不能据此宣称完成多节点生产验证。

## 协议互操作

HTTP、JSON-RPC 和 gRPC 兼容协议需要使用固定上游客户端版本执行 TCC、Saga、XA、Message、Workflow 和 callback Workflow 端到端矩阵。callback 还必须覆盖 HTTP `409/425`、JSON-RPC `-32901/-32902`、gRPC `ABORTED/FAILED_PRECONDITION`、TLS、超时和客户端在 callback 内提交终态的并发竞争。

当前状态：协议合同和源码测试已存在；真实 Go/Rust 跨语言互操作仍未执行。

## Dashboard

`GET /dashboard` 仅提供静态页面，`GET /v1/dashboard` 必须验证 Bearer 控制令牌。生产验收需要覆盖未授权拒绝、筛选与分页边界、空数据、各事务状态、窄屏和系统暗色模式，并检查浏览器网络响应中不存在分支 URL、payload、Header、metadata、Workflow 数据或依赖错误。控制令牌不得进入 URL、localStorage、sessionStorage、日志或表格 DOM。

当前状态：页面和脱敏快照源码测试已加入，视觉与信息层级已按 `roze-admin` 的 Workspace/Resource Page 规范对齐；已用本地静态 HTTP 服务完成浏览器渲染和控制台错误检查。受当前禁编译要求约束，尚未执行真实 Roze 服务、受保护 API 或端到端验证，因此状态保持 `inconclusive`。

## 生产证据要求

生产证据必须绑定确切 Git revision，记录依赖拓扑、命令、持续时间、工作负载、错误预算、故障注入时间线、资源趋势和最终判定。未执行、被中断或缺少工件的运行一律标记为 `inconclusive`，不得根据源码检查补写通过结论。
