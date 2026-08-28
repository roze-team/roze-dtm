# Roze DTM 功能路线

本项目参考 `dtm-labs/dtm` 的功能覆盖面，但实现必须遵循 Roze 的 Rust 原生 HTTP、配置、日志、指标、生命周期和治理契约。功能语义可以对齐，代码不直接移植。

## 当前能力

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| TCC | 已支持 | Try/Confirm/Cancel、空回滚、防悬挂、幂等屏障与失败恢复 |
| Saga | 已支持 | 顺序执行、逆序补偿、重试与超时恢复 |
| 存储 | 已支持 | Memory、SQLite、PostgreSQL、MySQL |
| 高可用恢复 | 已支持 | 持久化租约保证同一时刻只有一个恢复 worker 推进事务 |
| 控制面 | 已支持 | Roze HTTP、数字响应信封、鉴权、健康检查、指标与审计事件 |
| XA | 待实现 | 需要资源管理器能力探测、超时与启发式结果契约 |
| 二阶段消息 | 待实现 | 优先复用 `roze-transaction` persistent outbox 与 `roze-mq` |
| Workflow DSL | 待实现 | 作为 Saga 之上的应用拥有编排层，不侵入核心状态机 |
| Redis 存储 | 待实现 | 需要原子脚本、租约 fencing 和真实故障测试 |
| SDK | 待实现 | 先提供 Rust client，再根据稳定 HTTP 契约生成其他语言 SDK |
| Dashboard | 待实现 | 基于只读查询、统计和审计事件构建，不暴露密钥或分支载荷 |

## 实施顺序

1. 加固现有 TCC/Saga：并发状态推进、数据库故障测试、恢复 soak 与指标告警。
2. 接入 Roze Outbox/Inbox 和二阶段消息，形成数据库写入到可靠事件发布闭环。
3. 提供 Workflow DSL 与 Rust SDK，保持控制面协议可版本化。
4. 在有真实业务需求和数据库支持矩阵后增加 XA。
5. 增加 Redis 后端和管理 Dashboard，并补跨节点故障证据。

任何标记为“已支持”的生产能力都必须同时具备契约文档、自动测试和可复现的运行入口。
