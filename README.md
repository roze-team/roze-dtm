# Roze DTM

Roze 的独立分布式事务协调器，默认提供 TCC，并支持 Saga 工作流。代码从 `roze-team/roze` 的 `217274a134068f174cbe4a266a011bf719e15d0d` 提取，保留原有状态机、持久化、分支屏障、恢复租约和原生 Roze HTTP 控制面。

## 项目结构

- `src/lib.rs`：DTM 核心库，包含 TCC/Saga 状态机、内存、SQLite、PostgreSQL、MySQL 存储、HTTP 分支调用和恢复逻辑。
- `service/`：独立控制面服务。
- `proto/dtmgimp.proto`：与 dtm-labs/dtm 保持字段号兼容的 gRPC 协议合同和生成边界。
- `docs/dtm-grpc.md`：Roze gRPC 生命周期、鉴权、健康检查和客户端契约。
- `service/config.yaml`：开发环境示例配置，也是服务的默认配置。
- `docs/dtm.md`：API、部署与安全契约。
- `docs/roadmap.md`：参考 dtm-labs/dtm 的能力矩阵与 Roze 实施顺序。
- `docs/dtm-compatibility.md`：dtm-labs/dtm HTTP 兼容端点、调用顺序和差异。

## 验证

```bash
cargo test --workspace
```

设置以下变量后，工作区测试还会执行真实关系型数据库契约测试：

```bash
ROZE_DTM_TEST_POSTGRES_URL=postgres://user:password@localhost/roze_dtm
ROZE_DTM_TEST_MYSQL_URL=mysql://user:password@localhost/roze_dtm
cargo test --workspace
```

CI 会启动 PostgreSQL 和 MySQL，并强制执行这两组测试。

## 运行

开发环境可直接使用仓库内配置：

```bash
cargo run -p roze-dtm-service
```

也可以通过 `ROZE_CONFIG_PATH` 指定配置文件。生产环境必须使用持久化存储、独立控制令牌、唯一 worker id，并限制允许调用的分支来源；完整要求见 [DTM 服务契约](docs/dtm.md)。

## 容器运行

仓库提供 PostgreSQL 生产拓扑示例：

```bash
docker compose up --build
```

服务默认监听 HTTP `http://127.0.0.1:8090` 和 gRPC `127.0.0.1:36790`。Compose 中的令牌和数据库密码仅用于本地演示，部署前必须替换。生产配置模板位于 `service/config.production.yaml`。

## 存储后端

`application.dtm.store.kind` 支持：

- `memory`：仅限开发和测试。
- `sqlite`：单实例持久化。
- `postgres`：推荐的生产后端，支持多实例恢复租约。
- `mysql`：生产后端，支持多实例恢复租约。

所有关系型后端会在启动时幂等创建事务、分支屏障和恢复租约表。连接由 Roze `roze-sqlx` 管理，可通过 `max_connections` 设置连接池上限。

动态分支注册由存储层原子执行：内存后端使用写锁，PostgreSQL/MySQL 使用行锁，SQLite 使用带冲突重试的比较更新，避免多实例并发注册互相覆盖。四种后端也提供版本化通用 KV 和 topic 订阅；Message 的 `topic://name` 分支会在提交时展开为订阅 URL 快照。

## Rust 客户端

核心 crate 提供 `roze_dtm::client::DtmHttpClient` 和 `roze_dtm::grpc_client::DtmGrpcClient`。HTTP 客户端支持提交五类事务、逐事务 timeout/retry/Header、callback Workflow、状态转换、事务查询、topic/KV 和兼容 GID；gRPC 客户端覆盖 `dtmgimp.Dtm` 全部方法、提供二进制 Workflow 进度助手并传播 Roze Context。生产环境应配置 Bearer token。

## 上游同步

服务依赖的 Roze crates 固定到迁移时的上游提交。同步新版时，应一起审查核心库、服务、配置和契约，并更新 `Cargo.toml` 中所有 Roze 依赖的 `rev`。
