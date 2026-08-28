# Roze DTM

Roze 的独立分布式事务协调器，默认提供 TCC，并支持 Saga 工作流。代码从 `roze-team/roze` 的 `217274a134068f174cbe4a266a011bf719e15d0d` 提取，保留原有状态机、持久化、分支屏障、恢复租约和原生 Roze HTTP 控制面。

## 项目结构

- `src/lib.rs`：DTM 核心库，包含 TCC/Saga 状态机、内存与 SQLite 存储、HTTP 分支调用和恢复逻辑。
- `service/`：独立控制面服务。
- `service/config.yaml`：开发环境示例配置，也是服务的默认配置。
- `docs/dtm.md`：API、部署与安全契约。

## 验证

```bash
cargo test --workspace
```

## 运行

开发环境可直接使用仓库内配置：

```bash
cargo run -p roze-dtm-service
```

也可以通过 `ROZE_CONFIG_PATH` 指定配置文件。生产环境必须使用持久化存储、独立控制令牌、唯一 worker id，并限制允许调用的分支来源；完整要求见 [DTM 服务契约](docs/dtm.md)。

## 上游同步

服务依赖的 Roze crates 固定到迁移时的上游提交。同步新版时，应一起审查核心库、服务、配置和契约，并更新 `Cargo.toml` 中所有 Roze 依赖的 `rev`。
