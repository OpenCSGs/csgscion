# Scion

服务端优先的源码管理系统：历史、合并、rebase 都在服务端完成，客户端始终在线、按需取数。协议与语言无关，官方提供 Rust 服务端 `scion` 和 Rust 客户端 `scn`。

- 设计文档：[`docs/design/`](docs/design/)（[服务端](docs/design/rust-server-architecture.md) · [客户端](docs/design/rust-client-architecture.md) · [接入 starhub / CSGLite](docs/design/starhub-integration.md)）
- 对象格式规范：[`SPEC.md`](SPEC.md)，golden 测试向量在 [`testdata/vectors/`](testdata/vectors/)
- 协议：[`proto/scion/v1/`](proto/scion/v1/)

当前进度：**M0 协议与骨架**（内存存储，重启即丢失数据）。

## 目录结构

```
proto/scion/v1/      协议定义（.proto），语言无关，buf lint / breaking 检查
SPEC.md              对象模型与规范编码
testdata/vectors/    golden 测试向量，任何语言的实现都应通过

common/              服务端和客户端共用（只能依赖 common）
  protocol/          scion-proto   由 proto 生成的 Rust 代码
  object/            scion-object  ObjectId、规范编码、Tree / Changeset（纯计算，无 IO）

server/              服务端（可依赖 common 与更低层的 server crate）
  store/             scion-store   MetaStore / BlobStore trait、内存实现、后端一致性测试套件
  api/               scion-api     gRPC 服务实现与启动入口
  scion/             scion         服务端二进制

client/              客户端（只能依赖 common 与 client，禁止依赖 server）
  sdk/               scion-client  Rust SDK
  cli/               scion-cli     命令行二进制 scn

xtask/               仓库维护任务（cargo xtask layers：依赖方向检查）
deploy/dev/          本地开发依赖（MinIO、PostgreSQL）
docs/design/         设计文档与架构图
```

目录名表示位置和职责；crate 名统一带 `scion-` 前缀，避免与 crates.io 上的同名库（如 `object`）冲突。依赖方向由 `cargo xtask layers` 在 CI 中强制检查。

## 快速开始

```sh
# 启动服务端（内存存储），预建仓库 mono
cargo run -p scion -- server --listen 127.0.0.1:7080 --repo mono

# 另一个终端：检查连接与能力协商
cargo run -p scion-cli -- doctor
cargo run -p scion-cli -- doctor --json
```

## 开发

```sh
cargo test --workspace                          # 全部测试
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask layers                              # 依赖方向检查
cargo llvm-cov --workspace                      # 覆盖率（需 cargo install cargo-llvm-cov）
SCION_BLESS=1 cargo test -p scion-object --test vectors   # 新增 golden 向量后重新生成
docker compose -f deploy/dev/docker-compose.yml up -d     # MinIO + PostgreSQL（M1 起使用）
```

**测试要求**（[服务端设计文档 §12.1](docs/design/rust-server-architecture.md)）：每个含逻辑的函数都要有单元测试，覆盖正常路径、边界条件和每一个错误分支。核心 crate 行覆盖率不低于 90%，PR 新增代码不低于 90%，由 CI 强制执行。

## 许可证

Apache-2.0
