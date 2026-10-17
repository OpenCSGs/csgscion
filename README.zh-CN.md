# Scion

[English](README.md) | 简体中文

服务端优先的源码管理系统：历史、合并、rebase 都在服务端完成，客户端始终在线、按需取数。协议与语言无关，官方提供 Rust 服务端 `scion` 和 Rust 客户端 `scn`。

![Scion standalone architecture](docs/design/images/arch-01-standalone.en.png)

<sub>架构图源文件：[`arch-01-standalone.en.svg`](docs/design/images/arch-01-standalone.en.svg)（英文）· [`arch-01-standalone.svg`](docs/design/images/arch-01-standalone.svg)（中文）。图中包含规划中的组件（L2 缓存、deriver、git-gateway 等），当前进度见下文。</sub>

## 文档

**使用指南**
- [从 git 导入](docs/guide/git-import.md)：把已有的 git 仓库连同全部历史导入 Scion
- [对象存储](docs/guide/object-storage.md)：SeaweedFS、MinIO 与其他 S3 兼容服务；服务识别与启动检查
- [多节点部署与合入](docs/guide/cluster.md)：合入语义（rebase、恰好一次）、`--advertise-url` 集群模式、故障切换与过载保护
- [缓存](docs/guide/cache.md)：区域缓存 `scion cache`（分片、对冲、预热）与本机 daemon `scn daemon`
- [认证与鉴权](docs/guide/auth.md)：令牌、访问控制策略、OIDC 登录、外部签发方、OPA 兼容的鉴权回调、接入 starhub-server
- [Redaction 与数据校验](docs/guide/redaction.md)：清除泄露的内容（redact、purge）、`scion admin fsck`、`scion admin lease ls`

**设计与规范**
- 设计文档：[服务端](docs/design/rust-server-architecture.md) · [客户端](docs/design/rust-client-architecture.md) · [接入 starhub / CSGLite](docs/design/starhub-integration.md)
- 对象格式规范：[`SPEC.md`](SPEC.md)，golden 测试向量在 [`testdata/vectors/`](testdata/vectors/)
- 协议：[`proto/scion/v1/`](proto/scion/v1/)

当前进度：**M5 生产化**进行中，已完成：
- **认证与鉴权**：
  - 令牌：自签 ES256 访问令牌、PAT、刷新令牌；
  - 外部身份（只用标准协议）：JWKS 或 HS256 签发方、RFC 7662 内省、OPA 兼容的鉴权回调。接入 starhub-server 只需配置；
  - ACL：按主体、仓库、动作、ref、路径授予权限；
  - `scn login`：OIDC device flow，凭据存钥匙串。
- **Redaction**：
  - redact 泄露的文件内容：对象 id 与历史不变，读取返回 `REDACTED`；
  - 区域缓存和本机 daemon 10 秒内停止返回；
  - 合入时不会把 redact 过的内容合并进新提交；
  - purge 在宽限期后物理删除字节，前提是所有含该对象的仓库都已 redact。
- **运维**：`scion admin fsck` 对照对象索引与对象存储校验 ref、提交、目录树和文件；`scion admin lease ls` 列出租约。

M5 后续还有可观测性、YugabyteDB、Helm 与混沌测试。

**M4 缓存层**已完成：
- **区域缓存 `scion cache`（L2）**：
  - 存储：foyer 内存加 NVMe；
  - 多节点按 rendezvous 哈希分片，整个区域对同一对象只回源一次；
  - 并发未命中合并（single-flight），回源时对冲与熔断；
  - 按 WatchRefs 预热，只沿变化的目录树下钻。
- **本机 daemon `scn daemon`（L1）**：
  - 校验过的本机对象缓存；
  - 其余 RPC 透明转发；
  - 预取工作副本跟随的 ref；
  - `scn` 按需自动拉起。
- **WatchRefs**：订阅 ref 变化（先快照，之后每次前进），断开后可续上。
- **服务端读路径**：
  - 进程内对象缓存；
  - 合并并发读并限制后端并发；
  - `--read-endpoint` 向客户端推荐缓存。

缓存 99.5% 的读取不回源，单节点每秒 153 万个对象，详见下文「性能」。

之前的里程碑：
- M3 实现了服务端 rebase、恰好一次合入、批量合入（单 ref 每秒 885 次）、集群与过载保护；
- M2 实现了历史查询（10 万提交 p99 低于 20 ms）；
- M1 实现了写入路径、Lite 模式、`scn` 工作副本与 git 导入。

M5 剩余工作：OpenTelemetry 与指标、YugabyteDB 档、Helm 与发布构建。

## 性能

基准在一台机器上运行（Apple M5，10 核），对象放在内存里，客户端、缓存和服务端在同一台机器。每一行都链接了对应的基准测试，可以自行复现（`cargo test --release -p <crate> --test bench -- --ignored --nocapture`）。

| 方面 | 负载 | 结果 | 目标 |
|---|---|---|---|
| 缓存命中率（[`server/cache/tests/bench.rs`](server/cache/tests/bench.rs)） | 冷缓存，200 个客户端检出同一组 20000 个对象（1–4 KiB） | 99.5% 的读取不回源 L3；每个对象只从 L3 取一次 | ≥ 99% |
| 缓存吞吐（同上） | 热缓存，64 个客户端，gRPC 每批 64 个对象 | 单节点每秒 153 万个对象 | ≥ 每秒 20 万 |
| 缓存吞吐（同上） | 热缓存，64 个客户端，每次请求 1 个对象 | 单节点每秒 16.4 万次请求 | — |
| 检出（[`client/daemon/tests/bench.rs`](client/daemon/tests/bench.rs)） | `scn clone` 1000 个目录、10 万个文件 | 经热 daemon 5.3 秒；本机磁盘上不经网络直接创建这些文件需要 5.4 秒，daemon 没有可测出的额外开销 | < 5 秒（受磁盘限制） |
| 合入（[`server/land/tests/bench.rs`](server/land/tests/bench.rs)） | 200 个并发客户端向同一个 ref 合入 2000 次 | PostgreSQL 每秒 885 次（p99 379 ms）；SQLite 每秒 1921 次（p99 173 ms） | 每秒 ≥ 200 次，p99 < 500 ms |
| 历史查询（[`server/query/tests/bench.rs`](server/query/tests/bench.rs)） | 10 万提交：log、diff、祖先判断、merge-base、blame | SQLite 与 PostgreSQL 下 p99 均低于 20 ms（冷 blame 13.6 ms，`log --path` 15.6 ms） | p99 < 100 ms |

## 目录结构

```
proto/scion/v1/      协议定义（.proto），语言无关，buf lint / breaking 检查
SPEC.md              对象模型与规范编码
testdata/vectors/    golden 测试向量，任何语言的实现都应通过

common/              服务端和客户端共用（只能依赖 common）
  protocol/          scion-proto   由 proto 生成的 Rust 代码
  object/            scion-object  ObjectId、规范编码、Tree / Changeset（纯计算，无 IO）
  flight/            scion-flight  合并并发请求（single-flight），服务端缓存与 daemon 共用

server/              服务端（可依赖 common 与更低层的 server crate）
  store/             scion-store       MetaStore / BlobStore trait、内存实现、后端一致性测试套件
  store-sql/         scion-store-sql   SQLite / PostgreSQL 元数据存储
  store-blob/        scion-store-blob  本地目录 / S3 兼容对象存储
  auth/              scion-auth        令牌、外部签发方（JWKS / HS256）、内省、OIDC、ACL
  tree/              scion-tree        增量建树
  merge/             scion-merge       服务端 rebase：把 delta 重放到更新的目录树上，三方合并
  land/              scion-land        创建提交、提交栈 rebase、LandCoordinator、租约
  query/             scion-query       历史查询（log / diff / blame / merge-base）与重建索引
  cache/             scion-cache       区域对象缓存（scion cache）：foyer、rendezvous、对冲、预热
  api/               scion-api         gRPC 服务实现与启动入口
  scion/             scion             服务端二进制（scion server / scion cache / scion admin）

client/              客户端（只能依赖 common 与 client，禁止依赖 server）
  sdk/               scion-client  Rust SDK
  wc/                scion-wc      工作副本（clone / status / commit / land / sync）
  daemon/            scion-daemon  本机 L1 缓存（scn daemon）：对象缓存、透明转发、预取
  gitimport/         scion-gitimport  git 历史导入
  cli/               scion-cli     命令行二进制 scn

xtask/               仓库维护任务（cargo xtask layers：依赖方向检查）
deploy/dev/          本地开发依赖（SeaweedFS、PostgreSQL）
docs/guide/          使用指南
docs/design/         设计文档与架构图
```

目录名表示位置和职责；crate 名统一带 `scion-` 前缀，避免与 crates.io 上的同名库（如 `object`）冲突。依赖方向由 `cargo xtask layers` 在 CI 中强制检查。

## 快速开始

```sh
# 启动服务端（Lite 档：SQLite + 本地目录，数据落盘），预建仓库 mono
cargo run -p scion -- server --lite --data-dir ./.scion-data --repo mono

# 其它后端：--meta mem|sqlite://PATH|postgres://…  --blobs mem|DIR|s3://bucket/prefix
# S3 连接参数：SCION_S3_ENDPOINT、SCION_S3_ACCESS_KEY_ID、SCION_S3_SECRET_ACCESS_KEY、SCION_S3_REGION、
# SCION_S3_PROVIDER=auto|minio|seaweedfs|aws|generic（CI 中测试 SeaweedFS 与 MinIO，详见 docs/guide/object-storage.md）

# 另一个终端：安装 scn 并检查连接
cargo install --path client/cli
scn doctor

# 日常流程
scn clone mono && cd mono          # 检出（默认 ref main，空仓库也可以）
echo 'hello' > README.md
scn status                          # A README.md
scn commit -m "add readme" --author "Ada <ada@example.com>"
scn land                            # 合入 main；main 已前进时由服务端 rebase（三方合并）
scn sync                            # 拉取别人合入的改动；有未合入的提交时自动做文件级 rebase
scn cat main:README.md              # 不需要工作副本也能读文件（工作副本外加 --repo）
scn ls main:                        # 列目录；所有命令都支持 --json
scn log --path src -n 10            # 历史，从新到旧，可按路径过滤
scn diff                            # 本地未提交的改动（unified diff）
scn diff main                       # main 最新提交引入的改动；`scn diff A B` 比较两个版本
scn blame src/lib.rs                # 每一行最后被哪个提交改动

# scn 经本机 daemon（scn daemon）连接服务端，按需自动拉起；SCION_NO_DAEMON=1 关闭
scn daemon status

# 在一个或多个服务端前部署区域缓存，并用 scion server --read-endpoint 推荐给客户端（详见 docs/guide/cache.md）
cargo run -p scion -- cache --upstream http://127.0.0.1:7080 --listen 127.0.0.1:7090 --warm mono:main

# 导入已有的 git 仓库（保留全部历史、作者与时间），详见 docs/guide/git-import.md
scn import git ~/src/my-repo --repo mono

# 升级已有数据的服务端后、启动之前：修复谱系并补建派生索引（可重复执行）
cargo run -p scion -- admin reindex --lite --data-dir ./.scion-data
```

## 开发

```sh
cargo test --workspace                          # 全部测试
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask layers                              # 依赖方向检查
cargo llvm-cov --workspace                      # 覆盖率（需 cargo install cargo-llvm-cov）
SCION_BLESS=1 cargo test -p scion-object --test vectors   # 新增 golden 向量后重新生成
docker compose -f deploy/dev/docker-compose.yml up -d     # SeaweedFS（S3）+ PostgreSQL
```

**测试要求**（[服务端设计文档 §12.1](docs/design/rust-server-architecture.md)）：每个含逻辑的函数都要有单元测试，覆盖正常路径、边界条件和每一个错误分支。核心 crate 行覆盖率不低于 90%，PR 新增代码不低于 90%，由 CI 强制执行。

## 许可证

Apache-2.0
