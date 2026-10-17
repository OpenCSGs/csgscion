# 多节点部署与合入

Scion 服务端是无状态的：多个 `scion server` 节点共享同一个元数据库（PostgreSQL / YugabyteDB）和同一个对象存储（S3 兼容）即可组成集群，放在负载均衡后面。本文说明集群模式下合入（land）怎样工作、怎样配置，以及节点故障时会发生什么。

## 合入的语义

`Land` 把一个提交合入 ref（例如 `main`）：

- 提交是 ref 当前提交的后继：直接快进。
- ref 已经前进：服务端把待合入的提交栈 rebase 到最新的 ref 上（三方合并）。互不重叠的改动直接合入，双方改了同一个文件的不同位置时自动合并，改了同一处时返回 `MERGE_CONFLICT`，并附上冲突路径和 base / ours / theirs 三个版本的 id。
- `fast_forward_only=true` 时只允许快进（`scn import git` 使用）。
- 一次合入最多 256 个提交（快进、新建 ref 和 rebase 都算）：服务端要记下每个进入 ref 的提交的 change id 用于去重，超过时返回 `FAILED_PRECONDITION`，客户端应分段合入。`scn import git` 自动按 256 个一段快进。
- **恰好一次**：每个请求带 request id。网络中断或超时后用同一个 request id 重试，服务端返回第一次成功的结果（结果保留 24 小时）。同一个 ref 上每个 change id 也只合入一次，即使客户端换了 request id 重新提交。`GetLandStatus` 可以按 request id 查询结果。Rust SDK 在可重试的错误下会自动用同一个 request id 重试。

同一个 ref 上的并发合入由 LandCoordinator 合成批次（最多 64 个或 5 毫秒），按顺序 rebase 后一次提交。冲突的请求单独失败，不影响同批的其它请求。

## 开启集群模式

每个节点用 `--advertise-url` 声明其它节点能访问到它的地址：

```sh
# 节点 1
scion server --listen 0.0.0.0:7080 --advertise-url http://10.0.0.11:7080 \
  --meta postgres://scion:…@db:5432/scion --blobs s3://scion-objects/prod

# 节点 2
scion server --listen 0.0.0.0:7080 --advertise-url http://10.0.0.12:7080 \
  --meta postgres://scion:…@db:5432/scion --blobs s3://scion-objects/prod
```

也可以用环境变量 `SCION_ADVERTISE_URL`。Lite 模式（`--lite`）是单机模式，不能开启集群。

开启后，每个 ref 的合入由一个节点处理（leader）：

- 第一次收到某个 ref 的合入的节点申请这个 ref 的租约，成为 leader。租约有效期 5 秒，每 2 秒续约。
- 其它节点收到这个 ref 的合入时，把请求转发给 leader（节点之间直连，转发的请求不会再转发第二次）。
- 一个 ref 空闲 30 秒后，leader 释放租约，下一次合入时重新选出。

租约只用来避免多个节点争抢同一个 ref。正确性由 ref 的条件更新保证：ref 的版本号就是 fencing token，即使两个节点短暂地都认为自己是 leader，也不会写坏 ref。

## 节点故障

| 情况 | 结果 |
|---|---|
| leader 正常退出（SIGTERM） | 处理完手上的批，释放租约。其它节点立即接管。 |
| leader 进程被杀或断网 | 最多 5 秒后租约到期，其它节点接管。期间这个 ref 的合入返回可重试的错误（`UNAVAILABLE` / `LAND_CONTENTION`）。 |
| 客户端在等待结果时连接断开 | 用同一个 request id 重试。已经合入的直接返回原结果，没有合入的重新处理，不会重复也不会丢失。 |

这些行为由线性一致性测试覆盖（`server/land/tests/lincheck.rs`），测试场景包括合入进行中杀掉 leader。

## 过载保护

| 参数 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| `--max-concurrent-requests N` | `SCION_MAX_CONCURRENT_REQUESTS` | 10000 | 每个节点的在途请求上限。超过时立即拒绝（`RESOURCE_EXHAUSTED` / `RATE_LIMITED`，带 `retry_after_ms`），不排队。健康检查不受限制。 |
| `--repo-write-rate RATE[/BURST]` | `SCION_REPO_WRITE_RATE` | 不限制 | 每个节点、每个仓库的写入速率（上传、创建提交、合入各算一次），超过时返回 `RATE_LIMITED`。 |

另外，每个 ref 的合入队列最多 1024 个请求，排满时同样返回 `RATE_LIMITED`。

## 性能参考

在一个 ref 上合入，2000 个请求、200 个并发客户端，每个请求改不同的文件（`server/land/tests/bench.rs`，对象在内存）：

| 元数据库 | 吞吐 | p99 延迟 |
|---|---|---|
| PostgreSQL 16 | 885 次/秒 | 379 ms |
| SQLite | 1921 次/秒 | 173 ms |
