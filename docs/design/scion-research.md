# 复刻 Anthropic Scion：调研与设计方案

> 调研日期：2026-10-16
> 一手资料：Anthropic Engineering Blog《Redesigning source control at Anthropic》（2026-10-13）
> https://www.anthropic.com/engineering/redesigning-source-control
>
> 说明：本文是早期调研稿。最终决策（Rust、三档元数据库等）以 `rust-server-architecture.md` 和 `rust-client-architecture.md` 为准。
>
> 标注约定：**[原文]** = Anthropic 博客明确提到；**[推断]** = 原文未说明、由本方案补全的设计。

---

## 1. Scion 是什么（先对齐概念）

Scion 是 Anthropic 内部用来**替代 Git** 的源码管理系统（不是 Google 那个同名的多 Agent 编排工具 `GoogleCloudPlatform/scion`）。它**没有开源**，Anthropic 只公开了设计思路。

### 1.1 为什么要造它 [原文]

| 指标 | 数值 |
|---|---|
| 每日合入数 | 2026-05 约 2,500 → 2026-09 超 30,000（约 12 倍） |
| main 分支历史 | 约 100 GB |
| 压缩后的工作副本 | 超 25 GB |
| CI checkout | 约 2,500 万次/天（平均约 290 次/秒） |
| 用 Git 直接拉取的估算流量 | 超 600 PB/天 |
| 原方案 | 数百个 git mirror + seed 快照，越来越难运维，且与 main 逐渐偏离 |

根本矛盾在于：Git 是为**离线、人类、全量克隆**设计的；而现在的主要客户端是**常在线的 Agent 和 CI 任务**，数量巨大、生命周期很短，而且通常只需要仓库的一小部分。

### 1.2 七条核心设计原则 [原文]

1. **客户端常在线**：假设服务端始终可达，不做离线全量副本。
2. **客户端/服务端不对称**：history、blame、commit、merge、rebase 全部在**服务端**完成；客户端只负责有界的对象缓存、文件落盘和监听文件变化。
3. **源码管理即 API**：很多任务根本不需要本地仓库，直接调 API 即可。
4. **水平扩展**：任何地方都没有一份完整物化的仓库；没有 packfile，也就不需要 repack/gc 维护。
5. **提交廉价、增量、无锁**：提交的成本只与改动大小有关，与仓库大小无关；ref 尽量少（主要是 main 和 release 分支）。
6. **工作副本可选且默认稀疏**：支持 sparse checkout，以及按需物化文件的虚拟化 checkout。
7. **派生数据（derived data）统一机制**：blame、路径历史、提交图跳表、拷贝检测、路径搜索等都通过一套通用的递归机制计算，可以 eager 也可以 lazy，客户端看不到内部格式。

### 1.3 已知架构 [原文]

```
 scn (CLI) ──unix socket──▶ scion-daemon (每节点 1 个, 多 job 共享)
                                  │ 只允许连本区域 cache
                                  ▼
                           scion-cache (区域级, 内存流式返回, 无长期状态)
                                  │ 未命中才回源
                                  ▼
                           scion-server (权威, 小规模集群)
                              │                 │
                          Spanner            云对象存储
                   (refs、派生数据等)          (blob)
```

- **三层都提供同一套 RPC**，所以客户端不需要知道缓存拓扑。
- 每层命中率都在 **99% 以上**，且每层都**不持有长期状态**。
- **严格分层**：daemon 不能绕过 cache 直连 server，用来隔离故障域，避免某个区域的 cache 挂掉后流量直接压垮源站。
- **Changeset**：工作单元，本质是一条 WAL 记录，内容是「相对第一个父提交改了哪些文件」加上指向压缩 blob 的内容寻址指针。它**不存 tree 指针**，并且在服务端创建。
- **Ref**：对 Spanner 中单行做 CAS，与 changeset 的创建相互解耦。
- **Fetch 协议**：由客户端「想要什么」驱动，而不是 Git 那样协商「已经有什么」。客户端从 root tree 开始逐层请求缺失的 tree，再拉 blob；请求以单个对象为粒度、可断点续传，不会返回巨大的 packfile。sparse/虚拟化 checkout 因此**不需要任何特殊支持**：不想要的 tree 不请求就行。
- **合并**：没有全局 merge queue 锁，依靠合入后和部署前的自动化测试兜底；冲突通常由 Claude 修复。
- **权限**：支持仓库级和目录级 ACL；配套的 review 工具 Antique 支持语义化审批规则。
- **效果**：CPU 和内存比原 git mirror 体系低约 200 倍；checkout 从分钟级降到秒级；CI CPU 释放约 25%。

### 1.4 原文没讲、需要我们自己补的

wire 协议细节、对象格式与哈希算法、表结构、文件监听的实现、GC、ACL 与内容寻址缓存如何结合、多区域一致性、Git 互操作。下文用 **[推断]** 补全。

---

## 2. 可复用的开源参考（不要从零想）

| 项目 | 和 Scion 的关系 | 能直接用吗 |
|---|---|---|
| **Meta Sapling + Mononoke + EdenFS** | 思想最接近：bonsai changeset（只存文件变更）、derived data 框架、pushrebase 服务端变基、EdenFS 虚拟文件系统 | 概念强烈推荐照抄。但 Mononoke 开源版官方 README 写明缺少生产级 SQL 元数据（OSS 只有 sqlite）、S3 等 blobstore 和 thrift API，**很难直接用于生产** |
| **Bazel Remote Execution API (REv2)** | CAS 协议成熟：`FindMissingBlobs` + `BatchUpdateBlobs`/`ByteStream` 分大小上传，`BatchReadBlobs`/`ByteStream.Read` 读 | 直接借鉴 proto 设计；Buildbarn/BuildBuddy 的分层 CAS 缓存是现成参考 |
| **Microsoft VFS for Git / Scalar、Git partial clone** | 按需拉取 + 稀疏的先例 | 用作 Git 兼容层的参考 |
| **Google Piper / CitC** | 服务端中心 + 云端工作区的鼻祖 | 只有论文 |
| **Jujutsu (jj)** | 把工作副本当作提交、冲突一等公民 | 可借鉴客户端交互模型 |

**结论**：照搬 Mononoke 的**数据模型和派生数据思想**，照搬 REv2 的 **CAS 协议形态**，存储层和缓存层用成熟的云原生组件自建。

---

## 3. 目标与非目标

**目标**
- 生产级：多 AZ 高可用，RPO=0；元数据 RTO 不超过 1 分钟，无状态层秒级恢复。
- 高并发：设计容量为 **10 万次合入/天**（峰值 50 次 land/秒），**3,000 万次 checkout/天**，单仓库 TB 级历史。
- 简单易用：一个 CLI 二进制 + 一个 daemon；Agent 可以只用 SDK、不做 checkout；部署形态从单机（docker compose）到 K8s 多区域保持同一套代码。

**非目标（v1）**：离线工作、分布式多主写入、通用 Git 替代（保留 Git 桥接即可）。

---

## 4. 总体架构 [推断补全]

```
┌──────────────────── 客户端节点 ────────────────────┐
│ scn CLI / SDK(Go/Py/TS) / Agent / CI               │
│        │ gRPC over UDS                             │
│ scion-daemon                                       │
│  ├─ L1 对象缓存 (本地 SSD, LRU, 节点内共享, 内容寻址) │
│  ├─ Checkout 管理: VFS 模式 | 物化稀疏模式           │
│  ├─ Overlay/脏文件追踪 (VFS 写入 or fsnotify)       │
│  └─ Ref 订阅 (预取 main 新 tree)                    │
└────────────────────────┬───────────────────────────┘
                         │ gRPC/HTTP2 + mTLS (同一 ObjectService)
┌─────────────── 区域 (Region A/B/…) ─────────────────┐
│ scion-cache ×N (无状态, 一致性哈希分片, 内存+SSD,     │
│   singleflight 合并回源, 请求对冲)                    │
└────────────────────────┬────────────────────────────┘
                         │
┌──────────────── 核心 (权威层) ───────────────────────┐
│ scion-server ×N (无状态)                             │
│  ├─ ObjectService  (blob/tree 读写)                  │
│  ├─ ChangesetService (创建/查询)                     │
│  ├─ RefService (CAS / Watch)                         │
│  ├─ LandService (服务端 rebase + 冲突检测 + 批量合入) │
│  ├─ HistoryService (log/blame/diff/merge-base)       │
│  └─ AuthZ (仓库/目录 ACL)                            │
│ scion-deriver ×N (跟随 changeset 日志, 计算派生数据)  │
│ scion-ingest (Git 导入) / scion-gitexport (Git 镜像)  │
├──────────────────────────────────────────────────────┤
│ MetaStore 三档: SQLite / PostgreSQL / YugabyteDB      │
│   (同一套 sqlx 代码; 详见 rust-server-architecture §4.4)│
│ BlobStore: S3 兼容 (S3 / OSS / MinIO / Ceph)          │
│ EventBus: NATS JetStream / Kafka (ref 变更、合入事件) │
└──────────────────────────────────────────────────────┘
```

**关键不变式**：除了 ref 和日志游标，**所有对象都是不可变、内容寻址的**。因此各级缓存可以永不失效，不需要协调；唯一需要强一致的是 ref 的 CAS。

---

## 5. 数据模型 [推断，参考 Mononoke bonsai]

### 5.1 哈希与编码
- 哈希：**BLAKE3-256**（比 SHA-256 快数倍，可并行，抗碰撞）。对象 ID 使用 `type:hash` 前缀，如 `b:…`、`t:…`、`c:…`。
- 压缩：**zstd**（大仓库可训练字典）。哈希对**未压缩内容**计算，压缩只是存储格式。
- 序列化：**Protobuf**（确定性编码）或 CBOR，必须是规范化编码，保证同一内容得到同一哈希。
- 大文件（超过 4 MB）：用 **FastCDC** 分块，blob 存成 chunk 列表，便于去重和断点续传。

### 5.2 对象

```proto
// 文件内容
message Blob { bytes content = 1; }                // 或 ChunkedBlob { repeated Chunk chunks; }

// 目录（不是 changeset 的一部分，而是派生数据！）
message Tree {
  repeated Entry entries = 1;                     // 按 name 字节序排序
  message Entry { string name; Kind kind; uint32 mode; bytes hash; uint64 size; }
}

// 核心：只记录相对第一个父的文件变更
message Changeset {
  repeated bytes parents = 1;
  string author = 2; int64 author_time = 3;
  string committer = 4; int64 commit_time = 5;
  string message = 6;
  repeated FileChange changes = 7;                // 按 path 排序
  map<string, string> extra = 8;                  // 例如 review id、agent id
}
message FileChange {
  string path = 1;
  oneof op {
    Modify modify = 2;     // {blob_hash, mode, size}
    Delete delete = 3;
    CopyFrom copy = 4;     // {from_cs, from_path} 显式拷贝/重命名
  }
}
```

**为什么 changeset 不存 tree**：创建 changeset 的成本只和改动数量 O(k) 有关。root tree 作为派生数据异步算出，只需要重写被改路径上的 O(k·depth) 个 tree 节点，与仓库规模无关。这正好对应原文的「提交廉价、增量」。

### 5.3 元数据表（MetaStore）

| 表 | 主键 | 内容 | 说明 |
|---|---|---|---|
| `changesets` | (repo, cs_id) | changeset 原文、生成号 gen | 不可变 |
| `cs_log` | (repo, seq) | cs_id | **WAL**：仓库内单调递增序号，派生 worker 按它追赶 |
| `refs` | (repo, name) | cs_id, version | **唯一可变**，`UPDATE … WHERE version = ?` 实现 CAS |
| `derived` | (repo, type, version, cs_id) | value 或指针 | 派生数据映射 |
| `git_map` | (repo, git_sha) ↔ cs_id | | Git 桥接 |
| `acl` | (repo, path_prefix) | 规则 | 目录级权限 |

blob 和 tree 存在对象存储中，key 为 `repo/<hash 前两位>/<hash>`（也可以跨仓库全局去重）。小对象（小于 4 KB）可以内联进 MetaStore，减少对象存储请求。

### 5.4 派生数据框架（Scion 的精髓之一）

```go
type Deriver interface {
    Name() string                   // "tree", "blame", "path_history", "skiplist", "gitsha"…
    Version() int                   // 格式升级后可以并存、后台回填
    Deps() []string                 // 依赖的其它派生类型，例如 blame 依赖 tree
    // 由父提交的派生值加本 changeset 递推得出
    Derive(ctx, cs *Changeset, parents []Value, deps map[string]Value) (Value, error)
}
```
- **递归定义**：`D(cs) = f(cs, D(parent...))`。
- **lazy**：查询时如果缺失就沿祖先回溯到最近已计算的节点，再向前计算；用 singleflight 防止重复计算。
- **eager**：`scion-deriver` 跟随 `cs_log`，对 main 上的新提交立即计算 tree、gen、skiplist，保证 checkout 无延迟。
- 首批派生类型：`tree`（root manifest）、`gen`（拓扑深度，加速 merge-base）、`skiplist`（祖先跳表）、`path_history`（每个路径的上一次修改，用于 log/blame/冲突检测）、`blame`、`gitsha`（Git 互操作）、`path_index`（路径搜索）。
- 客户端只通过 RPC 拿结果，**派生格式可以随意演进**，不需要升级客户端。

---

## 6. 写路径：提交与合入（高并发的核心）

### 6.1 创建 changeset
1. 客户端拿到脏文件列表（VFS 模式下直接从 overlay 得到；物化模式下用 fsnotify 或 watchman 加 mtime 校验得到）。
2. `FindMissingBlobs(hashes)`，然后用 `BatchUploadBlobs` 上传小文件、用 `ByteStream.Write` 上传大文件。上传只发生在**有改动的文件**上。
3. `CreateChangeset(parents, changes)`：服务端校验 blob 存在、ACL、钩子（大小限制、密钥扫描），然后写入 `changesets` 和 `cs_log`，返回 cs_id。**此时不移动任何 ref。**

Agent 也可以**完全不 checkout**：直接调 `CreateChangeset`，传入路径和内容（SDK 负责自动上传 blob）。

### 6.2 Land（服务端 rebase + CAS）—— 无锁合入

```
Land(repo, ref="main", cs, base):
  loop:
    head, ver = refs.get(ref)
    // 冲突检测：只看 base..head 期间被改过的路径与 cs 改动路径的交集
    touched = path_history.changed_paths_between(base, head) ∩ cs.paths
    for p in touched: 对 p 做三方合并(base, head, cs)；失败就返回 CONFLICT(p)
    new_cs = Changeset{parents=[head], changes=cs.changes ⊕ 合并结果}
    write(new_cs)
    if refs.cas(ref, expect=ver, new=new_cs): emit event; return new_cs
    // CAS 失败说明别人先合入了，用新 head 重试（指数退避 + 抖动）
```

- 成本 = O(本次改动) + O(期间与之重叠的路径)，**与仓库大小无关**。
- 与原文一致：不设全局 merge queue 锁，测试放在合入后和部署前。

### 6.3 超高并发合入：Land 批处理器 [推断]
单行 CAS 在 PostgreSQL 或 YugabyteDB 上大约能做到几百次/秒。如果所有 land 都直接 CAS，热点行重试率会飙升。建议：
- 每个 `(repo, ref)` 通过租约（lease，存在 MetaStore）选出一个 **land leader**，其它 server 把请求转发给它。
- leader 在内存中维护 head，按 **5–20 ms 窗口**收集请求，逐个在内存中 rebase（彼此不冲突的直接线性串起来），最后**一次 CAS** 推进 ref，把 N 个合入合并成一次写。
- leader 挂了，租约过期后（秒级）由别人接管；之前未确认的请求由客户端幂等重试（请求带 `idempotency_key`）。
- 实测目标：单 ref 每秒数百次合入。

### 6.4 一致性顺序（不变式）
`blob 已持久化` → `changeset 已持久化` → `ref CAS`。读路径永远不可能看到指向不存在对象的 ref，所以**崩溃不会产生悬挂引用**。

---

## 7. 读路径：fetch 协议与三级缓存

### 7.1 RPC（三层都实现同一套 `ObjectService`）
```proto
service ObjectService {
  rpc GetTrees(GetTreesReq) returns (stream TreeObj);   // 支持 depth、path 前缀过滤
  rpc BatchGetBlobs(BatchReq) returns (BatchResp);      // 小对象批量读
  rpc ReadBlob(ReadReq) returns (stream Chunk);         // 大对象流式读，带 offset，可续传
  rpc FindMissing(FindReq) returns (FindResp);
  rpc BatchUploadBlobs / WriteBlob(stream)              // 写只透传到 server
}
service RefService { rpc Resolve; rpc Watch(stream); }  // ref 永不缓存，或 TTL ≤ 1s 并带版本号
```

checkout 流程：`Resolve("main")` → cs_id → 派生出 `root_tree` → 只 `GetTrees` 需要的子目录 → 打开文件时才 `GetBlob`。sparse 和虚拟化**不需要协议支持**。

### 7.2 各层实现要点
| 层 | 存储 | 要点 |
|---|---|---|
| **L1 daemon** | 本地 SSD CAS 目录 + 内存 LRU，有容量上限 | 节点内所有 checkout/job 共享；启动时从**周期性快照**（main 热对象打包，放对象存储或 OCI 镜像）预热；订阅 ref Watch 预取新 main 的 tree |
| **L2 scion-cache** | 内存（如 ristretto）+ 本地 NVMe | **一致性哈希**按对象 hash 分片（rendezvous hashing），节点间不复制；**singleflight** 合并并发回源；p99 慢时做请求对冲（hedged request）；无状态，挂了只损失命中率 |
| **L3 scion-server** | 无状态 + 小内存缓存 | 读 MetaStore 和对象存储；派生数据 lazy 计算入口 |

**严格分层**：daemon 的配置里只有区域 cache 的地址；cache 对上游限流并熔断。区域 cache 整体故障时，daemon 返回可重试错误并退避，**不会降级直连源站**（这正是原文强调的故障域隔离）。

### 7.3 容量估算（3,000 万 checkout/天）
平均每次 checkout 只拉 root 和少量路径的 tree（VFS 模式），实际访问的 blob 大多命中 L1。即便 L2 的 QPS 达到数十万，L3 在 99% 命中率下也只承担约 1%，因此 server 集群可以很小，与原文「small fleet」相符。

---

## 8. 客户端设计（简单易用的关键）

### 8.1 组件
- `scn`：薄 CLI，所有逻辑在 daemon 中。daemon 没启动时，CLI 会自动拉起它（socket activation）。
- `scion-daemon`：每节点一个，进程内管理多个 checkout。
- SDK（Go / Python / TypeScript）：同一份 gRPC proto 生成，供 Agent 和 CI 直接使用。

### 8.2 三种使用模式
| 模式 | 适用 | 实现 |
|---|---|---|
| **API-only** | Agent 读代码、改少量文件、查 blame/log | 不落盘；SDK 直接 `ReadFile(rev, path)` 和 `CreateChangeset` |
| **物化稀疏 checkout** | CI、容器内（没有 FUSE 权限） | 只物化 `scn sparse add dir/` 指定的目录；用 fsnotify（Linux inotify / macOS FSEvents）加 mtime/size 快速比对来检测改动 |
| **虚拟化 checkout** | 人和 Agent 的交互式开发、全仓库视图 | Linux 用 **FUSE**（`hanwen/go-fuse`）；macOS 用 **NFSv3 loopback**（EdenFS 的现行方案，FSKit 尚未成熟）；Windows 用 **ProjFS**。读时按需拉取；写入落到 overlay，脏文件集合天然已知，`status` 的成本为 O(脏文件数) |

### 8.3 命令（刻意保持精简，贴近 Git/Sapling 心智）
```
scn clone <repo> [--sparse dir1,dir2 | --virtual]   # 秒级完成：只拉 root tree
scn status | diff | log [path] | blame <file>       # history 类全部走服务端 RPC
scn commit -m "…"                                    # 上传脏 blob 并 CreateChangeset
scn land [--onto main]                               # 服务端 rebase + CAS；冲突时返回冲突文件
scn sync                                             # 工作副本移到最新 main（只改有差异的路径）
scn cat <rev>:<path> / scn ls <rev>:<dir>            # 不需要 checkout
scn sparse add|rm <dir>
```

---

## 9. 高可用与生产级

| 关注点 | 方案 |
|---|---|
| 元数据 | 三档：SQLite（Lite 单机）/ PostgreSQL + Patroni（Standard，默认）/ YugabyteDB 跨 3 AZ（Distributed，Raft，RPO=0）。不用 CockroachDB（已改为商业许可） |
| 对象 | S3 兼容存储（11 个 9）+ 跨区域复制；对象不可变，复制不需要冲突处理 |
| 无状态层 | server/cache/deriver 部署在 K8s，配置 HPA、PodDisruptionBudget，跨 AZ 打散 |
| 多区域 | 写只进主区域（ref CAS 需要强一致）；读走区域 cache 并回源到最近的 server；对象存储跨区复制。对象未复制到达时，回源到主区域对象存储 |
| 过载保护 | 按租户/仓库做令牌桶限流；用并发数限制做 load shedding；gRPC deadline 逐层传递；熔断；客户端统一使用指数退避加抖动 |
| 幂等 | 所有写 RPC 都带 `idempotency_key`；内容寻址本身让 blob 上传天然幂等 |
| GC | v1 **不删除可达对象**（存储便宜，原文的 Scion 也无 packfile 维护）；只清理上传后超过 7 天仍未被任何 changeset 引用的孤儿 blob（mark 阶段跑在派生数据上） |
| 备份 | MetaStore 每日全量 + PITR；对象存储开启版本化或 Object Lock |
| 安全 | 服务间 mTLS（SPIFFE/SPIRE）；用户和 Agent 用 OIDC 短期 token；审计日志写入 EventBus |
| 可观测 | OpenTelemetry trace 贯穿 daemon→cache→server；核心 SLI 为每层命中率、GetTree/GetBlob p99、land 延迟、CAS 重试率、派生滞后（log seq 差值）；可参考原文的「superfan」做法，用 Agent 盯组件遥测 |
| 验证 | 混沌测试（杀 cache、MetaStore leader 切换、网络分区）；Jepsen 风格的 ref CAS 线性一致性测试；用 k6/ghz 压测 land 和 fetch |

### 9.1 ACL 与内容寻址缓存的冲突 [推断，需要重点设计]
内容寻址的 blob 可能被多个路径共享，cache 层不知道路径。方案如下：
- **在 tree 层鉴权**：server 返回 tree 时，对无权限的子目录**隐藏条目**（或只返回占位）。拿不到子树 hash，就无法请求其内容。
- 256-bit hash 不可猜测，可以视为 **capability**；cache 只校验调用方身份合法（mTLS/token）。
- 高敏感目录（如密钥、安全代码）放在**独立的 blob 命名空间**（key 带 ACL 域前缀），cache 对这类 key 必须回源 server 做鉴权或拒绝缓存。

---

## 10. Git 互操作与迁移

1. **导入**：`scion-ingest` 读取 `git fast-export` 流，按拓扑序生成 changeset，同时写入 `git_map`。可以增量运行，迁移期间持续跟随 Git 主仓库。原文提到 Scion 早期只服务元数据查询，再逐步替换客户端。
2. **导出**：`gitsha` 作为派生数据，`scion-gitexport` 单向镜像到 Git 托管平台，供尚未迁移的工具（IDE、GitHub Actions）使用。
3. **兼容层（可选）**：`git-remote-scion` remote helper，让 `git fetch/push` 能对接 Scion。

迁移顺序：只读查询 API → CI 改用 Scion checkout（收益最大，原文 CI CPU 降 25%）→ 开发者客户端 → 写路径切换 → Git 降级为只读镜像。

---

## 11. 技术选型建议

| 部分 | 选择 | 理由 |
|---|---|---|
| 主语言 | **Go**（server、cache、daemon、CLI 统一） | 高并发、gRPC 生态、`go-fuse` 和 NFS 库成熟、单二进制易分发、团队上手快。daemon 的 VFS 热路径若遇到瓶颈，再局部改用 Rust |
| RPC | gRPC + Protobuf（外部 HTTP 可用 Connect-Go 同时提供 JSON） | 流式传输、deadline、拦截器 |
| MetaStore | SQLite / PostgreSQL / YugabyteDB 三档 | 都是宽松的开源许可证，适合私有化部署；共用 PG 方言的 sqlx 代码 |
| BlobStore | S3 API（MinIO 用于本地和私有化部署） | |
| 缓存 | ristretto（内存）+ 本地文件 CAS | |
| 事件 | NATS JetStream（轻）/ Kafka（重） | ref 变更、CI 触发 |
| 哈希/压缩/分块 | BLAKE3 / zstd / FastCDC | |
| 部署 | Helm Chart；本地 `docker compose up` 一键启动全栈 | 「简单易用」 |

### 11.1 仓库结构建议
```
scion/
  proto/scion/v1/*.proto        # 唯一事实来源，三层共用
  pkg/object/                   # 哈希、编码、Tree/Changeset、diff3
  pkg/metastore/{sqlite,pg,yb}/ # 接口 + 实现
  pkg/blobstore/{s3,fs}/
  pkg/derived/                  # 派生数据框架 + 各 deriver
  pkg/land/                     # rebase、冲突检测、批处理 leader
  cmd/scion-server/  cmd/scion-cache/  cmd/scion-deriver/
  cmd/scion-daemon/  cmd/scn/   cmd/scion-ingest/
  sdk/{go,python,ts}/
  deploy/{compose,helm}/
  test/{e2e,chaos,load}/
```

---

## 12. 分阶段路线图

| 阶段 | 周期 | 交付 | 验收标准 |
|---|---|---|---|
| **P0 核心** | 4–6 周 | 对象模型、PG+MinIO、server（Object/Changeset/Ref/Land 基础版）、`tree` 派生、`scn` 直连模式下的物化 sparse checkout、Git 导入 | 导入一个真实中型仓库；clone/commit/land/log 跑通；并发 land 正确性测试 |
| **P1 缓存与派生** | 4–6 周 | daemon + L1、scion-cache + 一致性哈希、快照预热、`gen/skiplist/path_history/blame`、Go/Python SDK、ref Watch | 每层命中率 ≥ 99%；在 100 GB 级仓库上 checkout 为秒级 |
| **P2 生产化** | 6–8 周 | Land 批处理 leader、YugabyteDB、ACL、mTLS/OIDC、限流熔断、OTel、Helm、混沌和压测 | 50 次 land/秒、单 AZ 故障无感、RPO=0 |
| **P3 体验** | 持续 | VFS（FUSE / macOS NFS / ProjFS）、Git 导出与 remote helper、代码搜索、review 集成 | 开发者全仓库视图秒开 |

---

## 13. 主要风险

1. **VFS 跨平台复杂度高**：EdenFS 在 macOS 上从 macFUSE 迁到 NFSv3，FSKit 还在评估中。建议先用物化 sparse 模式覆盖 80% 的场景。
2. **热点 ref 的 CAS 争用**：必须实现 §6.3 的批处理 leader，否则合入量上万后重试风暴会出现。
3. **ACL 与内容寻址**：§9.1 的模型需要安全评审。
4. **服务端 rebase 语义**：三方合并的正确性（换行、二进制、重命名）需要大量测试；可以参考 Mononoke pushrebase。
5. **信息来源有限**：Scion 的实现细节只来自一篇博客，本方案中 **[推断]** 的部分是工程补全，不代表 Anthropic 的真实实现。

---

## 参考来源
- Anthropic, *Redesigning source control at Anthropic*: https://www.anthropic.com/engineering/redesigning-source-control
- heise: https://www.heise.de/en/news/Scion-Anthropic-s-lean-Git-alternative-for-agentic-workflows-11489274.html
- dot-ai（日文解读）: https://dot-ai.myuuu.co.jp/times/articles/1545
- Mononoke README: https://github.com/facebook/sapling/blob/main/eden/mononoke/README.md
- EdenFS × FSKit 讨论: https://developer.apple.com/forums/thread/766793
- Buildbarn 存储 ADR: https://github.com/buildbarn/bb-adrs/blob/master/0002-storage.md
- 同名但无关的项目（Google 多 Agent 编排）: https://infoq.com/news/2026/04/google-agent-testbed-scion
