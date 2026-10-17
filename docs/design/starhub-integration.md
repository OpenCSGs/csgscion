# Scion 接入 starhub-server（与 Gitaly 并行）设计

> 版本：Draft v0.1 · 2026-10-16
> 调研对象：本地 `~/git/starhub-server`（module `opencsg.com/csghub-server`，Go，gin + bun + Temporal）
> 前置：[`rust-server-architecture.md`](./rust-server-architecture.md)、[`rust-client-architecture.md`](./rust-client-architecture.md)

目标：Scion 既能作为**独立产品**部署，也能作为 starhub 的**第二种 Git 存储后端**，与现有 Gitaly **按仓库并行**运行，支持仓库在两个后端之间迁移，必要时可以回滚。

---

## 1. starhub-server 现状（与 Git 相关的部分）

### 1.1 分层与 Git 抽象
- 分层规则：`handler → component → builder`（见 `AGENTS.md`）。
- **唯一的 Git 抽象**：`builder/git/gitserver/interface.go` 中的 `GitServer` 接口，共约 45 个方法，覆盖仓库、分支、提交、文件树、读写文件、diff、archive、LFS 指针、mirror、smart HTTP、fork、size 等。
- **唯一的实现**：`builder/git/gitserver/gitaly/`，约 4 千行，基于 Gitaly v16 的 gRPC 客户端。
- **工厂函数是写死的**：`builder/git/gitserver.go` 中的 `NewGitServer()` 直接返回 `gitaly.NewClient(config)`。
  - docker-compose 里设置的 `STARHUB_SERVER_GITSERVER_TYPE: gitaly` **在代码中没有被使用**。
  - Gitaly 只配置了**单个 storage 和单个地址**（`config.GitalyServer.Address/Storage`）。
- **使用面很广**：约 60 个非测试文件依赖 `gitserver.GitServer`，`git.NewGitServer` 有 53 处调用，分布在 component、callback、checker、dataviewer、moderation、mirror、user、workflow 中。

### 1.2 Git 协议入口
| 入口 | 路径 | 实现 |
|---|---|---|
| Smart HTTP | `/{type}s/:ns/:name.git/info/refs`、`git-upload-pack`、`git-receive-pack` | `api/handler/git_http.go` → `GitServer.InfoRefs/UploadPack/ReceivePack` → Gitaly SmartHTTPService |
| SSH | gitlab-shell v16 → `/api/v4/internal/allowed` 鉴权 | **响应中直接带上 Gitaly 的地址和 token**（`component/internal.go:139`），gitlab-shell 随后直连 Gitaly |
| 推送钩子 | Gitaly hooks → `/api/v4/internal/pre_receive`、`/post_receive` | `post_receive` 用 `GetDiffBetweenTwoCommits` 算出 added/removed/modified 文件列表，组装成 `types.GiteaCallbackPushReq`，启动 Temporal `HandlePushWorkflow` |

### 1.3 推送后的业务管线（`component/callback/git_callback.go`）
仓库元数据和标签更新、模型和数据集信息解析、Space 变更监听、仓库关系、同步版本号、仓库大小统计、敏感内容检查等。**所有逻辑都只依赖 `GiteaCallbackPushReq`，以及通过 `GitServer` 回读文件**，与具体用哪个后端无关。

### 1.4 大文件
- starhub 自带 LFS 服务：batch、上传下载、locks、multipart；元数据存在 `lfs_meta_objects` 表（sha256 oid），对象存在 S3/MinIO。
- **Xnet**：另一种大文件后端（Xet 风格）。实现方式是：在仓库表加开关 `repositories.xnet_enabled`，用 `xnet_migration_task` 表加 Temporal 工作流 `batch_migrate_to_xnet` 做迁移；开启 Xnet 的仓库**直接拒绝 git 操作**（`ErrUsingGitInXnetRepository`，提示改用 SDK/CLI）。
- 对外还提供 **HuggingFace 兼容 API**（`hfGroup` 路由），依赖 LFS 的 sha256 和 HF 的文件树语义。

> **结论**：Xnet 就是「按仓库切换后端 + 迁移任务 + 工作流」的现成模板，接入 Scion 应**完全照搬这个模式**。

---

## 2. 总体方案

![Scion 接入 starhub-server](./images/arch-03-starhub-integration.png)

```
                       starhub-server (Go)
   handler ──▶ component ──▶ builder/git
                                │
                     RoutingGitServer  (新增, 实现 gitserver.GitServer)
                     按 repositories.storage_backend 分派
                     ┌──────────┴───────────┐
               gitaly.Client           scion.Client (新增 adapter)
                     │                       │ gRPC (buf 生成的 Go stub)
                  Gitaly                scion server / cache (Rust)
                     ▲                       ▲
 git CLI ─ Smart HTTP┤                       ├─ scion-git-gateway (Rust, 阶段 3)
 git CLI ─ SSH ──────┘ gitlab-shell ─────────┘   实现 Gitaly 的 SmartHTTP/SSH 服务子集
                                              (由 /allowed 按仓库返回不同地址)
 scn / Agent / CI ─────────────────────────────▶ scion (原生协议, 默认客户端)

 Scion 事件 ──NATS (starhub 已在使用)──▶ starhub 消费者 ──▶ 转成 GiteaCallbackPushReq
                                                  ──▶ 同一个 Temporal HandlePushWorkflow
 Scion pre-land webhook ──▶ starhub /internal/scion/pre_land (复用 checker: 文件大小/LFS/敏感)
 starhub 签发 JWT ──JWKS──▶ Scion 验签 (仓库级权限 claims)
```

**原则**：
1. **Scion 服务端不依赖 starhub**：所有对接点都是 Scion 的通用扩展机制（外部 JWKS、pre-land webhook、NATS 事件、仓库管理 API）。独立部署时不开启这些机制即可。
2. **starhub 侧的改动只放在 builder 层**，加上最少量的路由和配置；component 及以上的业务逻辑**零改动**，推送后的处理管线也不用改。

---

## 3. starhub 侧的改动

### 3.1 数据模型（用仓库自带的 migration CLI 生成，不要手写）
```
repositories
  + storage_backend   text not null default 'gitaly'   -- 'gitaly' | 'scion'
  + current_backend_migration_task_id bigint null

backend_migration_tasks          (仿照 xnet_migration_task)
  id, repository_id, from_backend, to_backend, status, phase, last_message, times
```

### 3.2 `RoutingGitServer`
- 实现 `gitserver.GitServer`，持有 `gitaly.Client` 和 `scion.Client` 两个实现。每个方法先根据请求里的 `(RepoType, Namespace, Name)` 或 `relativePath` 查出该仓库的后端（结果用 `golang-lru` 本地缓存，加 Redis pub/sub 失效），再转发给对应实现。
- `NewGitServer()` 改为返回 `RoutingGitServer`。**53 处调用点无需改动**。
- `CreateRepo` 的默认后端由配置 `STARHUB_SERVER_GITSERVER_DEFAULT_BACKEND` 决定，也可以按仓库类型或组织做灰度。

### 3.3 `builder/git/gitserver/scion`（Go adapter）
> 说明：基于 buf 从 Scion proto 生成的 Go stub，加上方法映射代码，预计 1.5 到 2.5 千行，需要时随时可以实现，不在关键路径上。

**方法映射**（需要 Scion 补充的能力用 ★ 标出，详见 §5）：

| GitServer 方法 | Scion 对应 |
|---|---|
| CreateRepo / DeleteRepo / GetRepo / UpdateRepo / RepositoryExists | ★ `RepoAdminService.Create/Delete/Get/Update` |
| CreateFork / CopyRepository | ★ `RepoAdminService.Fork`：对象全局去重，fork 只需复制 ref，O(1) |
| CreateBranch / DeleteRepoBranch / GetRepoBranches / GetRepoBranchByName / SetDefaultBranch / UpdateRef | `RefService`（★ 加上默认分支属性） |
| GetRepoCommits / GetRepoLastCommit / GetSingleCommit | `QueryService.Log` / `ResolveRevision` |
| GetCommitDiff / GetDiffBetweenTwoCommits | `QueryService.Diff`（GetDiffBetweenTwoCommits 返回的 added/removed/modified 可以直接从 changeset delta 得到） |
| GetRepoFileTree / GetTree / GetRepoAllFiles / GetRepoFiles / GetFilesByRevisionAndPaths | `ListDir` / `GetTrees`（递归） |
| GetLogsTree（目录中每个条目的最后一次提交） | ★ 派生数据 `LastCommitPerEntry` |
| GetRepoFileRaw / GetRepoFileReader / GetRepoFileContents | `QueryService.ReadFile`（流式） |
| CreateRepoFile / UpdateRepoFile / DeleteRepoFile / CommitFiles | `CreateChangeset`（内联内容）+ `Land` |
| GetArchive | ★ `QueryService.Archive`（tar.gz/zip 流式） |
| GetRepoLfsPointers / GetRepoAllLfsPointers | ★ 派生数据 `LfsPointerIndex` |
| GetRepoSize / GetLastCommitSize / GetRepoLfsSize | ★ 派生数据 `TreeStats`（大小、文件数） |
| CreateMirrorRepo / MirrorSync | ★ `ImportService`：服务端从远端 git 增量导入，即在线版的 scion-import |
| InfoRefs / UploadPack / ReceivePack | 阶段 1–2：返回 `ErrUsingGitInScionRepository`（照搬 Xnet 的处理）；阶段 3：转到 git gateway |
| BuildRelativePath | 本地计算 Scion 仓库名，与 Gitaly 的路径规则解耦 |

**提交 ID 兼容**：starhub、前端和 HF API 中很多地方默认提交 ID 是 40 位十六进制 SHA。adapter 对外统一返回 Scion 派生数据 `GitSha` 中的 **git 兼容 SHA-1**，内部再映射回 Scion 的 changeset id。这样 URL、缓存 key、HF 的 `revision` 参数都保持原有格式，切换后端对用户透明。

### 3.4 推送事件
新增一个 NATS 消费者 `scion-ref-events`：订阅 `scion.<repo>.ref.updated`，把事件转成 `GiteaCallbackPushReq`（ref、提交列表、各提交的 added/removed/modified、head_commit），再以**相同的 workflow ID 规则**启动 `HandlePushWorkflow`。下游业务完全复用，Temporal 的 workflow ID 本身就提供去重。

### 3.5 推送前检查
新增内部接口 `POST /api/v1/internal/scion/pre_land`，复用 `component/checker`（文件大小、LFS 对象存在性、skill 文件规则）和 `pre_receive` 中的逻辑。Scion 在 land 之前同步调用它。

### 3.6 认证与鉴权
> 2026-10-17 按 starhub-server 的实际实现修订（之前假设 starhub 提供 JWKS 和 `repos` claim，与实际不符）。Scion 侧只用标准协议，没有 starhub 专用代码，配置见 [`docs/guide/auth.md`](../guide/auth.md#接入-starhub-server)。

- **starhub 的 JWT**：HS256 共享密钥签发（`STARHUB_JWT_SIGNING_KEY`），`iss = "OpenCSG"`，用户名在 `current_user`。Scion 把 starhub 配成一个 HS256 签发方（`hs256_secret_env`、`subject_claim = "current_user"`）直接验签。
- **starhub 的 access token**（不透明）：Scion 按 RFC 7662 调用 starhub 的内省接口。starhub 侧新增 `POST /api/v1/internal/scion/introspect`，内部调用 `VerifyByAccessToken`。
- **权限**：留在 starhub（rebac、组织继承、公开/私有仓库）。Scion 按 OPA 兼容的格式调用 starhub 的鉴权接口。starhub 侧新增 `POST /api/v1/internal/scion/authz`，把仓库名映射回 starhub 仓库并调用 `CheckUserRepoPermission`。结果在 Scion 侧缓存（允许 30 秒、拒绝 5 秒）。
- `scn` 访问 starhub 托管的 Scion 时，`scn login` 走 starhub 的 Casdoor OIDC（device flow），或者用 `scn login --token <access token>`。

---

## 4. 迁移：Gitaly ↔ Scion（照搬 Xnet 模式，Temporal 工作流）

```
MigrateRepoToScion(repo):
  1. Precheck   : 仓库大小、LFS 状态、是否开启了 Xnet、是否有正在进行的 mirror
  2. Bulk import: scion ImportService 从 Gitaly 读取（内部 git URL）→ changesets + GitSha 映射
  3. Freeze     : 仓库置为只读(pre_receive 拒绝写入)，记录各 ref 的 sha
  4. Catch-up   : 增量导入从 Bulk 到 Freeze 之间的新提交
  5. Verify     : 对每个 ref 比较 git tree sha 与 Scion 派生出的 GitSha tree，必须完全一致
  6. Flip       : repositories.storage_backend = 'scion'，失效路由缓存，解除冻结
  7. Retain     : Gitaly 中的副本保留 N 天，供只读回滚，之后再删除
回滚: 执行反向流程(Scion → git fast-export → Gitaly)，或在保留期内直接切回并补齐增量
```
- 批量迁移沿用 `batch_migrate_to_xnet` 的并发控制和管理后台入口（`api/router/admin*.go`）。
- **冻结窗口只覆盖 Catch-up 到 Flip 这一段**，通常是秒级。

---

## 5. Scion 服务端需要新增的通用能力

以下内容会同步写进服务端架构文档的 §16。它们都是**通用机制**，独立部署时同样有用：

| 能力 | 说明 |
|---|---|
| `RepoAdminService` | Create/Delete/Get/Update/Fork/List；仓库属性（默认分支、可见性标记、配额）。从第一天起支持多租户和大量小仓库（starhub 的模型、数据集、Space 都是独立仓库） |
| 外部身份（M5 已实现） | 受信任的签发方：JWKS（标准 OIDC）或 HS256 共享密钥（starhub 现状），claims 映射可配；RFC 7662 内省校验外部的不透明令牌 |
| 外部鉴权回调（M5 已实现） | OPA 兼容：`{"input": {subject, groups, repo, action, ref, path}}` → `{"result": bool}`，带 TTL 缓存；starhub 用它接入 rebac |
| pre-land webhook | 同步调用，可配置超时和故障时的处理（fail-open 或 fail-closed）；把 delta 和文件元数据发给外部系统做检查 |
| 事件 | `scion.<repo>.ref.updated`（包含每个提交的路径级 delta）发到 NATS；同时可以选择 webhook 方式 |
| 派生数据 | `GitSha`（git 兼容的 commit/tree/blob SHA-1）、`LastCommitPerEntry`、`LfsPointerIndex`、`TreeStats`、blob 的 `Sha256`（供 LFS/HF 使用） |
| `QueryService.Archive` | 流式生成 tar.gz/zip，支持路径过滤 |
| `ImportService` | 从 git 远端或本地路径做增量导入，供 mirror 和迁移使用 |
| `scion git-gateway`（新角色） | 用 Rust 实现 **Gitaly 的 SmartHTTPService / SSHService 子集**（info-refs、upload-pack、receive-pack，包括 sidechannel）。git 对象由 `GitSha` 派生数据按需生成；push 时把收到的 packfile 解析成 changeset，再执行 land |

**元数据库**：接入 starhub 时用 **Standard 档（PostgreSQL）**。可以使用 starhub 那套 PG 集群里的**独立 database**（例如 `scion`），共用 Patroni 和备份体系，但不和 starhub 的表混在一起；规模变大后再单独迁到 YugabyteDB。

**对象存储（MinIO）共用**：
- Scion 直接使用 starhub 现有的 MinIO（compose 中的 `minio` 服务，以及生产环境的 MinIO 或云 OSS）。默认使用**独立的 bucket `scion-objects`**，也可以在同一个 bucket 下用 `prefix = "scion/"` 隔离。凭证从 starhub 的部署配置注入。
- MinIO 的兼容性配置（path-style 寻址、checksum 设为 `when_required`、启动自检）见服务端文档 §4.6。
- 大文件下载可以走预签名 URL 直连 MinIO，和 starhub 现有 LFS 下载返回对象存储地址的方式一致，对 HF SDK 很友好。

**大文件策略**：
- 阶段 1 中，Scion 仓库里**仍然保存 LFS 指针文件**，实际数据继续留在 starhub 的 LFS 和 Xnet 存储中。LFS 相关接口、`lfs_meta_objects` 表和 Xnet **全部不需要改**。
- 后续可以把大文件改为 Scion 原生的分块 blob，通过 `Sha256` 派生数据继续兼容 LFS 和 HF 协议。
- **迁移时不复制数据**：S4 阶段引入一种「外部 blob」对象类型（`ExternalBlob{bucket, key, sha256, size}`），直接引用 starhub 在 MinIO 中已有的 LFS 对象，按需在后台转成 Scion 原生的分块存储。这样迁移 TB 级模型仓库时，不需要第二次全量复制。

---

## 6. 分阶段落地

| 阶段 | 内容 | starhub 侧 | Scion 侧 | 用户可见效果 |
|---|---|---|---|---|
| **S0 准备** | 后端路由层上线，全部仓库仍走 Gitaly | `storage_backend` 字段、`RoutingGitServer`、配置项 | — | 无变化（安全上线） |
| **S1 新仓库试点** | 指定组织或类型的新仓库创建在 Scion 上 | scion adapter（读写 API、分支、提交、文件）、NATS 消费者、pre_land 接口、JWT/JWKS | RepoAdmin、JWKS、pre-land webhook、事件、GitSha/LastCommitPerEntry/TreeStats、Archive | Web 页面、HF SDK、`scn` 都可用；git CLI 会提示「请使用 scn」（同 Xnet 的处理） |
| **S2 迁移** | 存量仓库双向迁移 | 迁移工作流、管理后台 | ImportService | 可以批量迁移，也可以回滚 |
| **S3 Git 兼容** | Scion 仓库支持 git clone/push | `/allowed` 和 smart HTTP 按仓库返回 gateway 地址 | git-gateway | git CLI 对 Scion 仓库透明可用 |
| **S4 原生大文件** | LFS 数据迁入 Scion 的分块 blob | LFS handler 路由到 Scion | Sha256 派生数据、分块存储 | 大模型文件按需拉取、全局去重 |

---

## 7. 风险与待确认

1. **Gitaly 协议兼容的深度**：sidechannel、gitlab-shell 版本差异等，需要先用 Gitaly 的 proto 做协议级测试。git-gateway 是工作量最大的部分，因此放在 S3。
2. **假定提交 ID 是 40 位十六进制的代码**：需要在 starhub 中全面检查（前端路由、缓存 key、HF revision、`sync_version`），用 `GitSha` 映射兜底。
3. **小仓库的数量规模**：starhub 可能有几十万个仓库。Scion 元数据表都以 `repo_id` 为键，但每个仓库的固定开销（eager 派生 worker 的分片、land leader）需要按需懒加载，不能为每个仓库常驻。
4. **SaaS / EE / CE 三个版本**：`RoutingGitServer` 应放在共享代码里；Scion 后端可以先只在 EE/SaaS 中开启（照搬 Xnet 的 `_ce/_ee` 文件划分方式）。
5. **权限模型**：starhub 有 rebac、组织继承、`org_inherit_blocked` 等规则。建议先用「实时鉴权回调 + 短 TTL 缓存」的方式，避免把复杂的权限计算放进 token 的 claims。
---

## 8. 与 CSGLite 的结合（后期）

![Scion × CSGLite](./images/arch-04-csglite-integration.png)

CSGLite（`~/git/csglite`）是用 Go 写的单二进制本地推理工具：嵌入 SQLite（`modernc.org/sqlite`），推理用 llama.cpp/MLX，局域网集群靠 mDNS 发现。目前它通过 starhub 的 HF 兼容接口 `/api/v1/{type}/{ns}/{name}/resolve/{path}` 下载模型（`internal/csghub/download.go`）。

下面四种方式可以叠加使用，由浅入深：

| # | 方式 | CSGLite 改动 | 价值 |
|---|---|---|---|
| ① | **间接接入**：CSGLite 照旧调用 `/resolve`，starhub 内部把请求路由到 Scion，大文件 302 到 MinIO 预签名地址 | 无 | 仓库迁到 Scion 后 CSGLite 立即可用 |
| ② | **原生拉取**：内置 Scion 拉取器（Go，按 proto 生成），按分片拉取并校验 BLAKE3 | 新增拉取器 | 不同模型之间相同的分片只下载一次（量化版、微调版之间重复很多）；断点续传是协议自带的 |
| ③ | **局域网对等缓存**：每个节点都是 L1，邻居之间用同一套 `ObjectService` 互相取数据，取不到再回源 L2 | 新增读缓存，接入现有的 mDNS 集群 | 取代现有「节点之间复制模型」的做法；按对象 hash 共享，天然能校验 |
| ④ | **本地 Lite 版本库**：内嵌 `scion server --lite`（SQLite + 本地文件系统，与 CSGLite 技术栈相同） | 可选嵌入 | 本地微调的模型、数据集、Agent 工作区可以做版本管理，再 land 到 CSGHub |
