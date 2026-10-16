# 从 git 导入

把已有的 git 仓库连同全部历史导入 Scion。导入后，每个 git 提交对应一个 Scion 提交，作者、提交者、时间、时区和提交说明都原样保留。

- [前提](#前提)
- [快速开始](#快速开始)
- [命令参考](#命令参考)
- [导入了什么](#导入了什么)
- [增量导入](#增量导入)
- [验证导入结果](#验证导入结果)
- [限制](#限制)
- [常见问题](#常见问题)

## 前提

1. **本机装有 git**。导入器通过 git 命令行读取仓库（`rev-list`、`diff-tree`、`cat-file`），任何较新的 git 版本都可以。
2. **服务端上已有目标仓库**。启动服务端时用 `--repo` 创建，重复指定不会报错：

   ```sh
   scion server --lite --data-dir ./.scion-data --repo mono
   ```

3. **装好 `scn`**：`cargo install --path client/cli`。

导入只通过公开协议写入，和普通用户提交走同一条服务端校验，所以可以导入到任何 Scion 服务端（本机的 Lite 模式，或接入 PostgreSQL 与 S3 的集群）。

## 快速开始

```sh
scn import git ~/src/my-repo --repo mono
# imported 1287 commits into mono (main at c1a3f09b2c41)
```

然后就可以克隆和查看：

```sh
scn clone mono && cd mono
scn ls main:
scn cat main:README.md
```

## 命令参考

```text
scn import git <PATH> --repo <NAME> [--git-ref <REF>] [--ref <REF>] [--endpoint <URL>] [--json]
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `<PATH>` | — | git 仓库目录（工作目录或裸仓库均可） |
| `--repo` | — | 目标 Scion 仓库，必填；也可用环境变量 `SCION_REPO` |
| `--git-ref` | `main` | 要导入的 git 引用：分支、标签或提交 sha |
| `--ref` | `main` | 导入后更新的 Scion ref |
| `--endpoint` | `http://127.0.0.1:7080` | 服务端地址；也可用环境变量 `SCION_ENDPOINT` |
| `--json` | — | 输出 JSON，schema 为 `scion.cli.import/v1` |

`--json` 输出示例：

```json
{"schema":"scion.cli.import/v1","repo":"mono","ref":"main","commits":1287,"skipped_gitlinks":0,"tip":"c1a3f09b2c41…"}
```

退出码：`0` 成功；`1` 一般错误（不是 git 仓库、git 引用不存在、历史分叉等）；`2` 用法错误（如缺少 `--repo`）；`4` 可重试的网络错误；`5` 认证失败。

需要导入多个分支时，对每个分支各执行一次，用 `--git-ref` 和 `--ref` 指定对应关系：

```sh
scn import git ~/src/my-repo --repo mono --git-ref main --ref main
scn import git ~/src/my-repo --repo mono --git-ref release/1.x --ref release/1.x
```

两个分支共有的提交不会重复上传，也会得到相同的 Scion 提交 id。

## 导入了什么

| git | Scion | 说明 |
|---|---|---|
| 提交 | 提交 | 按拓扑顺序导入，父提交总是先于子提交 |
| 父提交（含合并提交的多个父提交） | `parents` | 顺序不变，第一个是 first parent |
| 相对第一个父提交的改动 | `delta` | 根提交相对空目录树 |
| 作者 / 提交者 | `author` / `committer` | 姓名、邮箱、时间（精确到秒）和时区都保留 |
| 提交说明 | `message` | 原样保留；非 UTF-8 字节按有损方式转换 |
| 提交 sha | 扩展字段 `git.sha1` | 20 字节，可据此从 Scion 提交反查 git 提交 |
| — | `change_id` | 取 git sha 的前 16 字节 |
| 普通文件 `100644`（及旧格式 `100664`） | 文件 | |
| 可执行文件 `100755` | 可执行文件 | |
| 符号链接 `120000` | 符号链接 | 链接目标作为内容 |
| 子模块 `160000` | — | 跳过，数量计入 `skipped_gitlinks` |

**结果是确定的**：同样的 git 历史无论导入多少次、导入到哪个服务端，都会得到完全相同的 Scion 提交 id。已经上传过的文件内容不会重复上传，所以重复执行导入的代价很小。

## 增量导入

git 仓库有了新提交以后，再执行一次同样的命令：

```sh
scn import git ~/src/my-repo --repo mono
```

导入器会复用已有的提交，只导入新增的部分，然后从 Scion ref 的当前位置，沿着第一父提交链把 ref 逐个快进到最新提交。

如果 git 历史被改写过（`rebase`、`commit --amend`、`reset` 后强推），Scion ref 当前指向的提交已不在新历史中，导入会失败并报 `not in the imported first-parent history`，ref 保持不变。这时可以导入到一个新 ref：

```sh
scn import git ~/src/my-repo --repo mono --ref main-rewritten
```

## 验证导入结果

- 查看文件：`scn ls main:src`、`scn cat main:src/lib.rs`。
- 按提交读取历史版本：`scn cat <提交 id>:path`。提交 id 可以从 `scn import git --json` 输出的 `tip`，或工作副本中 `scn status --json` 的 `base` 字段得到。
- 对照 git：每个 Scion 提交的扩展字段 `git.sha1` 记录了对应的 git 提交，供程序通过 SDK 查询（读取提交对象后用 `scion_object::Changeset::decode` 解码）。

导入器的正确性由差分测试保证（`client/gitimport/tests/differential.rs`）。测试用真实 git 构造一段历史，覆盖改名、删除、可执行位、符号链接、二进制文件、中文路径、分支合并、目录与文件互换、子模块和空提交；导入后逐个提交比对文件列表、mode、内容、时间、时区和父提交，结果必须与 git 完全一致。

## 限制

| 限制 | 说明 |
|---|---|
| 子模块 | 跳过，不导入子模块指针 |
| 提交签名 | GPG / SSH 签名（`gpgsig`）不导入。Scion 提交的 id 由内容决定，与签名无关 |
| 标签 | 不导入标签对象；需要时用 `--git-ref <tag> --ref <name>` 把标签指向的历史导入为一个 ref |
| 改名 | 记为「删除 + 新增」。内容不变的文件不会重复上传 |
| 单个文件大小 | 受服务端上传上限约束，默认 256 MiB；更大的文件会导致导入失败。分块存储会在后续版本支持更大的文件 |
| Git LFS | 导入的是 LFS 指针文件本身，不会下载 LFS 对象 |
| 文件名 | 必须是合法的 UTF-8；否则导入失败并报出路径 |
| 速度 | 每个提交调用一次 `git diff-tree` 和一次服务端请求，适合中小型仓库。超大仓库的批量迁移（如接入 starhub）后续改为服务端直写 |

## 常见问题

**`repository "mono" not found`**
服务端上还没有这个仓库。启动服务端时加上 `--repo mono`。

**`cannot connect to http://127.0.0.1:7080`**
服务端没有启动，或地址不对。用 `--endpoint` 或环境变量 `SCION_ENDPOINT` 指定地址。如果设置了 HTTP 代理（`http_proxy`），访问本机服务端时请设置 `NO_PROXY=127.0.0.1,localhost`。

**`not a git repository`** 或 **`git rev-list … failed`**
`<PATH>` 不是 git 仓库，或 `--git-ref` 指定的引用不存在。用 `git -C <PATH> branch -a` 查看可用的分支。

**导入中途失败了怎么办？**
直接重新执行。已导入的提交和已上传的内容都会被复用，Scion ref 只在全部提交都导入成功之后才会更新。
