# 认证与鉴权

Scion 默认不要求认证（适合本机和 Lite 模式）。生产环境用 `--auth-required` 开启。认证与鉴权只用标准协议（JWT / JWKS、RFC 7662 令牌内省、RFC 8628 device flow、OPA 风格的鉴权回调），不包含针对某个系统的代码。接入 starhub-server、Keycloak、Casdoor 或 OPA 都只需要配置。

## 开启认证

```sh
scion admin jwt-keygen > /etc/scion/jwt.pem          # 签发访问令牌用的 ES256 私钥，集群各节点共用
scion server --auth-required --jwt-key /etc/scion/jwt.pem --auth-config /etc/scion/auth.toml ...
```

| 参数 | 环境变量 | 说明 |
|---|---|---|
| `--auth-required` | `SCION_AUTH_REQUIRED` | 要求认证。没有有效令牌的请求只能访问策略允许 `anonymous` 的仓库 |
| `--jwt-key FILE` | `SCION_JWT_KEY_FILE` | 签发访问令牌的私钥。没有它也能用 PAT 和外部令牌，但不能换取访问令牌、不能用浏览器登录 |
| `--auth-config FILE` | `SCION_AUTH_CONFIG` | `auth.toml`：外部签发方、令牌内省、鉴权回调、OIDC |

配置有误（例如密钥读不到、环境变量没设）时服务端拒绝启动。

## 令牌

请求在 metadata `authorization: Bearer <token>` 中带令牌。服务端接受以下几种：

| 令牌 | 形式 | 校验 |
|---|---|---|
| 访问令牌 | ES256 JWT，默认 15 分钟 | 服务端自己的公钥 |
| PAT / service token | `scn_pat_…`，可设有效期 | 元数据库只存 SHA-256，可随时吊销 |
| 刷新令牌 | `scn_refresh_…`，30 天 | 只能用来换取访问令牌（`scn login` 得到） |
| 外部 JWT | 受信任签发方的 JWT | JWKS（RS*/PS*/ES*）或 HS256 共享密钥 |
| 外部不透明令牌 | 任意 | RFC 7662 内省接口 |

客户端通常用长期凭据（PAT、刷新令牌、外部 JWT）调用 `AuthService.ExchangeToken` 换取访问令牌，并在过期前 2 分钟自动重新换取（SDK 的 `Auth::Exchange`，`scn` 默认如此）。服务端没有签名密钥时，客户端直接发送原凭据。

管理令牌：

```sh
scion admin token create --subject ci-bot --group ci --ttl 90d --description "release pipeline"
scion admin token list [--subject ci-bot]
scion admin token revoke <id>
```

这些命令直接读写元数据库，存储参数与 `scion server` 相同，例如 `--meta postgres://…` 或 `--lite --data-dir DIR`。也可以通过 `AdminService` 远程管理。

## 访问控制策略

策略是一组「允许」规则，存在元数据库里，所有节点在 30 秒内生效：

```sh
scion admin policy set acl.toml     # 校验后保存
scion admin policy show
```

```toml
[[rule]]
subjects = ["group:eng"]              # user:NAME、group:NAME、"*"（任何已认证身份）、"anonymous"
repos    = ["mono", "models/*"]       # 仓库名，* 匹配任意字符（包括 /）
actions  = ["land"]                   # read < write < land < admin，高的包含低的

[[rule]]
subjects = ["user:release-bot"]
repos    = ["mono"]
actions  = ["land"]
refs     = ["release/*"]              # 只约束合入的目标 ref

[[rule]]
subjects = ["group:contractors"]
repos    = ["mono"]
actions  = ["write"]
paths    = ["src/ui", "docs"]         # 目录前缀；不写表示整个仓库

[[rule]]
subjects = ["anonymous", "*"]
repos    = ["public/*"]
actions  = ["read"]

[[rule]]
subjects = ["group:admins"]
repos    = ["*"]
actions  = ["admin"]                  # repos 含 "*" 的 admin 规则授予系统管理（策略、令牌）
```

各接口需要的权限：

| 操作 | 需要 |
|---|---|
| 按 id 读对象（`GetObjects`、检出） | 整个仓库的 read（带 `paths` 的规则不够：按 id 读对象无法判断路径） |
| 读文件、Stat、Blame、按路径的 Log / Diff | 该路径的 read |
| 列目录 | 仓库中任意 read；只显示可读的条目和通向可读路径的上级目录 |
| 不限路径的 Diff | 只返回可读路径的改动 |
| 读 ref、Log、祖先判断、ResolveRevision | 仓库中任意 read |
| 上传对象、`HasObjects` | 仓库中任意 write（或整个仓库的 read） |
| 创建提交 | 每个改动路径的 write |
| 合入 | 目标 ref 的 land |
| 管理接口 | 系统管理 |

没有设置策略、也没有配置鉴权回调时，已认证的身份可以读写和合入所有仓库；系统管理始终需要策略明确授予。第一份策略用 `scion admin policy set` 直接写入元数据库。

- **空策略**：显式保存的空策略（没有任何规则）拒绝一切，不等于「没有设置策略」。
- **策略加载失败**：例如元数据库不可用，或库里的策略无法解析。
  - 启动时加载失败，`scion server --auth-required` 拒绝启动。
  - 嵌入使用 `scion-api` 时，服务端在加载成功前对所有请求返回 `UNAVAILABLE`，每秒重试。
  - 运行中重新加载失败，保留上一份策略。

## auth.toml

```toml
[token]                                # 服务端签发的访问令牌
issuer = "https://scion.example.com"   # JWT 的 iss，默认 "scion"
ttl = "15m"

[[issuer]]                             # 受信任的外部签发方，可以有多个
issuer = "https://sso.example.com/realms/dev"
jwks_url = "https://sso.example.com/realms/dev/protocol/openid-connect/certs"
audience = "scion"                     # 可选
subject_claim = "preferred_username"   # 默认 sub
groups_claim = "groups"                # 可选，字符串数组或逗号分隔
repos_claim = "repos"                  # 可选：{"models/a": "write"} 形式的仓库授权

[introspection]                        # RFC 7662：外部签发的不透明令牌
url = "https://sso.example.com/oauth2/introspect"
client_id = "scion"
client_secret_env = "SCION_INTROSPECT_SECRET"   # HTTP Basic；也可以用 bearer_env
cache_ttl = "60s"

[authz_webhook]                        # 外部鉴权（OPA 兼容）
url = "http://opa:8181/v1/data/scion/allow"
cache_ttl = "30s"
timeout = "2s"
on_error = "closed"                    # 回调不可用时：closed 拒绝（默认），open 只按 ACL 判断

[oidc]                                 # scn login（浏览器登录）
issuer = "https://sso.example.com/realms/dev"
client_id = "scn"
subject_claim = "preferred_username"
groups_claim = "groups"
```

密钥一律可以用 `*_env`（环境变量）或 `*_file`（文件）给出，不必写在配置文件里。

### 外部鉴权回调

服务端在 ACL 和令牌自带的授权都不允许时调用回调。请求与响应格式与 OPA 的 REST 数据接口相同：

```json
POST /v1/data/scion/allow
{"input": {"subject": "alice", "groups": ["eng"], "authenticated": true,
           "repo": "models/ns/qwen", "action": "write", "ref": null, "path": "README.md"}}

{"result": true}                 // 或 {"result": {"allow": true}}；其它（包括没有 result）为拒绝
```

允许的结果缓存 `cache_ttl`，拒绝的结果缓存 5 秒。直接用 OPA 时，一条最简单的策略：

```rego
package scion
default allow := false
allow if input.action == "read"; startswith(input.repo, "public/")
allow if "eng" in input.groups
```

## 接入 starhub-server

starhub-server 的实际情况：
- JWT 用 HS256 共享密钥签发（`STARHUB_JWT_SIGNING_KEY`），`iss` 是 `OpenCSG`，用户名在 `current_user` claim。
- 另有不透明的 access token，由 user 服务校验。
- 权限在 starhub 自己的数据库里按 rebac 计算，区分公开和私有仓库。

对应的配置：

```toml
[[issuer]]
issuer = "OpenCSG"
hs256_secret_env = "STARHUB_JWT_SIGNING_KEY"
subject_claim = "current_user"

[introspection]                        # starhub 的 access token
url = "http://starhub-server:8080/api/v1/internal/scion/introspect"
bearer_env = "STARHUB_API_TOKEN"
subject_claim = "username"

[authz_webhook]                        # 权限由 starhub 的 rebac 决定（含公开仓库的匿名读）
url = "http://starhub-server:8080/api/v1/internal/scion/authz"
bearer_env = "STARHUB_API_TOKEN"
cache_ttl = "30s"
```

starhub 侧需要提供两个内部接口，都只是对现有逻辑的薄包装：
- **`introspect`**：按 RFC 7662 接收 `token=…`，内部调用 `VerifyByAccessToken`，返回 `{"active": true, "username": "…"}`。
- **`authz`**：按上面的回调格式接收请求，把 Scion 的仓库名（例如 `models/ns/name`）映射回 starhub 的仓库，再调用 `CheckUserRepoPermission`（read / write / admin）。
  - `land` 映射为 write，或者按需要再加一层保护分支规则。
  - `anonymous` 只对公开仓库的 read 返回允许。

`scn login` 可以走 starhub 使用的 Casdoor OIDC（`[oidc]`），也可以用 `scn login --token <starhub access token>`。

## 缓存与 daemon

区域缓存（`scion cache`）和本机 daemon 不解析令牌。
- **读权限**：返回对象前（包括本地命中），用调用者自己的令牌向服务端调用 `CheckAccess(repo, read)`，结果按（令牌, 仓库）缓存 60 秒。吊销令牌或修改策略后，缓存层最多 60 秒内生效。
- **daemon 离线**：服务端不可达（而不是拒绝）时，24 小时内确认过的身份可以继续读本机缓存。
- **预热**：区域缓存在服务端要求认证时用 `--warm-token`（`SCION_CACHE_WARM_TOKEN`）指定预热用的身份。

## 客户端

```sh
scn login                 # 在浏览器中登录（服务端配置了 [oidc]）
scn login --token TOKEN   # 用 PAT 或外部令牌；--with-token 从标准输入读取
scn whoami
scn logout
```

- **凭据存放**：按服务端地址保存。macOS 存进钥匙串，其它平台存进 `~/.config/scion/credentials.json`（权限 0600）。`SCION_CREDENTIALS_FILE` 可以指定文件。
- **CI**：设置 `SCION_TOKEN`（PAT 或外部令牌），优先于保存的凭据。
