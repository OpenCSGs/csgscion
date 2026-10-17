//! `scion`：服务端单二进制，按子命令区分角色。
//!
//! 目前有 `server`、`cache`、`admin` 和 `version`；`deriver`、`janitor` 按里程碑逐个加入。

mod authcmd;
mod fsck;
mod opscmd;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_api::{App, ServerConfig};

#[derive(Debug, Parser)]
#[command(name = "scion", version, about = "Scion source control server")]
struct Cli {
    /// 日志格式。
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text, env = "SCION_LOG_FORMAT")]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// 运行 L3 服务端。
    Server(ServerArgs),
    /// 运行区域对象缓存（L2）。
    Cache(CacheArgs),
    /// 打印版本与协议版本。
    Version,
    /// 运维命令。
    #[command(subcommand)]
    Admin(AdminCmd),
}

#[derive(Debug, Subcommand)]
enum AdminCmd {
    /// 为旧数据补建派生索引（父提交与跳表、PathHistory、GitSha）。可重复执行。
    /// 运行中的服务端会缓存提交记录：请在启动服务端之前运行，或运行后重启服务端。
    Reindex(ReindexArgs),
    /// 生成签发访问令牌用的 ES256 私钥（PKCS#8 PEM，输出到标准输出）。
    JwtKeygen,
    /// 管理 PAT / service token。
    Token {
        #[command(subcommand)]
        cmd: authcmd::TokenCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 查看或设置访问控制策略。
    Policy {
        #[command(subcommand)]
        cmd: authcmd::PolicyCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// redact 文件内容：对象 id 与历史不变，读取返回 REDACTED（docs/guide/redaction.md）。
    Redact {
        #[command(flatten)]
        args: opscmd::RedactArgs,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 撤销一次 redaction（物理删除之前）。
    Unredact {
        redaction_id: String,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 安排物理删除 redact 过的对象（宽限期后由服务端执行，或 --now 立即执行）。
    Purge {
        #[command(flatten)]
        args: opscmd::PurgeArgs,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 列出 redaction 记录。
    Redactions {
        #[arg(long)]
        repo: Option<String>,
        #[arg(long = "id", value_name = "REDACTION_ID")]
        redaction_id: Option<String>,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 查看租约（合入 leader 等）。
    Lease {
        #[command(subcommand)]
        cmd: opscmd::LeaseCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// 校验仓库数据：ref、提交、目录树与文件的引用闭包，对象索引与对象存储。只读。
    Fsck {
        /// 只检查这个仓库（可以重复）；不指定时检查全部。
        #[arg(long = "repo", value_name = "NAME")]
        repos: Vec<String>,
        /// 读出每个文件并校验哈希（默认只确认存在）。
        #[arg(long)]
        verify: bool,
        #[command(flatten)]
        storage: StorageArgs,
    },
}

#[derive(Debug, clap::Args)]
struct ReindexArgs {
    /// 只处理这个仓库；不指定时处理全部仓库。
    #[arg(long = "repo", value_name = "NAME")]
    repo: Option<String>,
    #[command(flatten)]
    storage: StorageArgs,
}

/// 存储后端选项，server 与 admin 共用。
#[derive(Debug, clap::Args)]
struct StorageArgs {
    /// Lite 档：SQLite + 本地目录，单进程、零外部依赖。需要 --data-dir。
    #[arg(long, global = true, requires = "data_dir", conflicts_with_all = ["meta", "blobs"])]
    lite: bool,
    /// Lite 档的数据目录（meta.db 与 objects/）。
    #[arg(long, global = true, value_name = "DIR", env = "SCION_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// 元数据后端：mem、sqlite://PATH、postgres://…
    #[arg(long, global = true, value_name = "URL", default_value = "mem", env = "SCION_META")]
    meta: String,
    /// 对象存储：mem、目录路径、s3://bucket[/prefix]（连接参数见 SCION_S3_* 环境变量）。
    #[arg(long, global = true, value_name = "SPEC", default_value = "mem", env = "SCION_BLOBS")]
    blobs: String,
}

impl StorageArgs {
    fn specs(&self) -> anyhow::Result<(storage::MetaSpec, storage::BlobSpec)> {
        Ok(match (&self.data_dir, self.lite) {
            (Some(dir), true) => storage::lite(dir),
            _ => (storage::parse_meta(&self.meta)?, storage::parse_blobs(&self.blobs)?),
        })
    }
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// gRPC 监听地址。
    #[arg(long, default_value = "127.0.0.1:7080", env = "SCION_LISTEN")]
    listen: SocketAddr,
    /// 启动时确保存在的仓库（已存在则跳过），可重复指定。
    #[arg(long = "repo", value_name = "NAME")]
    repos: Vec<String>,
    /// 集群模式：本节点供其它节点访问的地址（如 http://10.0.0.5:7080）。多个节点共享同一个元数据库与
    /// 对象存储时，每个 ref 的合入由持有租约的节点处理，其它节点转发给它。
    #[arg(long, value_name = "URL", env = "SCION_ADVERTISE_URL", conflicts_with = "lite")]
    advertise_url: Option<String>,
    /// 全局在途请求上限，超过时立即拒绝（客户端可重试）。
    #[arg(
        long,
        value_name = "N",
        default_value_t = 10_000,
        env = "SCION_MAX_CONCURRENT_REQUESTS"
    )]
    max_concurrent_requests: usize,
    /// 按仓库的写入速率限制：每秒 RATE 次，可选突发 BURST（默认等于 2×RATE），如 200 或 200/1000。
    #[arg(long, value_name = "RATE[/BURST]", value_parser = parse_write_rate, env = "SCION_REPO_WRITE_RATE")]
    repo_write_rate: Option<scion_api::WriteRate>,
    /// 推荐给客户端的只读端点（区域缓存 `scion cache` 的地址），可附带区域名：URL 或 URL@REGION。
    /// 可重复指定，客户端从中选择读取对象的地址。
    #[arg(long = "read-endpoint", value_name = "URL[@REGION]", value_parser = parse_read_endpoint,
        env = "SCION_READ_ENDPOINTS", value_delimiter = ',')]
    read_endpoints: Vec<scion_api::ReadEndpoint>,
    /// WatchRefs 轮询元数据库的间隔（集群中其它节点的合入通知到订阅者的最大延迟），如 500ms、2s。
    #[arg(long, value_name = "DURATION", default_value = "1s", value_parser = parse_duration,
        env = "SCION_REF_POLL_INTERVAL")]
    ref_poll_interval: std::time::Duration,
    /// redaction 清除的默认宽限期（AdminService.Purge 未指定时），期间可以撤销。
    #[arg(long, value_name = "DURATION", default_value = "24h", value_parser = parse_duration,
        env = "SCION_PURGE_GRACE")]
    purge_grace: std::time::Duration,
    #[command(flatten)]
    auth: authcmd::AuthArgs,
    #[command(flatten)]
    storage: StorageArgs,
}

#[derive(Debug, clap::Args)]
struct CacheArgs {
    /// gRPC 监听地址。
    #[arg(long, default_value = "127.0.0.1:7090", env = "SCION_CACHE_LISTEN")]
    listen: SocketAddr,
    /// 上游 L3 服务端地址，可重复指定（同一集群的多个节点，互为副本）。
    #[arg(
        long = "upstream",
        value_name = "URL",
        required = true,
        env = "SCION_CACHE_UPSTREAMS",
        value_delimiter = ','
    )]
    upstreams: Vec<String>,
    /// 本节点供其它缓存节点访问的地址；与 --peer 一起按 rendezvous 哈希分片。
    #[arg(long, value_name = "URL", env = "SCION_CACHE_ADVERTISE_URL")]
    advertise_url: Option<String>,
    /// 区域内其它缓存节点的地址，可重复指定。
    #[arg(
        long = "peer",
        value_name = "URL",
        env = "SCION_CACHE_PEERS",
        value_delimiter = ',',
        requires = "advertise_url"
    )]
    peers: Vec<String>,
    /// 内存层容量，如 512MiB、8GiB。
    #[arg(long, value_name = "SIZE", default_value = "1GiB", value_parser = parse_size, env = "SCION_CACHE_MEMORY")]
    memory: u64,
    /// 磁盘层目录（建议放在 NVMe 上）；不指定时只用内存。
    #[arg(long, value_name = "DIR", env = "SCION_CACHE_DISK_DIR")]
    disk_dir: Option<PathBuf>,
    /// 磁盘层容量。
    #[arg(long, value_name = "SIZE", default_value = "64GiB", value_parser = parse_size, env = "SCION_CACHE_DISK_SIZE")]
    disk_size: u64,
    /// 超过这个大小的对象不缓存（最大 16MiB）。
    #[arg(long, value_name = "SIZE", default_value = "8MiB", value_parser = parse_size)]
    max_object: u64,
    /// 预热：订阅上游的 ref 变化并预取变化的目录树。REPO 或 REPO:REF[,REF...]，可重复指定。
    #[arg(
        long = "warm",
        value_name = "REPO[:REFS]",
        env = "SCION_CACHE_WARM",
        value_delimiter = ';'
    )]
    warm: Vec<scion_cache::WarmTarget>,
    /// 预热时连同不超过这个大小的文件一起预取；0 只预取目录树。
    #[arg(long, value_name = "SIZE", default_value = "0", value_parser = parse_size)]
    warm_blob_max: u64,
    /// 关闭对冲（慢请求向另一个上游重发）。
    #[arg(long)]
    no_hedge: bool,
    /// 预热时使用的令牌（服务端要求认证时需要），建议用环境变量传入。
    #[arg(long, value_name = "TOKEN", env = "SCION_CACHE_WARM_TOKEN", hide_env_values = true)]
    warm_token: Option<String>,
}

/// 字节数：纯数字，或带 K/M/G/T（KiB/MiB/…，均按 1024 进位）后缀。
fn parse_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid size {s:?}; expected e.g. 512MiB"))?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        _ => return Err(format!("invalid size unit in {s:?}; use K, M, G or T")),
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("size {s:?} is too large"))
}

/// 时长：数字加 ms、s、m 后缀。
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration {s:?}; expected e.g. 500ms or 2s"))?;
    match unit {
        "ms" => Ok(std::time::Duration::from_millis(n)),
        "s" => Ok(std::time::Duration::from_secs(n)),
        "m" => Ok(std::time::Duration::from_secs(n * 60)),
        "h" => Ok(std::time::Duration::from_secs(n * 3600)),
        "d" => Ok(std::time::Duration::from_secs(n * 86_400)),
        _ => Err(format!("invalid duration unit in {s:?}; use ms, s, m, h or d")),
    }
}

fn parse_read_endpoint(s: &str) -> Result<scion_api::ReadEndpoint, String> {
    let (url, region) = match s.rsplit_once('@') {
        Some((u, r)) => (u.trim(), r.trim()),
        None => (s.trim(), ""),
    };
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!("read endpoint must be an http:// or https:// URL, got {url:?}"));
    }
    Ok(scion_api::ReadEndpoint {
        url: url.trim_end_matches('/').to_owned(),
        region: region.to_owned(),
    })
}

fn cache_config(args: &CacheArgs) -> anyhow::Result<scion_cache::CacheConfig> {
    let size = |v: u64, what: &str| usize::try_from(v).with_context(|| format!("{what} is too large"));
    Ok(scion_cache::CacheConfig {
        upstreams: args.upstreams.clone(),
        advertise_url: args.advertise_url.clone(),
        peers: args.peers.clone(),
        storage: scion_cache::StorageConfig {
            memory_bytes: size(args.memory, "--memory")?,
            disk: match &args.disk_dir {
                Some(dir) => Some(scion_cache::DiskConfig {
                    dir: dir.clone(),
                    capacity_bytes: size(args.disk_size, "--disk-size")?,
                }),
                None => None,
            },
            max_object_bytes: size(args.max_object, "--max-object")?,
        },
        pool: scion_cache::PoolConfig {
            hedge: !args.no_hedge,
            ..scion_cache::PoolConfig::default()
        },
        warm: args.warm.clone(),
        warm_options: scion_cache::WarmOptions {
            token: match &args.warm_token {
                Some(t) => Some(
                    format!("Bearer {}", t.trim())
                        .parse()
                        .map_err(|_| anyhow::anyhow!("--warm-token contains invalid characters"))?,
                ),
                None => None,
            },
            max_blob_bytes: args.warm_blob_max,
            ..scion_cache::WarmOptions::default()
        },
        ..scion_cache::CacheConfig::default()
    })
}

fn parse_write_rate(s: &str) -> Result<scion_api::WriteRate, String> {
    let (rate, burst) = match s.split_once('/') {
        Some((r, b)) => (r, Some(b)),
        None => (s, None),
    };
    let rate: u32 = rate
        .trim()
        .parse()
        .ok()
        .filter(|r| *r > 0)
        .ok_or_else(|| format!("invalid rate {rate:?}; expected a positive integer"))?;
    let burst = match burst {
        Some(b) => b
            .trim()
            .parse()
            .ok()
            .filter(|b| *b > 0)
            .ok_or_else(|| format!("invalid burst {b:?}; expected a positive integer"))?,
        None => rate.saturating_mul(2),
    };
    Ok(scion_api::WriteRate {
        per_second: rate,
        burst,
    })
}

fn init_tracing(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("SCION_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // 写日志失败（例如日志采集进程重启导致管道断开）时直接丢弃这条日志。默认行为是改用 eprintln!
    // 报告，而 stderr 已坏时 eprintln! 会 panic，导致正在处理的请求失败。
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .log_internal_errors(false);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

fn version_text() -> String {
    format!(
        "scion {} (protocol {})",
        env!("CARGO_PKG_VERSION"),
        scion_proto_version()
    )
}

fn scion_proto_version() -> &'static str {
    // scion-api 依赖 scion-proto；这里通过 API 层暴露的常量避免二进制直接依赖生成代码
    scion_api::PROTOCOL_VERSION
}

/// 打开存储并确保 `--repo` 指定的仓库存在。
async fn build_app(args: &ServerArgs, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<(App, String)> {
    let cluster = match &args.advertise_url {
        Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            Some(scion_land::ClusterConfig::new(url.trim_end_matches('/')))
        }
        Some(url) => anyhow::bail!("--advertise-url must be an http:// or https:// URL, got {url:?}"),
        None => None,
    };
    let auth = args.auth.guard_config()?;
    let (meta_spec, blob_spec) = args.storage.specs()?;
    let (meta, blobs) = storage::open(&meta_spec, &blob_spec, env).await?;
    // 提前校验认证配置（密钥、外部签发方的密钥来源）和库里的访问控制策略，有误时拒绝启动
    let guard =
        scion_api::scion_auth::Guard::build(auth.clone(), meta.clone(), scion_api::scion_auth::remote::env_lookup())
            .map_err(|e| anyhow::anyhow!("invalid auth configuration: {e}"))?;
    if auth.required {
        guard
            .reload_policy()
            .await
            .map_err(|e| anyhow::anyhow!("loading the access policy: {e}"))?;
    }
    if auth.required && auth.signing_key_pem.is_none() {
        tracing::warn!(
            "authentication required but no --jwt-key: access tokens cannot be issued (PATs and external tokens still work)"
        );
    }
    for repo in &args.repos {
        match meta.create_repo(repo).await {
            Ok(()) | Err(scion_store::Error::RepoAlreadyExists(_)) => {}
            Err(e) => return Err(e).with_context(|| format!("create repository {repo:?}")),
        }
    }
    let app = App {
        config: Arc::new(ServerConfig {
            cluster,
            max_concurrent_requests: args.max_concurrent_requests,
            repo_write_rate: args.repo_write_rate,
            read_endpoints: args.read_endpoints.clone(),
            auth,
            ref_poll_interval: args.ref_poll_interval,
            purge_grace: args.purge_grace,
            ..ServerConfig::default()
        }),
        meta,
        blobs,
    };
    Ok((app, storage::describe(&meta_spec, &blob_spec)))
}

/// 只打开元数据库（管理命令用）。
async fn open_meta(storage: &StorageArgs) -> anyhow::Result<Arc<dyn scion_store::MetaStore>> {
    let (meta_spec, blob_spec) = storage.specs()?;
    let (meta, _) = storage::open(&meta_spec, &blob_spec, |k| std::env::var(k).ok()).await?;
    Ok(meta)
}

/// `scion admin reindex`：返回每个仓库的一行摘要。
async fn reindex(args: &ReindexArgs, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<Vec<String>> {
    let (meta_spec, blob_spec) = args.storage.specs()?;
    let (meta, blobs) = storage::open(&meta_spec, &blob_spec, env).await?;
    let repos = match &args.repo {
        Some(r) => vec![r.clone()],
        None => meta.list_repos().await?,
    };
    let mut out = Vec::with_capacity(repos.len());
    for repo in repos {
        let stats = scion_query::reindex(meta.as_ref(), blobs.as_ref(), &repo)
            .await
            .with_context(|| format!("reindex {repo:?}"))?;
        out.push(scion_query::describe(&stats, &repo));
    }
    Ok(out)
}

/// 立即注册 SIGINT / SIGTERM（在打印「listening」之前，避免信号在注册前到达时进程被默认处理杀掉），
/// 返回收到信号时完成的 future。
fn shutdown_signal() -> impl std::future::Future<Output = ()> {
    #[cfg(unix)]
    let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    #[cfg(unix)]
    let int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    async move {
        #[cfg(unix)]
        {
            let wait = |s: Option<tokio::signal::unix::Signal>| async move {
                match s {
                    Some(mut s) => {
                        s.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                () = wait(term) => {},
                () = wait(int) => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        tracing::info!("shutdown signal received, draining");
    }
}

async fn run_server(args: ServerArgs) -> anyhow::Result<()> {
    let (app, backends) = build_app(&args, |k| std::env::var(k).ok()).await?;
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let addr = listener.local_addr()?;
    if backends.contains("meta=mem") || backends.contains("blobs=mem") {
        tracing::warn!("in-memory storage in use; data is lost on exit");
    }
    let shutdown = shutdown_signal();
    // 集成测试按这一行解析实际监听地址，修改时同步 tests/
    tracing::info!(%addr, repos = ?args.repos, %backends, "scion server listening");
    scion_api::serve(listener, app, shutdown).await?;
    tracing::info!("scion server stopped");
    Ok(())
}

async fn run_cache(args: CacheArgs) -> anyhow::Result<()> {
    let config = cache_config(&args)?;
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let addr = listener.local_addr()?;
    let (node, info, channels) = scion_cache::build(&config).await?;
    let shutdown = shutdown_signal();
    // 集成测试按这一行解析实际监听地址
    tracing::info!(%addr, upstreams = ?config.upstreams, peers = ?config.peers,
        disk = ?config.storage.disk.as_ref().map(|d| d.dir.display().to_string()), "scion cache listening");
    scion_cache::serve_node(listener, node, info, channels, &config, shutdown).await?;
    tracing::info!("scion cache stopped");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", version_text());
            Ok(())
        }
        Command::Server(args) => {
            init_tracing(cli.log_format);
            run_server(args).await
        }
        Command::Cache(args) => {
            init_tracing(cli.log_format);
            run_cache(args).await
        }
        Command::Admin(AdminCmd::Reindex(args)) => {
            for line in reindex(&args, |k| std::env::var(k).ok()).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(AdminCmd::JwtKeygen) => {
            print!("{}", scion_api::scion_auth::token::generate_signing_key());
            Ok(())
        }
        Command::Admin(AdminCmd::Token { cmd, storage }) => {
            let meta = open_meta(&storage).await?;
            for line in authcmd::token(&cmd, meta).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(AdminCmd::Policy { cmd, storage }) => {
            let meta = open_meta(&storage).await?;
            for line in authcmd::policy(&cmd, meta).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(cmd) => {
            let (lines, ok) = admin_ops(cmd, |k| std::env::var(k).ok()).await?;
            for line in lines {
                println!("{line}");
            }
            if ok {
                Ok(())
            } else {
                anyhow::bail!("fsck found problems")
            }
        }
    }
}

/// 运维类 admin 命令：返回输出与是否成功（fsck 发现问题时为 false）。
async fn admin_ops(cmd: AdminCmd, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<(Vec<String>, bool)> {
    let open = |storage: &StorageArgs| {
        let specs = storage.specs();
        let env = &env;
        async move {
            let (meta_spec, blob_spec) = specs?;
            storage::open(&meta_spec, &blob_spec, env).await
        }
    };
    Ok(match cmd {
        AdminCmd::Redact { args, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::redact(&args, meta, blobs, &env).await?, true)
        }
        AdminCmd::Unredact { redaction_id, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::unredact(&redaction_id, meta, blobs).await?, true)
        }
        AdminCmd::Purge { args, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::purge(&args, meta, blobs).await?, true)
        }
        AdminCmd::Redactions {
            repo,
            redaction_id,
            storage,
        } => {
            let (meta, blobs) = open(&storage).await?;
            (
                opscmd::redactions(repo.as_deref(), redaction_id.as_deref(), meta, blobs).await?,
                true,
            )
        }
        AdminCmd::Lease { cmd, storage } => {
            let (meta, _) = open(&storage).await?;
            (opscmd::lease(&cmd, meta).await?, true)
        }
        AdminCmd::Fsck { repos, verify, storage } => {
            let (meta, blobs) = open(&storage).await?;
            opscmd::run_fsck(&fsck::FsckOptions { repos, verify }, meta, blobs).await?
        }
        other => anyhow::bail!("not an operations command: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parse_sizes_and_durations() {
        assert_eq!(parse_size("1024"), Ok(1024));
        assert_eq!(parse_size("512MiB"), Ok(512 << 20));
        assert_eq!(parse_size("8gib"), Ok(8 << 30));
        assert_eq!(parse_size("2T"), Ok(2 << 40));
        assert_eq!(parse_size(" 4 k "), Ok(4096));
        assert_eq!(parse_size("7b"), Ok(7));
        assert!(parse_size("MiB").is_err());
        assert!(parse_size("5 parsecs").is_err());
        assert!(parse_size("99999999999T").is_err());
        assert_eq!(parse_duration("500ms"), Ok(std::time::Duration::from_millis(500)));
        assert_eq!(parse_duration("2s"), Ok(std::time::Duration::from_secs(2)));
        assert_eq!(parse_duration("3m"), Ok(std::time::Duration::from_secs(180)));
        assert_eq!(parse_duration("2h"), Ok(std::time::Duration::from_secs(7200)));
        assert_eq!(parse_duration("1d"), Ok(std::time::Duration::from_secs(86_400)));
        assert!(parse_duration("2w").is_err());
        assert!(parse_duration("s").is_err());
    }

    #[test]
    fn parse_read_endpoints() {
        assert_eq!(
            parse_read_endpoint("http://cache:7090/@cn-beijing"),
            Ok(scion_api::ReadEndpoint {
                url: "http://cache:7090".into(),
                region: "cn-beijing".into()
            })
        );
        assert_eq!(parse_read_endpoint("https://c").map(|e| e.region), Ok(String::new()));
        assert!(parse_read_endpoint("cache:7090").is_err());
        let cli = Cli::try_parse_from([
            "scion",
            "server",
            "--read-endpoint",
            "http://a:7090@r1",
            "--read-endpoint",
            "http://b:7090",
            "--ref-poll-interval",
            "250ms",
        ])
        .expect("parse");
        let Command::Server(a) = cli.command else {
            panic!("server");
        };
        assert_eq!(a.read_endpoints.len(), 2);
        assert_eq!(a.ref_poll_interval, std::time::Duration::from_millis(250));
    }

    #[test]
    fn parse_cache_args() {
        let cli = Cli::try_parse_from([
            "scion",
            "cache",
            "--upstream",
            "http://l3a:7080",
            "--upstream",
            "http://l3b:7080",
            "--advertise-url",
            "http://c1:7090",
            "--peer",
            "http://c2:7090",
            "--memory",
            "256MiB",
            "--disk-dir",
            "/var/cache/scion",
            "--disk-size",
            "10GiB",
            "--warm",
            "mono:main,release",
            "--warm-blob-max",
            "64KiB",
            "--no-hedge",
        ])
        .expect("parse");
        let Command::Cache(a) = cli.command else {
            panic!("cache");
        };
        let c = cache_config(&a).expect("config");
        assert_eq!(c.upstreams.len(), 2);
        assert_eq!(c.peers, ["http://c2:7090"]);
        assert_eq!(c.storage.memory_bytes, 256 << 20);
        assert_eq!(c.storage.disk.as_ref().map(|d| d.capacity_bytes), Some(10 << 30));
        assert_eq!(c.storage.max_object_bytes, 8 << 20);
        assert_eq!(c.warm[0].refs, ["main", "release"]);
        assert_eq!(c.warm_options.max_blob_bytes, 64 << 10);
        assert!(!c.pool.hedge);
        // 默认值
        let cli = Cli::try_parse_from(["scion", "cache", "--upstream", "http://l3:7080"]).expect("parse");
        let Command::Cache(a) = cli.command else {
            panic!("cache");
        };
        let c = cache_config(&a).expect("config");
        assert_eq!(
            (c.storage.memory_bytes, c.storage.disk.is_none(), c.pool.hedge),
            (1 << 30, true, true)
        );
        assert_eq!(a.listen, "127.0.0.1:7090".parse().expect("addr"));
        // 必须有上游；--peer 需要 --advertise-url
        assert!(Cli::try_parse_from(["scion", "cache"]).is_err());
        assert!(Cli::try_parse_from(["scion", "cache", "--upstream", "http://l3", "--peer", "http://c2"]).is_err());
    }

    #[test]
    fn parse_server_defaults() {
        let cli = Cli::try_parse_from(["scion", "server"]).expect("parse");
        match cli.command {
            Command::Server(a) => {
                assert_eq!(a.listen, "127.0.0.1:7080".parse().expect("addr"));
                assert!(a.repos.is_empty());
            }
            _ => panic!("expected server"),
        }
        assert_eq!(cli.log_format, LogFormat::Text);
    }

    #[test]
    fn parse_server_repos_and_json_logs() {
        let cli = Cli::try_parse_from([
            "scion",
            "--log-format",
            "json",
            "server",
            "--repo",
            "a",
            "--repo",
            "b/c",
        ])
        .expect("parse");
        assert_eq!(cli.log_format, LogFormat::Json);
        match cli.command {
            Command::Server(a) => assert_eq!(a.repos, ["a", "b/c"]),
            _ => panic!("expected server"),
        }
    }

    #[test]
    fn parse_rejects_bad_listen() {
        assert!(Cli::try_parse_from(["scion", "server", "--listen", "nope"]).is_err());
    }

    #[test]
    fn version_text_mentions_protocol() {
        assert!(version_text().contains(scion_api::PROTOCOL_VERSION));
        assert!(version_text().starts_with("scion "));
    }

    fn args(extra: &[&str]) -> ServerArgs {
        let mut argv = vec!["scion", "server"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).expect("parse").command {
            Command::Server(a) => a,
            _ => panic!("expected server"),
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[tokio::test]
    async fn build_app_creates_repos_idempotently() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let a = args(&["--lite", "--data-dir", d, "--repo", "a", "--repo", "b"]);
        let (app, backends) = build_app(&a, no_env).await.expect("app");
        assert_eq!(app.meta.list_repos().await.expect("list"), ["a", "b"]);
        assert!(backends.contains("meta.db"));
        drop(app);
        // 重启时仓库已存在，不应失败
        let (app, _) = build_app(&a, no_env).await.expect("restart");
        assert_eq!(app.meta.list_repos().await.expect("list"), ["a", "b"]);
    }

    #[tokio::test]
    async fn build_app_cluster_mode_from_advertise_url() {
        let (app, _) = build_app(&args(&["--advertise-url", "http://10.0.0.5:7080/"]), no_env)
            .await
            .expect("app");
        assert_eq!(
            app.config.cluster.as_ref().map(|c| c.node.as_str()),
            Some("http://10.0.0.5:7080")
        );
        let (app, _) = build_app(&args(&[]), no_env).await.expect("app");
        assert!(app.config.cluster.is_none());
        assert!(
            build_app(&args(&["--advertise-url", "10.0.0.5:7080"]), no_env)
                .await
                .is_err()
        );
        let lite = Cli::try_parse_from([
            "scion",
            "server",
            "--lite",
            "--data-dir",
            "/d",
            "--advertise-url",
            "http://x",
        ]);
        assert!(lite.is_err(), "lite mode is single-node");
    }

    #[tokio::test]
    async fn build_app_refuses_an_unloadable_policy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let (app, _) = build_app(&args(&["--lite", "--data-dir", d]), no_env)
            .await
            .expect("app");
        app.meta
            .put_setting(scion_api::scion_auth::POLICY_KEY, b"broken [")
            .await
            .expect("store");
        drop(app);
        let err = build_app(&args(&["--lite", "--data-dir", d, "--auth-required"]), no_env)
            .await
            .err()
            .expect("refused");
        assert!(err.to_string().contains("access policy"), "{err}");
        // 不要求认证时不加载策略
        build_app(&args(&["--lite", "--data-dir", d]), no_env)
            .await
            .expect("auth not required");
    }

    #[tokio::test]
    async fn admin_operations_through_the_cli() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let (app, _) = build_app(&args(&["--lite", "--data-dir", d, "--repo", "r"]), no_env)
            .await
            .expect("app");
        drop(app);
        let run = |extra: &[&str]| {
            let mut argv = vec!["scion", "admin"];
            argv.extend_from_slice(extra);
            argv.extend_from_slice(&["--lite", "--data-dir", d]);
            let cli = Cli::try_parse_from(argv).expect("parse");
            let Command::Admin(cmd) = cli.command else {
                panic!("admin");
            };
            admin_ops(cmd, no_env)
        };
        let (lines, ok) = run(&["fsck", "--verify"]).await.expect("fsck");
        assert!(ok, "{lines:?}");
        let (lines, ok) = run(&["fsck", "--repo", "missing"]).await.expect("fsck");
        assert!(!ok && lines.iter().any(|l| l.contains("does not exist")));
        assert_eq!(run(&["lease", "ls"]).await.expect("lease"), (vec![], true));
        assert_eq!(run(&["redactions"]).await.expect("ls"), (vec![], true));
        assert!(run(&["unredact", "rd_nope"]).await.is_err());
        assert!(run(&["purge", "rd_nope", "--now"]).await.is_err());
        let id = scion_object::ObjectId::of_blob(b"never uploaded").to_string();
        assert!(
            run(&["redact", "--repo", "r", "--reason", "x", &id]).await.is_err(),
            "not in the repository"
        );
        assert!(Cli::try_parse_from(["scion", "admin", "purge", "rd", "--now", "--grace", "1h"]).is_err());
        // 不是运维命令
        assert!(admin_ops(AdminCmd::JwtKeygen, no_env).await.is_err());
    }

    #[test]
    fn write_rate_flag() {
        let r = |v| parse_write_rate(v).map(|w| (w.per_second, w.burst));
        assert_eq!(r("200"), Ok((200, 400)));
        assert_eq!(r("200/1000"), Ok((200, 1000)));
        assert_eq!(r(" 5 / 7 "), Ok((5, 7)));
        for bad in ["", "0", "-1", "x", "5/0", "5/x"] {
            assert!(r(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn build_app_applies_overload_settings() {
        let (app, _) = build_app(
            &args(&["--max-concurrent-requests", "64", "--repo-write-rate", "10/20"]),
            no_env,
        )
        .await
        .expect("app");
        assert_eq!(app.config.max_concurrent_requests, 64);
        assert_eq!(
            app.config.repo_write_rate,
            Some(scion_api::WriteRate {
                per_second: 10,
                burst: 20
            })
        );
        let (app, _) = build_app(&args(&[]), no_env).await.expect("defaults");
        assert_eq!(
            (app.config.max_concurrent_requests, app.config.repo_write_rate),
            (10_000, None)
        );
    }

    #[tokio::test]
    async fn build_app_rejects_invalid_repo_and_specs() {
        assert!(build_app(&args(&["--repo", "../x"]), no_env).await.is_err());
        assert!(build_app(&args(&["--meta", "mysql://x"]), no_env).await.is_err());
        assert!(build_app(&args(&["--blobs", "s3://"]), no_env).await.is_err());
    }

    #[tokio::test]
    async fn build_app_defaults_to_memory() {
        let (_, backends) = build_app(&args(&[]), no_env).await.expect("app");
        assert_eq!(backends, "meta=mem blobs=mem");
    }

    #[tokio::test]
    async fn admin_reindex_runs_on_lite_storage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        // 先用 server 的方式建两个仓库
        drop(
            build_app(
                &args(&["--lite", "--data-dir", d, "--repo", "a", "--repo", "b"]),
                no_env,
            )
            .await
            .expect("app"),
        );
        let parse = |extra: &[&str]| {
            let mut argv = vec!["scion", "admin", "reindex", "--lite", "--data-dir", d];
            argv.extend_from_slice(extra);
            match Cli::try_parse_from(argv).expect("parse").command {
                Command::Admin(AdminCmd::Reindex(r)) => r,
                _ => panic!("expected reindex"),
            }
        };
        let all = reindex(&parse(&[]), no_env).await.expect("all repos");
        assert_eq!(
            all,
            [
                "a: 0 changesets, 0 lineage repaired, 0 path entries, 0 git shas",
                "b: 0 changesets, 0 lineage repaired, 0 path entries, 0 git shas"
            ]
        );
        assert_eq!(reindex(&parse(&["--repo", "a"]), no_env).await.expect("one").len(), 1);
        assert!(reindex(&parse(&["--repo", "nope"]), no_env).await.is_err());
    }

    #[test]
    fn parse_lite_requires_data_dir_and_excludes_meta() {
        assert!(Cli::try_parse_from(["scion", "server", "--lite"]).is_err());
        assert!(Cli::try_parse_from(["scion", "server", "--lite", "--data-dir", "/d", "--meta", "mem"]).is_err());
        let a = args(&["--meta", "sqlite:///x.db", "--blobs", "/b"]);
        assert_eq!(
            (a.storage.meta.as_str(), a.storage.blobs.as_str()),
            ("sqlite:///x.db", "/b")
        );
    }
}
