//! `scion`：服务端单二进制，按子命令区分角色。
//!
//! 目前有 `server` 和 `version`；`cache`、`deriver`、`janitor`、`import`、`admin` 按里程碑逐个加入。

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
    #[arg(long, requires = "data_dir", conflicts_with_all = ["meta", "blobs"])]
    lite: bool,
    /// Lite 档的数据目录（meta.db 与 objects/）。
    #[arg(long, value_name = "DIR", env = "SCION_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// 元数据后端：mem、sqlite://PATH、postgres://…
    #[arg(long, value_name = "URL", default_value = "mem", env = "SCION_META")]
    meta: String,
    /// 对象存储：mem、目录路径、s3://bucket[/prefix]（连接参数见 SCION_S3_* 环境变量）。
    #[arg(long, value_name = "SPEC", default_value = "mem", env = "SCION_BLOBS")]
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
    #[command(flatten)]
    storage: StorageArgs,
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
    let (meta_spec, blob_spec) = args.storage.specs()?;
    let (meta, blobs) = storage::open(&meta_spec, &blob_spec, env).await?;
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
            ..ServerConfig::default()
        }),
        meta,
        blobs,
    };
    Ok((app, storage::describe(&meta_spec, &blob_spec)))
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

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
    tracing::info!("shutdown signal received, draining");
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
    // 集成测试按这一行解析实际监听地址，修改时同步 tests/
    tracing::info!(%addr, repos = ?args.repos, %backends, "scion server listening");
    scion_api::serve(listener, app, shutdown_signal()).await?;
    tracing::info!("scion server stopped");
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
        Command::Admin(AdminCmd::Reindex(args)) => {
            for line in reindex(&args, |k| std::env::var(k).ok()).await? {
                println!("{line}");
            }
            Ok(())
        }
    }
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
