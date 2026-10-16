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
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// gRPC 监听地址。
    #[arg(long, default_value = "127.0.0.1:7080", env = "SCION_LISTEN")]
    listen: SocketAddr,
    /// 启动时确保存在的仓库（已存在则跳过），可重复指定。
    #[arg(long = "repo", value_name = "NAME")]
    repos: Vec<String>,
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
    let (meta_spec, blob_spec) = match (&args.data_dir, args.lite) {
        (Some(dir), true) => storage::lite(dir),
        _ => (storage::parse_meta(&args.meta)?, storage::parse_blobs(&args.blobs)?),
    };
    let (meta, blobs) = storage::open(&meta_spec, &blob_spec, env).await?;
    for repo in &args.repos {
        match meta.create_repo(repo).await {
            Ok(()) | Err(scion_store::Error::RepoAlreadyExists(_)) => {}
            Err(e) => return Err(e).with_context(|| format!("create repository {repo:?}")),
        }
    }
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta,
        blobs,
    };
    Ok((app, storage::describe(&meta_spec, &blob_spec)))
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
            Command::Version => panic!("expected server"),
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
            Command::Version => panic!("expected server"),
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
            Command::Version => panic!("expected server"),
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

    #[test]
    fn parse_lite_requires_data_dir_and_excludes_meta() {
        assert!(Cli::try_parse_from(["scion", "server", "--lite"]).is_err());
        assert!(Cli::try_parse_from(["scion", "server", "--lite", "--data-dir", "/d", "--meta", "mem"]).is_err());
        let a = args(&["--meta", "sqlite:///x.db", "--blobs", "/b"]);
        assert_eq!((a.meta.as_str(), a.blobs.as_str()), ("sqlite:///x.db", "/b"));
    }
}
