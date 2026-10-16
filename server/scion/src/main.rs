//! `scion`：服务端单二进制，按子命令区分角色。
//!
//! M0 只有 `server`（内存存储）和 `version`；`cache`、`deriver`、`janitor`、`import`、`admin`
//! 按里程碑逐个加入。

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_api::{App, ServerConfig};
use scion_store::{MemStore, MetaStore};

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
    /// 运行 L3 服务端（M0：内存存储，重启即丢失数据）。
    Server(ServerArgs),
    /// 打印版本与协议版本。
    Version,
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// gRPC 监听地址。
    #[arg(long, default_value = "127.0.0.1:7080", env = "SCION_LISTEN")]
    listen: SocketAddr,
    /// 启动时创建的仓库，可重复指定。
    #[arg(long = "repo", value_name = "NAME")]
    repos: Vec<String>,
}

fn init_tracing(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("SCION_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
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

/// 创建内存存储并预建仓库。
async fn build_app(args: &ServerArgs) -> anyhow::Result<App> {
    let store = Arc::new(MemStore::new());
    for repo in &args.repos {
        store
            .create_repo(repo)
            .await
            .with_context(|| format!("create repository {repo:?}"))?;
    }
    Ok(App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    })
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
    let app = build_app(&args).await?;
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let addr = listener.local_addr()?;
    tracing::warn!("M0 uses in-memory storage; all data is lost on exit");
    // 集成测试按这一行解析实际监听地址，修改时同步 tests/
    tracing::info!(%addr, repos = ?args.repos, "scion server listening");
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

    #[tokio::test]
    async fn build_app_creates_repos() {
        let args = ServerArgs {
            listen: "127.0.0.1:0".parse().expect("addr"),
            repos: vec!["a".into(), "b".into()],
        };
        let app = build_app(&args).await.expect("app");
        assert_eq!(app.meta.list_repos().await.expect("list"), ["a", "b"]);
    }

    #[tokio::test]
    async fn build_app_rejects_invalid_and_duplicate_repo() {
        let bad = ServerArgs {
            listen: "127.0.0.1:0".parse().expect("addr"),
            repos: vec!["../x".into()],
        };
        assert!(build_app(&bad).await.is_err());
        let dup = ServerArgs {
            listen: "127.0.0.1:0".parse().expect("addr"),
            repos: vec!["a".into(), "a".into()],
        };
        assert!(build_app(&dup).await.is_err());
    }
}
