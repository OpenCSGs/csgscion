//! `scn`：Scion 命令行客户端。
//!
//! 退出码（客户端文档 §8.2）：0 成功、1 一般错误、2 用法错误、3 冲突、
//! 4 可重试错误（已用尽重试）、5 认证失败。

mod commands;
mod output;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::commands::Ctx;
use crate::output::{CliError, Render, error_out, render, version_out};

#[derive(Debug, Parser)]
#[command(name = "scn", version, about = "Scion command line client")]
struct Cli {
    /// 服务端地址；默认取工作副本记录的地址，再默认 http://127.0.0.1:7080。
    #[arg(long, global = true, env = "SCION_ENDPOINT")]
    endpoint: Option<String>,
    /// 仓库（cat / ls 在工作副本之外使用）。
    #[arg(long, global = true, env = "SCION_REPO")]
    repo: Option<String>,
    /// 输出 JSON（带 schema 字段，供 Agent 和脚本解析）。
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// 检查与服务端的连接和能力协商。
    Doctor,
    /// 打印客户端版本与协议版本。
    Version,
    /// 把仓库检出到本地目录。
    Clone {
        repo: String,
        dir: Option<PathBuf>,
        #[arg(long = "ref", default_value = "main")]
        reference: String,
    },
    /// 显示本地改动。
    Status,
    /// 把本地改动创建为提交（尚未合入）。
    Commit {
        #[arg(short, long)]
        message: String,
        /// 作者，格式为 "Name <email>"。
        #[arg(long, env = "SCION_AUTHOR")]
        author: Option<String>,
    },
    /// 合入已提交的改动。
    Land,
    /// 更新到 ref 的最新提交；有未合入的提交时在其上做 rebase。
    Sync,
    /// 输出文件内容：[REV:]PATH。
    Cat { spec: String },
    /// 列出目录：[REV:][DIR]。
    Ls { spec: Option<String> },
}

fn emit<T: Render>(v: Result<T, CliError>, json: bool) -> ExitCode {
    match v {
        Ok(v) => {
            println!("{}", render(&v, json));
            ExitCode::SUCCESS
        }
        Err(e) => fail(&e, json),
    }
}

fn fail(e: &CliError, json: bool) -> ExitCode {
    let out = error_out(e);
    if json {
        println!("{}", serde_json::to_string(&out).unwrap_or_default());
    } else {
        eprintln!("scn: {}", out.error);
        if let Some(h) = out.hint {
            eprintln!("hint: {h}");
        }
    }
    ExitCode::from(out.exit_code)
}

async fn run(cli: Cli) -> ExitCode {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            return fail(
                &CliError::Usage(format!("cannot read current directory: {e}")),
                cli.json,
            );
        }
    };
    let ctx = Ctx {
        endpoint: cli.endpoint.clone(),
        repo: cli.repo.clone(),
        cwd,
    };
    let json = cli.json;
    match cli.command {
        Command::Version => emit(Ok(version_out()), json),
        Command::Doctor => emit(commands::doctor(&ctx).await, json),
        Command::Clone { repo, dir, reference } => {
            emit(commands::clone(&ctx, &repo, dir.as_deref(), &reference).await, json)
        }
        Command::Status => emit(commands::status(&ctx), json),
        Command::Commit { message, author } => emit(commands::commit(&ctx, &message, author.as_deref()).await, json),
        Command::Land => emit(commands::land(&ctx).await, json),
        Command::Sync => emit(commands::sync(&ctx).await, json),
        Command::Ls { spec } => emit(commands::ls(&ctx, spec.as_deref()).await, json),
        // 文本模式下原样输出字节（文件可能不是文本）
        Command::Cat { spec } => match commands::cat(&ctx, &spec).await {
            Ok(out) if !json => {
                let mut stdout = std::io::stdout().lock();
                match stdout.write_all(&out.data).and_then(|()| stdout.flush()) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => fail(&CliError::Usage(format!("write stdout: {e}")), false),
                }
            }
            other => emit(other, json),
        },
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // clap 对用法错误以退出码 2 结束，符合约定
    run(Cli::parse()).await
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
    fn parse_global_flags_after_subcommand() {
        let cli = Cli::try_parse_from(["scn", "doctor", "--json", "--endpoint", "http://h:1"]).expect("parse");
        assert!(cli.json);
        assert_eq!(cli.endpoint.as_deref(), Some("http://h:1"));
    }

    #[test]
    fn parse_commands() {
        let cli = Cli::try_parse_from(["scn", "clone", "org/repo", "dir", "--ref", "dev"]).expect("parse");
        assert!(
            matches!(cli.command, Command::Clone { ref repo, ref reference, .. } if repo == "org/repo" && reference == "dev")
        );
        let cli = Cli::try_parse_from(["scn", "commit", "-m", "msg", "--author", "A <a@b>"]).expect("parse");
        assert!(matches!(cli.command, Command::Commit { ref message, .. } if message == "msg"));
        assert!(Cli::try_parse_from(["scn", "commit"]).is_err(), "message required");
        let cli = Cli::try_parse_from(["scn", "ls"]).expect("parse");
        assert!(matches!(cli.command, Command::Ls { spec: None }));
    }

    #[test]
    fn fail_returns_exit_code() {
        assert_eq!(fail(&CliError::Usage("x".into()), true), ExitCode::from(2));
        assert_eq!(fail(&CliError::Wc(scion_wc::Error::Dirty), false), ExitCode::from(1));
    }
}
