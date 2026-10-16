//! `scn`：Scion 命令行客户端。
//!
//! 退出码（客户端文档 §8.2）：0 成功、1 一般错误、2 用法错误、3 合并冲突、
//! 4 可重试错误（已用尽重试）、5 认证失败。

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use scion_client::{Auth, Client, Code, Error, Role, ServerInfo};
use scion_proto::v1::ErrorReason;
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(name = "scn", version, about = "Scion command line client")]
struct Cli {
    /// 服务端地址。
    #[arg(long, global = true, env = "SCION_ENDPOINT", default_value = "http://127.0.0.1:7080")]
    endpoint: String,
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
}

const EXIT_GENERAL: u8 = 1;
const EXIT_CONFLICT: u8 = 3;
const EXIT_RETRYABLE: u8 = 4;
const EXIT_AUTH: u8 = 5;

/// 错误到稳定退出码的映射。
fn exit_code(err: &Error) -> u8 {
    match err {
        Error::Rpc {
            code: Code::Unauthenticated | Code::PermissionDenied,
            ..
        } => EXIT_AUTH,
        e if e.reason() == Some(ErrorReason::MergeConflict) => EXIT_CONFLICT,
        e if e.is_retryable() => EXIT_RETRYABLE,
        _ => EXIT_GENERAL,
    }
}

#[derive(Debug, Serialize)]
struct VersionOut {
    schema: &'static str,
    client_version: &'static str,
    protocol_version: &'static str,
}

fn version_out() -> VersionOut {
    VersionOut {
        schema: "scion.cli.version/v1",
        client_version: env!("CARGO_PKG_VERSION"),
        protocol_version: scion_proto::PROTOCOL_VERSION,
    }
}

#[derive(Debug, Serialize)]
struct DoctorOut {
    schema: &'static str,
    endpoint: String,
    ok: bool,
    protocol_version: String,
    server_version: String,
    role: &'static str,
    features: Vec<String>,
    max_batch_objects: u32,
    max_chunk_bytes: u64,
    read_endpoints: Vec<String>,
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::Server => "server",
        Role::Cache => "cache",
        Role::Daemon => "daemon",
        Role::Unknown => "unknown",
    }
}

fn doctor_out(endpoint: &str, info: &ServerInfo) -> DoctorOut {
    DoctorOut {
        schema: "scion.cli.doctor/v1",
        endpoint: endpoint.to_string(),
        ok: true,
        protocol_version: info.protocol_version.to_string(),
        server_version: info.server_version.clone(),
        role: role_str(info.role),
        features: info.features.iter().cloned().collect(),
        max_batch_objects: info.limits.max_batch_objects,
        max_chunk_bytes: info.limits.max_chunk_bytes,
        read_endpoints: info
            .read_endpoints
            .iter()
            .map(|e| format!("{} ({})", e.url, e.region))
            .collect(),
    }
}

fn doctor_text(d: &DoctorOut) -> String {
    let mut s = format!(
        "endpoint:  {}\nstatus:    ok\nprotocol:  {}\nserver:    {} ({})\nfeatures:  {}\nlimits:    batch={} chunk={}B\n",
        d.endpoint,
        d.protocol_version,
        d.server_version,
        d.role,
        if d.features.is_empty() {
            "-".to_string()
        } else {
            d.features.join(", ")
        },
        d.max_batch_objects,
        d.max_chunk_bytes,
    );
    for e in &d.read_endpoints {
        s.push_str(&format!("read via:  {e}\n"));
    }
    s
}

#[derive(Debug, Serialize)]
struct ErrorOut {
    schema: &'static str,
    error: String,
    reason: Option<&'static str>,
    retryable: bool,
    exit_code: u8,
    hint: Option<&'static str>,
}

fn hint(err: &Error) -> Option<&'static str> {
    match err {
        Error::Connect { .. } => Some("check that the server is running and --endpoint / SCION_ENDPOINT is correct"),
        Error::IncompatibleProtocol { .. } => Some("upgrade scn to a version matching the server's protocol"),
        _ if exit_code(err) == EXIT_AUTH => Some("set SCION_TOKEN or run `scn login`"),
        _ => None,
    }
}

fn error_out(err: &Error) -> ErrorOut {
    ErrorOut {
        schema: "scion.cli.error/v1",
        error: err.to_string(),
        reason: err.reason().map(scion_proto::reason_name),
        retryable: err.is_retryable(),
        exit_code: exit_code(err),
        hint: hint(err),
    }
}

fn print_json<T: Serialize>(v: &T) {
    match serde_json::to_string(v) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("scn: failed to encode JSON: {e}"),
    }
}

async fn doctor(cli: &Cli) -> Result<DoctorOut, Error> {
    let client = Client::builder()
        .endpoint(&cli.endpoint)
        .auth(Auth::from_env())
        .client_name("scn")
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;
    Ok(doctor_out(&cli.endpoint, client.server_info()))
}

async fn run(cli: Cli) -> ExitCode {
    match cli.command {
        Command::Version => {
            let v = version_out();
            if cli.json {
                print_json(&v);
            } else {
                println!("scn {} (protocol {})", v.client_version, v.protocol_version);
            }
            ExitCode::SUCCESS
        }
        Command::Doctor => match doctor(&cli).await {
            Ok(d) => {
                if cli.json {
                    print_json(&d);
                } else {
                    print!("{}", doctor_text(&d));
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                let out = error_out(&e);
                if cli.json {
                    print_json(&out);
                } else {
                    eprintln!("scn: {}", out.error);
                    if let Some(h) = out.hint {
                        eprintln!("hint: {h}");
                    }
                }
                ExitCode::from(out.exit_code)
            }
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
    use scion_client::{Limits, ProtocolVersion, ReadEndpoint};
    use std::collections::{BTreeSet, HashMap};

    fn rpc(code: Code, reason: Option<ErrorReason>) -> Error {
        Error::Rpc {
            code,
            reason,
            message: "m".into(),
            metadata: HashMap::new(),
        }
    }

    fn info() -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion { major: 1, minor: 0 },
            features: BTreeSet::from(["objects.get".to_string()]),
            limits: Limits {
                max_batch_objects: 1000,
                max_chunk_bytes: 1024,
                max_changes_per_changeset: 1,
                max_inline_bytes: 1,
                max_upload_object_bytes: 1,
            },
            read_endpoints: vec![ReadEndpoint {
                url: "http://cache".into(),
                region: "bj".into(),
            }],
            server_version: "0.1.0".into(),
            role: Role::Server,
        }
    }

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn exit_codes_follow_contract() {
        assert_eq!(exit_code(&rpc(Code::Unauthenticated, None)), EXIT_AUTH);
        assert_eq!(exit_code(&rpc(Code::PermissionDenied, None)), EXIT_AUTH);
        assert_eq!(
            exit_code(&rpc(Code::Aborted, Some(ErrorReason::MergeConflict))),
            EXIT_CONFLICT
        );
        assert_eq!(exit_code(&rpc(Code::Unavailable, None)), EXIT_RETRYABLE);
        assert_eq!(
            exit_code(&Error::Connect {
                endpoint: "e".into(),
                message: "m".into()
            }),
            EXIT_RETRYABLE
        );
        assert_eq!(
            exit_code(&rpc(Code::NotFound, Some(ErrorReason::RepoNotFound))),
            EXIT_GENERAL
        );
        assert_eq!(exit_code(&Error::MissingRepo), EXIT_GENERAL);
    }

    #[test]
    fn hints_for_common_failures() {
        assert!(
            hint(&Error::Connect {
                endpoint: "e".into(),
                message: "m".into()
            })
            .is_some()
        );
        assert!(
            hint(&Error::IncompatibleProtocol {
                server: "2.0".into(),
                client: "1.0"
            })
            .is_some()
        );
        assert!(
            hint(&rpc(Code::Unauthenticated, None))
                .expect("hint")
                .contains("SCION_TOKEN")
        );
        assert!(hint(&Error::MissingRepo).is_none());
    }

    #[test]
    fn error_out_fields() {
        let e = error_out(&rpc(Code::NotFound, Some(ErrorReason::ObjectMissing)));
        assert_eq!(e.schema, "scion.cli.error/v1");
        assert_eq!(e.reason, Some("OBJECT_MISSING"));
        assert!(!e.retryable);
        assert_eq!(e.exit_code, EXIT_GENERAL);
        assert_eq!(error_out(&Error::MissingRepo).reason, None);
    }

    #[test]
    fn doctor_out_and_text() {
        let d = doctor_out("http://x", &info());
        assert_eq!(d.schema, "scion.cli.doctor/v1");
        assert_eq!(d.role, "server");
        let text = doctor_text(&d);
        assert!(text.contains("protocol:  1.0"));
        assert!(text.contains("features:  objects.get"));
        assert!(text.contains("read via:  http://cache (bj)"));
        let json = serde_json::to_value(&d).expect("json");
        assert_eq!(json["max_batch_objects"], 1000);
    }

    #[test]
    fn doctor_text_without_features() {
        let mut i = info();
        i.features.clear();
        i.read_endpoints.clear();
        let text = doctor_text(&doctor_out("http://x", &i));
        assert!(text.contains("features:  -"));
        assert!(!text.contains("read via"));
    }

    #[test]
    fn role_str_all() {
        assert_eq!(role_str(Role::Cache), "cache");
        assert_eq!(role_str(Role::Daemon), "daemon");
        assert_eq!(role_str(Role::Unknown), "unknown");
    }

    #[test]
    fn version_out_schema() {
        let v = version_out();
        assert_eq!(v.schema, "scion.cli.version/v1");
        assert_eq!(v.protocol_version, scion_proto::PROTOCOL_VERSION);
    }

    #[test]
    fn parse_global_flags_after_subcommand() {
        let cli = Cli::try_parse_from(["scn", "doctor", "--json", "--endpoint", "http://h:1"]).expect("parse");
        assert!(cli.json);
        assert_eq!(cli.endpoint, "http://h:1");
    }
}
