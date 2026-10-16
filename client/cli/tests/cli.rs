//! 运行编译好的 `scn` 二进制，检查输出格式与退出码。

use std::process::Command;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_store::MemStore;
use tokio::net::TcpListener;

fn scn(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .output()
        .expect("run scn")
}

#[test]
fn version_json() {
    let out = scn(&["version", "--json"]);
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.version/v1");
}

#[test]
fn usage_error_exits_2() {
    assert_eq!(scn(&["no-such-command"]).status.code(), Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_against_live_server() {
    let store = Arc::new(MemStore::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let u = url.clone();
    let out = tokio::task::spawn_blocking(move || scn(&["doctor", "--json", "--endpoint", &u]))
        .await
        .expect("join");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.doctor/v1");
    assert_eq!(v["ok"], true);
    assert_eq!(v["protocol_version"], scion_proto::PROTOCOL_VERSION);

    let u = url.clone();
    let out = tokio::task::spawn_blocking(move || scn(&["doctor", "--endpoint", &u]))
        .await
        .expect("join");
    assert!(String::from_utf8_lossy(&out.stdout).contains("status:    ok"));
    let _ = tx.send(());
}

#[tokio::test]
async fn doctor_unreachable_exits_4_with_hint() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind")
        .local_addr()
        .expect("addr");
    let url = format!("http://{addr}");
    let out = scn(&["doctor", "--json", "--endpoint", &url]);
    assert_eq!(out.status.code(), Some(4));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.error/v1");
    assert_eq!(v["retryable"], true);
    assert!(v["hint"].as_str().expect("hint").contains("--endpoint"));

    let out = scn(&["doctor", "--endpoint", &url]);
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("hint:"));
}

#[test]
fn doctor_invalid_endpoint_exits_1() {
    let out = scn(&["doctor", "--endpoint", "ftp://x"]);
    assert_eq!(out.status.code(), Some(1));
}
