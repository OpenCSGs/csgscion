//! 运行编译好的 `scion` 二进制：启动、能力协商、SIGTERM 优雅退出。

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn version_prints_protocol() {
    let out = Command::new(env!("CARGO_BIN_EXE_scion"))
        .arg("version")
        .output()
        .expect("run");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.starts_with("scion ") && text.contains("protocol"), "{text}");
}

#[test]
fn server_rejects_invalid_repo_name() {
    let out = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["server", "--listen", "127.0.0.1:0", "--repo", "../bad"])
        .output()
        .expect("run");
    assert!(!out.status.success());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn server_serves_and_stops_on_sigterm() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args([
            "--log-format",
            "json",
            "server",
            "--listen",
            "127.0.0.1:0",
            "--repo",
            "mono",
        ])
        .env("SCION_LOG", "info")
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");

    // 从 JSON 日志中解析实际监听地址
    let stderr = child.stderr.take().expect("stderr");
    let mut lines = BufReader::new(stderr).lines();
    let addr = loop {
        let line = lines.next().expect("server exited before listening").expect("read");
        let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if v["fields"]["message"] == "scion server listening" {
            break v["fields"]["addr"].as_str().expect("addr").to_string();
        }
    };

    let client = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(
        client
            .missing_objects(&[scion_client::ObjectId::of_blob(b"x")])
            .await
            .expect("has")
            .len()
            == 1
    );

    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let exit = child.wait().expect("wait");
    assert!(exit.success(), "server should exit cleanly on SIGTERM: {exit:?}");
}
