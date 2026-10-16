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

/// 测试中启动的服务端进程；测试中途失败时在 Drop 中杀掉，避免残留进程挂住 cargo。
struct ServerProcess(Option<std::process::Child>);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// 启动 scion server，返回进程守卫和实际监听地址（从 JSON 日志解析）。
fn spawn_server(extra: &[&str]) -> (ServerProcess, String) {
    spawn_server_with(extra, true)
}

/// `drain_logs = false` 时读到监听地址后立即关闭日志管道，模拟日志采集进程退出。
fn spawn_server_with(extra: &[&str], drain_logs: bool) -> (ServerProcess, String) {
    let mut argv = vec!["--log-format", "json", "server", "--listen", "127.0.0.1:0"];
    argv.extend_from_slice(extra);
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(&argv)
        .env("SCION_LOG", "info")
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let stderr = child.stderr.take().expect("stderr");
    let mut lines = BufReader::new(stderr).lines();
    let addr = loop {
        let line = lines.next().expect("server exited before listening").expect("read");
        let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if v["fields"]["message"] == "scion server listening" {
            break v["fields"]["addr"].as_str().expect("addr").to_string();
        }
    };
    if drain_logs {
        // 继续读取（并丢弃）日志，避免管道写满阻塞服务端
        std::thread::spawn(move || for _ in lines {});
    }
    (ServerProcess(Some(child)), addr)
}

#[cfg(unix)]
fn stop(mut proc: ServerProcess) {
    let mut child = proc.0.take().expect("running");
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let exit = child.wait().expect("wait");
    assert!(exit.success(), "server should exit cleanly on SIGTERM: {exit:?}");
}

/// 回归测试：日志管道断开后，写日志失败不能让请求失败（tracing 的回退 eprintln! 会 panic）。
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn broken_log_pipe_does_not_fail_requests() {
    let (child, addr) = spawn_server_with(&["--repo", "mono"], false);
    let c = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect");
    // 创建提交会写 info 日志
    let cs = c
        .changeset(None)
        .put("a", "a")
        .create()
        .await
        .expect("create despite broken log pipe");
    c.land("main", &cs.id).await.expect("land despite broken log pipe");
    drop(c);
    stop(child);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn lite_mode_persists_across_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = dir.path().to_str().expect("utf8");
    let (child, addr) = spawn_server(&["--lite", "--data-dir", d, "--repo", "mono"]);
    let c = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let cs = c
        .changeset(None)
        .put("README.md", "persist me\n")
        .message("init")
        .create()
        .await
        .expect("create");
    c.land("main", &cs.id).await.expect("land");
    drop(c);
    stop(child);

    let (child, addr) = spawn_server(&["--lite", "--data-dir", d, "--repo", "mono"]);
    let c = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("reconnect");
    let tip = c
        .resolve_ref("main")
        .await
        .expect("resolve")
        .expect("ref survives restart");
    assert_eq!(tip.changeset, cs.id);
    assert_eq!(
        c.read_file("main", "README.md").await.expect("read").as_ref(),
        b"persist me\n"
    );
    drop(c);
    stop(child);
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
