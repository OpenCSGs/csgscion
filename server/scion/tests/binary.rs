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
    spawn_role("server", extra, drain_logs)
}

/// 启动一个角色（server / cache），等它打印「scion <role> listening」。
fn spawn_role(role: &str, extra: &[&str], drain_logs: bool) -> (ServerProcess, String) {
    let mut argv = vec!["--log-format", "json", role, "--listen", "127.0.0.1:0"];
    argv.extend_from_slice(extra);
    let marker = format!("scion {role} listening");
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(&argv)
        .env("SCION_LOG", "info")
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let stderr = child.stderr.take().expect("stderr");
    let mut lines = BufReader::new(stderr).lines();
    let addr = loop {
        let line = lines.next().expect("process exited before listening").expect("read");
        let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if v["fields"]["message"] == marker.as_str() {
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

/// `scion cache` 进程：经缓存读对象、预热订阅，两个进程都能收到 SIGTERM 后干净退出。
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn cache_serves_objects_from_the_server() {
    let (server, addr) = spawn_server(&["--repo", "mono"]);
    let upstream = format!("http://{addr}");
    let direct = scion_client::Client::builder()
        .endpoint(&upstream)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let cs = direct
        .changeset(None)
        .put("a.txt", "hello")
        .create()
        .await
        .expect("create");
    direct.land("main", &cs.id).await.expect("land");

    let (cache, cache_addr) = spawn_role("cache", &["--upstream", &upstream, "--warm", "mono:main"], true);
    let via = scion_client::Client::builder()
        .endpoint(format!("http://{cache_addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect cache");
    assert_eq!(via.server_info().role, scion_client::Role::Cache);
    let blob = scion_client::ObjectId::of_blob(b"hello");
    assert_eq!(via.get_object(&blob).await.expect("read").as_ref(), b"hello");
    drop((via, direct));
    stop(cache);
    // 缓存的 WatchRefs 订阅不能挡住服务端的优雅退出
    stop(server);
}

#[test]
fn cache_requires_reachable_upstream() {
    let out = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["cache", "--listen", "127.0.0.1:0", "--upstream", "http://127.0.0.1:1"])
        .output()
        .expect("run");
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("upstream"));
}

/// 认证：生成密钥、在 Lite 数据目录里创建令牌、要求认证的服务端只接受有效令牌；配置有误时拒绝启动。
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn auth_required_server_with_admin_commands() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = dir.path().to_str().expect("utf8");
    let key = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["admin", "jwt-keygen"])
        .output()
        .expect("keygen");
    assert!(key.status.success());
    let key_file = dir.path().join("jwt.pem");
    std::fs::write(&key_file, &key.stdout).expect("write key");
    assert!(String::from_utf8_lossy(&key.stdout).contains("BEGIN PRIVATE KEY"));

    // 先建好仓库，再离线创建令牌
    let (srv, _) = spawn_server(&["--lite", "--data-dir", d, "--repo", "mono"]);
    stop(srv);
    let created = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["admin", "token", "create", "--subject", "ci", "--lite", "--data-dir", d])
        .output()
        .expect("token create");
    assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
    let out = String::from_utf8_lossy(&created.stdout).to_string();
    let token = out
        .lines()
        .find_map(|l| l.strip_prefix("token: "))
        .expect("token line")
        .to_string();
    let list = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["admin", "token", "list", "--lite", "--data-dir", d])
        .output()
        .expect("list");
    assert!(String::from_utf8_lossy(&list.stdout).contains("ci"));

    let key_path = key_file.to_str().expect("utf8");
    let (srv, addr) = spawn_server(&["--lite", "--data-dir", d, "--auth-required", "--jwt-key", key_path]);
    let anon = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(anon.has_feature(scion_proto::features::AUTH_REQUIRED));
    assert!(anon.list_refs().await.is_err(), "anonymous rejected");
    let ci = scion_client::Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .auth(scion_client::Auth::Bearer(token))
        .build()
        .await
        .expect("connect");
    ci.list_refs().await.expect("token accepted");
    drop((anon, ci));
    stop(srv);

    // 认证配置有误：拒绝启动
    let bad = dir.path().join("auth.toml");
    std::fs::write(
        &bad,
        "[[issuer]]\nissuer = \"x\"\nhs256_secret_env = \"SCION_TEST_UNSET_SECRET\"\n",
    )
    .expect("write");
    let out = Command::new(env!("CARGO_BIN_EXE_scion"))
        .args(["server", "--listen", "127.0.0.1:0", "--lite", "--data-dir", d])
        .arg("--auth-config")
        .arg(&bad)
        .env_remove("SCION_TEST_UNSET_SECRET")
        .output()
        .expect("run");
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("auth"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
