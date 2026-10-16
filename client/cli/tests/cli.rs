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

/// 在指定目录运行 scn。
fn scn_in(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(dir)
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_REPO")
        .env_remove("SCION_AUTHOR")
        .output()
        .expect("run scn")
}

fn json_of(out: &std::process::Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stdout).expect("json")
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_commit_land_sync_workflow() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("team/app").await.expect("repo");
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        // clone 到默认目录（仓库名最后一段）
        let v = json_of(&scn_in(&root, &["clone", "team/app", "--endpoint", &u, "--json"]));
        assert_eq!(v["schema"], "scion.cli.clone/v1");
        let wc = root.join("app");
        assert!(wc.join(".scion/state.json").exists());

        // 新增文件 → status → commit → land（endpoint 取自工作副本）
        std::fs::create_dir_all(wc.join("src")).expect("mkdir");
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() {}\n").expect("write");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["added"], serde_json::json!(["src/lib.rs"]));
        let text = scn_in(&wc.join("src"), &["status"]);
        assert_eq!(
            String::from_utf8_lossy(&text.stdout).trim(),
            "A src/lib.rs",
            "works from a subdirectory"
        );
        let v = json_of(&scn_in(
            &wc,
            &["commit", "-m", "add lib", "--author", "Ada <ada@example.com>", "--json"],
        ));
        assert!(v["changeset"].as_str().expect("id").starts_with("c1"));
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert!(v["pending"].is_string());
        let v = json_of(&scn_in(&wc, &["land", "--json"]));
        assert_eq!(v["version"], 1);

        // cat / ls：工作副本内不需要 --repo
        let out = scn_in(&wc, &["cat", "main:src/lib.rs"]);
        assert_eq!(out.stdout, b"pub fn f() {}\n");
        let v = json_of(&scn_in(&wc, &["ls", "--json"]));
        assert_eq!(v["entries"][0]["name"], "src");
        assert_eq!(v["entries"][0]["kind"], "dir");
        // 工作副本外需要 --repo
        let out = scn_in(&root, &["cat", "src/lib.rs", "--endpoint", &u]);
        assert_eq!(out.status.code(), Some(2));
        let out = scn_in(&root, &["cat", "src/lib.rs", "--endpoint", &u, "--repo", "team/app"]);
        assert_eq!(out.stdout, b"pub fn f() {}\n");

        // 第二个工作副本落后后 sync
        let v = json_of(&scn_in(
            &root,
            &["clone", "team/app", "second", "--endpoint", &u, "--json"],
        ));
        assert!(v["base"].is_string());
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() -> u8 { 1 }\n").expect("edit");
        json_of(&scn_in(&wc, &["commit", "-m", "edit", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let v = json_of(&scn_in(&root.join("second"), &["sync", "--json"]));
        assert_eq!(v["outcome"], "updated");
        assert_eq!(
            std::fs::read_to_string(root.join("second/src/lib.rs")).expect("read"),
            "pub fn f() -> u8 { 1 }\n"
        );

        // 错误：没有改动时 land → 退出码 1 并给出提示
        let out = scn_in(&wc, &["land"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&out.stderr).contains("hint:"));
        // 不在工作副本中
        let out = scn_in(&root, &["status", "--json"]);
        assert_eq!(out.status.code(), Some(1));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn import_git_repository() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("imported").await.expect("repo");
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .current_dir(root.join("src-repo"))
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "A")
                .env("GIT_AUTHOR_EMAIL", "a@x")
                .env("GIT_COMMITTER_NAME", "A")
                .env("GIT_COMMITTER_EMAIL", "a@x")
                .output()
                .expect("git");
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        };
        std::fs::create_dir(root.join("src-repo")).expect("mkdir");
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(root.join("src-repo/hello.txt"), "from git\n").expect("write");
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "first"]);

        let v = json_of(&scn_in(
            &root,
            &[
                "import",
                "git",
                "src-repo",
                "--repo",
                "imported",
                "--endpoint",
                &u,
                "--json",
            ],
        ));
        assert_eq!(v["schema"], "scion.cli.import/v1");
        assert_eq!(v["commits"], 1);
        let out = scn_in(&root, &["cat", "hello.txt", "--repo", "imported", "--endpoint", &u]);
        assert_eq!(out.stdout, b"from git\n");
        let text = scn_in(
            &root,
            &["import", "git", "src-repo", "--repo", "imported", "--endpoint", &u],
        );
        assert!(String::from_utf8_lossy(&text.stdout).contains("imported 1 commits"));
        // 不是 git 仓库
        let out = scn_in(&root, &["import", "git", ".", "--repo", "imported", "--endpoint", &u]);
        assert_eq!(out.status.code(), Some(1));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn log_and_diff_commands() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("hist").await.expect("repo");
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        json_of(&scn_in(&root, &["clone", "hist", "--endpoint", &u, "--json"]));
        let wc = root.join("hist");
        std::fs::write(wc.join("a.txt"), "one\ntwo\n").expect("write");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "first", "--author", "Ada <a@x>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        std::fs::write(wc.join("a.txt"), "one\nthree\n").expect("edit");
        std::fs::write(wc.join("b.bin"), [0u8, 1, 2]).expect("binary");

        // 本地未提交的改动
        let out = scn_in(&wc, &["diff"]);
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        assert!(text.contains("-two") && text.contains("+three"), "{text}");
        assert!(text.contains("Binary files a/b.bin and b/b.bin differ"), "{text}");
        let v = json_of(&scn_in(&wc, &["diff", "--name-only", "--json"]));
        assert_eq!(v["files"].as_array().expect("files").len(), 2);
        let v = json_of(&scn_in(&wc, &["diff", "--path", "a.txt", "--json"]));
        assert_eq!(v["files"][0]["status"], "M");

        json_of(&scn_in(&wc, &["commit", "-m", "second", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));

        // log：从新到旧，按路径过滤
        let v = json_of(&scn_in(&wc, &["log", "--json"]));
        let msgs: Vec<_> = v["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .map(|e| e["message"].as_str().expect("msg").to_owned())
            .collect();
        assert_eq!(msgs, ["second", "first"]);
        let first_id = v["entries"][1]["id"].as_str().expect("id").to_owned();
        let v = json_of(&scn_in(&wc, &["log", "--path", "b.bin", "--json"]));
        assert_eq!(v["entries"].as_array().expect("entries").len(), 1);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["log", "-n", "1"]).stdout).to_string();
        assert!(text.contains("second") && !text.contains("first"), "{text}");

        // 某个提交引入的改动；两个版本之间的差异
        let v = json_of(&scn_in(&wc, &["diff", "main", "--name-only", "--json"]));
        let names: Vec<_> = v["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|f| f["path"].as_str().expect("p").to_owned())
            .collect();
        assert_eq!(names, ["a.txt", "b.bin"]);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["diff", &first_id, "main"]).stdout).to_string();
        assert!(text.contains("+three"), "{text}");
        // blame：a.txt 第 1 行来自 first，第 2 行来自 second
        let v = json_of(&scn_in(&wc, &["blame", "a.txt", "--json"]));
        let commits: Vec<_> = v["lines"]
            .as_array()
            .expect("lines")
            .iter()
            .map(|l| l["commit"].as_str().expect("c").to_owned())
            .collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0], first_id);
        assert_ne!(commits[1], first_id);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["blame", "main:a.txt"]).stdout).to_string();
        assert!(text.contains("| three"), "{text}");
        let out = scn_in(&wc, &["diff", "a", "b", "c"]);
        assert_eq!(out.status.code(), Some(2));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}
