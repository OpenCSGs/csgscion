//! 差分测试：用真实 git 构造历史，导入 Scion 后逐个提交与 git 比对文件列表、mode、内容与元数据。

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_client::Client;
use scion_gitimport::{Error, GIT_SHA1_EXTENSION, import, parse_commit};
use scion_object::{Changeset, EntryMode, ObjectId, Tree};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

struct Server {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn server() -> Server {
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
    Server { url, _shutdown: tx }
}

/// 在 `dir` 中运行 git；时间和身份固定，不读取用户的全局配置。
fn git(dir: &Path, args: &[&str]) -> Vec<u8> {
    git_at(dir, args, "1700000000 +0800")
}

fn git_at(dir: &Path, args: &[&str], date: &str) -> Vec<u8> {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada Lovelace")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "CI Bot")
        .env("GIT_COMMITTER_EMAIL", "ci@example.com")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn write(dir: &Path, rel: &str, content: &[u8]) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    std::fs::write(p, content).expect("write");
}

fn commit(dir: &Path, msg: &str, date: &str) {
    git(dir, &["add", "-A"]);
    git_at(dir, &["commit", "-q", "--allow-empty", "-m", msg], date);
}

/// 构造覆盖各种边界情况的 git 历史。
fn build_history(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "README.md", b"hello\n");
    write(dir, "src/a.rs", b"fn a() {}\n");
    write(dir, "data/blob.bin", &[0, 159, 146, 150, 255, 0, 1]);
    write(dir, "模型/权重.txt", "中文路径\n".as_bytes());
    commit(dir, "init", "1700000000 +0800");

    write(dir, "src/a.rs", b"fn a() { println!(\"changed\"); }\n");
    write(dir, "run.sh", b"#!/bin/sh\necho run\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.join("run.sh"), std::fs::Permissions::from_mode(0o755)).expect("chmod");
        std::os::unix::fs::symlink("README.md", dir.join("link")).expect("symlink");
    }
    commit(dir, "modify, exec, symlink", "1700000100 -0500");

    git(dir, &["mv", "src/a.rs", "src/b.rs"]);
    std::fs::remove_file(dir.join("data/blob.bin")).expect("rm");
    commit(dir, "rename and delete", "1700000200 +0000");

    // 分支与合并
    git(dir, &["checkout", "-q", "-b", "feature"]);
    write(dir, "feature.txt", b"feature\n");
    commit(dir, "feature work", "1700000300 +0100");
    git(dir, &["checkout", "-q", "main"]);
    write(dir, "main.txt", b"main\n");
    commit(dir, "main work", "1700000400 +0100");
    git_at(
        dir,
        &["merge", "-q", "--no-ff", "-m", "merge feature", "feature"],
        "1700000500 +0100",
    );

    // 目录变文件、文件变目录
    write(dir, "x/1", b"1");
    write(dir, "x/sub/2", b"2");
    commit(dir, "x is a dir", "1700000600 +0000");
    std::fs::remove_dir_all(dir.join("x")).expect("rm dir");
    write(dir, "x", b"now a file");
    commit(dir, "x is a file", "1700000700 +0000");
    std::fs::remove_file(dir.join("x")).expect("rm file");
    write(dir, "x/y", b"dir again");
    commit(dir, "x is a dir again", "1700000800 +0000");

    // 子模块（gitlink）与空提交
    git(
        dir,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,1111111111111111111111111111111111111111,vendor/sub",
        ],
    );
    git_at(dir, &["commit", "-q", "-m", "add submodule"], "1700000900 +0000");
    git_at(
        dir,
        &["commit", "-q", "--allow-empty", "-m", "empty commit\n\nwith body"],
        "1700001000 +0000",
    );
}

/// git 某个提交的全部文件：路径 → (mode, 内容的 Scion blob id)。子模块跳过。
fn git_files(dir: &Path, sha: &str) -> BTreeMap<String, (EntryMode, ObjectId)> {
    let out = git(dir, &["ls-tree", "-r", "-z", sha]);
    let mut files = BTreeMap::new();
    for rec in out.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        let rec = String::from_utf8(rec.to_vec()).expect("utf8");
        let (meta, path) = rec.split_once('\t').expect("tab");
        let parts: Vec<&str> = meta.split(' ').collect();
        let mode = match parts[0] {
            "100644" => EntryMode::File,
            "100755" => EntryMode::Executable,
            "120000" => EntryMode::Symlink,
            "160000" => continue,
            other => panic!("unexpected mode {other}"),
        };
        let content = git(dir, &["cat-file", "blob", parts[2]]);
        files.insert(path.to_owned(), (mode, ObjectId::of_blob(&content)));
    }
    files
}

/// Scion 某个提交的全部文件。
async fn scion_files(c: &Client, cs: ObjectId) -> BTreeMap<String, (EntryMode, ObjectId)> {
    let root = c.root_tree(&cs.to_string()).await.expect("root");
    let mut out = BTreeMap::new();
    let mut stack = vec![(String::new(), root)];
    while let Some((prefix, id)) = stack.pop() {
        let tree = Tree::decode(&c.get_object(&id).await.expect("tree")).expect("decode");
        for e in tree.entries() {
            let p = if prefix.is_empty() {
                e.name.clone()
            } else {
                format!("{prefix}/{}", e.name)
            };
            if e.mode == EntryMode::Tree {
                stack.push((p, e.id));
            } else {
                out.insert(p, (e.mode, e.id));
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn import_matches_git_commit_by_commit() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    build_history(dir);

    let mut calls = 0;
    let summary = import(&c, dir, "main", "main", |done, total| {
        calls += 1;
        assert!(done <= total);
    })
    .await
    .expect("import");
    let shas: Vec<String> = String::from_utf8(git(dir, &["rev-list", "--reverse", "--topo-order", "main"]))
        .expect("utf8")
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(summary.commits, shas.len());
    assert_eq!(calls, shas.len());
    assert_eq!(summary.skipped_gitlinks, 1);

    for sha in &shas {
        let cs = summary.mapping[sha];
        // 文件列表、mode、内容
        assert_eq!(scion_files(&c, cs).await, git_files(dir, sha), "tree mismatch at {sha}");
        // 元数据
        let git_commit = parse_commit(&git(dir, &["cat-file", "commit", sha])).expect("parse");
        let scion = Changeset::decode(&c.get_object(&cs).await.expect("changeset")).expect("decode");
        assert_eq!(scion.message, git_commit.message);
        assert_eq!(
            (scion.author.time_ms, scion.author.tz_offset_minutes),
            (git_commit.author.time_ms, git_commit.author.tz_offset_minutes)
        );
        assert_eq!(scion.committer.name, "CI Bot");
        assert_eq!(scion.extensions[GIT_SHA1_EXTENSION], hex::decode(sha).expect("hex"));
        let parents: Vec<ObjectId> = git_commit.parents.iter().map(|p| summary.mapping[p]).collect();
        assert_eq!(scion.parents, parents, "parents at {sha}");
    }

    let tip = c.resolve_ref("main").await.expect("resolve").expect("main exists");
    assert_eq!(Some(tip.changeset), summary.tip);
    assert_eq!(summary.mapping[shas.last().expect("tip")], tip.changeset);
}

#[tokio::test(flavor = "multi_thread")]
async fn reimport_is_idempotent_and_incremental_import_fast_forwards() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "a", b"1");
    commit(dir, "one", "1700000000 +0000");

    let first = import(&c, dir, "main", "main", |_, _| {}).await.expect("first");
    let again = import(&c, dir, "main", "main", |_, _| {}).await.expect("again");
    assert_eq!(first.mapping, again.mapping, "deterministic ids");
    assert_eq!(
        c.resolve_ref("main").await.expect("ok").expect("ref").version,
        1,
        "nothing to land"
    );

    // 新增两个提交：沿第一父提交链逐个快进
    write(dir, "a", b"2");
    commit(dir, "two", "1700000100 +0000");
    write(dir, "b", b"b");
    commit(dir, "three", "1700000200 +0000");
    let third = import(&c, dir, "main", "main", |_, _| {}).await.expect("incremental");
    let tip = c.resolve_ref("main").await.expect("ok").expect("ref");
    assert_eq!(Some(tip.changeset), third.tip);
    assert_eq!(tip.version, 3);

    // 改写历史后再导入：拒绝
    git(dir, &["reset", "-q", "--hard", "HEAD~2"]);
    write(dir, "a", b"rewritten");
    commit(dir, "rewritten", "1700000300 +0000");
    let err = import(&c, dir, "main", "main", |_, _| {}).await.expect_err("diverged");
    assert!(matches!(err, Error::Diverged { .. }), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn import_errors() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    // 不是 git 仓库
    assert!(matches!(
        import(&c, tmp.path(), "main", "main", |_, _| {}).await,
        Err(Error::Git(_))
    ));
    // 引用不存在
    git(tmp.path(), &["init", "-q", "-b", "main"]);
    assert!(matches!(
        import(&c, tmp.path(), "nope", "main", |_, _| {}).await,
        Err(Error::Git(_))
    ));
    // 服务端仓库不存在
    write(tmp.path(), "a", b"1");
    commit(tmp.path(), "one", "1700000000 +0000");
    let other = Client::builder()
        .endpoint(&srv.url)
        .repo("missing")
        .build()
        .await
        .expect("connect");
    assert!(matches!(
        import(&other, tmp.path(), "main", "main", |_, _| {}).await,
        Err(Error::Client(_))
    ));
}
