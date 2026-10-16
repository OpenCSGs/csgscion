//! 工作副本端到端：进程内启动服务端，两个工作副本模拟两个开发者协作。

use std::path::Path;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_client::Client;
use scion_proto::v1::ErrorReason;
use scion_store::{MemStore, MetaStore};
use scion_wc::{Error, Status, SyncOutcome, WorkingCopy};
use tokio::net::TcpListener;

struct Server {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn server() -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
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

async fn client(url: &str) -> Client {
    Client::builder()
        .endpoint(url)
        .repo("mono")
        .build()
        .await
        .expect("connect")
}

fn write(root: &Path, rel: &str, content: &str) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    std::fs::write(p, content).expect("write");
}

fn status(added: &[&str], modified: &[&str], deleted: &[&str]) -> Status {
    let v = |x: &[&str]| x.iter().map(|s| s.to_string()).collect();
    Status {
        added: v(added),
        modified: v(modified),
        deleted: v(deleted),
    }
}

#[tokio::test]
async fn two_developers_collaborate() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("alice"), tmp.path().join("bob"));

    // Alice 克隆空仓库，写文件，提交并合入
    let mut alice = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir)
        .await
        .expect("clone empty");
    assert!(alice.status().expect("status").is_clean());
    write(&a_dir, "src/main.rs", "fn main() {}\n");
    write(&a_dir, "README.md", "hello\n");
    assert_eq!(
        alice.status().expect("status"),
        status(&["README.md", "src/main.rs"], &[], &[])
    );
    let first = alice
        .commit(&c, "init", Some(("Alice", "alice@example.com")))
        .await
        .expect("commit")
        .expect("changes");
    assert!(
        alice.status().expect("status").is_clean(),
        "committed changes are part of the snapshot"
    );
    // ref 还没动：没有需要同步的
    assert_eq!(alice.sync(&c).await.expect("sync"), SyncOutcome::UpToDate);
    let landed = alice.land(&c).await.expect("land");
    assert_eq!(landed.tip, first.id);
    assert!(matches!(alice.land(&c).await, Err(Error::NothingToLand)));
    assert_eq!(alice.commit(&c, "noop", None).await.expect("noop"), None);

    // Bob 克隆，看到 Alice 的文件
    let mut bob = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir)
        .await
        .expect("clone");
    assert_eq!(
        std::fs::read_to_string(b_dir.join("src/main.rs")).expect("read"),
        "fn main() {}\n"
    );
    assert!(bob.status().expect("status").is_clean());
    assert_eq!(bob.state().base.as_deref(), Some(first.id.to_string().as_str()));

    // Bob 修改、删除、新增，并合入
    write(&b_dir, "src/main.rs", "fn main() { println!(\"hi\"); }\n");
    std::fs::remove_file(b_dir.join("README.md")).expect("rm");
    write(&b_dir, "docs/guide.md", "guide\n");
    assert_eq!(
        bob.status().expect("status"),
        status(&["docs/guide.md"], &["src/main.rs"], &["README.md"])
    );
    bob.commit(&c, "edit", None).await.expect("commit").expect("changes");
    bob.land(&c).await.expect("land");

    // Alice 同步，得到 Bob 的改动；被删的文件和变空的目录消失
    let outcome = alice.sync(&c).await.expect("sync");
    assert!(
        matches!(
            outcome,
            SyncOutcome::Updated {
                written: 2,
                removed: 1,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        std::fs::read_to_string(a_dir.join("docs/guide.md")).expect("read"),
        "guide\n"
    );
    assert!(!a_dir.join("README.md").exists());
    assert!(alice.status().expect("status").is_clean());
    assert_eq!(alice.sync(&c).await.expect("again"), SyncOutcome::UpToDate);

    // Bob 落后于 ref：直接合入被拒；sync 做文件级 rebase 后即可合入
    write(&b_dir, "late.txt", "late\n");
    write(&a_dir, "early.txt", "early\n");
    alice.commit(&c, "a", None).await.expect("commit").expect("changes");
    alice.land(&c).await.expect("alice lands first");
    bob.commit(&c, "bob's change", Some(("Bob", "bob@example.com")))
        .await
        .expect("commit")
        .expect("changes");
    let err = bob.land(&c).await.expect_err("bob is behind");
    assert!(
        matches!(&err, Error::Client(e) if e.reason() == Some(ErrorReason::NotFastForward)),
        "{err:?}"
    );
    let outcome = bob.sync(&c).await.expect("rebase");
    assert!(
        matches!(
            outcome,
            SyncOutcome::Rebased {
                written: 1,
                removed: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert!(b_dir.join("early.txt").exists(), "upstream change applied locally");
    assert!(bob.status().expect("status").is_clean());
    let landed = bob.land(&c).await.expect("land after rebase");
    // 合入后两边的文件一致
    alice.sync(&c).await.expect("alice syncs");
    assert_eq!(std::fs::read_to_string(a_dir.join("late.txt")).expect("read"), "late\n");
    assert_eq!(alice.state().base.as_deref(), Some(landed.tip.to_string().as_str()));
    let meta = c
        .read_file(&landed.tip.to_string(), "late.txt")
        .await
        .expect("server has it");
    assert_eq!(meta.as_ref(), b"late\n");
}

#[tokio::test]
async fn rebase_conflict_reported_and_identical_changes_merge() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir)
        .await
        .expect("clone");
    write(&a_dir, "shared.txt", "base\n");
    a.commit(&c, "base", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir)
        .await
        .expect("clone");

    // 双方都改了同一个文件且内容不同：冲突
    write(&a_dir, "shared.txt", "alice\n");
    a.commit(&c, "a", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    write(&b_dir, "shared.txt", "bob\n");
    b.commit(&c, "b", None).await.expect("commit").expect("changes");
    let err = b.sync(&c).await.expect_err("conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["shared.txt".to_string()]),
        "{err:?}"
    );
    // 冲突时本地不变
    assert_eq!(
        std::fs::read_to_string(b_dir.join("shared.txt")).expect("read"),
        "bob\n"
    );

    // 双方做了完全相同的改动：rebase 后没有剩余改动，直接同步
    let c_dir = tmp.path().join("c");
    let mut cc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &c_dir)
        .await
        .expect("clone");
    write(&a_dir, "same.txt", "same\n");
    a.commit(&c, "same", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    write(&c_dir, "same.txt", "same\n");
    cc.commit(&c, "same too", None).await.expect("commit").expect("changes");
    let outcome = cc.sync(&c).await.expect("sync");
    assert!(matches!(outcome, SyncOutcome::Updated { .. }), "{outcome:?}");
    assert!(cc.state().head.is_none());
    assert!(matches!(cc.land(&c).await, Err(Error::NothingToLand)));
}

#[tokio::test]
async fn sync_refuses_dirty_tree() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir)
        .await
        .expect("clone");
    assert_eq!(wc.sync(&c).await.expect("empty repo, no ref"), SyncOutcome::UpToDate);
    write(&dir, "a/b/c.txt", "x");
    assert!(matches!(wc.sync(&c).await, Err(Error::Dirty)));
}

#[tokio::test]
async fn open_from_subdirectory() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir)
        .await
        .expect("clone");
    write(&dir, "a/b/c.txt", "x");
    let wc = WorkingCopy::open(&dir.join("a/b")).expect("open from subdir");
    assert_eq!(wc.root(), dir.as_path());
    assert_eq!(wc.state().repo, "mono");
    assert_eq!(wc.state().endpoint, srv.url);
    assert!(matches!(WorkingCopy::open(tmp.path()), Err(Error::NotAWorkingCopy(_))));
}

#[tokio::test]
async fn clone_into_non_empty_dir_rejected() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    write(tmp.path(), "existing", "x");
    let err = WorkingCopy::clone(&c, &srv.url, "mono", "main", tmp.path())
        .await
        .err()
        .expect("rejected");
    assert!(matches!(err, Error::TargetNotEmpty(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn modes_roundtrip_and_mode_change_detected() {
    use std::os::unix::fs::PermissionsExt;
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir)
        .await
        .expect("clone");
    write(&a_dir, "run.sh", "#!/bin/sh\n");
    std::fs::set_permissions(a_dir.join("run.sh"), std::fs::Permissions::from_mode(0o755)).expect("chmod");
    std::os::unix::fs::symlink("run.sh", a_dir.join("latest")).expect("symlink");
    a.commit(&c, "modes", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");

    WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir)
        .await
        .expect("clone");
    let mode = std::fs::metadata(b_dir.join("run.sh"))
        .expect("meta")
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111, "executable bit restored");
    assert_eq!(
        std::fs::read_link(b_dir.join("latest")).expect("link").to_str(),
        Some("run.sh")
    );

    // 只改权限也算修改
    std::fs::set_permissions(a_dir.join("run.sh"), std::fs::Permissions::from_mode(0o644)).expect("chmod");
    assert_eq!(a.status().expect("status"), status(&[], &["run.sh"], &[]));
}

#[tokio::test]
async fn racy_same_size_edit_detected_by_hash() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir)
        .await
        .expect("clone");
    write(&dir, "f.txt", "aaaa");
    wc.commit(&c, "v1", None).await.expect("commit").expect("changes");
    // 同样大小的改动，并把修改时间改回与记录完全一致：只靠时间戳无法发现
    let recorded = std::fs::metadata(dir.join("f.txt"))
        .expect("meta")
        .modified()
        .expect("mtime");
    std::fs::write(dir.join("f.txt"), "bbbb").expect("edit");
    std::fs::File::options()
        .write(true)
        .open(dir.join("f.txt"))
        .expect("open")
        .set_modified(recorded)
        .expect("touch");
    // 记录的修改时间早于 dirstate 写入时间时，按规则视为未修改（与 git 相同的取舍）；
    // 但如果修改时间不早于写入时间（同一时间粒度内），必须重新计算哈希并发现改动。
    // 这里把 dirstate 的写入时间改回过去，模拟这种情况。
    let ds_path = dir.join(".scion/dirstate.json");
    let mut ds: serde_json::Value = serde_json::from_slice(&std::fs::read(&ds_path).expect("read")).expect("json");
    ds["written_at_ns"] = serde_json::json!(0);
    std::fs::write(&ds_path, serde_json::to_vec(&ds).expect("ser")).expect("write");
    assert_eq!(wc.status().expect("status"), status(&[], &["f.txt"], &[]));
}

#[tokio::test]
async fn concurrent_operations_are_locked_out() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir)
        .await
        .expect("clone");
    std::fs::write(dir.join(".scion/lock"), "12345").expect("simulate other process");
    write(&dir, "a", "a");
    assert!(matches!(wc.commit(&c, "m", None).await, Err(Error::Locked(_))));
    std::fs::remove_file(dir.join(".scion/lock")).expect("unlock");
    assert!(wc.commit(&c, "m", None).await.expect("commit").is_some());
}
