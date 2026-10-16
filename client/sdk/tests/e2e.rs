//! SDK ↔ 服务端端到端测试（M0 验收：能连上并完成能力协商）。

use std::sync::Arc;

use bytes::Bytes;
use scion_api::{App, Limits, ServerConfig, serve};
use scion_client::{Auth, Client, Error, ObjectId, ProtocolVersion, Role};
use scion_proto::v1::ErrorReason;
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use tokio::net::TcpListener;

struct TestServer {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn start(limits: Limits, blobs: &[&'static [u8]]) -> TestServer {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    for b in blobs {
        let id = ObjectId::of_blob(b);
        store.put(id, Bytes::from_static(b)).await.expect("put");
        store
            .index_objects(
                "mono",
                &[IndexedObject {
                    id,
                    size: b.len() as u64,
                }],
            )
            .await
            .expect("index");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let config = ServerConfig {
        limits,
        ..ServerConfig::default()
    };
    let app = App {
        config: Arc::new(config),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    TestServer { url, _shutdown: tx }
}

#[tokio::test]
async fn connect_negotiates_capabilities() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .auth(Auth::Bearer("dev-token".into()))
        .client_name("e2e")
        .build()
        .await
        .expect("connect");
    let info = c.server_info();
    assert_eq!(info.protocol_version, ProtocolVersion::client());
    assert_eq!(info.role, Role::Server);
    assert!(c.has_feature(scion_proto::features::OBJECTS_GET));
    assert!(!c.has_feature("land.batch"));
    assert_eq!(info.limits.max_batch_objects, 1_000);
}

#[tokio::test]
async fn get_object_multi_chunk_verified() {
    static BIG: [u8; 10_000] = [42; 10_000];
    let srv = start(
        Limits {
            max_chunk_bytes: 1_024,
            ..Limits::default()
        },
        &[&BIG],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let got = c.get_object(&ObjectId::of_blob(&BIG)).await.expect("get");
    assert_eq!(got.as_ref(), &BIG[..]);
}

#[tokio::test]
async fn missing_objects_batches_by_server_limit() {
    let srv = start(
        Limits {
            max_batch_objects: 2,
            ..Limits::default()
        },
        &[b"a", b"c"],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let ids: Vec<_> = [b"a", b"b", b"c", b"d", b"e"]
        .iter()
        .map(|d| ObjectId::of_blob(*d))
        .collect();
    let missing = c.missing_objects(&ids).await.expect("has");
    assert_eq!(missing, vec![ids[1], ids[3], ids[4]]);
    assert_eq!(c.missing_objects(&[]).await.expect("empty"), vec![]);
}

#[tokio::test]
async fn errors_carry_reasons() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let err = c.get_object(&ObjectId::of_blob(b"absent")).await.expect_err("missing");
    assert_eq!(err.reason(), Some(ErrorReason::ObjectMissing));
    assert!(err.is_not_found());
    assert!(!err.is_retryable());

    let other = Client::builder()
        .endpoint(&srv.url)
        .repo("nope")
        .build()
        .await
        .expect("connect");
    let err = other
        .missing_objects(&[ObjectId::of_blob(b"")])
        .await
        .expect_err("no repo");
    assert_eq!(err.reason(), Some(ErrorReason::RepoNotFound));

    let norepo = Client::builder().endpoint(&srv.url).build().await.expect("connect");
    assert!(matches!(
        norepo.get_object(&ObjectId::of_blob(b"")).await,
        Err(Error::MissingRepo)
    ));
}

#[tokio::test]
async fn connect_refused_is_retryable_connect_error() {
    // 绑定后立即释放端口，确保没有服务在监听
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind")
        .local_addr()
        .expect("addr");
    let err = Client::builder()
        .endpoint(format!("http://{addr}"))
        .connect_timeout(std::time::Duration::from_secs(2))
        .build()
        .await
        .expect_err("refused");
    assert!(matches!(err, Error::Connect { .. }), "{err:?}");
    assert!(err.is_retryable());
}

#[tokio::test]
async fn write_land_and_query_end_to_end() {
    // 小分块：强制上传和读取都走多块路径
    let srv = start(
        Limits {
            max_chunk_bytes: 4,
            ..Limits::default()
        },
        &[],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(c.has_feature(scion_proto::features::WRITE_CHANGESET));
    assert_eq!(c.resolve_ref("main").await.expect("resolve"), None);

    // 根提交：两个文件，其中一个可执行
    let root = c
        .changeset(None)
        .put("src/main.rs", "fn main() {}\n")
        .put_with_mode("bin/run.sh", scion_client::EntryMode::Executable, "#!/bin/sh\n")
        .message("init")
        .author("Ada", "ada@example.com")
        .create()
        .await
        .expect("create root");
    assert_eq!(root.generation, 1);
    let landed = c.land("main", &root.id).await.expect("land root");
    assert_eq!((landed.tip, landed.version), (root.id, 1));
    // 重试安全
    assert_eq!(c.land("main", &root.id).await.expect("retry"), landed);

    let tip = c.resolve_ref("main").await.expect("resolve").expect("exists");
    assert_eq!((tip.changeset, tip.root_tree), (root.id, root.root_tree));
    assert_eq!(
        c.read_file("main", "src/main.rs").await.expect("read").as_ref(),
        b"fn main() {}\n"
    );
    let top: Vec<_> = c
        .list_dir("main", "")
        .await
        .expect("ls")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(top, ["bin", "src"]);
    let bin = c.list_dir("main", "bin").await.expect("ls bin");
    assert_eq!(bin[0].mode, scion_client::EntryMode::Executable);

    // 子提交：修改 + 删除；同样的内容已上传，不会重复上传
    let child = c
        .changeset(Some(root.id))
        .put("src/main.rs", "fn main() { println!(\"hi\"); }\n")
        .delete("bin/run.sh")
        .message("edit")
        .create()
        .await
        .expect("create child");
    assert_eq!(child.generation, 2);
    c.land("main", &child.id).await.expect("land child");
    assert_eq!(
        c.list_dir("main", "").await.expect("ls").len(),
        1,
        "bin/ removed when emptied"
    );
    // 历史版本仍可按提交 id 读取
    assert_eq!(
        c.read_file(&root.id.to_string(), "bin/run.sh")
            .await
            .expect("old")
            .as_ref(),
        b"#!/bin/sh\n"
    );
    assert_eq!(
        c.root_tree(&root.id.to_string()).await.expect("root tree"),
        root.root_tree
    );
    assert_eq!(c.root_tree("main").await.expect("root tree"), child.root_tree);
    let refs = c.list_refs().await.expect("list refs");
    assert_eq!(refs, vec![("main".to_string(), child.id, 2)]);

    // 基于旧提交的改动不能快进合入
    let stale = c.changeset(Some(root.id)).put("x", "x").create().await.expect("stale");
    let err = c.land("main", &stale.id).await.expect_err("not fast forward");
    assert_eq!(err.reason(), Some(ErrorReason::NotFastForward));

    // 错误映射
    let err = c.read_file("main", "nope").await.expect_err("missing path");
    assert_eq!(err.reason(), Some(ErrorReason::PathNotFound));
    let err = c
        .changeset(Some(root.id))
        .delete("nope")
        .create()
        .await
        .expect_err("bad delta");
    assert_eq!(err.reason(), Some(ErrorReason::InvalidChangeset));
}

#[tokio::test]
async fn explicit_signatures_and_change_id_make_ids_deterministic() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let sig = |name: &str, t| scion_client::Signature {
        name: name.into(),
        email: format!("{name}@example.com"),
        time_ms: t,
        tz_offset_minutes: 480,
    };
    let make = || {
        c.changeset(None)
            .put("a", "a")
            .message("imported")
            .author_signature(sig("ada", 1_000))
            .committer_signature(sig("bot", 2_000))
            .change_id([9; 16])
            .extension("git.sha1", vec![0xab; 20])
    };
    let first = make().create().await.expect("first");
    let second = make().create().await.expect("second");
    assert_eq!(first.id, second.id, "same input gives the same changeset id");
    let other = c
        .changeset(None)
        .put("a", "a")
        .message("imported")
        .create()
        .await
        .expect("random ids");
    assert_ne!(other.id, first.id);
}

#[tokio::test]
async fn upload_above_server_limit_rejected_client_side() {
    let srv = start(
        Limits {
            max_upload_object_bytes: 8,
            ..Limits::default()
        },
        &[],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let err = c
        .upload_blobs(&[Bytes::from_static(b"far too large")])
        .await
        .expect_err("too big");
    assert!(matches!(err, Error::Protocol(_)));
    // 已存在的内容不会再上传
    let ids = c.upload_blobs(&[Bytes::from_static(b"ok")]).await.expect("small");
    assert_eq!(c.upload_blobs(&[Bytes::from_static(b"ok")]).await.expect("again"), ids);
}

#[tokio::test]
async fn history_queries_end_to_end() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(c.has_feature(scion_proto::features::QUERY_LOG));
    let a = c
        .changeset(None)
        .put("README", "1")
        .message("a")
        .create()
        .await
        .expect("a");
    let b = c
        .changeset(Some(a.id))
        .put("src/x.rs", "2")
        .message("b")
        .create()
        .await
        .expect("b");
    let d = c
        .changeset(Some(a.id))
        .put("other", "3")
        .message("d")
        .create()
        .await
        .expect("d");
    // 合并提交的 delta 相对第一个父提交：从 d 合入的 other 也要写进 delta
    let m = c
        .changeset(Some(b.id))
        .parent(d.id)
        .put("README", "4")
        .put("other", "3")
        .message("merge")
        .create()
        .await
        .expect("m");
    for x in [a.id, b.id, m.id] {
        c.land("main", &x).await.expect("land");
    }

    // 分页遍历全部历史
    let mut token: Option<String> = None;
    let mut seen = Vec::new();
    loop {
        let page = c.log("main", None, 2, token.as_deref()).await.expect("log");
        seen.extend(page.entries.iter().map(|e| e.id));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(seen.len(), 4);
    assert_eq!(seen[0], m.id);
    let first = c.log("main", None, 1, None).await.expect("log");
    assert_eq!(first.entries[0].parents, vec![b.id, d.id]);
    assert_eq!(first.entries[0].message, "merge");
    // 按路径过滤
    let readme = c.log("main", Some("README"), 10, None).await.expect("log");
    assert_eq!(
        readme.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![m.id, a.id]
    );
    let other = c.log("main", Some("other"), 10, None).await.expect("log");
    assert_eq!(
        other.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![m.id, d.id],
        "d reached via the second parent"
    );

    assert!(c.is_ancestor(&d.id.to_string(), "main").await.expect("ok"));
    assert!(!c.is_ancestor("main", &a.id.to_string()).await.expect("ok"));
    assert_eq!(
        c.merge_bases(&b.id.to_string(), &d.id.to_string()).await.expect("ok"),
        vec![a.id]
    );

    let changes = c.diff(Some(&a.id.to_string()), "main", None).await.expect("diff");
    let paths: Vec<_> = changes.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["README", "other", "src/x.rs"]);
    assert!(changes[1].old.is_none() && changes[1].new.is_some());
    assert_eq!(c.diff(None, "main", Some("src")).await.expect("diff").len(), 1);
    let err = c.diff(Some("nope"), "main", None).await.expect_err("bad revision");
    assert_eq!(err.reason(), Some(ErrorReason::RefNotFound));
}
