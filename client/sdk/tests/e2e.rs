//! SDK ↔ 服务端端到端测试（M0 验收：能连上并完成能力协商）。

use std::sync::Arc;

use bytes::Bytes;
use scion_api::{App, Limits, ServerConfig, serve};
use scion_client::{Auth, Client, Error, ObjectId, ProtocolVersion, Role};
use scion_proto::v1::ErrorReason;
use scion_store::{BlobStore, MemStore, MetaStore};
use tokio::net::TcpListener;

struct TestServer {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn start(limits: Limits, blobs: &[&'static [u8]]) -> TestServer {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    for b in blobs {
        store
            .put("mono", ObjectId::of_blob(b), Bytes::from_static(b))
            .await
            .expect("put");
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
