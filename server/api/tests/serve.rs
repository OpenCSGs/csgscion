//! 通过真实 TCP 连接调用服务，覆盖 `serve` 的装配、健康检查与优雅退出。

use std::sync::Arc;

use bytes::Bytes;
use scion_api::{App, ServerConfig, serve};
use scion_object::ObjectId;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::server_info_service_client::ServerInfoServiceClient;
use scion_proto::v1::{GetObjectsRequest, GetServerInfoRequest, HasObjectsRequest, ObjectWant};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, health_check_response::ServingStatus};

#[tokio::test]
async fn serve_end_to_end_and_shutdown() {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let data = Bytes::from(vec![7u8; 3_000_000]); // 大于单块上限，触发多块
    let id = ObjectId::of_blob(&data);
    store.put(id, data.clone()).await.expect("put");
    store
        .index_objects(
            "mono",
            &[IndexedObject {
                id,
                size: data.len() as u64,
            }],
        )
        .await
        .expect("index");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    let server = tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::from_shared(url)
        .expect("url")
        .connect()
        .await
        .expect("connect");

    let info = ServerInfoServiceClient::new(channel.clone())
        .get_server_info(GetServerInfoRequest::default())
        .await
        .expect("server info")
        .into_inner()
        .info
        .expect("info");
    assert_eq!(info.protocol_version, scion_proto::PROTOCOL_VERSION);

    let health = HealthClient::new(channel.clone())
        .check(HealthCheckRequest {
            service: "scion.v1.ObjectService".into(),
        })
        .await
        .expect("health")
        .into_inner();
    assert_eq!(health.status(), ServingStatus::Serving);

    let mut objects = ObjectServiceClient::new(channel);
    let mut stream = objects
        .get_objects(GetObjectsRequest {
            repo: "mono".into(),
            wants: vec![ObjectWant {
                id: Bytes::copy_from_slice(&id.to_bytes()),
                offset: 0,
            }],
            accept_encodings: vec![],
        })
        .await
        .expect("get")
        .into_inner();
    let mut got = Vec::new();
    let mut chunks = 0;
    while let Some(part) = stream.message().await.expect("chunk") {
        got.extend_from_slice(&part.data);
        chunks += 1;
    }
    assert_eq!(chunks, 3);
    assert!(id.verify(&got));

    let missing = objects
        .has_objects(HasObjectsRequest {
            repo: "mono".into(),
            ids: vec![Bytes::copy_from_slice(&id.to_bytes())],
        })
        .await
        .expect("has")
        .into_inner();
    assert!(missing.missing_ids.is_empty());

    tx.send(()).expect("signal shutdown");
    server.await.expect("join").expect("server exited cleanly");
}

#[tokio::test]
async fn upload_blobs_stream_errors() {
    use scion_proto::v1::UploadBlobsRequest;
    use scion_proto::v1::write_service_client::WriteServiceClient;

    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    let server = tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let mut w = WriteServiceClient::new(channel);
    let id = ObjectId::of_blob(b"hello");
    let msg = |repo: &str, data: &'static [u8], offset, last| UploadBlobsRequest {
        repo: repo.into(),
        id: Bytes::copy_from_slice(&id.to_bytes()),
        offset,
        data: Bytes::from_static(data),
        last,
    };
    let code = |r: Result<tonic::Response<_>, tonic::Status>| r.map(|_| ()).map_err(|s| s.code());

    // 正常：两块
    let ok = w
        .upload_blobs(tokio_stream::iter(vec![
            msg("mono", b"hel", 0, false),
            msg("", b"lo", 3, true),
        ]))
        .await;
    assert_eq!(ok.expect("upload").into_inner().stored_ids.len(), 1);
    // 空流：成功，什么都不写
    let empty = w
        .upload_blobs(tokio_stream::iter(Vec::<UploadBlobsRequest>::new()))
        .await;
    assert!(empty.expect("empty").into_inner().stored_ids.is_empty());
    // 仓库不存在
    let r = w
        .upload_blobs(tokio_stream::iter(vec![msg("nope", b"hello", 0, true)]))
        .await;
    assert_eq!(code(r), Err(tonic::Code::NotFound));
    // 流在对象中途结束
    let r = w
        .upload_blobs(tokio_stream::iter(vec![msg("mono", b"hel", 0, false)]))
        .await;
    assert_eq!(code(r), Err(tonic::Code::InvalidArgument));
    // 内容与 id 不符
    let r = w
        .upload_blobs(tokio_stream::iter(vec![msg("mono", b"jello", 0, true)]))
        .await;
    assert_eq!(code(r), Err(tonic::Code::InvalidArgument));

    tx.send(()).expect("shutdown");
    server.await.expect("join").expect("clean exit");
}

/// 按仓库的写入速率限制：超过后返回可重试的 RESOURCE_EXHAUSTED / RATE_LIMITED，其它仓库不受影响。
#[tokio::test]
async fn repo_write_rate_limit() {
    use scion_proto::v1::write_service_client::WriteServiceClient;
    use scion_proto::v1::{ChangesetSpec, CreateChangesetRequest, Signature};
    use tonic_types::StatusExt;

    let store = Arc::new(MemStore::new());
    store.create_repo("hot").await.expect("repo");
    store.create_repo("cold").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    let config = ServerConfig {
        repo_write_rate: Some(scion_api::WriteRate {
            per_second: 1,
            burst: 2,
        }),
        ..ServerConfig::default()
    };
    let app = App {
        config: Arc::new(config),
        meta: store.clone(),
        blobs: store,
    };
    let server = tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let mut c = WriteServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let sig = Signature {
        name: "a".into(),
        email: "a@x".into(),
        time_ms: 0,
        tz_offset_minutes: 0,
    };
    let req = |repo: &str, n: u8| CreateChangesetRequest {
        repo: repo.into(),
        request_id: String::new(),
        changeset: Some(ChangesetSpec {
            parents: vec![],
            author: Some(sig.clone()),
            committer: Some(sig.clone()),
            message: format!("m{n}"),
            change_id: Bytes::from(vec![n; 16]),
            delta: vec![],
            extensions: Default::default(),
        }),
    };
    c.create_changeset(req("hot", 1)).await.expect("1");
    c.create_changeset(req("hot", 2)).await.expect("2 (burst)");
    let err = c.create_changeset(req("hot", 3)).await.expect_err("limited");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    let info = err.get_error_details().error_info().cloned().expect("info");
    assert_eq!(info.reason, "RATE_LIMITED");
    assert!(info.metadata["retry_after_ms"].parse::<u64>().expect("ms") > 0);
    c.create_changeset(req("cold", 4)).await.expect("other repo unaffected");
    let _ = tx.send(());
    server.await.expect("join").expect("serve");
}
