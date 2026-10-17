//! 只读端点：服务端在 ServerInfo 中推荐区域缓存，SDK 从它读对象；不可达时改为直接从服务端读。

use std::sync::Arc;

use scion_api::{App, Limits, ReadEndpoint, ServerConfig};
use scion_client::{Client, ObjectId, ReadEndpoints};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

async fn l3(read_endpoints: Vec<ReadEndpoint>) -> (String, tokio::sync::oneshot::Sender<()>) {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            read_endpoints,
            limits: Limits {
                max_batch_objects: 3,
                max_chunk_bytes: 512,
                ..Limits::default()
            },
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    (url, tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn objects_are_read_through_the_advertised_cache() {
    // 先占好缓存的端口，服务端才能在 ServerInfo 中推荐它
    let cache_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let cache_url = format!("http://{}", cache_listener.local_addr().expect("addr"));
    let (server, _stop) = l3(vec![ReadEndpoint {
        url: cache_url.clone(),
        region: "local".into(),
    }])
    .await;
    let config = scion_cache::CacheConfig {
        upstreams: vec![server.clone()],
        stats_interval: None,
        ..scion_cache::CacheConfig::default()
    };
    let (node, info, channels) = scion_cache::build(&config).await.expect("cache");
    let n = node.clone();
    tokio::spawn(async move {
        scion_cache::serve_node(cache_listener, n, info, channels, &config, std::future::pending()).await
    });

    let c = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert_eq!(c.object_endpoint(), cache_url);
    assert!(!c.via_daemon());
    let mut b = c.changeset(None).message("seed");
    let files: Vec<Vec<u8>> = (0..7).map(|i| vec![i as u8; 1500]).collect();
    for (i, f) in files.iter().enumerate() {
        b = b.put(format!("f{i}"), f.clone());
    }
    b.create().await.expect("create");
    // 批量读取：跨多个批次（每批 3 个）、每个对象多个分块（每块 512 字节）
    let ids: Vec<ObjectId> = files.iter().map(|f| ObjectId::of_blob(f)).collect();
    let got = c.get_objects(&ids).await.expect("batch");
    assert_eq!(got.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), files);
    assert_eq!(node.stats().upstream_objects, 7, "served by the cache");
    // 直接读服务端
    let direct = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .read_endpoints(ReadEndpoints::Off)
        .build()
        .await
        .expect("connect");
    assert_eq!(direct.object_endpoint(), server);
    assert_eq!(direct.get_object(&ids[0]).await.expect("direct").to_vec(), files[0]);
    assert_eq!(node.stats().hits, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_read_endpoint_falls_back_to_the_server() {
    let (server, _stop) = l3(vec![ReadEndpoint {
        url: "http://127.0.0.1:1".into(),
        region: String::new(),
    }])
    .await;
    let c = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .read_endpoints(ReadEndpoints::Region("anywhere".into()))
        .build()
        .await
        .expect("connect");
    assert_eq!(c.object_endpoint(), server);
    let cs = c.changeset(None).put("a", "x").create().await.expect("create");
    assert!(c.get_object(&cs.root_tree).await.is_ok());
}
