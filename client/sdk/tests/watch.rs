//! WatchRefs 端到端：订阅、合入后收到变化、服务端重启后自动续订且不漏掉变化。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scion_api::{App, ServerConfig, serve};
use scion_client::Client;
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

async fn run(store: Arc<MemStore>, listener: TcpListener) -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    tx
}

async fn next(w: &mut scion_client::RefWatch) -> scion_client::RefChange {
    tokio::time::timeout(Duration::from_secs(10), w.next())
        .await
        .expect("timely")
        .expect("change")
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_follows_lands_across_server_restarts() {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    let stop = run(store.clone(), listener).await;
    let c = Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(c.has_feature(scion_proto::features::REFS_WATCH));

    let a = c.changeset(None).put("f", "1").message("a").create().await.expect("a");
    c.land("main", &a.id).await.expect("land a");
    let mut w = c.watch_refs(&["main"]).await.expect("watch");
    let first = next(&mut w).await;
    assert!(first.initial);
    assert_eq!((first.name.as_str(), first.changeset, first.version), ("main", a.id, 1));
    assert_eq!(first.root_tree, Some(a.root_tree));

    let b = c
        .changeset(Some(a.id))
        .put("f", "2")
        .message("b")
        .create()
        .await
        .expect("b");
    c.land("main", &b.id).await.expect("land b");
    c.land("dev", &b.id).await.expect("dev is not watched");
    let second = next(&mut w).await;
    assert!(!second.initial);
    assert_eq!((second.changeset, second.version), (b.id, 2));

    // 服务端停机期间 ref 前进（例如由集群中的其它节点合入）
    drop(stop);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rec = store.get_changeset("mono", &a.id).await.expect("get").expect("rec");
    store
        .cas_ref("mono", "main", scion_store::RefExpect::Version(2), rec.id)
        .await
        .expect("move main back to a");
    let listener = TcpListener::bind(addr).await.expect("rebind");
    let _stop = run(store.clone(), listener).await;
    // 重新订阅时带上已知版本：只收到新的状态
    let third = next(&mut w).await;
    assert!(third.initial, "comes from the snapshot of the new subscription");
    assert_eq!((third.changeset, third.version), (a.id, 3));
    assert_eq!(w.known_versions().get("main"), Some(&3));
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_reports_missing_repository() {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let _stop = run(store, listener).await;
    let c = Client::builder()
        .endpoint(format!("http://{addr}"))
        .repo("nope")
        .build()
        .await
        .expect("connect");
    let err = c.watch_refs(&[]).await.err().expect("missing repo");
    assert!(err.is_not_found(), "{err}");
}
