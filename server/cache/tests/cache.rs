//! 缓存节点与真实 L3 服务端的集成测试：读穿透与命中、分块重组、两节点分片（整个区域只回源一次）、
//! WatchRefs 预热、HasObjects 转发。

use std::sync::Arc;
use std::time::Duration;

use scion_api::{App, Limits, ServerConfig};
use scion_cache::{CacheConfig, CacheNode, StorageConfig, WarmOptions, WarmTarget};
use scion_client::{Client, ObjectId, Role};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const REPO: &str = "mono";

struct Running {
    url: String,
    _stop: oneshot::Sender<()>,
}

async fn l3(store: Arc<MemStore>) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let config = ServerConfig {
        limits: Limits {
            max_chunk_bytes: 1024,
            ..Limits::default()
        },
        ref_poll_interval: Duration::from_millis(50),
        ..ServerConfig::default()
    };
    let app = App {
        config: Arc::new(config),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    Running { url, _stop: tx }
}

/// 启动一个缓存节点，返回地址、节点（用于查看统计）与停止信号。
async fn cache(config: CacheConfig, listener: TcpListener) -> (String, Arc<CacheNode>, oneshot::Sender<()>) {
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (node, info, channels) = scion_cache::build(&config).await.expect("build");
    let (tx, rx) = oneshot::channel::<()>();
    let n = node.clone();
    tokio::spawn(async move {
        scion_cache::serve_node(listener, n, info, channels, &config, async {
            let _ = rx.await;
        })
        .await
        .expect("serve")
    });
    (url, node, tx)
}

fn config(upstream: &str) -> CacheConfig {
    CacheConfig {
        upstreams: vec![upstream.to_owned()],
        storage: StorageConfig {
            memory_bytes: 64 << 20,
            ..StorageConfig::default()
        },
        stats_interval: None,
        ..CacheConfig::default()
    }
}

async fn client(url: &str) -> Client {
    Client::builder()
        .endpoint(url)
        .repo(REPO)
        .build()
        .await
        .expect("connect")
}

async fn seed(l3: &Running, files: &[(&str, Vec<u8>)]) -> (Client, scion_client::CreatedChangeset) {
    let c = client(&l3.url).await;
    let mut b = c.changeset(None).message("seed");
    for (p, data) in files {
        b = b.put(*p, data.clone());
    }
    let cs = b.create().await.expect("create");
    c.land("main", &cs.id).await.expect("land");
    (c, cs)
}

fn store() -> Arc<MemStore> {
    Arc::new(MemStore::new())
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_through_and_then_hits() {
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let up = l3(s.clone()).await;
    let big = vec![7u8; 5000];
    let (_c, cs) = seed(&up, &[("big.bin", big.clone()), ("a.txt", b"a".to_vec())]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let (url, node, _stop) = cache(config(&up.url), listener).await;

    let via_cache = client(&url).await;
    assert_eq!(via_cache.server_info().role, Role::Cache);
    assert!(via_cache.has_feature(scion_proto::features::OBJECTS_GET));
    assert!(!via_cache.has_feature(scion_proto::features::LAND_DIRECT));
    let id = ObjectId::of_blob(&big);
    // 分块（L3 每块 1 KiB）在缓存里重组、校验后再按块返回
    assert_eq!(via_cache.get_object(&id).await.expect("miss").to_vec(), big);
    assert_eq!(via_cache.get_object(&id).await.expect("hit").to_vec(), big);
    assert!(via_cache.get_object(&cs.root_tree).await.is_ok());
    let st = node.stats();
    assert_eq!((st.hits, st.misses, st.upstream_objects), (1, 2, 2));

    // 不存在的对象：L3 的错误原样返回，且不影响缓存
    let err = via_cache
        .get_object(&ObjectId::of_blob(b"nope"))
        .await
        .expect_err("missing");
    assert_eq!(err.reason(), Some(scion_proto::v1::ErrorReason::ObjectMissing));
    // HasObjects 转给 L3
    let missing = via_cache
        .missing_objects(&[id, ObjectId::of_blob(b"nope")])
        .await
        .expect("has");
    assert_eq!(missing, [ObjectId::of_blob(b"nope")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn sharded_nodes_fetch_each_object_from_upstream_once() {
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let up = l3(s.clone()).await;
    let files: Vec<(String, Vec<u8>)> = (0..40)
        .map(|i| (format!("f{i}"), format!("content {i}").into_bytes()))
        .collect();
    let refs: Vec<(&str, Vec<u8>)> = files.iter().map(|(p, d)| (p.as_str(), d.clone())).collect();
    seed(&up, &refs).await;
    let ids: Vec<ObjectId> = files.iter().map(|(_, d)| ObjectId::of_blob(d)).collect();

    let la = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let lb = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let (ua, ub) = (
        format!("http://{}", la.local_addr().expect("a")),
        format!("http://{}", lb.local_addr().expect("b")),
    );
    let mut ca = config(&up.url);
    ca.advertise_url = Some(ua.clone());
    ca.peers = vec![ub.clone()];
    let mut cb = config(&up.url);
    cb.advertise_url = Some(ub.clone());
    cb.peers = vec![ua.clone()];
    let (_, na, _sa) = cache(ca, la).await;
    let (_, nb, _sb) = cache(cb, lb).await;

    // 客户端分别从两个节点读全部对象
    let (a, b) = (client(&ua).await, client(&ub).await);
    for id in &ids {
        a.get_object(id).await.expect("via a");
    }
    for id in &ids {
        b.get_object(id).await.expect("via b");
    }
    let (sa, sb) = (na.stats(), nb.stats());
    assert_eq!(
        sa.upstream_objects + sb.upstream_objects,
        ids.len() as u64,
        "each object fetched from L3 exactly once across the region"
    );
    assert!(sa.peer_objects > 0 && sb.upstream_objects > 0, "{sa:?} {sb:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn warms_trees_when_refs_advance() {
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let up = l3(s.clone()).await;
    let (c, first) = seed(&up, &[("src/a.rs", b"fn a() {}".to_vec())]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let mut cfg = config(&up.url);
    cfg.warm = vec![WarmTarget {
        repo: REPO.into(),
        refs: vec!["main".into()],
    }];
    cfg.warm_options = WarmOptions {
        max_blob_bytes: 1024,
        ..WarmOptions::default()
    };
    let (_url, node, _stop) = cache(cfg, listener).await;

    let wait_for = |id: ObjectId| {
        let node = node.clone();
        async move {
            for _ in 0..200 {
                if node.store().in_memory(&id) {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            false
        }
    };
    // 订阅时的快照：预热整棵树
    assert!(wait_for(first.root_tree).await, "initial tree warmed");
    assert!(wait_for(ObjectId::of_blob(b"fn a() {}")).await, "small blob warmed");

    let second = c
        .changeset(Some(first.id))
        .put("src/b.rs", "fn b() {}")
        .message("b")
        .create()
        .await
        .expect("b");
    c.land("main", &second.id).await.expect("land b");
    assert!(
        wait_for(second.root_tree).await,
        "new tree warmed after the ref advanced"
    );
    assert!(wait_for(ObjectId::of_blob(b"fn b() {}")).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn configuration_errors() {
    assert!(matches!(
        scion_cache::build(&CacheConfig::default()).await,
        Err(scion_cache::CacheError::Config(_))
    ));
    let mut bad = config("not a url");
    assert!(matches!(
        scion_cache::build(&bad).await,
        Err(scion_cache::CacheError::Config(_))
    ));
    // 上游不可达
    bad.upstreams = vec!["http://127.0.0.1:1".into()];
    assert!(matches!(
        scion_cache::build(&bad).await,
        Err(scion_cache::CacheError::Upstream(_))
    ));
    // --peer 需要 --advertise-url
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let up = l3(s).await;
    let mut peers = config(&up.url);
    peers.peers = vec!["http://127.0.0.1:2".into()];
    assert!(matches!(
        scion_cache::build(&peers).await,
        Err(scion_cache::CacheError::Config(_))
    ));
    // serve 直接使用配置
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let mut ok = config(&up.url);
    ok.stats_interval = Some(Duration::from_millis(10));
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(scion_cache::serve(listener, ok, async {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = tx.send(());
    task.await.expect("join").expect("clean shutdown");
}

/// 与 L3 一致：对象只对索引中有它的仓库可见。另一个仓库读过、已在缓存里的对象，对看不到它的仓库
/// 返回与 L3 相同的错误；对能看到它的仓库直接复用缓存的字节。
#[tokio::test(flavor = "multi_thread")]
async fn cached_objects_are_not_visible_across_repositories() {
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    s.create_repo("other").await.expect("other repo");
    let up = l3(s.clone()).await;
    seed(&up, &[("secret.txt", b"only in mono".to_vec())]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let (url, node, _stop) = cache(config(&up.url), listener).await;
    let id = ObjectId::of_blob(b"only in mono");

    let mono = client(&url).await;
    mono.get_object(&id).await.expect("visible in mono");
    let other = Client::builder()
        .endpoint(&url)
        .repo("other")
        .build()
        .await
        .expect("connect");
    let direct_other = Client::builder()
        .endpoint(&up.url)
        .repo("other")
        .build()
        .await
        .expect("connect");
    let via_cache = other.get_object(&id).await.expect_err("not visible in other");
    let from_l3 = direct_other.get_object(&id).await.expect_err("L3 agrees");
    assert_eq!(via_cache.reason(), from_l3.reason());
    assert_eq!(via_cache.reason(), Some(scion_proto::v1::ErrorReason::ObjectMissing));
    assert_eq!(node.stats().upstream_objects, 1, "bytes fetched once");

    // 之后对象也进入了 other 的索引（例如上传了同样的内容）：从缓存复用，不再回源字节
    let c = Client::builder()
        .endpoint(&up.url)
        .repo("other")
        .build()
        .await
        .expect("connect");
    c.changeset(None)
        .put("copy.txt", "only in mono")
        .create()
        .await
        .expect("upload");
    assert_eq!(
        other.get_object(&id).await.expect("now visible").as_ref(),
        b"only in mono"
    );
    assert_eq!(node.stats().upstream_objects, 1);
    // 不存在的仓库：上游的错误原样返回
    let nowhere = Client::builder()
        .endpoint(&url)
        .repo("nowhere")
        .build()
        .await
        .expect("connect");
    assert!(nowhere.get_object(&id).await.expect_err("no repo").is_not_found());
}

/// 服务端要求认证：缓存命中前也按调用者的令牌确认读权限；只能读部分路径的身份不能按 id 读对象；
/// 预热用配置的令牌订阅和回源。
#[tokio::test(flavor = "multi_thread")]
async fn cache_enforces_upstream_access_control() {
    use scion_api::scion_auth::{Guard, GuardConfig};
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let auth = GuardConfig {
        required: true,
        ..GuardConfig::default()
    };
    let guard = Guard::build(auth.clone(), s.clone(), std::sync::Arc::new(|_: &str| None)).expect("guard");
    guard
        .set_policy(
            "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"land\"]\n\n\
             [[rule]]\nsubjects=[\"user:contractor\"]\nrepos=[\"mono\"]\nactions=[\"read\"]\npaths=[\"src/ui\"]\n",
        )
        .await
        .expect("policy");
    let (eng, _) = guard
        .create_token("pat", "bob", vec!["eng".into()], "", None)
        .await
        .expect("eng");
    let (ct, _) = guard
        .create_token("pat", "contractor", vec![], "", None)
        .await
        .expect("ct");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let up_url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            ref_poll_interval: Duration::from_millis(50),
            ..ServerConfig::default()
        }),
        meta: s.clone(),
        blobs: s.clone(),
    };
    let (_stop_l3, rx) = oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));

    let as_user = |url: String, token: Option<String>| async move {
        let mut b = Client::builder().endpoint(url).repo(REPO);
        if let Some(t) = token {
            b = b.auth(scion_client::Auth::Bearer(t));
        }
        b.build().await.expect("connect")
    };
    let writer = as_user(up_url.clone(), Some(eng.clone())).await;
    let cs = writer
        .changeset(None)
        .put("src/ui/a.rs", "ui")
        .create()
        .await
        .expect("create");
    writer.land("main", &cs.id).await.expect("land");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let mut cfg = config(&up_url);
    cfg.warm = vec![WarmTarget {
        repo: REPO.into(),
        refs: vec!["main".into()],
    }];
    cfg.warm_options.token = Some(format!("Bearer {eng}").parse().expect("header"));
    let (url, node, _stop) = cache(cfg, listener).await;

    let blob = ObjectId::of_blob(b"ui");
    let via_eng = as_user(url.clone(), Some(eng.clone())).await;
    assert_eq!(via_eng.get_object(&blob).await.expect("eng reads").as_ref(), b"ui");
    // 已在缓存里，但 contractor 只能读部分路径，不能按 id 读对象；匿名需要认证
    let via_ct = as_user(url.clone(), Some(ct)).await;
    let e = via_ct.get_object(&blob).await.expect_err("path-scoped reader");
    assert_eq!(e.reason(), Some(scion_proto::v1::ErrorReason::PermissionDenied));
    let anon = as_user(url, None).await;
    assert!(matches!(
        anon.get_object(&blob).await.expect_err("anonymous"),
        scion_client::Error::Rpc {
            code: tonic::Code::Unauthenticated,
            ..
        }
    ));
    // 预热带令牌：订阅成功，根目录树进入缓存
    let mut warmed = false;
    for _ in 0..200 {
        if node.store().in_memory(&cs.root_tree) {
            warmed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(warmed, "warm-up authenticated with the configured token");
}

/// redaction：缓存层用调用者的身份同步，redact 过的对象返回 REDACTED 并从本地删除；撤销后恢复。
#[tokio::test(flavor = "multi_thread")]
async fn redacted_objects_are_refused_by_the_cache() {
    let s = store();
    s.create_repo(REPO).await.expect("repo");
    let up = l3(s.clone()).await;
    let secret = b"password=hunter2".to_vec();
    seed(&up, &[("secret.txt", secret.clone()), ("a.txt", b"a".to_vec())]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let (url, node, _stop) = cache(
        CacheConfig {
            redaction_ttl: Duration::ZERO,
            ..config(&up.url)
        },
        listener,
    )
    .await;
    let via_cache = client(&url).await;
    assert!(via_cache.has_feature(scion_proto::features::REDACTION));
    let id = ObjectId::of_blob(&secret);
    assert_eq!(via_cache.get_object(&id).await.expect("cached").to_vec(), secret);
    assert!(node.store().get(&id).await.is_some());

    let red = scion_api::redaction::Redactor::new(s.clone(), s.clone());
    let (rid, _) = red
        .redact(REPO, &[id], &[], "leak", "root", false, 1)
        .await
        .expect("redact");
    let err = via_cache.get_object(&id).await.expect_err("redacted");
    assert!(err.is_redacted(), "{err:?}");
    assert!(node.store().get(&id).await.is_none(), "evicted from the cache");
    assert_eq!(
        via_cache
            .get_object(&ObjectId::of_blob(b"a"))
            .await
            .expect("other")
            .as_ref(),
        b"a"
    );
    // 经由缓存转给 L3
    let page = via_cache.list_redacted_objects(0).await.expect("feed");
    assert_eq!(page.objects.len(), 1);

    red.unredact(&rid).await.expect("unredact");
    assert_eq!(via_cache.get_object(&id).await.expect("restored").to_vec(), secret);
    assert_eq!(scion_cache::REDACTION_TTL, Duration::from_secs(10));
}
