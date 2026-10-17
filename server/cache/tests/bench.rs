//! 缓存层基准（M4 验收：L2 命中率不低于 99%，单节点每秒 20 万次以上对象读）。手动运行：
//!
//! ```sh
//! cargo test --release -p scion-cache --test bench -- --ignored --nocapture
//! ```
//!
//! 进程内拉起一个 L3（对象在内存）和一个缓存节点，客户端全部经 gRPC 访问缓存节点：
//! - **命中率**：`SCION_BENCH_CHECKOUTS`（默认 200）个客户端并发「检出」同一组
//!   `SCION_BENCH_OBJECTS`（默认 20000）个对象（每个 1–4 KiB），各自按随机顺序、每批 64 个读取。
//!   命中率按「没有回源的对象读」计算；
//! - **吞吐**：缓存已热，`SCION_BENCH_CLIENTS`（默认 64）个并发客户端持续 `SCION_BENCH_SECS`（默认 5）
//!   秒读取随机对象，分别测每批 64 个与每次 1 个。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use scion_api::{App, ServerConfig};
use scion_cache::{CacheConfig, StorageConfig};
use scion_object::ObjectId;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::{GetObjectsRequest, ObjectWant};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint};

const REPO: &str = "bench";

fn env_or<T: std::str::FromStr>(k: &str, default: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// 简单的 xorshift，避免引入随机数依赖。
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

async fn seed(n: usize) -> (Arc<MemStore>, Vec<ObjectId>) {
    let store = Arc::new(MemStore::new());
    store.create_repo(REPO).await.expect("repo");
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut ids = Vec::with_capacity(n);
    let mut index = Vec::with_capacity(n);
    for i in 0..n {
        let len = 1024 + rng.below(3072);
        let mut data = vec![0u8; len];
        data[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let id = ObjectId::of_blob(&data);
        store.put(id, Bytes::from(data)).await.expect("put");
        index.push(IndexedObject { id, size: len as u64 });
        ids.push(id);
    }
    for part in index.chunks(1000) {
        store.index_objects(REPO, part).await.expect("index");
    }
    (store, ids)
}

async fn start_l3(store: Arc<MemStore>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            warm_record_cache: false,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(scion_api::serve(listener, app, std::future::pending()));
    url
}

fn connect(url: &str) -> Channel {
    Endpoint::from_shared(url.to_owned())
        .expect("url")
        .tcp_nodelay(true)
        .connect_lazy()
}

async fn read(client: &mut ObjectServiceClient<Channel>, ids: &[ObjectId]) -> usize {
    let req = GetObjectsRequest {
        repo: REPO.into(),
        wants: ids
            .iter()
            .map(|id| ObjectWant {
                id: Bytes::copy_from_slice(&id.to_bytes()),
                offset: 0,
            })
            .collect(),
        accept_encodings: vec![],
    };
    let mut stream = client.get_objects(req).await.expect("get").into_inner();
    let mut n = 0;
    while let Some(part) = stream.message().await.expect("chunk") {
        if part.last {
            n += 1;
        }
    }
    assert_eq!(n, ids.len());
    n
}

/// `clients` 个并发客户端在 `secs` 秒内每次读 `batch` 个随机对象，返回每秒对象数。
async fn throughput(channels: &[Channel], ids: &Arc<Vec<ObjectId>>, clients: usize, batch: usize, secs: u64) -> f64 {
    let total = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let mut client = ObjectServiceClient::new(channels[c % channels.len()].clone());
        let (ids, total) = (ids.clone(), total.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(c as u64 + 1);
            let mut want = Vec::with_capacity(batch);
            while Instant::now() < deadline {
                want.clear();
                for _ in 0..batch {
                    want.push(ids[rng.below(ids.len())]);
                }
                let n = read(&mut client, &want).await;
                total.fetch_add(n as u64, Ordering::Relaxed);
            }
        }));
    }
    for t in tasks {
        t.await.expect("join");
    }
    total.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run manually with --release"]
async fn cache_hit_rate_and_throughput() {
    let objects = env_or("SCION_BENCH_OBJECTS", 20_000usize);
    let checkouts = env_or("SCION_BENCH_CHECKOUTS", 200usize);
    let clients = env_or("SCION_BENCH_CLIENTS", 64usize);
    let secs = env_or("SCION_BENCH_SECS", 5u64);
    let (store, ids) = seed(objects).await;
    let l3 = start_l3(store).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let cache_url = format!("http://{}", listener.local_addr().expect("addr"));
    let config = CacheConfig {
        upstreams: vec![l3],
        storage: StorageConfig {
            memory_bytes: 1 << 30,
            ..StorageConfig::default()
        },
        stats_interval: None,
        ..CacheConfig::default()
    };
    let (node, info, upstream_channels) = scion_cache::build(&config).await.expect("build");
    let n = node.clone();
    tokio::spawn(async move {
        scion_cache::serve_node(listener, n, info, upstream_channels, &config, std::future::pending()).await
    });
    // 多个 HTTP/2 连接，避免单连接成为瓶颈
    let channels: Vec<Channel> = (0..16).map(|_| connect(&cache_url)).collect();
    let ids = Arc::new(ids);

    // 1. 命中率：冷启动，checkouts 个客户端并发读同一组对象
    let started = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..checkouts {
        let mut client = ObjectServiceClient::new(channels[c % channels.len()].clone());
        let ids = ids.clone();
        tasks.push(tokio::spawn(async move {
            let mut order: Vec<ObjectId> = ids.to_vec();
            let mut rng = Rng(c as u64 * 7919 + 13);
            for i in (1..order.len()).rev() {
                order.swap(i, rng.below(i + 1));
            }
            let mut n = 0;
            for part in order.chunks(64) {
                n += read(&mut client, part).await;
            }
            n
        }));
    }
    let mut reads = 0usize;
    for t in tasks {
        reads += t.await.expect("join");
    }
    let elapsed = started.elapsed();
    let s = node.stats();
    let offload = 1.0 - s.upstream_objects as f64 / reads as f64;
    println!(
        "checkouts: {checkouts} x {objects} objects = {reads} reads in {:.2}s ({:.0} objects/s)",
        elapsed.as_secs_f64(),
        reads as f64 / elapsed.as_secs_f64()
    );
    println!(
        "hit rate (reads not sent to L3): {:.4}  upstream objects: {}  upstream requests: {}  local hit ratio: {:.4}",
        offload,
        s.upstream_objects,
        s.upstream_requests,
        s.hit_ratio()
    );
    assert_eq!(s.upstream_objects as usize, objects, "each object fetched from L3 once");
    assert!(offload >= 0.99, "hit rate {offload}");

    // 2. 吞吐：缓存已热
    let batched = throughput(&channels, &ids, clients, 64, secs).await;
    println!("hot reads, batch 64, {clients} clients: {batched:.0} objects/s");
    let single = throughput(&channels, &ids, clients, 1, secs).await;
    println!("hot reads, batch 1, {clients} clients: {single:.0} objects/s");
    assert!(batched >= 200_000.0, "{batched:.0} objects/s");
}
