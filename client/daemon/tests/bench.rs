//! 检出基准（客户端性能目标：命中 daemon 时 10 万文件的检出小于 5 秒）。手动运行：
//!
//! ```sh
//! cargo test --release -p scion-daemon --test bench -- --ignored --nocapture
//! ```
//!
//! 进程内拉起 L3（对象在内存）与 daemon，仓库有 `SCION_BENCH_FILES`（默认 100000）个文件，
//! 分布在 1000 个目录里。分别计时：直接创建这些文件（基线，检出的下限）、直连检出、经 daemon 冷检出、
//! 经 daemon 热检出（另一个目录再检出一次）。目标是 5 秒；在创建文件本身就更慢的文件系统上，
//! 要求热检出不超过基线的 1.25 倍加 1 秒。
#![cfg(unix)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_api::{App, Limits, ServerConfig};
use scion_client::Client;
use scion_daemon::{DaemonConfig, PrefetchOptions};
use scion_store::{MemStore, MetaStore};
use scion_wc::WorkingCopy;
use tokio::net::TcpListener;

fn env_or<T: std::str::FromStr>(k: &str, default: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn clone(c: &Client, endpoint: &str, dir: &Path) -> Duration {
    let started = Instant::now();
    WorkingCopy::clone(c, endpoint, "mono", "main", dir)
        .await
        .expect("clone");
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run manually with --release"]
async fn clone_through_daemon() {
    let files = env_or("SCION_BENCH_FILES", 100_000usize);
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            limits: Limits {
                max_changes_per_changeset: files.max(1) as u32,
                ..Limits::default()
            },
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(scion_api::serve(listener, app, std::future::pending()));

    let direct = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let seeded = Instant::now();
    let mut b = direct.changeset(None).message("seed");
    for i in 0..files {
        b = b.put(format!("d{:03}/f{i:06}.txt", i % 1000), format!("file {i}\n"));
    }
    let cs = b.create().await.expect("create");
    direct.land("main", &cs.id).await.expect("land");
    println!("seeded {files} files in {:.1}s", seeded.elapsed().as_secs_f64());

    let dir = tempfile::tempdir().expect("tempdir");
    // 基线：直接创建同样的 10 万个文件（不涉及网络），检出的下限
    let baseline = {
        let root = dir.path().join("baseline");
        let t = Instant::now();
        for d in 0..1000 {
            std::fs::create_dir_all(root.join(format!("d{d:03}"))).expect("dir");
        }
        for i in 0..files {
            std::fs::write(
                root.join(format!("d{:03}/f{i:06}.txt", i % 1000)),
                format!("file {i}\n"),
            )
            .expect("write");
        }
        t.elapsed()
    };
    println!("baseline (create files): {:.2}s", baseline.as_secs_f64());
    let t_direct = clone(&direct, &server, &dir.path().join("direct")).await;
    println!("clone direct:            {:.2}s", t_direct.as_secs_f64());

    let cfg = DaemonConfig {
        socket: dir.path().join("d.sock"),
        cache_dir: dir.path().join("cache"),
        disk_bytes: 4 << 30,
        memory_bytes: 512 << 20,
        idle_timeout: None,
        prefetch: PrefetchOptions::default(),
        redaction_ttl: scion_daemon::REDACTION_TTL,
    };
    let socket = cfg.socket.clone();
    tokio::spawn(scion_daemon::run(cfg, std::future::pending()));
    for _ in 0..200 {
        if tokio::net::UnixStream::connect(&socket).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let via = Client::builder()
        .endpoint(&server)
        .repo("mono")
        .daemon_socket(&socket)
        .build()
        .await
        .expect("daemon");
    let t_cold = clone(&via, &server, &dir.path().join("cold")).await;
    println!("clone via daemon (cold): {:.2}s", t_cold.as_secs_f64());
    let t_hot = clone(&via, &server, &dir.path().join("hot")).await;
    println!("clone via daemon (hot):  {:.2}s", t_hot.as_secs_f64());
    // 本机文件系统创建文件的速度决定了检出的下限；经 daemon 的额外开销应当很小
    println!(
        "hot clone overhead over creating the files: {:.2}s",
        (t_hot.as_secs_f64() - baseline.as_secs_f64()).max(0.0)
    );
    assert!(
        t_hot < Duration::from_secs(5) || t_hot < baseline + baseline / 4 + Duration::from_secs(1),
        "hot clone took {t_hot:?} (baseline {baseline:?})"
    );
}
