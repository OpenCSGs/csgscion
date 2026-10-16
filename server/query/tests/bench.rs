//! 历史查询基准（M2 验收：10 万提交，查询 p99 < 100 ms）。
//!
//! 平时不运行；手动执行：
//!   SCION_BENCH_COMMITS=100000 cargo test --release -p scion-query --test bench -- --ignored --nocapture

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Signature, Tree};
use scion_query::{blame, is_ancestor, log, merge_bases};
use scion_store::{BlobStore, ChangesetRecord, MemStore, MetaStore};
use scion_tree::{TreeError, TreeSource, apply_delta, diff};

const REPO: &str = "bench";

struct Src<'a>(&'a MemStore);

#[async_trait]
impl TreeSource for Src<'_> {
    async fn load_tree(&self, id: &ObjectId) -> scion_tree::Result<Tree> {
        let b = self.0.get(id).await.map_err(|e| TreeError::Storage(e.to_string()))?;
        let b = b.ok_or_else(|| TreeError::BadTree(*id, "missing".into()))?;
        Tree::decode(&b).map_err(|e| TreeError::BadTree(*id, e.to_string()))
    }
}

/// 确定性的伪随机数（xorshift），保证每次生成的历史相同。
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct History {
    /// 对象字节（内存）。
    store: MemStore,
    /// 提交记录：内存或 SQLite（SCION_BENCH_META=sqlite）。
    meta: Box<dyn MetaStore>,
    /// 主线提交，按时间顺序。
    main: Vec<ObjectId>,
    /// 旁支提交（每段旁支的末端，用于 merge-base）。
    side_tips: Vec<ObjectId>,
    files: Vec<String>,
}

async fn commit(
    store: &MemStore,
    meta: &dyn MetaStore,
    parents: &[ObjectId],
    delta: Vec<FileChange>,
    n: u64,
) -> ObjectId {
    let sig = Signature {
        name: format!("dev{}", n % 50),
        email: "dev@example.com".into(),
        time_ms: n as i64 * 60_000,
        tz_offset_minutes: 480,
    };
    let mut change_id = [0u8; 16];
    change_id[..8].copy_from_slice(&n.to_le_bytes());
    let cs = Changeset::new(
        parents.to_vec(),
        sig.clone(),
        sig,
        format!("change {n}"),
        change_id,
        delta,
        BTreeMap::new(),
    )
    .expect("valid");
    let base = match parents.first() {
        Some(p) => Some(
            meta.get_changeset(REPO, p)
                .await
                .expect("ok")
                .expect("parent")
                .root_tree,
        ),
        None => None,
    };
    let built = apply_delta(&Src(store), base, &cs.delta).await.expect("tree");
    for (id, b) in built.new_trees {
        store.put(id, b).await.expect("tree");
    }
    let id = cs.id();
    store.put(id, Bytes::from(cs.encode())).await.expect("cs");
    let mut generation = 0;
    for p in parents {
        generation = generation.max(meta.get_changeset(REPO, p).await.expect("ok").expect("p").generation);
    }
    let changes: Vec<_> = cs
        .delta
        .iter()
        .map(|c| scion_store::PathChange {
            path: c.path.clone(),
            deleted: matches!(c.op, scion_object::FileOp::Delete),
        })
        .collect();
    meta.index_paths(REPO, id, generation + 1, &changes)
        .await
        .expect("index");
    let (fp_depth, fp_jump) = scion_store::fp_skip(meta, REPO, parents.first().copied())
        .await
        .expect("skip");
    meta.put_changeset(
        REPO,
        &ChangesetRecord {
            id,
            root_tree: built.root,
            parents: parents.to_vec(),
            generation: generation + 1,
            fp_depth,
            fp_jump,
        },
    )
    .await
    .expect("record");
    id
}

async fn put_file(store: &MemStore, path: &str, n: u64) -> FileChange {
    // 每个文件 20 行，修改其中一行，让 blame 有意义
    let mut lines: Vec<String> = (0..20).map(|i| format!("{path} line {i}\n")).collect();
    lines[(n % 20) as usize] = format!("{path} changed in {n}\n");
    let content = lines.concat();
    let id = ObjectId::of_blob(content.as_bytes());
    store.put(id, Bytes::from(content.clone())).await.expect("blob");
    FileChange::put(path, EntryMode::File, id, content.len() as u64)
}

async fn build(commits: u64, meta: Box<dyn MetaStore>) -> History {
    let store = MemStore::new();
    store.create_repo(REPO).await.expect("repo");
    meta.create_repo(REPO).await.expect("repo");
    let files: Vec<String> = (0..2000)
        .map(|i| format!("dir{:03}/file{:04}.rs", i % 100, i))
        .collect();
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut main = Vec::with_capacity(commits as usize);
    let mut side_tips = Vec::new();
    // 根提交包含全部文件
    let mut delta = Vec::new();
    for f in &files {
        delta.push(put_file(&store, f, 0).await);
    }
    main.push(commit(&store, meta.as_ref(), &[], delta, 0).await);
    let started = Instant::now();
    let mut n = 1;
    while n < commits {
        if n % 1000 == 0 && n + 3 < commits {
            // 旁支：从 10 个提交之前分出 2 个提交，再合并回来
            let fork = main[main.len() - 10];
            let mut side = fork;
            for _ in 0..2 {
                let f = &files[rng.below(files.len() as u64) as usize];
                side = commit(&store, meta.as_ref(), &[side], vec![put_file(&store, f, n).await], n).await;
                n += 1;
            }
            side_tips.push(side);
            let tip = *main.last().expect("tip");
            main.push(commit(&store, meta.as_ref(), &[tip, side], vec![], n).await);
            n += 1;
            continue;
        }
        let k = 1 + rng.below(3);
        let mut delta = Vec::new();
        let mut used = std::collections::BTreeSet::new();
        for _ in 0..k {
            let f = &files[rng.below(files.len() as u64) as usize];
            if used.insert(f.clone()) {
                delta.push(put_file(&store, f, n).await);
            }
        }
        let tip = *main.last().expect("tip");
        main.push(commit(&store, meta.as_ref(), &[tip], delta, n).await);
        n += 1;
        if n % 20_000 == 0 {
            eprintln!("  built {n} commits ({:.1}s)", started.elapsed().as_secs_f64());
        }
    }
    History {
        store,
        meta,
        main,
        side_tips,
        files,
    }
}

fn report(name: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let pct = |p: f64| samples[((samples.len() as f64 * p) as usize).min(samples.len() - 1)];
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    println!(
        "{name:<44} n={:<4} p50={:>8.2}ms p99={:>8.2}ms max={:>8.2}ms",
        samples.len(),
        ms(pct(0.50)),
        ms(pct(0.99)),
        ms(*samples.last().expect("samples"))
    );
}

async fn time<F: std::future::Future<Output = ()>>(f: F) -> Duration {
    let t = Instant::now();
    f.await;
    t.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run manually with --release"]
async fn history_queries_on_large_repo() {
    let commits: u64 = std::env::var("SCION_BENCH_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let backend = std::env::var("SCION_BENCH_META").unwrap_or_else(|_| "mem".into());
    let _dir = tempfile::tempdir().expect("tempdir");
    let meta: Box<dyn MetaStore> = match backend.as_str() {
        "sqlite" => {
            let url = format!("sqlite://{}", _dir.path().join("meta.db").display());
            Box::new(scion_store_sql::SqliteMetaStore::connect(&url).await.expect("sqlite"))
        }
        _ => Box::new(MemStore::new()),
    };
    eprintln!("building {commits} commits (meta: {backend}) ...");
    let t = Instant::now();
    let h = build(commits, meta).await;
    eprintln!("built in {:.1}s", t.elapsed().as_secs_f64());
    // 与服务端一致：查询经过进程级的提交记录缓存（所有采样共享，模拟运行中的服务端）
    let cached = scion_store::CachedMeta::new(std::sync::Arc::from(h.meta), 1_000_000);
    let h = History {
        meta: Box::new(scion_store::MemStore::new()),
        ..h
    };
    let warm = Instant::now();
    let loaded = cached.warm(REPO, 4096).await.expect("warm");
    println!(
        "{:<44} {loaded} records in {:.0}ms",
        "record cache warm-up",
        warm.elapsed().as_secs_f64() * 1000.0
    );
    let s: &dyn MetaStore = &cached;

    let blobs = &h.store;
    let tip = *h.main.last().expect("tip");
    let mut rng = Rng(42);
    let samples = 100;

    let mut v = Vec::new();
    for _ in 0..samples {
        v.push(
            time(async {
                log(s, blobs, REPO, &[tip], None, 50, 10_000).await.expect("log");
            })
            .await,
        );
    }
    report("log first page (50)", v);

    let mut v = Vec::new();
    for _ in 0..samples {
        let dir = format!("dir{:03}", rng.below(100));
        v.push(
            time(async {
                log(s, blobs, REPO, &[tip], Some(&dir), 20, 10_000).await.expect("log");
            })
            .await,
        );
    }
    report("log --path dir (20, scan budget 10k)", v);

    let mut v = Vec::new();
    for _ in 0..samples {
        let i = h.main.len() - 1 - rng.below(h.main.len() as u64 - 2) as usize;
        let (a, b) = (h.main[i - 1], h.main[i]);
        let (ra, rb) = (
            s.get_changeset(REPO, &a).await.expect("ok").expect("a").root_tree,
            s.get_changeset(REPO, &b).await.expect("ok").expect("b").root_tree,
        );
        v.push(
            time(async {
                diff(&Src(blobs), Some(ra), Some(rb), "").await.expect("diff");
            })
            .await,
        );
    }
    report("diff adjacent commits", v);

    let mut v = Vec::new();
    for _ in 0..samples {
        let i = h.main.len() - 1 - rng.below(h.main.len() as u64 - 101) as usize;
        let (a, b) = (h.main[i - 100], h.main[i]);
        let (ra, rb) = (
            s.get_changeset(REPO, &a).await.expect("ok").expect("a").root_tree,
            s.get_changeset(REPO, &b).await.expect("ok").expect("b").root_tree,
        );
        v.push(
            time(async {
                diff(&Src(blobs), Some(ra), Some(rb), "").await.expect("diff");
            })
            .await,
        );
    }
    report("diff 100 commits apart", v);

    let mut v = Vec::new();
    for _ in 0..samples {
        let k = 1 + rng.below(1000) as usize;
        let a = h.main[h.main.len() - 1 - k];
        v.push(
            time(async {
                assert!(is_ancestor(s, REPO, a, tip).await.expect("ok"));
            })
            .await,
        );
    }
    report("is_ancestor(tip~k, tip), k<=1000", v);

    let mut v = Vec::new();
    for _ in 0..10 {
        v.push(
            time(async {
                assert!(is_ancestor(s, REPO, h.main[0], tip).await.expect("ok"));
            })
            .await,
        );
    }
    report("is_ancestor(root, tip)", v);

    let mut v = Vec::new();
    for _ in 0..samples.min(h.side_tips.len()) {
        let side = h.side_tips[h.side_tips.len() - 1 - rng.below(h.side_tips.len().min(20) as u64) as usize];
        v.push(
            time(async {
                merge_bases(s, REPO, side, tip).await.expect("mb");
            })
            .await,
        );
    }
    report("merge_bases(recent side branch, tip)", v);

    // 每个文件第一次 blame：派生数据缓存为空。第一次采样时记录缓存也基本为空（进程刚启动）
    let mut cold = Vec::new();
    let mut warm = Vec::new();
    let mut files: Vec<String> = h.files.clone();
    for i in (1..files.len()).rev() {
        files.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for f in files.iter().take(100) {
        cold.push(
            time(async {
                blame(s, blobs, REPO, tip, f).await.expect("blame");
            })
            .await,
        );
        warm.push(
            time(async {
                blame(s, blobs, REPO, tip, f).await.expect("blame");
            })
            .await,
        );
    }
    println!(
        "{:<44} {:.2}ms",
        "blame first query after start",
        cold[0].as_secs_f64() * 1000.0
    );
    report("blame new file (derived cache empty)", cold[1..].to_vec());
    report("blame same file again (derived cached)", warm);
}
