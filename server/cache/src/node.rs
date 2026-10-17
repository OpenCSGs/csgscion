//! 缓存节点的读路径：本地命中 → 合并并发未命中（single-flight）→ 按 rendezvous 分片交给负责的
//! 节点，或自己回源。
//!
//! 一个 id 只由负责它的节点回源，整个区域对同一个对象只回源一次。负责的节点不可达时自己回源
//! （区域内绕路，但绝不让客户端绕过 L2 直连源站）。转发来的请求（[`CACHE_FORWARDED_HEADER`]）不再
//! 转发，避免环路。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::future::join_all;
use scion_flight::{Claim, Group};
use scion_object::ObjectId;
use tonic::Status;

use crate::ring::Ring;
use crate::store::ObjectCache;
use crate::upstream::{RequestCtx, Upstream, UpstreamPool, is_upstream_failure};

#[cfg(doc)]
use crate::upstream::CACHE_FORWARDED_HEADER;

/// 命中率等计数。
#[derive(Debug, Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// 交给负责节点的对象数。
    pub peer_objects: AtomicU64,
    /// 自己回源的对象数。
    pub upstream_objects: AtomicU64,
    /// 回源请求数（一次请求可含多个对象）。
    pub upstream_requests: AtomicU64,
}

/// [`Stats`] 的一个快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub peer_objects: u64,
    pub upstream_objects: u64,
    pub upstream_requests: u64,
}

impl StatsSnapshot {
    /// 命中率（0..=1）；没有请求时为 1。
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            1.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            peer_objects: self.peer_objects.load(Ordering::Relaxed),
            upstream_objects: self.upstream_objects.load(Ordering::Relaxed),
            upstream_requests: self.upstream_requests.load(Ordering::Relaxed),
        }
    }
}

type Outcome = Result<Bytes, Status>;

pub struct CacheNode {
    store: ObjectCache,
    flight: Group<ObjectId, Outcome>,
    ring: Ring,
    /// 本节点在 ring 中的地址；不在 ring 中（单节点）时为 `None`。
    me: Option<String>,
    /// 其它缓存节点（请求带转发标记）。
    peers: HashMap<String, Arc<dyn Upstream>>,
    upstream: UpstreamPool,
    stats: Stats,
}

/// 一组要一起取的对象：交给某个节点，或回源。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Route {
    Peer(String),
    Upstream,
}

impl CacheNode {
    pub fn new(
        store: ObjectCache,
        ring: Ring,
        me: Option<String>,
        peers: HashMap<String, Arc<dyn Upstream>>,
        upstream: UpstreamPool,
    ) -> Self {
        Self {
            store,
            flight: Group::new(),
            ring,
            me,
            peers,
            upstream,
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn store(&self) -> &ObjectCache {
        &self.store
    }

    fn route(&self, id: &ObjectId, forwarded: bool) -> Route {
        if forwarded {
            return Route::Upstream;
        }
        match self.ring.owner(id) {
            Some(owner) if Some(owner) != self.me.as_deref() && self.peers.contains_key(owner) => {
                Route::Peer(owner.to_owned())
            }
            _ => Route::Upstream,
        }
    }

    /// 取一批对象（顺序与 `ids` 相同）。任何一个对象取不到时整个请求失败（与 L3 的语义一致）。
    pub async fn get_many(
        &self,
        repo: &str,
        ids: &[ObjectId],
        ctx: &RequestCtx,
        forwarded: bool,
    ) -> Result<Vec<Bytes>, Status> {
        let mut unique: Vec<ObjectId> = ids.to_vec();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        let found = self.resolve(repo, &unique, ctx, forwarded).await;
        ids.iter()
            .map(|id| match found.get(id) {
                Some(Ok(b)) => Ok(b.clone()),
                Some(Err(e)) => Err(e.clone()),
                None => Err(Status::internal(format!("object {id} was not resolved"))),
            })
            .collect()
    }

    /// 解析一组不重复的 id。
    async fn resolve(
        &self,
        repo: &str,
        ids: &[ObjectId],
        ctx: &RequestCtx,
        forwarded: bool,
    ) -> HashMap<ObjectId, Outcome> {
        let mut out = HashMap::with_capacity(ids.len());
        let cached = join_all(ids.iter().map(|id| self.store.get(id))).await;
        let mut leaders = Vec::new();
        let mut followers = Vec::new();
        for (id, hit) in ids.iter().zip(cached) {
            match hit {
                Some(b) => {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    out.insert(*id, Ok(b));
                }
                None => {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    match self.flight.claim(*id) {
                        Claim::Leader(l) => leaders.push(l),
                        Claim::Follower(f) => followers.push((*id, f)),
                    }
                }
            }
        }

        // 自己负责回源的 id，按去向分组，各组并发
        let mut groups: HashMap<Route, Vec<scion_flight::Leader<ObjectId, Outcome>>> = HashMap::new();
        for l in leaders {
            groups.entry(self.route(l.key(), forwarded)).or_default().push(l);
        }
        let fetched = join_all(groups.into_iter().map(|(route, group)| async move {
            let ids: Vec<ObjectId> = group.iter().map(|l| *l.key()).collect();
            let results = self.fetch_group(&route, repo, &ids, ctx).await;
            let mut done = Vec::with_capacity(group.len());
            for (l, r) in group.into_iter().zip(results) {
                if let Ok(b) = &r {
                    self.store.insert(*l.key(), b.clone());
                }
                let id = *l.key();
                l.complete(r.clone());
                done.push((id, r));
            }
            done
        }))
        .await;
        out.extend(fetched.into_iter().flatten());

        // 别人正在取的 id：等结果；对方被取消时重新解析
        let mut retry = Vec::new();
        for (id, f) in followers {
            match f.wait().await {
                Some(r) => {
                    out.insert(id, r);
                }
                None => retry.push(id),
            }
        }
        if !retry.is_empty() {
            out.extend(Box::pin(self.resolve(repo, &retry, ctx, forwarded)).await);
        }
        out
    }

    /// 取一组对象，返回每个 id 的结果（顺序与 `ids` 相同）。
    async fn fetch_group(&self, route: &Route, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Vec<Outcome> {
        let batch = match route {
            Route::Peer(p) => {
                self.stats.peer_objects.fetch_add(ids.len() as u64, Ordering::Relaxed);
                let peer = self.peers.get(p).cloned();
                match peer {
                    Some(peer) => match peer.fetch(repo, ids, ctx).await {
                        // 负责的节点不可达：自己回源
                        Err(e) if is_upstream_failure(&e) => {
                            tracing::warn!(peer = %p, error = %e, "cache peer unavailable; fetching from upstream");
                            self.fetch_upstream(repo, ids, ctx).await
                        }
                        r => r,
                    },
                    None => self.fetch_upstream(repo, ids, ctx).await,
                }
            }
            Route::Upstream => self.fetch_upstream(repo, ids, ctx).await,
        };
        match batch {
            Ok(objs) => objs.into_iter().map(Ok).collect(),
            Err(e) if ids.len() == 1 => vec![Err(e)],
            // 整批失败：可能只是其中某个对象的问题（例如不存在），逐个重试，其它对象的等待者不受连累
            Err(e) if !is_upstream_failure(&e) => {
                join_all(ids.iter().map(|id| async move {
                    self.fetch_upstream(repo, std::slice::from_ref(id), ctx)
                        .await
                        .and_then(|mut v| v.pop().ok_or_else(|| Status::internal("empty upstream response")))
                }))
                .await
            }
            Err(e) => vec![Err(e); ids.len()],
        }
    }

    async fn fetch_upstream(&self, repo: &str, ids: &[ObjectId], ctx: &RequestCtx) -> Result<Vec<Bytes>, Status> {
        self.stats.upstream_requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .upstream_objects
            .fetch_add(ids.len() as u64, Ordering::Relaxed);
        self.upstream.fetch(repo, ids, ctx).await
    }

    /// 关闭本地存储（刷盘）。
    pub async fn close(&self) {
        self.store.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StorageConfig;
    use crate::upstream::PoolConfig;
    use crate::upstream::tests::Fake;
    use std::time::Duration;
    use tonic::Code;

    const A: &[u8] = b"alpha";
    const B: &[u8] = b"beta";
    const C: &[u8] = b"gamma";

    async fn store() -> ObjectCache {
        ObjectCache::open(&StorageConfig {
            memory_bytes: 1 << 20,
            ..StorageConfig::default()
        })
        .await
        .expect("cache")
    }

    fn pool(f: &Arc<Fake>) -> UpstreamPool {
        UpstreamPool::new(
            vec![f.clone() as Arc<dyn Upstream>],
            PoolConfig {
                hedge: false,
                ..PoolConfig::default()
            },
        )
    }

    fn id(b: &[u8]) -> ObjectId {
        ObjectId::of_blob(b)
    }

    async fn single(f: &Arc<Fake>) -> CacheNode {
        CacheNode::new(store().await, Ring::new(Vec::new()), None, HashMap::new(), pool(f))
    }

    #[tokio::test]
    async fn misses_are_fetched_once_and_then_hit() {
        let up = Arc::new(Fake::new("l3", &[A, B]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        let got = n.get_many("r", &[id(A), id(B), id(A)], &ctx, false).await.expect("ok");
        assert_eq!(
            got,
            [Bytes::from_static(A), Bytes::from_static(B), Bytes::from_static(A)]
        );
        assert_eq!(up.calls(), 1, "one batched upstream request");
        n.get_many("r", &[id(B), id(A)], &ctx, false).await.expect("hit");
        assert_eq!(up.calls(), 1);
        let s = n.stats();
        assert_eq!(
            (s.hits, s.misses, s.upstream_objects, s.upstream_requests),
            (2, 2, 2, 1)
        );
        assert!((s.hit_ratio() - 0.5).abs() < f64::EPSILON);
        assert!((StatsSnapshot::default().hit_ratio() - 1.0).abs() < f64::EPSILON);
        assert_eq!(n.store().get(&id(A)).await, Some(Bytes::from_static(A)));
        n.close().await;
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce() {
        let up = Arc::new(Fake::new("l3", &[A]).delay(Duration::from_millis(30)));
        let n = Arc::new(single(&up).await);
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let n = n.clone();
                tokio::spawn(async move { n.get_many("r", &[id(A)], &RequestCtx::default(), false).await })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.expect("join").expect("ok"), [Bytes::from_static(A)]);
        }
        assert_eq!(up.calls(), 1);
    }

    #[tokio::test]
    async fn missing_object_fails_only_its_own_requests() {
        let up = Arc::new(Fake::new("l3", &[A]));
        let n = single(&up).await;
        let ctx = RequestCtx::default();
        let err = n
            .get_many("r", &[id(A), id(C)], &ctx, false)
            .await
            .expect_err("C missing");
        assert_eq!(err.code(), Code::FailedPrecondition);
        // A 在逐个重试时取到并缓存了
        assert_eq!(n.store().get(&id(A)).await, Some(Bytes::from_static(A)));
        // 上游故障：整组同样的错误
        let down = Arc::new(Fake::new("down", &[A]).script(vec![Err(Status::unavailable("down"))]));
        let n = single(&down).await;
        let err = n.get_many("r", &[id(A), id(B)], &ctx, false).await.expect_err("down");
        assert_eq!(err.code(), Code::Unavailable);
    }

    #[tokio::test]
    async fn abandoned_fetch_is_retried_by_waiters() {
        let up = Arc::new(Fake::new("l3", &[A]));
        let n = single(&up).await;
        // 模拟另一个请求领取后被取消
        let Claim::Leader(l) = n.flight.claim(id(A)) else {
            panic!("leader");
        };
        let ctx = RequestCtx::default();
        let want = [id(A)];
        let (got, ()) = tokio::join!(n.get_many("r", &want, &ctx, false), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(l);
        });
        assert_eq!(got.expect("retried"), [Bytes::from_static(A)]);
    }

    #[tokio::test]
    async fn objects_owned_by_peers_are_fetched_from_them() {
        let up = Arc::new(Fake::new("l3", &[A, B, C]));
        let peer = Arc::new(Fake::new("peer", &[A, B, C]));
        let ring = Ring::new(["me".to_string(), "peer".into()]);
        let ids: Vec<ObjectId> = [A, B, C].iter().map(|b| id(b)).collect();
        let (mine, theirs): (Vec<&ObjectId>, Vec<&ObjectId>) = ids.iter().partition(|i| ring.owner(i) == Some("me"));
        let mut peers: HashMap<String, Arc<dyn Upstream>> = HashMap::new();
        peers.insert("peer".into(), peer.clone());
        let n = CacheNode::new(store().await, ring.clone(), Some("me".into()), peers, pool(&up));
        n.get_many("r", &ids, &RequestCtx::default(), false).await.expect("ok");
        let s = n.stats();
        assert_eq!(s.peer_objects as usize, theirs.len());
        assert_eq!(s.upstream_objects as usize, mine.len());
        assert_eq!(peer.calls(), u32::from(!theirs.is_empty()));

        // 转发来的请求不再转发
        let n2 = CacheNode::new(
            store().await,
            ring.clone(),
            Some("me".into()),
            HashMap::from([("peer".to_string(), peer.clone() as Arc<dyn Upstream>)]),
            pool(&up),
        );
        n2.get_many("r", &ids, &RequestCtx::default(), true).await.expect("ok");
        assert_eq!(n2.stats().peer_objects, 0);

        // 负责的节点不可达：自己回源
        let dead = Arc::new(Fake::new("peer", &[]).script(vec![Err(Status::unavailable("gone"))]));
        let n3 = CacheNode::new(
            store().await,
            ring.clone(),
            Some("me".into()),
            HashMap::from([("peer".to_string(), dead as Arc<dyn Upstream>)]),
            pool(&up),
        );
        n3.get_many("r", &ids, &RequestCtx::default(), false)
            .await
            .expect("fallback");
        assert_eq!(n3.stats().upstream_objects, 3);

        // ring 中有、但没有连接的节点：回源
        let n4 = CacheNode::new(store().await, ring, Some("me".into()), HashMap::new(), pool(&up));
        n4.get_many("r", &ids, &RequestCtx::default(), false).await.expect("ok");
        assert_eq!(n4.stats().peer_objects, 0);
    }
}
