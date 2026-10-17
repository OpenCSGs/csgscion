//! 预热：订阅上游的 WatchRefs，ref 每次前进时把新提交中变化了的目录树（可选连同不超过一定大小的
//! 文件）取进缓存。只沿变化的路径下钻：子树 id 没变的目录整个跳过，代价与改动大小成正比，与仓库
//! 大小无关。第一次看到某个 ref 时（没有旧的根目录树）预热整棵树，受 `max_objects` 限制。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use scion_object::{EntryMode, ObjectId, Tree};
use scion_proto::v1::WatchRefsRequest;
use scion_proto::v1::ref_service_client::RefServiceClient;
use tonic::transport::Channel;

use crate::node::CacheNode;
use crate::upstream::RequestCtx;

/// 要预热的仓库与 ref。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmTarget {
    pub repo: String,
    /// 为空表示仓库中的全部 ref。
    pub refs: Vec<String>,
}

impl std::str::FromStr for WarmTarget {
    type Err = String;

    /// `repo` 或 `repo:ref1,ref2`。
    fn from_str(s: &str) -> Result<Self, String> {
        let (repo, refs) = match s.split_once(':') {
            Some((r, refs)) => (r, refs.split(',').map(str::trim).filter(|r| !r.is_empty()).collect()),
            None => (s, Vec::new()),
        };
        let repo = repo.trim();
        if repo.is_empty() {
            return Err(format!("invalid warm target {s:?}; expected REPO or REPO:REF[,REF...]"));
        }
        Ok(Self {
            repo: repo.to_owned(),
            refs: refs.into_iter().map(str::to_owned).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmOptions {
    /// 回源与订阅时使用的身份（服务端要求认证时需要）；`None` 为匿名。
    pub token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    /// 一次 ref 变化最多预取的对象数。
    pub max_objects: usize,
    /// 同时预取不超过这个大小的文件；0 只预取目录树。
    pub max_blob_bytes: u64,
    /// 每次请求的对象数。
    pub batch: usize,
}

impl Default for WarmOptions {
    fn default() -> Self {
        Self {
            token: None,
            max_objects: 100_000,
            max_blob_bytes: 0,
            batch: 256,
        }
    }
}

/// 预热一次 ref 变化：从 `old`（之前的根目录树）到 `new`。返回取到的对象数。
pub async fn warm_tree(
    node: &CacheNode,
    repo: &str,
    old: Option<ObjectId>,
    new: ObjectId,
    opts: WarmOptions,
) -> Result<usize, tonic::Status> {
    let ctx = RequestCtx {
        authorization: opts.token.clone(),
    };
    let batch = opts.batch.max(1);
    let mut fetched = 0;
    // 当前层要比较的 (旧子树, 新子树)
    let mut level: Vec<(Option<ObjectId>, ObjectId)> = vec![(old, new)];
    while !level.is_empty() && fetched < opts.max_objects {
        let mut next = Vec::new();
        let mut blobs = Vec::new();
        for part in level.chunks(batch) {
            let mut want: Vec<ObjectId> = part.iter().map(|(_, n)| *n).collect();
            want.extend(part.iter().filter_map(|(o, _)| *o));
            let objects = node.get_many(repo, &want, &ctx, false).await?;
            fetched += part.len();
            let decoded: HashMap<ObjectId, Bytes> = want.into_iter().zip(objects).collect();
            for (o, n) in part {
                let new_tree = decode(&decoded, n)?;
                let old_tree = match o {
                    Some(o) => Some(decode(&decoded, o)?),
                    None => None,
                };
                for e in new_tree.entries() {
                    let before = old_tree.as_ref().and_then(|t| t.get(&e.name));
                    if before.is_some_and(|b| b.id == e.id) {
                        continue;
                    }
                    if e.mode == EntryMode::Tree {
                        let old_sub = before.filter(|b| b.mode == EntryMode::Tree).map(|b| b.id);
                        next.push((old_sub, e.id));
                    } else if opts.max_blob_bytes > 0 && e.size <= opts.max_blob_bytes {
                        blobs.push(e.id);
                    }
                }
            }
            if fetched >= opts.max_objects {
                break;
            }
        }
        let room = opts.max_objects.saturating_sub(fetched);
        blobs.truncate(room);
        for part in blobs.chunks(batch) {
            node.get_many(repo, part, &ctx, false).await?;
            fetched += part.len();
        }
        next.truncate(opts.max_objects.saturating_sub(fetched));
        level = next;
    }
    Ok(fetched)
}

fn decode(objects: &HashMap<ObjectId, Bytes>, id: &ObjectId) -> Result<Tree, tonic::Status> {
    let bytes = objects
        .get(id)
        .ok_or_else(|| tonic::Status::internal(format!("tree {id} not fetched")))?;
    Tree::decode(bytes).map_err(|e| tonic::Status::data_loss(format!("tree {id}: {e}")))
}

/// 持续跟随一个目标的 ref 变化并预热，直到 `stop` 完成。连接断开后带上已知版本重新订阅。
pub async fn follow(
    node: Arc<CacheNode>,
    upstream: Channel,
    target: WarmTarget,
    opts: WarmOptions,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut roots: HashMap<String, ObjectId> = HashMap::new();
    let mut known: HashMap<String, u64> = HashMap::new();
    let mut backoff = Duration::from_millis(200);
    loop {
        if *stop.borrow() {
            return;
        }
        let mut req = tonic::Request::new(WatchRefsRequest {
            repo: target.repo.clone(),
            refs: target.refs.clone(),
            known_versions: known.clone(),
        });
        if let Some(t) = &opts.token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        let mut stream = match RefServiceClient::new(upstream.clone()).watch_refs(req).await {
            Ok(s) => {
                backoff = Duration::from_millis(200);
                s.into_inner()
            }
            Err(e) => {
                tracing::warn!(repo = %target.repo, error = %e, "subscribing to ref changes failed");
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = stop.changed() => return,
                }
                backoff = (backoff * 2).min(Duration::from_secs(10));
                continue;
            }
        };
        loop {
            let msg = tokio::select! {
                m = stream.message() => m,
                _ = stop.changed() => return,
            };
            let resp = match msg {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(repo = %target.repo, error = %e, "ref watch interrupted");
                    break;
                }
            };
            for ev in resp.events {
                let Ok(root) = ObjectId::from_bytes(&ev.root_tree) else {
                    continue;
                };
                known.insert(ev.name.clone(), ev.version);
                let old = roots.insert(ev.name.clone(), root);
                if old == Some(root) {
                    continue;
                }
                let started = std::time::Instant::now();
                match warm_tree(&node, &target.repo, old, root, opts.clone()).await {
                    Ok(n) => tracing::info!(repo = %target.repo, r#ref = %ev.name, version = ev.version,
                        objects = n, elapsed_ms = started.elapsed().as_millis() as u64, "warmed"),
                    Err(e) => tracing::warn!(repo = %target.repo, r#ref = %ev.name, error = %e, "warm-up failed"),
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            _ = stop.changed() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::Ring;
    use crate::store::{ObjectCache, StorageConfig};
    use crate::upstream::{PoolConfig, Upstream, UpstreamPool};
    use scion_object::TreeEntry;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// 内存中的「上游」：按 id 返回对象，并记录请求过哪些 id。
    struct Objects {
        data: HashMap<ObjectId, Bytes>,
        asked: Mutex<Vec<ObjectId>>,
        calls: AtomicU32,
    }

    #[tonic::async_trait]
    impl Upstream for Objects {
        async fn fetch(&self, _: &str, ids: &[ObjectId], _: &RequestCtx) -> Result<Vec<Bytes>, tonic::Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.asked.lock().expect("lock").extend_from_slice(ids);
            ids.iter()
                .map(|id| self.data.get(id).cloned().ok_or_else(|| tonic::Status::not_found("x")))
                .collect()
        }
        async fn can_read(&self, _: &str, _: &RequestCtx) -> Result<bool, tonic::Status> {
            Ok(true)
        }
        async fn missing(&self, _: &str, ids: &[ObjectId], _: &RequestCtx) -> Result<Vec<ObjectId>, tonic::Status> {
            Ok(ids.iter().filter(|id| !self.data.contains_key(id)).copied().collect())
        }
        async fn redactions(
            &self,
            _: &str,
            after_seq: u64,
            _: &RequestCtx,
        ) -> Result<crate::upstream::RedactionPage, tonic::Status> {
            Ok(crate::upstream::RedactionPage {
                next_seq: after_seq,
                ..Default::default()
            })
        }
        fn name(&self) -> &str {
            "objects"
        }
    }

    struct Repo {
        data: HashMap<ObjectId, Bytes>,
    }

    impl Repo {
        fn blob(&mut self, content: &str) -> TreeEntry {
            let id = ObjectId::of_blob(content.as_bytes());
            self.data.insert(id, Bytes::from(content.to_owned()));
            TreeEntry {
                name: String::new(),
                mode: EntryMode::File,
                id,
                size: content.len() as u64,
            }
        }

        fn tree(&mut self, entries: Vec<(&str, TreeEntry)>) -> TreeEntry {
            let t = Tree::new(
                entries
                    .into_iter()
                    .map(|(n, mut e)| {
                        e.name = n.to_owned();
                        e
                    })
                    .collect(),
            )
            .expect("tree");
            let id = t.id();
            self.data.insert(id, Bytes::from(t.encode()));
            TreeEntry {
                name: String::new(),
                mode: EntryMode::Tree,
                id,
                size: 0,
            }
        }
    }

    async fn node(data: HashMap<ObjectId, Bytes>) -> (CacheNode, Arc<Objects>) {
        let up = Arc::new(Objects {
            data,
            asked: Mutex::new(Vec::new()),
            calls: AtomicU32::new(0),
        });
        let pool = UpstreamPool::new(vec![up.clone() as Arc<dyn Upstream>], PoolConfig::default());
        let store = ObjectCache::open(&StorageConfig::default()).await.expect("cache");
        (
            CacheNode::new(store, Ring::new(Vec::new()), None, HashMap::new(), pool),
            up,
        )
    }

    #[test]
    fn parse_targets() {
        assert_eq!(
            "mono".parse::<WarmTarget>(),
            Ok(WarmTarget {
                repo: "mono".into(),
                refs: vec![]
            })
        );
        assert_eq!(
            "mono:main, dev,".parse::<WarmTarget>(),
            Ok(WarmTarget {
                repo: "mono".into(),
                refs: vec!["main".into(), "dev".into()]
            })
        );
        assert!(":main".parse::<WarmTarget>().is_err());
    }

    #[tokio::test]
    async fn warms_only_changed_paths() {
        let mut r = Repo { data: HashMap::new() };
        let a1 = r.blob("a1");
        let b = r.blob("b");
        let big = r.blob("a big file that exceeds the blob limit");
        let lib = r.tree(vec![("b.rs", b.clone())]);
        let src1 = r.tree(vec![("a.rs", a1)]);
        let root1 = r.tree(vec![("lib", lib.clone()), ("src", src1)]);
        let a2 = r.blob("a2");
        let src2 = r.tree(vec![("a.rs", a2.clone()), ("big", big.clone())]);
        let new_dir = r.tree(vec![("n", b.clone())]);
        let root2 = r.tree(vec![
            ("lib", lib.clone()),
            ("new", new_dir.clone()),
            ("src", src2.clone()),
        ]);
        let (n, up) = node(r.data.clone()).await;
        let opts = WarmOptions {
            max_blob_bytes: 8,
            ..WarmOptions::default()
        };

        // 第一次：整棵树（目录 + 小文件）
        let got = warm_tree(&n, "r", None, root1.id, opts.clone()).await.expect("warm");
        assert_eq!(got, 5, "root, lib, src, b.rs, a.rs");
        up.asked.lock().expect("lock").clear();

        // 之后只取变化的路径：lib 没变，跳过
        let got = warm_tree(&n, "r", Some(root1.id), root2.id, opts.clone())
            .await
            .expect("warm");
        let asked = up.asked.lock().expect("lock").clone();
        assert!(!asked.contains(&lib.id), "unchanged subtree skipped");
        assert!(!asked.contains(&big.id), "large blob skipped");
        assert!(asked.contains(&a2.id) && asked.contains(&new_dir.id) && asked.contains(&src2.id));
        assert_eq!(got, 5, "root2, src2, new, a2 and n (already cached, counted)");
        assert_eq!(n.store().get(&a2.id).await, Some(Bytes::from_static(b"a2")));

        // 只预取目录树
        let (n2, up2) = node(r.data.clone()).await;
        warm_tree(&n2, "r", None, root2.id, WarmOptions::default())
            .await
            .expect("trees");
        assert!(!up2.asked.lock().expect("lock").contains(&a2.id));

        // 上限
        let (n3, _) = node(r.data.clone()).await;
        let capped = WarmOptions { max_objects: 1, ..opts };
        assert_eq!(warm_tree(&n3, "r", None, root2.id, capped).await.expect("capped"), 1);
    }

    #[tokio::test]
    async fn warm_reports_bad_trees() {
        let mut r = Repo { data: HashMap::new() };
        let blob = r.blob("not a tree");
        let (n, _) = node(r.data.clone()).await;
        // 「树」其实是一个 blob：解码失败
        let err = warm_tree(&n, "r", None, blob.id, WarmOptions::default())
            .await
            .expect_err("bad tree");
        assert_eq!(err.code(), tonic::Code::DataLoss);
        let missing = warm_tree(&n, "r", None, ObjectId::of_blob(b"none"), WarmOptions::default()).await;
        assert!(missing.is_err());
        let objects = HashMap::new();
        assert!(decode(&objects, &blob.id).is_err());
    }
}
