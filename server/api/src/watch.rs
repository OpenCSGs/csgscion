//! ref 变化的订阅（WatchRefs）。
//!
//! 每个有订阅者的仓库一个 [`broadcast`] 通道。变化有两个来源：
//! - 本节点合入成功后由 LandService 直接发布（[`RefHub::publish`]），订阅者几乎立即收到；
//! - 后台轮询元数据库（[`RefHub::subscribe`] 为仓库启动，最后一个订阅者离开后退出），覆盖集群中
//!   其它节点的合入。轮询间隔即跨节点通知的最大延迟。
//!
//! 每个 ref 只发布比已发布版本更新的状态，两个来源重复发现同一次变化时只发一次。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scion_object::ObjectId;
use scion_store::{MetaStore, RefState};
use tokio::sync::broadcast;

/// 一个 ref 的新状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdate {
    pub name: String,
    pub target: ObjectId,
    /// 提交的根目录树；提交记录缺失时为 `None`（不应发生）。
    pub root_tree: Option<ObjectId>,
    pub version: u64,
}

/// 每个仓库的通道缓冲：订阅者落后超过这么多条时收到 Lagged，改为重新取快照。
const CHANNEL_CAPACITY: usize = 1024;

struct RepoWatch {
    tx: broadcast::Sender<RefUpdate>,
    /// 每个 ref 已发布的最新版本。
    published: Mutex<HashMap<String, u64>>,
}

impl RepoWatch {
    /// 只发布比已发布版本更新的状态。返回是否发布了。
    fn offer(&self, update: RefUpdate) -> bool {
        let Ok(mut published) = self.published.lock() else {
            return false;
        };
        let known = published.get(&update.name).copied().unwrap_or(0);
        if update.version <= known {
            return false;
        }
        published.insert(update.name.clone(), update.version);
        // 没有接收者时发送失败，忽略
        let _ = self.tx.send(update);
        true
    }
}

pub struct RefHub {
    meta: Arc<dyn MetaStore>,
    poll_interval: Duration,
    repos: Mutex<HashMap<String, Arc<RepoWatch>>>,
    /// 停机信号：订阅流随之结束，客户端重新订阅到其它节点（否则优雅停机会一直等这些长连接）。
    closed: tokio::sync::watch::Sender<bool>,
}

impl RefHub {
    pub fn new(meta: Arc<dyn MetaStore>, poll_interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            meta,
            poll_interval: poll_interval.max(Duration::from_millis(10)),
            repos: Mutex::new(HashMap::new()),
            closed: tokio::sync::watch::channel(false).0,
        })
    }

    /// 结束全部订阅流（停机时调用）。
    pub fn close(&self) {
        self.closed.send_replace(true);
    }

    /// 等到 [`RefHub::close`] 被调用。
    pub async fn wait_closed(&self) {
        let mut rx = self.closed.subscribe();
        // 发送端由 self 持有，不会先于 self 被丢弃
        let _ = rx.wait_for(|c| *c).await;
    }

    fn watch(&self, repo: &str) -> Option<Arc<RepoWatch>> {
        self.repos.lock().ok()?.get(repo).cloned()
    }

    /// 本节点合入成功后调用。仓库没有订阅者时什么都不做。
    pub async fn publish(&self, repo: &str, name: &str, state: RefState) {
        let Some(w) = self.watch(repo) else {
            return;
        };
        let update = self.update(repo, name, state).await;
        w.offer(update);
    }

    async fn update(&self, repo: &str, name: &str, state: RefState) -> RefUpdate {
        let root_tree = match self.meta.get_changeset(repo, &state.target).await {
            Ok(Some(rec)) => Some(rec.root_tree),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(repo, r#ref = name, error = %e, "reading root tree for ref event failed");
                None
            }
        };
        RefUpdate {
            name: name.to_owned(),
            target: state.target,
            root_tree,
            version: state.version,
        }
    }

    /// 订阅仓库的 ref 变化；第一个订阅者为仓库启动轮询。先订阅再取快照，才不会漏掉两者之间的变化。
    pub fn subscribe(self: &Arc<Self>, repo: &str) -> broadcast::Receiver<RefUpdate> {
        let Ok(mut repos) = self.repos.lock() else {
            // 锁中毒时退化为一个不会收到变化的通道（快照仍然正确）
            return broadcast::channel(1).1;
        };
        if let Some(w) = repos.get(repo) {
            return w.tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        let w = Arc::new(RepoWatch {
            tx,
            published: Mutex::new(HashMap::new()),
        });
        repos.insert(repo.to_owned(), w.clone());
        tokio::spawn(self.clone().poll(repo.to_owned(), w));
        rx
    }

    /// 仓库中全部 ref 的当前状态。
    pub async fn snapshot(&self, repo: &str) -> scion_store::Result<Vec<RefUpdate>> {
        let refs = self.meta.list_refs(repo).await?;
        let mut out = Vec::with_capacity(refs.len());
        for (name, state) in refs {
            out.push(self.update(repo, &name, state).await);
        }
        Ok(out)
    }

    /// 定期比对元数据库中的 ref，发布其它节点造成的变化；没有订阅者后退出。
    ///
    /// 第一轮会把已有的 ref 都发布一次：订阅者按版本过滤掉快照里已有的，但不会漏掉在它取快照之后、
    /// 第一轮轮询之前发生的变化。
    async fn poll(self: Arc<Self>, repo: String, w: Arc<RepoWatch>) {
        loop {
            {
                let Ok(mut repos) = self.repos.lock() else {
                    return;
                };
                if w.tx.receiver_count() == 0 {
                    if repos.get(&repo).is_some_and(|cur| Arc::ptr_eq(cur, &w)) {
                        repos.remove(&repo);
                    }
                    return;
                }
            }
            match self.meta.list_refs(&repo).await {
                Ok(refs) => {
                    for (name, state) in refs {
                        let newer = w
                            .published
                            .lock()
                            .map(|p| p.get(&name).copied().unwrap_or(0) < state.version)
                            .unwrap_or(false);
                        if newer {
                            let update = self.update(&repo, &name, state).await;
                            w.offer(update);
                        }
                    }
                }
                Err(e) => tracing::warn!(%repo, error = %e, "polling refs failed"),
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// 有订阅者（轮询任务在运行）的仓库数。
    pub fn watched_repos(&self) -> usize {
        self.repos.lock().map(|r| r.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::{ChangesetRecord, MemStore, RefExpect};

    async fn store() -> Arc<MemStore> {
        let s = Arc::new(MemStore::new());
        s.create_repo("r").await.expect("repo");
        s
    }

    async fn commit(s: &MemStore, n: u8) -> ChangesetRecord {
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, &[n]);
        let rec = ChangesetRecord {
            id,
            root_tree: ObjectId::hash(scion_object::ObjectKind::Tree, &[n]),
            parents: vec![],
            generation: 1,
            fp_depth: 1,
            fp_jump: None,
        };
        s.put_changeset("r", &rec).await.expect("put");
        rec
    }

    async fn advance(s: &MemStore, name: &str, rec: &ChangesetRecord) -> RefState {
        let expect = match s.get_ref("r", name).await.expect("get") {
            Some(st) => RefExpect::Version(st.version),
            None => RefExpect::Absent,
        };
        s.cas_ref("r", name, expect, rec.id).await.expect("cas")
    }

    #[tokio::test]
    async fn publish_reaches_subscribers_once_per_version() {
        let s = store().await;
        let hub = RefHub::new(s.clone(), Duration::from_secs(3600));
        let c1 = commit(&s, 1).await;
        let st = advance(&s, "main", &c1).await;
        // 没有订阅者：什么都不做
        hub.publish("r", "main", st).await;
        assert_eq!(hub.watched_repos(), 0);

        let mut rx = hub.subscribe("r");
        let mut rx2 = hub.subscribe("r");
        assert_eq!(hub.watched_repos(), 1);
        let c2 = commit(&s, 2).await;
        let st2 = advance(&s, "main", &c2).await;
        hub.publish("r", "main", st2).await;
        // 重复和过期的版本不再发布
        hub.publish("r", "main", st2).await;
        hub.publish("r", "main", st).await;
        let want = RefUpdate {
            name: "main".into(),
            target: c2.id,
            root_tree: Some(c2.root_tree),
            version: 2,
        };
        assert_eq!(rx.recv().await.expect("event"), want);
        assert_eq!(rx2.recv().await.expect("event"), want);
        assert!(rx.try_recv().is_err(), "published once");

        // 提交记录缺失：root_tree 为 None
        let ghost = RefState {
            target: ObjectId::of_blob(b"ghost"),
            version: 3,
        };
        hub.publish("r", "main", ghost).await;
        assert_eq!(rx.recv().await.expect("event").root_tree, None);
    }

    #[tokio::test]
    async fn close_wakes_waiters() {
        let hub = RefHub::new(store().await, Duration::from_secs(1));
        let h = hub.clone();
        let waiter = tokio::spawn(async move { h.wait_closed().await });
        tokio::task::yield_now().await;
        hub.close();
        waiter.await.expect("woken");
        // 关闭之后再等待立即返回
        hub.wait_closed().await;
    }

    #[tokio::test]
    async fn snapshot_lists_all_refs() {
        let s = store().await;
        let hub = RefHub::new(s.clone(), Duration::from_secs(3600));
        let c1 = commit(&s, 1).await;
        advance(&s, "main", &c1).await;
        advance(&s, "dev", &c1).await;
        let mut snap = hub.snapshot("r").await.expect("snapshot");
        snap.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            snap.iter().map(|u| (u.name.as_str(), u.version)).collect::<Vec<_>>(),
            [("dev", 1), ("main", 1)]
        );
        assert!(hub.snapshot("missing").await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn polling_finds_changes_from_other_nodes_and_stops_without_subscribers() {
        let s = store().await;
        let hub = RefHub::new(s.clone(), Duration::from_millis(100));
        let c1 = commit(&s, 1).await;
        advance(&s, "main", &c1).await;
        let mut rx = hub.subscribe("r");
        // 第一轮发布已有状态（订阅者按版本过滤）
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(rx.recv().await.expect("initial").version, 1);
        // 「其它节点」直接改了元数据库
        let c2 = commit(&s, 2).await;
        advance(&s, "main", &c2).await;
        advance(&s, "dev", &c2).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut got = [rx.recv().await.expect("e1"), rx.recv().await.expect("e2")];
        got.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            got.iter().map(|u| (u.name.as_str(), u.version)).collect::<Vec<_>>(),
            [("dev", 1), ("main", 2)]
        );
        // 最后一个订阅者离开：下一轮轮询退出并移除仓库
        drop(rx);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(hub.watched_repos(), 0);
        // 再次订阅会重新启动
        let _rx = hub.subscribe("r");
        assert_eq!(hub.watched_repos(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn polling_errors_are_logged_and_retried() {
        let s = Arc::new(MemStore::new());
        let hub = RefHub::new(s.clone(), Duration::from_millis(100));
        let mut rx = hub.subscribe("late");
        tokio::time::sleep(Duration::from_millis(150)).await;
        // 仓库后来才创建
        s.create_repo("late").await.expect("repo");
        let c = ChangesetRecord {
            id: ObjectId::hash(scion_object::ObjectKind::Changeset, b"x"),
            root_tree: ObjectId::hash(scion_object::ObjectKind::Tree, b"x"),
            parents: vec![],
            generation: 1,
            fp_depth: 1,
            fp_jump: None,
        };
        s.put_changeset("late", &c).await.expect("put");
        s.cas_ref("late", "main", RefExpect::Absent, c.id).await.expect("cas");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(rx.recv().await.expect("event").version, 1);
    }
}
