//! 提交记录的进程级缓存（服务端设计文档 §7）。
//!
//! 提交记录写入后不再改变（`put_changeset` 保留第一次写入），因此可以无失效地缓存。
//! 只缓存存在的记录，「不存在」的结果不缓存。其余方法原样转发。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use moka::sync::Cache;
use scion_object::ObjectId;

use crate::{ChangesetRecord, IndexedObject, MetaStore, PathChange, PathHistoryEntry, RefExpect, RefState, Result};

/// 在任意 [`MetaStore`] 外加一层提交记录缓存。
pub struct CachedMeta {
    inner: Arc<dyn MetaStore>,
    records: Cache<(Arc<str>, ObjectId), ChangesetRecord>,
}

impl CachedMeta {
    /// `capacity` 为最多缓存的提交记录条数。
    pub fn new(inner: Arc<dyn MetaStore>, capacity: u64) -> Self {
        Self {
            inner,
            records: Cache::new(capacity),
        }
    }

    fn key(repo: &str, id: ObjectId) -> (Arc<str>, ObjectId) {
        (Arc::from(repo), id)
    }

    fn remember(&self, repo: &str, rec: &ChangesetRecord) {
        self.records.insert(Self::key(repo, rec.id), rec.clone());
    }

    /// 预热：按代数从高到低分窗口读入仓库的全部提交记录（受容量上限约束）。返回读入的条数。
    /// 同一代数的记录超过一个窗口时，超出部分留待按需读取。
    pub async fn warm(&self, repo: &str, window: usize) -> Result<usize> {
        let window = window.max(1);
        let mut max_generation = u64::MAX >> 1;
        let mut loaded = 0;
        loop {
            let recs = self.inner.changesets_below(repo, max_generation, window).await?;
            let Some(lowest) = recs.iter().map(|r| r.generation).min() else {
                break;
            };
            for r in &recs {
                self.remember(repo, r);
            }
            loaded += recs.len();
            if lowest <= 1 || recs.len() < window {
                break;
            }
            max_generation = lowest - 1;
        }
        Ok(loaded)
    }
}

#[async_trait]
impl MetaStore for CachedMeta {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        self.inner.create_repo(repo).await
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        self.inner.repo_exists(repo).await
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        self.inner.list_repos().await
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        self.inner.get_ref(repo, name).await
    }

    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
        self.inner.cas_ref(repo, name, expect, target).await
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        self.inner.index_objects(repo, objects).await
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        self.inner.missing_objects(repo, ids).await
    }

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
        self.inner.object_sizes(repo, ids).await
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        // 不在写入时缓存：写入可能因为已存在而被忽略，以存储中的第一次写入为准
        self.inner.put_changeset(repo, rec).await
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        if let Some(rec) = self.cached_changeset(repo, id) {
            return Ok(Some(rec));
        }
        let rec = self.inner.get_changeset(repo, id).await?;
        if let Some(r) = &rec {
            self.remember(repo, r);
        }
        Ok(rec)
    }

    fn cached_changeset(&self, repo: &str, id: &ObjectId) -> Option<ChangesetRecord> {
        self.records.get(&Self::key(repo, *id))
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        self.inner.list_refs(repo).await
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let recs = self.inner.changesets_below(repo, max_generation, limit).await?;
        for r in &recs {
            self.remember(repo, r);
        }
        Ok(recs)
    }

    async fn set_fp_skip(&self, repo: &str, id: &ObjectId, fp_depth: u64, fp_jump: Option<ObjectId>) -> Result<()> {
        self.inner.set_fp_skip(repo, id, fp_depth, fp_jump).await?;
        // 唯一会变化的字段：清掉旧记录，下次从存储读取
        self.records.invalidate(&Self::key(repo, *id));
        Ok(())
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        self.inner.index_paths(repo, cs, generation, changes).await
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        self.inner.path_history(repo, path, max_generation, limit).await
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_derived(repo, kind, version, key).await
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_derived(repo, kind, version, key, value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStore, testsuite};
    use scion_object::ObjectKind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn cached() -> CachedMeta {
        CachedMeta::new(Arc::new(MemStore::new()), 1000)
    }

    // 缓存层必须保持全部语义：完整跑一遍一致性测试
    #[tokio::test]
    async fn conformance_meta() {
        testsuite::meta_store(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        testsuite::object_index(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        testsuite::changesets_and_refs(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        testsuite::derived(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        testsuite::history_index(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_fp_skip_chain() {
        testsuite::fp_skip_chain(&cached()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        testsuite::concurrent_cas(Arc::new(cached())).await;
    }

    /// 统计底层读取次数的 MetaStore。
    struct Counting {
        inner: MemStore,
        reads: AtomicUsize,
    }

    #[async_trait]
    impl MetaStore for Counting {
        async fn create_repo(&self, repo: &str) -> Result<()> {
            self.inner.create_repo(repo).await
        }
        async fn repo_exists(&self, repo: &str) -> Result<bool> {
            self.inner.repo_exists(repo).await
        }
        async fn list_repos(&self) -> Result<Vec<String>> {
            self.inner.list_repos().await
        }
        async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
            self.inner.get_ref(repo, name).await
        }
        async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
            self.inner.cas_ref(repo, name, expect, target).await
        }
        async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
            self.inner.index_objects(repo, objects).await
        }
        async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
            self.inner.missing_objects(repo, ids).await
        }
        async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
            self.inner.object_sizes(repo, ids).await
        }
        async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
            self.inner.put_changeset(repo, rec).await
        }
        async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_changeset(repo, id).await
        }
        async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
            self.inner.list_refs(repo).await
        }
        async fn changesets_below(
            &self,
            repo: &str,
            max_generation: u64,
            limit: usize,
        ) -> Result<Vec<ChangesetRecord>> {
            self.inner.changesets_below(repo, max_generation, limit).await
        }
        async fn set_fp_skip(&self, repo: &str, id: &ObjectId, d: u64, j: Option<ObjectId>) -> Result<()> {
            self.inner.set_fp_skip(repo, id, d, j).await
        }
        async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
            self.inner.index_paths(repo, cs, generation, changes).await
        }
        async fn path_history(
            &self,
            repo: &str,
            path: &str,
            max_generation: u64,
            limit: usize,
        ) -> Result<Vec<PathHistoryEntry>> {
            self.inner.path_history(repo, path, max_generation, limit).await
        }
        async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get_derived(repo, kind, version, key).await
        }
        async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
            self.inner.put_derived(repo, kind, version, key, value).await
        }
    }

    #[tokio::test]
    async fn caches_existing_records_only() {
        let counting = Arc::new(Counting {
            inner: MemStore::new(),
            reads: AtomicUsize::new(0),
        });
        counting.create_repo("r").await.expect("repo");
        let c = CachedMeta::new(counting.clone(), 100);
        let id = ObjectId::hash(ObjectKind::Changeset, b"c");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 1,
            fp_jump: None,
        };

        // 不存在：每次都访问底层，不缓存否定结果
        assert_eq!(c.get_changeset("r", &id).await, Ok(None));
        assert_eq!(c.get_changeset("r", &id).await, Ok(None));
        assert_eq!(counting.reads.load(Ordering::Relaxed), 2);
        assert_eq!(c.cached_changeset("r", &id), None);

        c.put_changeset("r", &rec).await.expect("put");
        assert_eq!(c.cached_changeset("r", &id), None, "writes are not cached");
        assert_eq!(c.get_changeset("r", &id).await, Ok(Some(rec.clone())));
        assert_eq!(c.get_changeset("r", &id).await, Ok(Some(rec.clone())));
        assert_eq!(
            counting.reads.load(Ordering::Relaxed),
            3,
            "second read served from cache"
        );
        assert_eq!(c.cached_changeset("r", &id), Some(rec.clone()));
        // 仓库隔离
        assert_eq!(c.cached_changeset("other", &id), None);

        // 批量预取的记录进入缓存
        let id2 = ObjectId::hash(ObjectKind::Changeset, b"c2");
        let rec2 = ChangesetRecord {
            id: id2,
            generation: 2,
            ..rec
        };
        c.put_changeset("r", &rec2).await.expect("put");
        assert_eq!(c.changesets_below("r", 10, 10).await.expect("below").len(), 2);
        assert_eq!(c.cached_changeset("r", &id2), Some(rec2));
    }

    #[tokio::test]
    async fn warm_loads_every_record_across_windows() {
        let c = cached();
        c.create_repo("r").await.expect("repo");
        let mut ids = Vec::new();
        for n in 1..=10u8 {
            let id = ObjectId::hash(ObjectKind::Changeset, &[n]);
            let rec = ChangesetRecord {
                id,
                root_tree: id,
                parents: vec![],
                generation: u64::from(n),
                fp_depth: 1,
                fp_jump: None,
            };
            c.put_changeset("r", &rec).await.expect("put");
            ids.push(id);
        }
        assert_eq!(c.warm("r", 3).await, Ok(10));
        assert!(ids.iter().all(|id| c.cached_changeset("r", id).is_some()));
        assert_eq!(c.warm("r", 0).await, Ok(10), "window clamped to 1");
        c.create_repo("empty").await.expect("repo");
        assert_eq!(c.warm("empty", 3).await, Ok(0));
        assert!(c.warm("nope", 3).await.is_err());
    }

    #[tokio::test]
    async fn set_fp_skip_invalidates_cached_record() {
        let c = cached();
        c.create_repo("r").await.expect("repo");
        let id = ObjectId::hash(ObjectKind::Changeset, b"legacy");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        c.put_changeset("r", &rec).await.expect("put");
        assert_eq!(
            c.get_changeset("r", &id).await.expect("ok").map(|r| r.fp_depth),
            Some(0)
        );
        c.set_fp_skip("r", &id, 1, None).await.expect("set");
        assert_eq!(c.cached_changeset("r", &id), None, "stale entry dropped");
        assert_eq!(
            c.get_changeset("r", &id).await.expect("ok").map(|r| r.fp_depth),
            Some(1)
        );
    }

    #[test]
    fn default_cached_changeset_is_none() {
        let m = MemStore::new();
        assert_eq!(MetaStore::cached_changeset(&m, "r", &ObjectId::of_blob(b"x")), None);
    }
}
