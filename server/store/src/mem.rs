//! 内存后端：用于测试和 `scion server --meta mem`。进程退出即丢失全部数据。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

use crate::error::{Error, Result};
use crate::repo::validate_repo_name;
use crate::{
    BlobStore, ChangesetRecord, IndexedObject, MetaStore, PathChange, PathHistoryEntry, RefExpect, RefState,
    path_within,
};

#[derive(Default)]
struct RepoData {
    refs: HashMap<String, RefState>,
    objects: HashMap<ObjectId, u64>,
    changesets: HashMap<ObjectId, ChangesetRecord>,
    derived: HashMap<(String, u32, Vec<u8>), Vec<u8>>,
    /// PathHistory：(路径, 提交) → (代数, 是否删除)。
    paths: BTreeMap<(String, ObjectId), (u64, bool)>,
}

#[derive(Default)]
struct Inner {
    repos: BTreeMap<String, RepoData>,
    blobs: HashMap<ObjectId, Bytes>,
}

/// 同时实现 [`MetaStore`] 与 [`BlobStore`] 的内存存储。
#[derive(Default)]
pub struct MemStore {
    inner: RwLock<Inner>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, Inner>> {
        self.inner
            .read()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, Inner>> {
        self.inner
            .write()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }
}

fn repo_mut<'a>(inner: &'a mut Inner, repo: &str) -> Result<&'a mut RepoData> {
    inner
        .repos
        .get_mut(repo)
        .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

fn repo_ref<'a>(inner: &'a Inner, repo: &str) -> Result<&'a RepoData> {
    inner
        .repos
        .get(repo)
        .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

#[async_trait]
impl MetaStore for MemStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        validate_repo_name(repo)?;
        let mut inner = self.write()?;
        if inner.repos.contains_key(repo) {
            return Err(Error::RepoAlreadyExists(repo.to_owned()));
        }
        inner.repos.insert(repo.to_owned(), RepoData::default());
        Ok(())
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        Ok(self.read()?.repos.contains_key(repo))
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        Ok(self.read()?.repos.keys().cloned().collect())
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.refs.get(name).copied())
    }

    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        let current = data.refs.get(name).map(|r| r.version);
        let next_version = match (expect, current) {
            (RefExpect::Absent, None) => 1,
            (RefExpect::Version(want), Some(cur)) if want == cur => cur
                .checked_add(1)
                .ok_or_else(|| Error::Backend("ref version overflow".into()))?,
            _ => {
                return Err(Error::RefVersionMismatch {
                    name: name.to_owned(),
                    current,
                });
            }
        };
        let state = RefState {
            target,
            version: next_version,
        };
        data.refs.insert(name.to_owned(), state);
        Ok(state)
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        for o in objects {
            data.objects.entry(o.id).or_insert(o.size);
        }
        Ok(())
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        let mut seen = HashSet::new();
        Ok(ids
            .iter()
            .filter(|id| !data.objects.contains_key(id) && seen.insert(**id))
            .copied()
            .collect())
    }

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        Ok(ids
            .iter()
            .filter_map(|id| data.objects.get(id).map(|s| (*id, *s)))
            .collect())
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?
            .changesets
            .entry(rec.id)
            .or_insert_with(|| rec.clone());
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.changesets.get(id).cloned())
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let inner = self.read()?;
        let mut recs: Vec<_> = repo_ref(&inner, repo)?
            .changesets
            .values()
            .filter(|r| r.generation <= max_generation)
            .cloned()
            .collect();
        recs.sort_unstable_by_key(|r| std::cmp::Reverse(r.generation));
        recs.truncate(limit);
        Ok(recs)
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        for c in changes {
            data.paths
                .entry((c.path.clone(), cs))
                .or_insert((generation, c.deleted));
        }
        Ok(())
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        let mut out: Vec<_> = data
            .paths
            .range(
                (
                    path.to_owned(),
                    ObjectId::new(
                        scion_object::HashAlgo::Blake3_256,
                        scion_object::ObjectKind::Blob,
                        [0; 32],
                    ),
                )..,
            )
            .take_while(|((p, _), _)| p.as_str() == path || p.starts_with(path))
            .filter(|((p, _), (g, _))| path_within(p, path) && *g <= max_generation)
            .map(|((p, cs), (g, d))| PathHistoryEntry {
                cs: *cs,
                generation: *g,
                path: p.clone(),
                deleted: *d,
            })
            .collect();
        out.sort_unstable_by(|a, b| b.generation.cmp(&a.generation).then_with(|| a.path.cmp(&b.path)));
        out.truncate(limit);
        Ok(out)
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?
            .derived
            .get(&(kind.to_owned(), version, key.to_vec()))
            .cloned())
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?
            .derived
            .entry((kind.to_owned(), version, key.to_vec()))
            .or_insert_with(|| value.to_vec());
        Ok(())
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        let inner = self.read()?;
        let mut refs: Vec<_> = repo_ref(&inner, repo)?
            .refs
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        refs.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Ok(refs)
    }
}

#[async_trait]
impl BlobStore for MemStore {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        if !id.verify(&data) {
            return Err(Error::Backend(format!(
                "refusing to store bytes that do not match {id}"
            )));
        }
        self.write()?.blobs.entry(id).or_insert(data);
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        Ok(self.read()?.blobs.get(id).cloned())
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        Ok(self.read()?.blobs.contains_key(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsuite;

    #[tokio::test]
    async fn conformance_meta() {
        testsuite::meta_store(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        testsuite::object_index(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        testsuite::changesets_and_refs(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        testsuite::derived(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        testsuite::history_index(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_blob() {
        testsuite::blob_store(&MemStore::new()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        testsuite::concurrent_cas(std::sync::Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    async fn cas_version_overflow_rejected() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        s.write().expect("lock").repos.get_mut("r").expect("r").refs.insert(
            "main".into(),
            RefState {
                target: id,
                version: u64::MAX,
            },
        );
        assert!(matches!(
            s.cas_ref("r", "main", RefExpect::Version(u64::MAX), id).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn poisoned_lock_reports_backend_error() {
        let s = std::sync::Arc::new(MemStore::new());
        let s2 = s.clone();
        let _ = std::thread::spawn(move || {
            let _g = s2.inner.write().expect("lock");
            panic!("poison the lock");
        })
        .join();
        assert!(matches!(s.repo_exists("a").await, Err(Error::Backend(_))));
        assert!(matches!(s.create_repo("a").await, Err(Error::Backend(_))));
    }
}
