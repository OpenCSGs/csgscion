//! 内存后端：用于测试和 `scion server --mem`。进程退出即丢失全部数据。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

use crate::error::{Error, Result};
use crate::repo::validate_repo_name;
use crate::{BlobStore, MetaStore, RefExpect, RefState};

#[derive(Default)]
struct RepoData {
    refs: HashMap<String, RefState>,
    blobs: HashMap<ObjectId, Bytes>,
}

/// 同时实现 [`MetaStore`] 与 [`BlobStore`] 的内存存储。
#[derive(Default)]
pub struct MemStore {
    repos: RwLock<BTreeMap<String, RepoData>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, BTreeMap<String, RepoData>>> {
        self.repos
            .read()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, BTreeMap<String, RepoData>>> {
        self.repos
            .write()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }
}

fn repo_mut<'a>(repos: &'a mut BTreeMap<String, RepoData>, repo: &str) -> Result<&'a mut RepoData> {
    repos.get_mut(repo).ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

fn repo_ref<'a>(repos: &'a BTreeMap<String, RepoData>, repo: &str) -> Result<&'a RepoData> {
    repos.get(repo).ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

#[async_trait]
impl MetaStore for MemStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        validate_repo_name(repo)?;
        let mut repos = self.write()?;
        if repos.contains_key(repo) {
            return Err(Error::RepoAlreadyExists(repo.to_owned()));
        }
        repos.insert(repo.to_owned(), RepoData::default());
        Ok(())
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        Ok(self.read()?.contains_key(repo))
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        Ok(self.read()?.keys().cloned().collect())
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let repos = self.read()?;
        Ok(repo_ref(&repos, repo)?.refs.get(name).copied())
    }

    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
        let mut repos = self.write()?;
        let data = repo_mut(&mut repos, repo)?;
        let current = data.refs.get(name).map(|r| r.version);
        let next_version = match (expect, current) {
            (RefExpect::Absent, None) => 1,
            (RefExpect::Version(want), Some(cur)) if want == cur => cur + 1,
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
}

#[async_trait]
impl BlobStore for MemStore {
    async fn put(&self, repo: &str, id: ObjectId, data: Bytes) -> Result<()> {
        let mut repos = self.write()?;
        repo_mut(&mut repos, repo)?.blobs.entry(id).or_insert(data);
        Ok(())
    }

    async fn get(&self, repo: &str, id: &ObjectId) -> Result<Option<Bytes>> {
        let repos = self.read()?;
        Ok(repo_ref(&repos, repo)?.blobs.get(id).cloned())
    }

    async fn missing(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let repos = self.read()?;
        let data = repo_ref(&repos, repo)?;
        let mut seen = HashSet::new();
        Ok(ids
            .iter()
            .filter(|id| !data.blobs.contains_key(id) && seen.insert(**id))
            .copied()
            .collect())
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
    async fn conformance_blob() {
        let s = MemStore::new();
        s.create_repo(testsuite::REPO).await.expect("create repo");
        testsuite::blob_store(&s).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        testsuite::concurrent_cas(std::sync::Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    async fn poisoned_lock_reports_backend_error() {
        let s = std::sync::Arc::new(MemStore::new());
        let s2 = s.clone();
        let _ = std::thread::spawn(move || {
            let _g = s2.repos.write().expect("lock");
            panic!("poison the lock");
        })
        .join();
        assert!(matches!(s.repo_exists("a").await, Err(Error::Backend(_))));
        assert!(matches!(s.create_repo("a").await, Err(Error::Backend(_))));
    }
}
