//! Scion 存储抽象。
//!
//! - [`MetaStore`]：仓库与 ref 等元数据。ref 是系统中**唯一的可变状态**，只能通过 CAS 修改。
//! - [`BlobStore`]：按内容寻址的对象字节，全局只存一份，写入天然幂等；
//!   仓库能看到哪些对象由 MetaStore 的对象索引决定。
//!
//! 本 crate 提供内存实现 [`MemStore`]；SQLite / PostgreSQL / YugabyteDB 与 S3 实现位于其它 crate，
//! 都必须通过 [`testsuite`]（feature `testsuite`）中的一致性测试。

mod error;
mod mem;
mod repo;
#[cfg(any(test, feature = "testsuite"))]
pub mod testsuite;

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

pub use error::{Error, Result};
pub use mem::MemStore;
pub use repo::{MAX_REPO_NAME_LEN, validate_repo_name};

/// ref 的当前状态。`version` 每次更新加 1，作为 CAS 条件和 fencing token。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefState {
    pub target: ObjectId,
    pub version: u64,
}

/// ref CAS 的前置条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefExpect {
    /// ref 必须不存在（创建）。
    Absent,
    /// ref 必须存在且版本等于该值。
    Version(u64),
}

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// 创建仓库；已存在时返回 [`Error::RepoAlreadyExists`]。
    async fn create_repo(&self, repo: &str) -> Result<()>;

    async fn repo_exists(&self, repo: &str) -> Result<bool>;

    /// 按名字字节序返回全部仓库。
    async fn list_repos(&self) -> Result<Vec<String>>;

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>>;

    /// 条件更新 ref：前置条件满足时把 target 设为 `target`，版本加 1（新建时版本为 1）。
    /// 条件不满足时返回 [`Error::RefVersionMismatch`]，其中带上当前版本。
    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState>;

    /// 把对象登记到仓库的对象索引（幂等）。调用前对象字节必须已写入 [`BlobStore`]：
    /// 索引里有的对象一定可读，这是写入顺序不变式（服务端设计文档 §4.5）。
    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()>;

    /// 返回 `ids` 中**该仓库**索引里没有的对象，保持输入中的相对顺序，重复的 id 只返回一次。
    /// 对象字节全局只存一份，但仓库之间互相看不到对方有哪些对象。
    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>>;

    /// 返回 `ids` 中**该仓库**已登记对象的大小；未登记的不出现在结果里。
    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>>;

    /// 记录一个已创建的 changeset 及其根目录树（服务端设计文档 §4.3 的 `cs_tree`）。
    /// 幂等：同一 id 重复写入时保留第一次的记录（相同 changeset 建出的树必然相同）。
    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()>;

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>>;

    /// 按名字字节序列出仓库的全部 ref。
    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>>;

    /// 代数不大于 `max_generation` 的提交记录，按代数从高到低，最多 `limit` 条（同代数时顺序不定）。
    /// 用于沿历史批量预取，避免逐条查询。
    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>>;

    /// 登记提交改动的路径（PathHistory 索引，相对第一个父提交）。幂等。
    /// 应在 [`MetaStore::put_changeset`] 之前写入：记录存在即表示索引完整。
    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()>;

    /// 改动了 `path` 或其下任意路径、且代数不大于 `max_generation` 的提交，按代数从高到低，最多 `limit` 条。
    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>>;

    /// 读取派生数据（服务端设计文档 §8）。`kind` 区分种类，`version` 为派生算法版本：
    /// 算法变化时提升版本，旧值自然失效，提交 id 不受影响。
    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 写入派生数据。派生值由输入唯一决定，重复写入保留第一次的值。
    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()>;
}

/// 已创建 changeset 的元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetRecord {
    pub id: ObjectId,
    /// 应用 delta 之后的根目录树。
    pub root_tree: ObjectId,
    /// 全部父提交，第一个为 first parent；根提交为空。
    pub parents: Vec<ObjectId>,
    /// 代数：根提交为 1，其余为所有 parent 中最大代数加 1。用于 log 排序和祖先判断剪枝。
    pub generation: u64,
}

impl ChangesetRecord {
    pub fn first_parent(&self) -> Option<ObjectId> {
        self.parents.first().copied()
    }
}

/// 提交对一个路径的改动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathChange {
    pub path: String,
    pub deleted: bool,
}

/// PathHistory 索引中的一条记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathHistoryEntry {
    pub cs: ObjectId,
    pub generation: u64,
    pub path: String,
    pub deleted: bool,
}

/// `candidate` 是否等于 `path` 或在 `path` 之下（按路径段）。
pub fn path_within(candidate: &str, path: &str) -> bool {
    candidate == path
        || (candidate.len() > path.len() && candidate.starts_with(path) && candidate.as_bytes()[path.len()] == b'/')
}

/// 对象索引中的一条记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedObject {
    pub id: ObjectId,
    /// 对象字节数（未压缩）。
    pub size: u64,
}

/// 按内容寻址的对象字节存储，**全局共享**：相同内容在所有仓库中只存一份。
/// 仓库能访问哪些对象由 [`MetaStore`] 的对象索引决定。
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// 写入对象。同一 id 重复写入是幂等的。调用方负责保证 `data` 与 `id` 相符。
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()>;

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>>;

    async fn exists(&self, id: &ObjectId) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_within_respects_segments() {
        assert!(path_within("src", "src"));
        assert!(path_within("src/a.rs", "src"));
        assert!(path_within("src/b/c", "src/b"));
        assert!(!path_within("src.txt", "src"));
        assert!(!path_within("srcx/a", "src"));
        assert!(!path_within("sr", "src"));
    }

    #[test]
    fn first_parent_of_record() {
        let id = ObjectId::of_blob(b"x");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
        };
        assert_eq!(rec.first_parent(), None);
    }
}
