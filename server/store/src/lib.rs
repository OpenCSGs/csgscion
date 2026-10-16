//! Scion 存储抽象。
//!
//! - [`MetaStore`]：仓库与 ref 等元数据。ref 是系统中**唯一的可变状态**，只能通过 CAS 修改。
//! - [`BlobStore`]：按内容寻址的对象字节，全局只存一份，写入天然幂等；
//!   仓库能看到哪些对象由 MetaStore 的对象索引决定。
//!
//! 本 crate 提供内存实现 [`MemStore`]；SQLite / PostgreSQL / YugabyteDB 与 S3 实现位于其它 crate，
//! 都必须通过 [`testsuite`]（feature `testsuite`）中的一致性测试。

mod cached;
mod error;
mod mem;
mod repo;
#[cfg(any(test, feature = "testsuite"))]
pub mod testsuite;

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

pub use cached::CachedMeta;
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

    /// 只查内存缓存、不访问存储的提交记录读取；没有缓存层的实现返回 None。
    /// 查询层在批量预取之前先调用它，命中缓存就不必发出窗口查询。
    fn cached_changeset(&self, _repo: &str, _id: &ObjectId) -> Option<ChangesetRecord> {
        None
    }

    /// 按名字字节序列出仓库的全部 ref。
    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>>;

    /// 代数不大于 `max_generation` 的提交记录，按代数从高到低，最多 `limit` 条（同代数时顺序不定）。
    /// 用于沿历史批量预取，避免逐条查询。
    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>>;

    /// 为跳表建立之前写入的提交补上 `fp_depth` / `fp_jump`：只在当前深度为 0（未知）时写入，
    /// 已有的值不会被覆盖。提交记录的其它字段不可变。
    async fn set_fp_skip(&self, repo: &str, id: &ObjectId, fp_depth: u64, fp_jump: Option<ObjectId>) -> Result<()>;

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

    /// 批量读取派生数据，结果与 `keys` 一一对应。远端数据库应覆盖默认实现，用一次往返完成。
    async fn get_derived_many(
        &self,
        repo: &str,
        kind: &str,
        version: u32,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.get_derived(repo, kind, version, key).await?);
        }
        Ok(out)
    }

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
    /// 在第一父提交链上的深度：根提交为 1；0 表示未知（跳表建立之前写入的数据）。
    pub fp_depth: u64,
    /// 第一父提交链上的跳跃指针（skew-binary 跳表）；None 表示指向自己（根提交）或未知。
    pub fp_jump: Option<ObjectId>,
}

impl ChangesetRecord {
    pub fn first_parent(&self) -> Option<ObjectId> {
        self.parents.first().copied()
    }

    /// 跳跃指针指向的提交（根提交指向自己）。
    pub fn jump(&self) -> ObjectId {
        self.fp_jump.unwrap_or(self.id)
    }
}

/// 由第一个父提交计算新提交的 `(fp_depth, fp_jump)`。
///
/// Myers 的 skew-binary 跳表：只存一个跳跃指针，即可在 O(log n) 步内找到第一父提交链上任意深度的祖先。
/// 规则：父提交 p 的跳跃 j = jump(p)、jj = jump(j)；若 depth(p) - depth(j) == depth(j) - depth(jj)
/// 则跳到 jj，否则跳到 p。父提交深度未知时结果也未知（0, None）。
pub async fn fp_skip(
    meta: &dyn MetaStore,
    repo: &str,
    first_parent: Option<ObjectId>,
) -> Result<(u64, Option<ObjectId>)> {
    let Some(p) = first_parent else { return Ok((1, None)) };
    let missing = |id: ObjectId| Error::Backend(format!("changeset {id} referenced but not recorded"));
    let p = meta.get_changeset(repo, &p).await?.ok_or_else(|| missing(p))?;
    if p.fp_depth == 0 {
        return Ok((0, None));
    }
    let j = meta
        .get_changeset(repo, &p.jump())
        .await?
        .ok_or_else(|| missing(p.jump()))?;
    let jj = meta
        .get_changeset(repo, &j.jump())
        .await?
        .ok_or_else(|| missing(j.jump()))?;
    let jump = if p.fp_depth - j.fp_depth == j.fp_depth - jj.fp_depth {
        jj.id
    } else {
        p.id
    };
    Ok((p.fp_depth + 1, Some(jump)))
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
            fp_depth: 0,
            fp_jump: None,
        };
        assert_eq!(rec.first_parent(), None);
    }
}
