//! Scion 存储抽象。
//!
//! - [`MetaStore`]：仓库与 ref 等元数据。ref 是系统中**唯一的可变状态**，只能通过 CAS 修改。
//! - [`BlobStore`]：按内容寻址的对象字节。写入天然幂等。
//!
//! M0 只提供内存实现 [`MemStore`]；SQLite / PostgreSQL / YugabyteDB 与 S3 实现位于其它 crate，
//! 都必须通过 [`testsuite`]（feature `testsuite`）中的一致性测试。

mod error;
mod mem;
mod repo;
#[cfg(any(test, feature = "testsuite"))]
pub mod testsuite;

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
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// 写入对象。同一 id 重复写入是幂等的。调用方负责保证 `data` 与 `id` 相符。
    async fn put(&self, repo: &str, id: ObjectId, data: Bytes) -> Result<()>;

    async fn get(&self, repo: &str, id: &ObjectId) -> Result<Option<Bytes>>;

    /// 返回 `ids` 中不存在的那些，保持输入中的相对顺序（重复的 id 只返回一次）。
    async fn missing(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>>;
}
