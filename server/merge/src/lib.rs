//! 服务端 rebase（服务端设计文档 §6.3）：把一个 changeset 的 delta 从它原来的父提交 `base`
//! 重放到更新的提交 `onto` 上。
//!
//! 只看 delta 涉及的路径：逐个比较这些路径在 `base` 与 `onto` 中的条目。没有变化就原样应用
//! （最常见的情况），代价是 O(改动数 × 目录深度)，与仓库大小和 `base..onto` 之间的提交数无关。
//! 双方都改了同一路径时做三方合并：
//!
//! - 普通文件与可执行文件：按行做 diff3；mode（可执行位）也按三方规则合并；
//! - 二进制文件、符号链接、超过大小上限的文件、修改与删除相冲突、双方新增同一路径：报冲突；
//! - 双方改动结果相同：去掉这条改动（`onto` 已经包含它）。
//!
//! 冲突带上 base / ours / theirs 三个 blob id，客户端或 Agent 可以据此解决后重新提交。

use bytes::Bytes;
use scion_object::{EntryMode, FileChange, FileOp, ObjectId, TreeEntry};
use scion_store::BlobStore;
use scion_tree::{BuildResult, TreeError, TreeSource, apply_delta, lookup};
use thiserror::Error;

/// 冲突类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictKind {
    /// 文本内容的改动重叠。
    Content,
    /// 双方新增了同一路径，内容不同。
    AddAdd,
    /// 本方修改，对方删除。
    ModifyDelete,
    /// 本方删除，对方修改。
    DeleteModify,
    /// 双方都改了二进制文件。
    Binary,
    /// 双方都改了文件，但文件超过合并大小上限。
    TooLarge,
    /// 涉及符号链接的改动重叠。
    Symlink,
    /// 文件与目录冲突（例如对方把上级目录改成了文件）。
    Path,
}

impl ConflictKind {
    /// 协议与日志中使用的名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::AddAdd => "add_add",
            Self::ModifyDelete => "modify_delete",
            Self::DeleteModify => "delete_modify",
            Self::Binary => "binary",
            Self::TooLarge => "too_large",
            Self::Symlink => "symlink",
            Self::Path => "path",
        }
    }
}

/// 一个冲突路径。blob id 为 `None` 表示该方没有这个文件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    pub kind: ConflictKind,
    pub base: Option<ObjectId>,
    pub ours: Option<ObjectId>,
    pub theirs: Option<ObjectId>,
}

/// 成功的 rebase。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rebased {
    /// 应用到 `onto` 上的新 delta（去掉了 `onto` 已包含的改动，合并过的文件指向新 blob）。
    pub delta: Vec<FileChange>,
    /// 三方合并产生的新 blob，调用方需写入存储并登记到仓库索引。
    pub merged_blobs: Vec<(ObjectId, Bytes)>,
    /// 新 delta 应用到 `onto` 后的目录树。
    pub built: BuildResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Clean(Rebased),
    Conflicts(Vec<Conflict>),
}

/// 无法完成 rebase（与冲突不同，这些错误与改动内容无关）。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MergeError {
    #[error(transparent)]
    Tree(TreeError),
    #[error("blob {0} is missing")]
    MissingBlob(ObjectId),
    #[error("storage error: {0}")]
    Storage(String),
    /// 参与合并的 blob 已被 redact：不能读取，也不能把它的内容合并进新提交。
    #[error("blob {id} is redacted ({redaction_id})")]
    Redacted { id: ObjectId, redaction_id: String },
}

pub type Result<T> = std::result::Result<T, MergeError>;

/// 合并参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// 参与三方合并的单个文件的大小上限（三方中任意一个超过即报 [`ConflictKind::TooLarge`]）。
    pub max_merge_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_merge_bytes: 8 << 20,
        }
    }
}

/// 把 `delta`（相对 `base`）重放到 `onto` 上。`None` 表示空树。
pub async fn rebase_delta(
    trees: &dyn TreeSource,
    blobs: &dyn BlobStore,
    base: Option<ObjectId>,
    onto: Option<ObjectId>,
    delta: &[FileChange],
    limits: Limits,
) -> Result<Outcome> {
    let mut out = Vec::with_capacity(delta.len());
    let mut merged_blobs = Vec::new();
    let mut conflicts = Vec::new();
    if base == onto {
        out = delta.to_vec();
    } else {
        for change in delta {
            let b = entry_at(trees, base, &change.path).await?;
            let t = entry_at(trees, onto, &change.path).await?;
            if same(b.as_ref(), t.as_ref()) {
                out.push(change.clone());
                continue;
            }
            match merge_path(blobs, change, b, t, limits).await? {
                Resolved::Keep(c) => out.push(c),
                Resolved::Drop => {}
                Resolved::Merged(c, blob) => {
                    out.push(c);
                    merged_blobs.push(blob);
                }
                Resolved::Conflict(c) => conflicts.push(c),
            }
        }
    }
    if !conflicts.is_empty() {
        return Ok(Outcome::Conflicts(conflicts));
    }
    match apply_delta(trees, onto, &out).await {
        Ok(built) => Ok(Outcome::Clean(Rebased {
            delta: out,
            merged_blobs,
            built,
        })),
        // 结构性冲突：例如对方把上级目录换成了文件
        Err(TreeError::PathConflict(path) | TreeError::DeleteMissing(path) | TreeError::DeleteDirectory(path)) => {
            let ours = out.iter().find(|c| c.path == path).and_then(|c| put_id(&c.op));
            Ok(Outcome::Conflicts(vec![Conflict {
                path,
                kind: ConflictKind::Path,
                base: None,
                ours,
                theirs: None,
            }]))
        }
        Err(e) => Err(MergeError::Tree(e)),
    }
}

enum Resolved {
    /// 原样保留（或改写后的）改动。
    Keep(FileChange),
    /// `onto` 已经包含这条改动。
    Drop,
    /// 三方合并得到的新内容。
    Merged(FileChange, (ObjectId, Bytes)),
    Conflict(Conflict),
}

/// 双方都动过这个路径（`b != t`）时的处理。
async fn merge_path(
    blobs: &dyn BlobStore,
    change: &FileChange,
    b: Option<TreeEntry>,
    t: Option<TreeEntry>,
    limits: Limits,
) -> Result<Resolved> {
    let path = change.path.clone();
    let ids = (file_id(b.as_ref()), put_id(&change.op), file_id(t.as_ref()));
    let conflict = |kind| {
        Resolved::Conflict(Conflict {
            path: path.clone(),
            kind,
            base: ids.0,
            ours: ids.1,
            theirs: ids.2,
        })
    };
    let (mode, id, t) = match (&change.op, &t) {
        // 双方都删除
        (FileOp::Delete, None) => return Ok(Resolved::Drop),
        (FileOp::Delete, Some(_)) => return Ok(conflict(ConflictKind::DeleteModify)),
        (FileOp::Put { .. }, Some(t)) if t.mode == EntryMode::Tree => return Ok(conflict(ConflictKind::Path)),
        (FileOp::Put { .. }, None) => return Ok(conflict(ConflictKind::ModifyDelete)),
        (FileOp::Put { mode, id, .. }, Some(t)) => (*mode, *id, t.clone()),
    };
    // 双方改成了同样的结果
    if t.mode == mode && t.id == id {
        return Ok(Resolved::Drop);
    }
    let Some(b) = b else {
        return Ok(conflict(ConflictKind::AddAdd));
    };
    if [b.mode, mode, t.mode].contains(&EntryMode::Symlink) {
        return Ok(conflict(ConflictKind::Symlink));
    }
    // 只剩普通文件与可执行文件两种 mode：双方都改了 mode 时结果必然相同，不会冲突
    let merged_mode = merge3(b.mode, mode, t.mode).unwrap_or(mode);
    if let Some(merged_id) = merge3(b.id, id, t.id) {
        // 内容只有一方改了：取改动的一方，再配上合并后的 mode
        let size = if merged_id == t.id {
            t.size
        } else {
            put_size(&change.op)
        };
        return Ok(if merged_mode == t.mode && merged_id == t.id {
            Resolved::Drop
        } else {
            Resolved::Keep(FileChange::put(path, merged_mode, merged_id, size))
        });
    }
    if [b.size, put_size(&change.op), t.size]
        .iter()
        .any(|s| *s > limits.max_merge_bytes)
    {
        return Ok(conflict(ConflictKind::TooLarge));
    }
    let base_bytes = read(blobs, b.id).await?;
    let ours_bytes = read(blobs, id).await?;
    let theirs_bytes = read(blobs, t.id).await?;
    if [&base_bytes, &ours_bytes, &theirs_bytes].iter().any(|d| is_binary(d)) {
        return Ok(conflict(ConflictKind::Binary));
    }
    match diffy::merge_bytes(&base_bytes, &ours_bytes, &theirs_bytes) {
        Ok(merged) => {
            let data = Bytes::from(merged);
            let merged_id = ObjectId::of_blob(&data);
            let change = FileChange::put(path, merged_mode, merged_id, data.len() as u64);
            Ok(Resolved::Merged(change, (merged_id, data)))
        }
        Err(_) => Ok(conflict(ConflictKind::Content)),
    }
}

/// 三方合并一个值：只有一方改了取改的一方，双方改成一样取该值，否则冲突（None）。
fn merge3<T: PartialEq + Copy>(base: T, ours: T, theirs: T) -> Option<T> {
    if ours == base {
        Some(theirs)
    } else if theirs == base || ours == theirs {
        Some(ours)
    } else {
        None
    }
}

/// 与 git 相同的启发式：前 8000 字节中有 NUL 即视为二进制。
fn is_binary(data: &[u8]) -> bool {
    data.iter().take(8000).any(|b| *b == 0)
}

async fn entry_at(trees: &dyn TreeSource, root: Option<ObjectId>, path: &str) -> Result<Option<TreeEntry>> {
    match root {
        None => Ok(None),
        Some(r) => lookup(trees, r, path).await.map_err(MergeError::Tree),
    }
}

async fn read(blobs: &dyn BlobStore, id: ObjectId) -> Result<Bytes> {
    blobs
        .get(&id)
        .await
        .map_err(|e| match e {
            scion_store::Error::Redacted { id, redaction_id } => MergeError::Redacted { id, redaction_id },
            e => MergeError::Storage(e.to_string()),
        })?
        .ok_or(MergeError::MissingBlob(id))
}

fn same(a: Option<&TreeEntry>, b: Option<&TreeEntry>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.mode == b.mode && a.id == b.id,
        _ => false,
    }
}

fn file_id(e: Option<&TreeEntry>) -> Option<ObjectId> {
    e.filter(|e| e.mode != EntryMode::Tree).map(|e| e.id)
}

fn put_id(op: &FileOp) -> Option<ObjectId> {
    match op {
        FileOp::Put { id, .. } => Some(*id),
        FileOp::Delete => None,
    }
}

fn put_size(op: &FileOp) -> u64 {
    match op {
        FileOp::Put { size, .. } => *size,
        FileOp::Delete => 0,
    }
}

#[cfg(test)]
mod tests;
