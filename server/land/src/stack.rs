//! 待合入提交栈的 rebase（服务端设计文档 §6.2、§6.3）。

use scion_merge::{Limits, Outcome, rebase_delta};
use scion_object::{Changeset, ObjectId};
use scion_query::{GIT_SHA1_EXTENSION, is_ancestor, load_changeset};
use scion_store::ChangesetRecord;
use scion_tree::BlobTreeSource;

use crate::{ChangesetWriter, LandError, Result};

/// 本次合入的一个提交：`original` 是客户端提交的，`landed` 是最终进入 ref 历史的（未经 rebase 时两者相同）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landed {
    pub original: ObjectId,
    pub landed: ObjectId,
    pub change_id: [u8; 16],
}

/// [`prepare`] 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    /// ref 应当指向的新提交。`landed` 为空时就是 `onto`（要合入的提交已经在 ref 的历史中）。
    pub tip: ObjectId,
    /// 本次合入的提交，从旧到新。
    pub landed: Vec<Landed>,
}

/// 合入选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StackOptions {
    /// 只允许快进。
    pub fast_forward_only: bool,
    /// 一次最多 rebase 的提交数。
    pub max_stack: usize,
    pub merge: Limits,
}

impl Default for StackOptions {
    fn default() -> Self {
        Self {
            fast_forward_only: false,
            max_stack: 256,
            merge: Limits::default(),
        }
    }
}

/// 把 `tip` 所在的提交栈接到 `onto`（ref 当前提交，`None` 表示 ref 不存在）之后。
///
/// 提交栈是从 `tip` 沿第一父提交往回、直到遇到 `onto` 的祖先为止的提交。栈底的父提交就是 `onto`
/// 时直接快进；否则逐个 rebase（三方合并），产生新的提交。不修改 ref。
pub async fn prepare(
    writer: &ChangesetWriter,
    repo: &str,
    tip: ObjectId,
    onto: Option<ObjectId>,
    opts: StackOptions,
) -> Result<Prepared> {
    let meta = writer.meta().as_ref();
    let tip_rec = record(writer, repo, tip).await?;
    let Some(onto) = onto else {
        // 新建 ref：直接指向 tip
        return Ok(Prepared {
            tip,
            landed: vec![landed_as_is(writer, tip_rec.id).await?],
        });
    };
    if tip == onto || is_ancestor(meta, repo, tip, onto).await? {
        return Ok(Prepared {
            tip: onto,
            landed: Vec::new(),
        });
    }

    // 快进：不需要 rebase，往回记录本次合入的提交（最多 max_stack 个，用于按 change id 去重）
    if is_ancestor(meta, repo, onto, tip).await? {
        let mut landed = Vec::new();
        let mut cur = tip_rec;
        while landed.len() < opts.max_stack {
            landed.push(landed_as_is(writer, cur.id).await?);
            match cur.first_parent() {
                Some(p) if p != onto && !is_ancestor(meta, repo, p, onto).await? => {
                    cur = record(writer, repo, p).await?;
                }
                _ => break,
            }
        }
        landed.reverse();
        return Ok(Prepared { tip, landed });
    }
    if opts.fast_forward_only {
        return Err(LandError::NotFastForward { tip: onto });
    }

    // 从 tip 往回找栈底：第一个是 onto 祖先的父提交
    let mut stack: Vec<ChangesetRecord> = Vec::new();
    let mut rec = tip_rec;
    loop {
        if stack.len() == opts.max_stack {
            return Err(LandError::StackTooDeep(opts.max_stack));
        }
        let parent = match rec.parents.as_slice() {
            [] => return Err(LandError::UnrelatedHistory { tip }),
            [p] => *p,
            _ => return Err(LandError::MergeInStack(rec.id)),
        };
        stack.push(rec);
        if is_ancestor(meta, repo, parent, onto).await? {
            break;
        }
        rec = record(writer, repo, parent).await?;
    }
    stack.reverse();

    let trees = BlobTreeSource(writer.blobs().clone());
    let mut parent = record(writer, repo, onto).await?;
    let mut landed = Vec::with_capacity(stack.len());
    for r in stack {
        let cs = load_changeset(writer.blobs().as_ref(), r.id).await?;
        let base_root = record(writer, repo, cs.parents[0]).await?.root_tree;
        let outcome = rebase_delta(
            &trees,
            writer.blobs().as_ref(),
            Some(base_root),
            Some(parent.root_tree),
            &cs.delta,
            opts.merge,
        )
        .await?;
        let rebased = match outcome {
            Outcome::Clean(r) => r,
            Outcome::Conflicts(conflicts) => {
                return Err(LandError::Conflict {
                    changeset: r.id,
                    conflicts,
                });
            }
        };
        // 内容已不同于导入时的 git 提交，不再携带 git sha
        let mut extensions = cs.extensions.clone();
        extensions.remove(GIT_SHA1_EXTENSION);
        let new_cs = Changeset::new(
            vec![parent.id],
            cs.author.clone(),
            cs.committer.clone(),
            cs.message.clone(),
            cs.change_id,
            rebased.delta.clone(),
            extensions,
        )
        .map_err(|e| LandError::Corrupt(format!("rebased changeset of {}: {e}", r.id)))?;
        let new_rec = writer.create_rebased(repo, &new_cs, rebased).await?;
        landed.push(Landed {
            original: r.id,
            landed: new_rec.id,
            change_id: cs.change_id,
        });
        parent = new_rec;
    }
    Ok(Prepared { tip: parent.id, landed })
}

async fn record(writer: &ChangesetWriter, repo: &str, id: ObjectId) -> Result<ChangesetRecord> {
    writer
        .meta()
        .get_changeset(repo, &id)
        .await?
        .ok_or_else(|| LandError::ObjectsNotUploaded(vec![id]))
}

async fn landed_as_is(writer: &ChangesetWriter, id: ObjectId) -> Result<Landed> {
    let cs = load_changeset(writer.blobs().as_ref(), id).await?;
    Ok(Landed {
        original: id,
        landed: id,
        change_id: cs.change_id,
    })
}
