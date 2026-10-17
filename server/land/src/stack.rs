//! 待合入提交栈的 rebase（服务端设计文档 §6.2、§6.3）。
//!
//! [`Batch`] 维护一批合入的链头：每个请求的提交栈依次接到链头之后（快进或 rebase），rebase 结果先
//! 留在内存（[`Pending`]）里，后面的请求直接在上面继续；整批确定后由调用方一次写入并更新 ref。

use std::collections::HashMap;
use std::sync::Arc;

use scion_merge::{Limits, Outcome, rebase_delta};
use scion_object::{Changeset, ObjectId};
use scion_query::{GIT_SHA1_EXTENSION, is_ancestor, load_changeset};
use scion_store::ChangesetRecord;

use crate::create::Built;
use crate::pending::Pending;
use crate::{ChangesetWriter, LandError, Result};

/// 本次合入的一个提交：`original` 是客户端提交的，`landed` 是最终进入 ref 历史的（未经 rebase 时两者相同）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landed {
    pub original: ObjectId,
    pub landed: ObjectId,
    pub change_id: [u8; 16],
}

/// 一个请求接到链头之后的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    /// ref 应当指向的新提交。`landed` 为空时就是原来的链头（要合入的提交已经在历史中）。
    pub tip: ObjectId,
    /// 本次合入的提交，从旧到新。
    pub landed: Vec<Landed>,
    /// 栈中早已合入过这个 ref 的 change（按 change id 去重，不再重复合入），从旧到新。
    pub previously_landed: Vec<Landed>,
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

/// 把 `tip` 所在的提交栈接到 `onto`（ref 当前提交，`None` 表示 ref 不存在）之后，并写入 rebase 产生的
/// 提交。不修改 ref。
///
/// 提交栈是从 `tip` 沿第一父提交往回、直到遇到 `onto` 的祖先为止的提交。栈底的父提交就是 `onto`
/// 时直接快进；否则逐个 rebase（三方合并）。栈中 change id 已经合入过 `reference` 的提交被跳过
/// （客户端超时后换了 request id 重试、或者重复提交同一个 change），不会重复合入。
pub async fn prepare(
    writer: &ChangesetWriter,
    repo: &str,
    reference: &str,
    tip: ObjectId,
    onto: Option<ObjectId>,
    opts: StackOptions,
) -> Result<Prepared> {
    let mut batch = Batch::new(writer, repo, reference, onto).await?;
    let p = batch.add(tip, opts).await?;
    writer.persist(repo, batch.built()).await?;
    Ok(p)
}

/// 一批合入的链头与尚未写入的 rebase 结果。
pub(crate) struct Batch<'a> {
    writer: &'a ChangesetWriter,
    repo: &'a str,
    reference: &'a str,
    /// 链上最近一个已在存储中的提交（ref 当前提交，或批内快进到的客户端提交），用来判断是否已包含。
    base: Option<ObjectId>,
    /// 链头。
    onto: Option<ChangesetRecord>,
    pending: Pending,
    built: Vec<Built>,
    /// 批内已接受的 change：change id → 进入历史的提交。
    in_batch: HashMap<[u8; 16], ObjectId>,
    /// 已查询过的 change：change id → 早先合入的提交（`None` 表示没有合入过）。
    landed_cache: HashMap<[u8; 16], Option<ObjectId>>,
}

impl<'a> Batch<'a> {
    pub(crate) async fn new(
        writer: &'a ChangesetWriter,
        repo: &'a str,
        reference: &'a str,
        head: Option<ObjectId>,
    ) -> Result<Self> {
        // 三方合并读取的 blob 按仓库检查 redaction：redact 过的内容不能被合并进新提交
        let pending = Pending::new(Arc::new(scion_store::RedactedBlobs::new(
            writer.blobs().clone(),
            writer.meta().clone(),
            repo,
        )));
        let onto = match head {
            Some(h) => Some(pending.record(writer.meta().as_ref(), repo, h).await?),
            None => None,
        };
        Ok(Self {
            writer,
            repo,
            reference,
            base: head,
            onto,
            pending,
            built: Vec::new(),
            in_batch: HashMap::new(),
            landed_cache: HashMap::new(),
        })
    }

    /// 当前链头。
    pub(crate) fn tip(&self) -> Option<ObjectId> {
        self.onto.as_ref().map(|r| r.id)
    }

    /// 尚未写入的 rebase 结果，按顺序。
    pub(crate) fn built(&self) -> &[Built] {
        &self.built
    }

    /// 批开始时的预取：并发读取整批请求的提交对象，并一次查出它们的 change 是否已合入过。
    /// 预取失败不影响正确性，之后按需读取。
    pub(crate) async fn prefetch(&mut self, tips: &[ObjectId]) {
        self.pending.prefetch(tips).await;
        let mut ids = Vec::with_capacity(tips.len());
        for t in tips {
            if let Ok(cs) = load_changeset(&self.pending, *t).await {
                ids.push(cs.change_id);
            }
        }
        let _ = self.lookup_landed(&ids).await;
    }

    /// 查询 change 是否已合入过 ref，结果缓存在批内。
    async fn lookup_landed(&mut self, ids: &[[u8; 16]]) -> Result<HashMap<[u8; 16], ObjectId>> {
        let unknown: Vec<[u8; 16]> = ids
            .iter()
            .filter(|id| !self.landed_cache.contains_key(*id))
            .copied()
            .collect();
        if !unknown.is_empty() {
            let found = self
                .writer
                .meta()
                .landed_changes(self.repo, self.reference, &unknown)
                .await?;
            for id in &unknown {
                self.landed_cache.insert(*id, None);
            }
            for l in found {
                self.landed_cache.insert(l.change_id, Some(l.landed));
            }
        }
        Ok(ids
            .iter()
            .filter_map(|id| self.landed_cache.get(id).copied().flatten().map(|l| (*id, l)))
            .collect())
    }

    async fn included(&self, x: ObjectId) -> Result<bool> {
        if self.pending.is_pending(&x) {
            return Ok(true);
        }
        match self.base {
            Some(b) => Ok(x == b || is_ancestor(self.writer.meta().as_ref(), self.repo, x, b).await?),
            None => Ok(false),
        }
    }

    async fn record(&self, id: ObjectId) -> Result<ChangesetRecord> {
        self.pending.record(self.writer.meta().as_ref(), self.repo, id).await
    }

    async fn landed_as_is(&self, id: ObjectId) -> Result<Landed> {
        let cs = load_changeset(&self.pending, id).await?;
        Ok(Landed {
            original: id,
            landed: id,
            change_id: cs.change_id,
        })
    }

    /// 快进或新建 ref 时本次进入 ref 的提交，从旧到新：从 `tip` 沿第一父提交往回，直到已在链中的提交
    /// （新建 ref 时直到根提交）。它们的 change id 都要记下来供之后去重，所以超过 `max` 个时拒绝合入，
    /// 而不是只记录最近的一部分；客户端应分段合入（`scn import git` 就是这样做的）。
    async fn fast_forward_stack(&self, tip: &ChangesetRecord, max: usize) -> Result<Vec<Landed>> {
        let mut landed = Vec::new();
        let mut cur = tip.clone();
        loop {
            if landed.len() == max {
                return Err(LandError::StackTooDeep(max));
            }
            landed.push(self.landed_as_is(cur.id).await?);
            match cur.first_parent() {
                Some(p) if !self.included(p).await? => cur = self.record(p).await?,
                _ => break,
            }
        }
        landed.reverse();
        Ok(landed)
    }

    fn accept(&mut self, onto: ChangesetRecord, landed: &[Landed]) {
        for l in landed {
            self.in_batch.insert(l.change_id, l.landed);
        }
        self.onto = Some(onto);
    }

    /// 把 `tip` 的提交栈接到链头之后。失败时批的状态不变，不影响批内其它请求。
    pub(crate) async fn add(&mut self, tip: ObjectId, opts: StackOptions) -> Result<Prepared> {
        let meta = self.writer.meta().as_ref();
        let tip_rec = self.record(tip).await?;
        let Some(onto) = self.onto.clone() else {
            // 新建 ref：直接指向 tip，整段历史都是本次进入 ref 的
            let landed = self.fast_forward_stack(&tip_rec, opts.max_stack).await?;
            self.base = Some(tip);
            self.accept(tip_rec, &landed);
            return Ok(Prepared {
                tip,
                landed,
                previously_landed: Vec::new(),
            });
        };
        if self.included(tip).await? {
            return Ok(Prepared {
                tip: onto.id,
                landed: Vec::new(),
                previously_landed: Vec::new(),
            });
        }

        // 快进（链头已在存储中时才可能）
        if !self.pending.is_pending(&onto.id) && is_ancestor(meta, self.repo, onto.id, tip).await? {
            let landed = self.fast_forward_stack(&tip_rec, opts.max_stack).await?;
            self.base = Some(tip);
            self.accept(tip_rec, &landed);
            return Ok(Prepared {
                tip,
                landed,
                previously_landed: Vec::new(),
            });
        }
        if opts.fast_forward_only {
            return Err(LandError::NotFastForward { tip: onto.id });
        }

        // 从 tip 往回找栈底：第一个已包含在链中的父提交
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
            if self.included(parent).await? {
                break;
            }
            rec = self.record(parent).await?;
        }
        stack.reverse();

        let mut changesets = Vec::with_capacity(stack.len());
        for r in &stack {
            changesets.push(load_changeset(&self.pending, r.id).await?);
        }
        let ids: Vec<[u8; 16]> = changesets.iter().map(|c| c.change_id).collect();
        let mut already = self.lookup_landed(&ids).await?;
        for id in &ids {
            if let Some(l) = self.in_batch.get(id) {
                already.entry(*id).or_insert(*l);
            }
        }

        let mut parent = onto;
        let mut landed = Vec::with_capacity(stack.len());
        let mut previously_landed = Vec::new();
        let mut built = Vec::new();
        for (r, cs) in stack.into_iter().zip(changesets) {
            if let Some(earlier) = already.get(&cs.change_id) {
                previously_landed.push(Landed {
                    original: r.id,
                    landed: *earlier,
                    change_id: cs.change_id,
                });
                continue;
            }
            let base_root = self.record(cs.parents[0]).await?.root_tree;
            let outcome = rebase_delta(
                &self.pending,
                &self.pending,
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
                rebased.delta,
                extensions,
            )
            .map_err(|e| LandError::Corrupt(format!("rebased changeset of {}: {e}", r.id)))?;
            let b = self
                .writer
                .build(self.repo, &self.pending, &new_cs, rebased.built, rebased.merged_blobs)
                .await?;
            // 后面的提交（本栈或批内后续请求）要读到这些对象和记录
            for (id, data) in &b.objects {
                self.pending.add_object(*id, data.clone());
            }
            self.pending.add_record(b.record.clone());
            landed.push(Landed {
                original: r.id,
                landed: b.record.id,
                change_id: cs.change_id,
            });
            parent = b.record.clone();
            built.push(b);
        }
        let tip = parent.id;
        self.built.extend(built);
        self.accept(parent, &landed);
        Ok(Prepared {
            tip,
            landed,
            previously_landed,
        })
    }
}
