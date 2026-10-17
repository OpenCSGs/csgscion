//! 单个合入请求：读 ref → rebase 提交栈 → 条件更新 ref，条件不满足时重试。

use std::sync::Arc;
use std::time::Duration;

use scion_object::ObjectId;
use scion_store::{RefExpect, RefState};

use crate::stack::StackOptions;
use crate::{ChangesetWriter, LandError, Landed, Result, prepare, validate_ref_name};

/// 合入结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandOutcome {
    /// 合入后的 ref。
    pub state: RefState,
    /// 本次合入的提交（从旧到新）；为空表示要合入的提交早已在 ref 的历史中。
    pub landed: Vec<Landed>,
}

/// 直接用条件更新合入（服务端设计文档 §6.2 中冷 ref 的路径）。
pub struct Lander {
    writer: Arc<ChangesetWriter>,
    opts: StackOptions,
    max_attempts: u32,
}

impl Lander {
    pub fn new(writer: Arc<ChangesetWriter>) -> Self {
        Self {
            writer,
            opts: StackOptions::default(),
            max_attempts: 16,
        }
    }

    /// 调整 rebase 参数（`fast_forward_only` 由每个请求决定，这里的值被忽略）。
    pub fn with_options(mut self, opts: StackOptions) -> Self {
        self.opts = opts;
        self
    }

    /// 条件更新失败后的最多尝试次数。
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    pub fn writer(&self) -> &Arc<ChangesetWriter> {
        &self.writer
    }

    /// 把 `tip` 合入 `reference`。ref 不存在时创建；`tip` 不是 ref 当前提交的后继时在服务端 rebase
    /// （`fast_forward_only` 时报 [`LandError::NotFastForward`]）。
    pub async fn land(
        &self,
        repo: &str,
        reference: &str,
        tip: ObjectId,
        fast_forward_only: bool,
    ) -> Result<LandOutcome> {
        validate_ref_name(reference)?;
        let meta = self.writer.meta().as_ref();
        if !meta.repo_exists(repo).await? {
            return Err(LandError::RepoNotFound(repo.to_owned()));
        }
        let opts = StackOptions {
            fast_forward_only,
            ..self.opts
        };
        for attempt in 0..self.max_attempts {
            let head = meta.get_ref(repo, reference).await?;
            let prepared = prepare(&self.writer, repo, tip, head.as_ref().map(|h| h.target), opts).await?;
            let expect = match &head {
                // 已经在历史中：重试或重复请求，直接返回当前状态
                Some(h) if prepared.landed.is_empty() => {
                    return Ok(LandOutcome {
                        state: *h,
                        landed: Vec::new(),
                    });
                }
                Some(h) => RefExpect::Version(h.version),
                None => RefExpect::Absent,
            };
            match meta.cas_ref(repo, reference, expect, prepared.tip).await {
                Ok(state) => {
                    return Ok(LandOutcome {
                        state,
                        landed: prepared.landed,
                    });
                }
                Err(scion_store::Error::RefVersionMismatch { .. }) => {
                    tracing::debug!(repo, reference, attempt, "ref moved during land; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(LandError::Contention)
    }
}

/// 抖动退避：上限随尝试次数翻倍（1、2、4… 毫秒，最多 64 毫秒），在 [上限/2, 上限] 中取值。
fn backoff(attempt: u32) -> Duration {
    let cap_us = 1000u64 << attempt.min(6);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    Duration::from_micros(cap_us / 2 + nanos % (cap_us / 2 + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_bounded() {
        for attempt in 0..20 {
            let cap = Duration::from_micros(1000u64 << attempt.min(6));
            let d = backoff(attempt);
            assert!(d >= cap / 2 && d <= cap, "attempt {attempt}: {d:?}");
        }
    }
}
