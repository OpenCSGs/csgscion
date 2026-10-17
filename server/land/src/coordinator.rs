//! LandCoordinator（服务端设计文档 §6.2）：每个 ref 一个任务，把并发的合入请求攒成批，
//! 顺序 rebase 后用一次条件更新提交整批。
//!
//! 热点 ref 上，逐个请求做条件更新会引发重试风暴（每次重试都要重新 rebase）。批处理后每批只有
//! 一次数据库往返，吞吐由「批大小 / 一次提交的耗时」决定，而不是「1 / 一次提交的耗时」。
//!
//! - 批内顺序处理：后面的请求 rebase 到前面已接受的结果上；某个请求冲突只让它自己失败。
//! - 整批原子提交：ref、landed 行与每个请求的幂等记录在同一个事务里写入。
//! - 条件更新失败（其它节点或外部写入移动了 ref）时重新读 ref，整批重做。
//! - ref 空闲一段时间后任务退出，下次请求时重新创建。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scion_object::ObjectId;
use scion_store::{IdempotencyRecord, LandedChange, RefExpect, RefState};
use tokio::sync::{mpsc, oneshot};

use crate::land::now_ms;
use crate::stack::{Batch, StackOptions};
use crate::{
    ChangesetWriter, LandError, LandOutcome, LandRequest, Landed, Prepared, Result, record, validate_ref_name,
};

/// 批处理参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    /// 一批最多的请求数。
    pub max_batch: usize,
    /// 收到第一个请求后最多再等多久凑批。
    pub max_wait: Duration,
    /// 每个 ref 排队请求数上限；满了返回 [`LandError::Overloaded`]。
    pub queue_capacity: usize,
    /// ref 空闲多久后任务退出。
    pub idle_timeout: Duration,
    /// 整批条件更新失败后的最多尝试次数。
    pub max_attempts: u32,
    pub stack: StackOptions,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            max_batch: 64,
            max_wait: Duration::from_millis(5),
            queue_capacity: 1024,
            idle_timeout: Duration::from_secs(30),
            max_attempts: 16,
            stack: StackOptions::default(),
        }
    }
}

struct Job {
    reference: String,
    tip: ObjectId,
    fast_forward_only: bool,
    request_id: Option<String>,
    fingerprint: Vec<u8>,
    reply: oneshot::Sender<Result<LandOutcome>>,
}

type Key = (String, String);

struct Shared {
    writer: Arc<ChangesetWriter>,
    config: CoordinatorConfig,
    queues: Mutex<HashMap<Key, mpsc::Sender<Job>>>,
}

/// 每个 (仓库, ref) 一个批处理任务。可以在多个请求之间共享（内部是 `Arc`）。
#[derive(Clone)]
pub struct Coordinator {
    shared: Arc<Shared>,
}

impl Coordinator {
    pub fn new(writer: Arc<ChangesetWriter>, config: CoordinatorConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                writer,
                config,
                queues: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn writer(&self) -> &Arc<ChangesetWriter> {
        &self.shared.writer
    }

    /// 当前活跃的 ref 任务数。
    pub fn active_refs(&self) -> usize {
        self.shared.queues.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// 合入一个请求，语义与 [`crate::Lander::land`] 相同。
    pub async fn land(&self, req: LandRequest<'_>) -> Result<LandOutcome> {
        validate_ref_name(req.reference)?;
        let meta = self.shared.writer.meta().as_ref();
        if !meta.repo_exists(req.repo).await? {
            return Err(LandError::RepoNotFound(req.repo.to_owned()));
        }
        let fingerprint = record::fingerprint(req.reference, &req.tip, req.fast_forward_only);
        // 快速路径：重试的请求不用排队
        if let Some(id) = req.request_id {
            if let Some(done) = replay(&self.shared.writer, req.repo, id, &fingerprint).await? {
                return Ok(done);
            }
        }
        let (tx, rx) = oneshot::channel();
        let job = Job {
            reference: req.reference.to_owned(),
            tip: req.tip,
            fast_forward_only: req.fast_forward_only,
            request_id: req.request_id.map(str::to_owned),
            fingerprint,
            reply: tx,
        };
        self.enqueue((req.repo.to_owned(), req.reference.to_owned()), job)?;
        rx.await
            .map_err(|_| LandError::Unavailable("land coordinator stopped".into()))?
    }

    /// 查询请求结果，语义与 [`crate::Lander::status`] 相同。
    pub async fn status(&self, repo: &str, request_id: &str) -> Result<Option<LandOutcome>> {
        match self.shared.writer.meta().get_idempotency(repo, request_id).await? {
            Some(r) => Ok(Some(record::decode(&r.response)?)),
            None => Ok(None),
        }
    }

    fn enqueue(&self, key: Key, job: Job) -> Result<()> {
        let mut queues = self
            .shared
            .queues
            .lock()
            .map_err(|_| LandError::Unavailable("land coordinator poisoned".into()))?;
        let mut job = job;
        // 已有任务但刚退出（接收端已关闭）时重建一次
        for _ in 0..2 {
            let tx = queues.entry(key.clone()).or_insert_with(|| {
                let (tx, rx) = mpsc::channel(self.shared.config.queue_capacity);
                tokio::spawn(run(self.shared.clone(), key.clone(), rx));
                tx
            });
            match tx.try_send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => return Err(LandError::Overloaded),
                Err(mpsc::error::TrySendError::Closed(j)) => {
                    queues.remove(&key);
                    job = j;
                }
            }
        }
        Err(LandError::Unavailable("land coordinator unavailable".into()))
    }
}

async fn replay(
    writer: &ChangesetWriter,
    repo: &str,
    request_id: &str,
    fingerprint: &[u8],
) -> Result<Option<LandOutcome>> {
    match writer.meta().get_idempotency(repo, request_id).await? {
        Some(r) if r.fingerprint == fingerprint => Ok(Some(record::decode(&r.response)?)),
        Some(_) => Err(LandError::RequestIdReused(request_id.to_owned())),
        None => Ok(None),
    }
}

/// 一个 ref 的批处理循环。
async fn run(shared: Arc<Shared>, key: Key, mut rx: mpsc::Receiver<Job>) {
    let cfg = shared.config;
    loop {
        let first = match tokio::time::timeout(cfg.idle_timeout, rx.recv()).await {
            Ok(Some(job)) => job,
            Ok(None) => return,
            Err(_) => {
                // 空闲：在持锁状态下确认没有新请求后退出，避免与 enqueue 竞争
                let Ok(mut queues) = shared.queues.lock() else { return };
                match rx.try_recv() {
                    Ok(job) => {
                        drop(queues);
                        process(&shared, &key.0, vec![job]).await;
                        continue;
                    }
                    Err(_) => {
                        queues.remove(&key);
                        return;
                    }
                }
            }
        };
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + cfg.max_wait;
        while batch.len() < cfg.max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(job)) => batch.push(job),
                Ok(None) | Err(_) => break,
            }
        }
        process(&shared, &key.0, batch).await;
    }
}

/// 一个请求在批中的处理结果。
enum Planned {
    /// 已经在历史中（或全部 change 早已合入），提交后回复当前 ref。
    Included { previously_landed: Vec<Landed> },
    /// 本批合入。
    Accepted(Prepared),
}

/// 处理一批（同一个仓库、同一个 ref）。
async fn process(shared: &Shared, repo: &str, batch: Vec<Job>) {
    let cfg = shared.config;
    let writer = shared.writer.as_ref();
    let meta = writer.meta().as_ref();
    let reference = batch[0].reference.clone();
    let mut pending = batch;
    for attempt in 0..cfg.max_attempts {
        let head = match meta.get_ref(repo, &reference).await {
            Ok(h) => h,
            Err(e) => return fail_all(pending, e.into()),
        };
        let mut batch = match Batch::new(writer, repo, &reference, head.map(|h| h.target)).await {
            Ok(b) => b,
            Err(e) => return fail_all(pending, e),
        };
        // 并发读取整批请求的提交对象，之后的 rebase 不再逐个等待对象存储
        let tips: Vec<ObjectId> = pending.iter().map(|j| j.tip).collect();
        batch.prefetch(&tips).await;
        let mut by_request: HashMap<String, usize> = HashMap::new();
        let mut planned: Vec<(Job, Planned)> = Vec::with_capacity(pending.len());
        for job in pending {
            // 同一批里的重复请求（相同 request id）：与第一个共用结果
            if let Some(id) = &job.request_id {
                if let Some(&i) = by_request.get(id) {
                    if planned[i].0.fingerprint != job.fingerprint {
                        let _ = job.reply.send(Err(LandError::RequestIdReused(id.clone())));
                    } else {
                        let dup = Planned::Included {
                            previously_landed: Vec::new(),
                        };
                        planned.push((job, dup));
                    }
                    continue;
                }
            }
            let opts = StackOptions {
                fast_forward_only: job.fast_forward_only,
                ..cfg.stack
            };
            match batch.add(job.tip, opts).await {
                Ok(p) if p.landed.is_empty() => {
                    let previously_landed = p.previously_landed;
                    planned.push((job, Planned::Included { previously_landed }));
                }
                Ok(p) => {
                    if let Some(id) = &job.request_id {
                        by_request.insert(id.clone(), planned.len());
                    }
                    planned.push((job, Planned::Accepted(p)));
                }
                Err(e) => {
                    let _ = job.reply.send(Err(e));
                }
            }
        }
        let onto = batch.tip();

        let accepted = planned.iter().any(|(_, p)| matches!(p, Planned::Accepted(_)));
        let (Some(target), true) = (onto, accepted) else {
            // 没有需要写入的请求
            if let Some(h) = head {
                reply_all(planned, h);
            }
            return;
        };
        let (expect, version) = match head {
            Some(h) => (RefExpect::Version(h.version), h.version.saturating_add(1)),
            None => (RefExpect::Absent, 1),
        };
        let state = RefState { target, version };
        let mut changes = Vec::new();
        let mut requests = Vec::new();
        for (job, p) in &planned {
            let Planned::Accepted(p) = p else { continue };
            changes.extend(p.landed.iter().map(|l| LandedChange {
                change_id: l.change_id,
                changeset: l.original,
                landed: l.landed,
                ref_version: version,
            }));
            if let Some(id) = &job.request_id {
                requests.push(IdempotencyRecord {
                    request_id: id.clone(),
                    fingerprint: job.fingerprint.clone(),
                    response: record::encode(&outcome(state, p)),
                    created_at_ms: now_ms(),
                });
            }
        }
        if let Err(e) = writer.persist(repo, batch.built()).await {
            return fail_all(planned.into_iter().map(|(j, _)| j).collect(), e);
        }
        match meta
            .land_commit(repo, &reference, expect, target, &changes, &requests)
            .await
        {
            Ok(state) => {
                tracing::debug!(repo, reference = %reference, batch = planned.len(), version = state.version, "batch landed");
                reply_all(planned, state);
                return;
            }
            Err(scion_store::Error::RefVersionMismatch { .. }) => {
                tracing::debug!(repo, reference = %reference, attempt, "ref moved under the coordinator; redoing batch");
                pending = planned.into_iter().map(|(j, _)| j).collect();
                tokio::time::sleep(crate::land::backoff(attempt)).await;
            }
            Err(e) => {
                return fail_all(planned.into_iter().map(|(j, _)| j).collect(), e.into());
            }
        }
    }
    fail_all(pending, LandError::Contention);
}

fn outcome(state: RefState, p: &Prepared) -> LandOutcome {
    LandOutcome {
        state,
        landed: p.landed.clone(),
        previously_landed: p.previously_landed.clone(),
    }
}

/// 批提交成功（或无需提交）后逐个回复。重复请求取同一批中第一个同 id 请求的结果。
fn reply_all(planned: Vec<(Job, Planned)>, state: RefState) {
    let mut results: HashMap<String, LandOutcome> = HashMap::new();
    for (job, p) in planned {
        let out = match p {
            Planned::Accepted(p) => outcome(state, &p),
            Planned::Included { previously_landed } => match job.request_id.as_ref().and_then(|id| results.get(id)) {
                Some(first) => first.clone(),
                None => LandOutcome {
                    state,
                    landed: Vec::new(),
                    previously_landed,
                },
            },
        };
        if let Some(id) = &job.request_id {
            results.entry(id.clone()).or_insert_with(|| out.clone());
        }
        let _ = job.reply.send(Ok(out));
    }
}

fn fail_all(jobs: Vec<Job>, err: LandError) {
    for job in jobs {
        let _ = job.reply.send(Err(err.clone()));
    }
}
