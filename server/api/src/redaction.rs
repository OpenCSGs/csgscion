//! redaction 的管理操作（服务端设计文档 §9），AdminService、`scion admin redact` 与 janitor 共用。
//!
//! - **redact**：在仓库的对象索引旁记下 redaction，对象 id 与历史不变，读取返回 REDACTED。可撤销。
//! - **purge**：安排在宽限期后物理删除对象字节。对象字节全局只存一份，只有对象索引中有它的每个
//!   仓库都已 redact 时 janitor 才会删除；否则保持待删除并记录日志。删除后不能撤销。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use scion_object::{EntryMode, ObjectId};
use scion_store::{BlobStore, MetaStore, Redaction, RedactionFilter, redactable};
use scion_tree::BlobTreeSource;
use tonic::Status;

use crate::service::support::{check_repo, resolve_revision};
use crate::status;

/// janitor 每轮最多处理的待删除记录数。
const PURGE_BATCH: usize = 1_000;

/// 一轮清除的结果。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PurgeReport {
    /// 已物理删除的对象。
    pub deleted: Vec<ObjectId>,
    /// 因为还有仓库没有 redact 而保留的对象，及这些仓库。
    pub blocked: Vec<(ObjectId, Vec<String>)>,
}

pub struct Redactor {
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
}

/// 新的 redaction 标识：`rd_` 加 16 位十六进制。
fn new_redaction_id(repo: &str, ids: &[ObjectId]) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut seed = format!(
        "{now}:{}:{}:{repo}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
    .into_bytes();
    for id in ids {
        seed.extend_from_slice(&id.to_bytes());
    }
    let digest = ObjectId::of_blob(&seed);
    let hex: String = digest.digest()[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("rd_{hex}")
}

impl Redactor {
    pub fn new(meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self { meta, blobs }
    }

    /// 把 `REV:PATH` 解析为文件的 blob id。
    async fn resolve_path(&self, repo: &str, spec: &str) -> Result<ObjectId, Status> {
        let Some((rev, path)) = spec.split_once(':') else {
            return Err(Status::invalid_argument(format!("{spec:?} is not REV:PATH")));
        };
        let path = path.trim_matches('/');
        if rev.is_empty() || path.is_empty() {
            return Err(Status::invalid_argument(format!("{spec:?} is not REV:PATH")));
        }
        let rec = resolve_revision(self.meta.as_ref(), repo, rev).await?;
        let entry = scion_tree::lookup(&BlobTreeSource(self.blobs.clone()), rec.root_tree, path)
            .await
            .map_err(status::from_tree)?
            .ok_or_else(|| status::path_not_found(path))?;
        if entry.mode == EntryMode::Tree {
            return Err(Status::invalid_argument(format!(
                "{path:?} is a directory; redact its files individually"
            )));
        }
        Ok(entry.id)
    }

    /// redact `repo` 中的对象（`ids` 与 `paths` 解析出的 blob）。`all_repos` 时同时 redact 对象索引中
    /// 有这些对象的所有仓库。已经 redact 的保持原样。返回 redaction 标识与新建的记录。
    #[allow(clippy::too_many_arguments)]
    pub async fn redact(
        &self,
        repo: &str,
        ids: &[ObjectId],
        paths: &[String],
        reason: &str,
        created_by: &str,
        all_repos: bool,
        now_ms: i64,
    ) -> Result<(String, Vec<Redaction>), Status> {
        check_repo(self.meta.as_ref(), repo).await?;
        if reason.trim().is_empty() {
            return Err(Status::invalid_argument("a reason is required"));
        }
        let mut targets: Vec<ObjectId> = ids.to_vec();
        for p in paths {
            targets.push(self.resolve_path(repo, p).await?);
        }
        targets.sort_unstable_by_key(|id| id.to_bytes());
        targets.dedup();
        if targets.is_empty() {
            return Err(Status::invalid_argument(
                "nothing to redact: give object ids or REV:PATH",
            ));
        }
        if let Some(id) = targets.iter().find(|id| !redactable(id)) {
            return Err(Status::invalid_argument(format!(
                "{id} is not a blob; only file contents can be redacted"
            )));
        }
        let missing = self
            .meta
            .missing_objects(repo, &targets)
            .await
            .map_err(status::from_store)?;
        if !missing.is_empty() {
            return Err(status::object_missing(&missing));
        }

        let mut per_repo: BTreeMap<String, Vec<ObjectId>> = BTreeMap::new();
        for id in &targets {
            let repos = if all_repos {
                self.meta.object_repos(id).await.map_err(status::from_store)?
            } else {
                vec![repo.to_owned()]
            };
            for r in repos {
                per_repo.entry(r).or_default().push(*id);
            }
        }
        let redaction_id = new_redaction_id(repo, &targets);
        let mut new = Vec::new();
        for (r, ids) in per_repo {
            let existing: BTreeSet<ObjectId> = self
                .meta
                .get_redactions(&r, &ids)
                .await
                .map_err(status::from_store)?
                .into_iter()
                .filter(|x| x.active)
                .map(|x| x.id)
                .collect();
            new.extend(ids.into_iter().filter(|id| !existing.contains(id)).map(|id| Redaction {
                repo: r.clone(),
                id,
                redaction_id: redaction_id.clone(),
                reason: reason.trim().to_owned(),
                created_by: created_by.to_owned(),
                created_at_ms: now_ms,
                active: true,
                purge_after_ms: None,
                purged_at_ms: None,
                seq: 0,
            }));
        }
        self.meta.put_redactions(&new).await.map_err(status::from_store)?;
        tracing::warn!(%redaction_id, repo, objects = new.len(), created_by, reason, "objects redacted");
        let stored = self.list(None, Some(&redaction_id)).await?;
        Ok((redaction_id, stored))
    }

    /// 一次 redaction 的全部记录；不存在时返回 NOT_FOUND。
    pub async fn records(&self, redaction_id: &str) -> Result<Vec<Redaction>, Status> {
        let v = self.list(None, Some(redaction_id)).await?;
        if v.is_empty() {
            return Err(Status::not_found(format!("redaction {redaction_id:?} not found")));
        }
        Ok(v)
    }

    /// 撤销一次 redaction。对象字节已经删除的不能撤销。返回撤销的记录数。
    pub async fn unredact(&self, redaction_id: &str) -> Result<usize, Status> {
        let recs = self.records(redaction_id).await?;
        if recs.iter().any(|r| r.purged_at_ms.is_some()) {
            return Err(Status::failed_precondition(format!(
                "redaction {redaction_id:?} has already been purged; the object bytes are gone"
            )));
        }
        let undo: Vec<Redaction> = recs
            .into_iter()
            .filter(|r| r.active)
            .map(|r| Redaction {
                active: false,
                purge_after_ms: None,
                ..r
            })
            .collect();
        self.meta.put_redactions(&undo).await.map_err(status::from_store)?;
        tracing::warn!(%redaction_id, objects = undo.len(), "redaction withdrawn");
        Ok(undo.len())
    }

    /// 安排在 `now_ms + grace_ms` 之后物理删除。返回安排的记录数与删除时间。
    pub async fn purge(&self, redaction_id: &str, grace_ms: i64, now_ms: i64) -> Result<(usize, i64), Status> {
        let recs = self.records(redaction_id).await?;
        let after = now_ms.saturating_add(grace_ms.max(0));
        let due: Vec<Redaction> = recs
            .into_iter()
            .filter(|r| r.active && r.purged_at_ms.is_none())
            .map(|r| Redaction {
                purge_after_ms: Some(after),
                ..r
            })
            .collect();
        if due.is_empty() {
            return Err(Status::failed_precondition(format!(
                "redaction {redaction_id:?} has nothing left to purge (withdrawn or already purged)"
            )));
        }
        self.meta.put_redactions(&due).await.map_err(status::from_store)?;
        tracing::warn!(%redaction_id, objects = due.len(), purge_after_ms = after, "purge scheduled");
        Ok((due.len(), after))
    }

    pub async fn list(&self, repo: Option<&str>, redaction_id: Option<&str>) -> Result<Vec<Redaction>, Status> {
        self.meta
            .list_redactions(&RedactionFilter {
                repo: repo.map(str::to_owned),
                redaction_id: redaction_id.map(str::to_owned),
                ..RedactionFilter::default()
            })
            .await
            .map_err(status::from_store)
    }

    /// 执行到期的清除（janitor 定期调用）。
    pub async fn run_purges(&self, now_ms: i64) -> Result<PurgeReport, Status> {
        let due = self
            .meta
            .list_redactions(&RedactionFilter {
                purge_due_at: Some(now_ms),
                limit: PURGE_BATCH,
                ..RedactionFilter::default()
            })
            .await
            .map_err(status::from_store)?;
        let ids: BTreeSet<ObjectId> = due.iter().map(|r| r.id).collect();
        let mut report = PurgeReport::default();
        for id in ids {
            // 对象索引中有它的每个仓库都必须已经 redact
            let mut redacted = Vec::new();
            let mut open = Vec::new();
            for r in self.meta.object_repos(&id).await.map_err(status::from_store)? {
                match self
                    .meta
                    .get_redactions(&r, &[id])
                    .await
                    .map_err(status::from_store)?
                    .into_iter()
                    .find(|x| x.active)
                {
                    Some(rec) => redacted.push(rec),
                    None => open.push(r),
                }
            }
            if !open.is_empty() {
                tracing::warn!(%id, repos = ?open, "purge postponed: the object is still readable in other repositories");
                report.blocked.push((id, open));
                continue;
            }
            self.blobs.delete(&id).await.map_err(status::from_store)?;
            let done: Vec<Redaction> = redacted
                .into_iter()
                .filter(|r| r.purged_at_ms.is_none())
                .map(|r| Redaction {
                    purged_at_ms: Some(now_ms),
                    purge_after_ms: r.purge_after_ms.or(Some(now_ms)),
                    ..r
                })
                .collect();
            self.meta.put_redactions(&done).await.map_err(status::from_store)?;
            tracing::warn!(%id, "redacted object purged");
            report.deleted.push(id);
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::ObjectKind;
    use scion_store::{IndexedObject, MemStore};
    use tonic::Code;

    async fn setup() -> (Arc<MemStore>, Redactor, ObjectId) {
        let store = Arc::new(MemStore::new());
        for r in ["a", "b"] {
            store.create_repo(r).await.expect("repo");
        }
        let secret = Bytes::from_static(b"password=hunter2");
        let id = ObjectId::of_blob(&secret);
        store.put(id, secret).await.expect("put");
        for r in ["a", "b"] {
            store
                .index_objects(r, &[IndexedObject { id, size: 16 }])
                .await
                .expect("index");
        }
        let red = Redactor::new(store.clone(), store.clone());
        (store, red, id)
    }

    #[tokio::test]
    async fn redact_validates_input() {
        let (_, red, id) = setup().await;
        let code = |r: Result<(String, Vec<Redaction>), Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(red.redact("nope", &[id], &[], "x", "root", false, 1).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(red.redact("a", &[id], &[], "  ", "root", false, 1).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(red.redact("a", &[], &[], "x", "root", false, 1).await),
            Err(Code::InvalidArgument)
        );
        let tree = ObjectId::hash(ObjectKind::Tree, b"t");
        assert_eq!(
            code(red.redact("a", &[tree], &[], "x", "root", false, 1).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(
                red.redact("a", &[ObjectId::of_blob(b"other")], &[], "x", "root", false, 1)
                    .await
            ),
            Err(Code::NotFound),
            "not in the repository"
        );
        for bad in ["nocolon", ":path", "main:", "main:/"] {
            assert_eq!(
                code(red.redact("a", &[], &[bad.to_string()], "x", "root", false, 1).await),
                Err(Code::InvalidArgument),
                "{bad}"
            );
        }
        assert_eq!(
            code(red.redact("a", &[], &["main:x".into()], "x", "root", false, 1).await),
            Err(Code::NotFound),
            "unknown revision"
        );
    }

    #[tokio::test]
    async fn redact_unredact_purge_lifecycle() {
        let (store, red, id) = setup().await;
        let (rid, recs) = red
            .redact("a", &[id, id], &[], " leaked ", "root", false, 10)
            .await
            .expect("redact");
        assert!(rid.starts_with("rd_") && rid.len() == 19, "{rid}");
        assert_eq!(recs.len(), 1);
        assert_eq!(
            (
                recs[0].repo.as_str(),
                recs[0].reason.as_str(),
                recs[0].created_by.as_str(),
                recs[0].created_at_ms
            ),
            ("a", "leaked", "root", 10)
        );
        // 再次 redact：已有的保持原样
        let (rid2, again) = red
            .redact("a", &[id], &[], "again", "root", false, 11)
            .await
            .expect("again");
        assert_ne!(rid, rid2);
        assert!(again.is_empty());
        assert_eq!(red.list(Some("a"), None).await.expect("list").len(), 1);

        // b 仍可读：不能物理删除
        assert_eq!(red.purge(&rid, 0, 20).await.expect("purge"), (1, 20));
        let report = red.run_purges(20).await.expect("run");
        assert_eq!(report.blocked, vec![(id, vec!["b".to_string()])]);
        assert!(report.deleted.is_empty());
        assert!(store.exists(&id).await.expect("exists"));

        // 撤销清除前的 redaction
        assert_eq!(red.unredact(&rid).await.expect("unredact"), 1);
        assert!(!red.records(&rid).await.expect("recs")[0].active);
        assert_eq!(
            red.purge(&rid, 0, 30).await.map_err(|s| s.code()),
            Err(Code::FailedPrecondition),
            "withdrawn"
        );
        assert_eq!(
            red.unredact("rd_missing").await.map_err(|s| s.code()),
            Err(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn purge_deletes_once_every_repo_is_redacted() {
        let (store, red, id) = setup().await;
        let (rid, recs) = red
            .redact("a", &[id], &[], "leak", "root", true, 10)
            .await
            .expect("all repos");
        assert_eq!(recs.iter().map(|r| r.repo.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        let (n, after) = red.purge(&rid, 1_000, 100).await.expect("purge");
        assert_eq!((n, after), (2, 1_100));
        assert_eq!(red.run_purges(1_099).await.expect("early"), PurgeReport::default());
        let report = red.run_purges(1_100).await.expect("due");
        assert_eq!(report.deleted, vec![id]);
        assert!(!store.exists(&id).await.expect("exists"));
        let recs = red.records(&rid).await.expect("recs");
        assert!(recs.iter().all(|r| r.purged_at_ms == Some(1_100) && r.active));
        assert_eq!(red.run_purges(2_000).await.expect("nothing"), PurgeReport::default());
        assert_eq!(
            red.unredact(&rid).await.map_err(|s| s.code()),
            Err(Code::FailedPrecondition),
            "purged"
        );
        assert_eq!(
            red.purge(&rid, 0, 3_000).await.map_err(|s| s.code()),
            Err(Code::FailedPrecondition)
        );
    }
}
