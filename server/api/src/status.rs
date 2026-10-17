//! 统一错误模型（服务端设计文档 §3.5）：`google.rpc.Status` + `ErrorInfo{reason, domain}`。

use std::collections::HashMap;

use scion_object::ObjectId;
use scion_proto::v1::ErrorReason;
use scion_proto::{ERROR_DOMAIN, reason_name};
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

/// `OBJECT_MISSING` 等错误在 metadata 中最多列出的 id 个数，避免错误详情过大。
pub const MAX_IDS_IN_ERROR: usize = 32;

/// 构造带 `ErrorInfo` 的 Status。
pub fn with_reason(
    code: Code,
    reason: ErrorReason,
    message: impl Into<String>,
    metadata: HashMap<String, String>,
) -> Status {
    let details = ErrorDetails::with_error_info(reason_name(reason), ERROR_DOMAIN, metadata);
    Status::with_error_details(code, message, details)
}

pub fn repo_not_found(repo: &str) -> Status {
    with_reason(
        Code::NotFound,
        ErrorReason::RepoNotFound,
        format!("repository {repo:?} not found"),
        HashMap::from([("repo".to_string(), repo.to_string())]),
    )
}

pub fn invalid_object_id(raw: &[u8], err: &scion_object::Error) -> Status {
    with_reason(
        Code::InvalidArgument,
        ErrorReason::InvalidObjectId,
        format!("invalid object id ({} bytes): {err}", raw.len()),
        HashMap::new(),
    )
}

pub fn limit_exceeded(what: &str, got: usize, max: usize) -> Status {
    with_reason(
        Code::InvalidArgument,
        ErrorReason::LimitExceeded,
        format!("{what}: {got} exceeds limit {max}"),
        HashMap::from([
            ("limit".to_string(), what.to_string()),
            ("max".to_string(), max.to_string()),
        ]),
    )
}

/// 缺失对象：metadata `ids` 为逗号分隔的文本 ID（最多 [`MAX_IDS_IN_ERROR`] 个），`count` 为总数。
pub fn object_missing(ids: &[ObjectId]) -> Status {
    let listed: Vec<String> = ids.iter().take(MAX_IDS_IN_ERROR).map(ToString::to_string).collect();
    with_reason(
        Code::NotFound,
        ErrorReason::ObjectMissing,
        format!("{} object(s) not found", ids.len()),
        HashMap::from([
            ("ids".to_string(), listed.join(",")),
            ("count".to_string(), ids.len().to_string()),
        ]),
    )
}

/// 对象已被 redact：NOT_FOUND / REDACTED，metadata `ids` 与 `redaction_ids` 一一对应（逗号分隔，
/// 最多 [`MAX_IDS_IN_ERROR`] 个），`count` 为总数。不可重试。
pub fn redacted(objects: &[(ObjectId, String)]) -> Status {
    let shown = &objects[..objects.len().min(MAX_IDS_IN_ERROR)];
    let ids: Vec<String> = shown.iter().map(|(id, _)| id.to_string()).collect();
    let rids: Vec<&str> = shown.iter().map(|(_, r)| r.as_str()).collect();
    with_reason(
        Code::NotFound,
        ErrorReason::Redacted,
        match objects {
            [(id, rid)] => format!("object {id} is redacted ({rid})"),
            _ => format!("{} object(s) are redacted", objects.len()),
        },
        HashMap::from([
            ("ids".to_string(), ids.join(",")),
            ("redaction_ids".to_string(), rids.join(",")),
            ("count".to_string(), objects.len().to_string()),
        ]),
    )
}

/// 写入时引用了尚未上传的对象：FAILED_PRECONDITION，客户端应先上传再重试（§3.5）。
pub fn objects_not_uploaded(ids: &[ObjectId]) -> Status {
    let listed: Vec<String> = ids.iter().take(MAX_IDS_IN_ERROR).map(ToString::to_string).collect();
    with_reason(
        Code::FailedPrecondition,
        ErrorReason::ObjectMissing,
        format!("{} object(s) must be uploaded first", ids.len()),
        HashMap::from([
            ("ids".to_string(), listed.join(",")),
            ("count".to_string(), ids.len().to_string()),
        ]),
    )
}

pub fn invalid_changeset(path: &str, message: impl Into<String>) -> Status {
    with_reason(
        Code::InvalidArgument,
        ErrorReason::InvalidChangeset,
        message,
        HashMap::from([("path".to_string(), path.to_string())]),
    )
}

/// 合入的提交不是 ref 当前提交的直接后继。
pub fn not_fast_forward(reference: &str, tip: &ObjectId) -> Status {
    with_reason(
        Code::Aborted,
        ErrorReason::NotFastForward,
        format!("ref {reference:?} has moved to {tip}; rebase onto it and land again"),
        HashMap::from([
            ("ref".to_string(), reference.to_string()),
            ("tip".to_string(), tip.to_string()),
        ]),
    )
}

pub fn ref_not_found(reference: &str) -> Status {
    with_reason(
        Code::NotFound,
        ErrorReason::RefNotFound,
        format!("revision {reference:?} not found"),
        HashMap::from([("ref".to_string(), reference.to_string())]),
    )
}

pub fn path_not_found(path: &str) -> Status {
    with_reason(
        Code::NotFound,
        ErrorReason::PathNotFound,
        format!("path {path:?} not found"),
        HashMap::from([("path".to_string(), path.to_string())]),
    )
}

/// 建树错误的映射：delta 本身的问题是 INVALID_CHANGESET，存储损坏是 INTERNAL，存储故障可重试。
pub fn from_tree(err: scion_tree::TreeError) -> Status {
    use scion_tree::TreeError as E;
    match &err {
        E::DeleteMissing(p) | E::DeleteDirectory(p) | E::PathConflict(p) => invalid_changeset(p, err.to_string()),
        E::BadTree(id, msg) => {
            tracing::error!(%id, error = %msg, "corrupt or missing tree");
            Status::internal(format!("tree {id} is missing or corrupt"))
        }
        E::Storage(msg) => {
            tracing::error!(error = %msg, "storage error while building tree");
            Status::unavailable("storage backend unavailable")
        }
    }
}

/// 过载或超过速率限制：RESOURCE_EXHAUSTED + RATE_LIMITED（可重试），metadata `retry_after_ms` 为建议的等待时间。
pub fn rate_limited(message: &str, retry_after: std::time::Duration) -> Status {
    with_reason(
        Code::ResourceExhausted,
        ErrorReason::RateLimited,
        message,
        HashMap::from([("retry_after_ms".to_string(), retry_after.as_millis().max(1).to_string())]),
    )
}

/// 冲突详情：`changeset`、`count`，以及每个冲突的 `path.N` / `kind.N` / `base.N` / `ours.N` / `theirs.N`。
pub fn merge_conflict(changeset: &ObjectId, conflicts: &[scion_merge::Conflict]) -> Status {
    let id = |v: Option<ObjectId>| v.map(|i| i.to_string()).unwrap_or_default();
    let mut md = HashMap::from([
        ("changeset".to_string(), changeset.to_string()),
        ("count".to_string(), conflicts.len().to_string()),
    ]);
    for (i, c) in conflicts.iter().take(MAX_IDS_IN_ERROR).enumerate() {
        md.insert(format!("path.{i}"), c.path.clone());
        md.insert(format!("kind.{i}"), c.kind.as_str().to_string());
        md.insert(format!("base.{i}"), id(c.base));
        md.insert(format!("ours.{i}"), id(c.ours));
        md.insert(format!("theirs.{i}"), id(c.theirs));
    }
    let first = conflicts.first().map(|c| c.path.as_str()).unwrap_or("");
    with_reason(
        Code::Aborted,
        ErrorReason::MergeConflict,
        format!(
            "changeset {changeset} conflicts with the ref on {} path(s), first {first:?}",
            conflicts.len()
        ),
        md,
    )
}

/// 写路径错误的映射。
pub fn from_land(err: scion_land::LandError) -> Status {
    use scion_land::LandError as E;
    match err {
        E::RepoNotFound(repo) => repo_not_found(&repo),
        E::InvalidRef(name) => Status::invalid_argument(format!("invalid ref name {name:?}")),
        E::ObjectsNotUploaded(ids) => objects_not_uploaded(&ids),
        E::InvalidChangeset { path, message } => invalid_changeset(&path, message),
        E::NotFastForward { tip } => with_reason(
            Code::Aborted,
            ErrorReason::NotFastForward,
            format!("not a fast-forward of {tip}"),
            HashMap::from([("tip".to_string(), tip.to_string())]),
        ),
        E::Conflict { changeset, conflicts } => merge_conflict(&changeset, &conflicts),
        e @ (E::UnrelatedHistory { .. } | E::MergeInStack(_) | E::StackTooDeep(_)) => {
            Status::failed_precondition(e.to_string())
        }
        E::RequestIdReused(id) => {
            Status::invalid_argument(format!("request_id {id:?} was already used for a different request"))
        }
        E::NotLeader { leader } => with_reason(
            Code::Unavailable,
            ErrorReason::LandContention,
            "the land leader for this ref moved; retry",
            HashMap::from([("leader".to_string(), leader)]),
        ),
        E::Overloaded => with_reason(
            Code::ResourceExhausted,
            ErrorReason::RateLimited,
            "too many pending lands on this ref; retry with backoff",
            HashMap::new(),
        ),
        E::Contention => with_reason(
            Code::Unavailable,
            ErrorReason::LandContention,
            "ref changed concurrently too many times; retry",
            HashMap::new(),
        ),
        E::Corrupt(msg) => {
            tracing::error!(error = %msg, "corrupt data on write path");
            Status::internal("corrupt data in storage")
        }
        E::Unavailable(msg) => {
            tracing::error!(error = %msg, "storage error on write path");
            Status::unavailable("storage backend unavailable")
        }
        E::Redacted { id, redaction_id } => redacted(&[(id, redaction_id)]),
    }
}

/// 存储层错误到 Status 的映射。
pub fn from_store(err: scion_store::Error) -> Status {
    use scion_store::Error as E;
    match err {
        E::RepoNotFound(repo) => repo_not_found(&repo),
        E::RepoAlreadyExists(repo) => Status::already_exists(format!("repository {repo:?} already exists")),
        E::InvalidRepoName(repo) => Status::invalid_argument(format!("invalid repository name {repo:?}")),
        E::RefVersionMismatch { name, current } => with_reason(
            Code::FailedPrecondition,
            ErrorReason::RefVersionMismatch,
            format!("ref {name:?} version mismatch"),
            HashMap::from([
                ("ref".to_string(), name),
                (
                    "current_version".to_string(),
                    current.map(|v| v.to_string()).unwrap_or_default(),
                ),
            ]),
        ),
        E::Redacted { id, redaction_id } => redacted(&[(id, redaction_id)]),
        E::Backend(msg) => {
            tracing::error!(error = %msg, "storage backend error");
            Status::unavailable("storage backend unavailable")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(s: &Status) -> (String, String, HashMap<String, String>) {
        let d = s.get_error_details();
        let i = d.error_info().expect("error info present");
        (i.reason.clone(), i.domain.clone(), i.metadata.clone())
    }

    #[test]
    fn repo_not_found_has_reason_and_repo() {
        let s = repo_not_found("a/b");
        assert_eq!(s.code(), Code::NotFound);
        let (reason, domain, md) = info(&s);
        assert_eq!(reason, "REPO_NOT_FOUND");
        assert_eq!(domain, "scion.dev");
        assert_eq!(md["repo"], "a/b");
    }

    #[test]
    fn invalid_object_id_reports_length() {
        let s = invalid_object_id(&[1, 2, 3], &scion_object::Error::InvalidIdLength(3));
        assert_eq!(s.code(), Code::InvalidArgument);
        assert_eq!(info(&s).0, "INVALID_OBJECT_ID");
        assert!(s.message().contains("3 bytes"));
    }

    #[test]
    fn limit_exceeded_metadata() {
        let s = limit_exceeded("max_batch_objects", 2000, 1000);
        assert_eq!(s.code(), Code::InvalidArgument);
        let (reason, _, md) = info(&s);
        assert_eq!(reason, "LIMIT_EXCEEDED");
        assert_eq!(md["max"], "1000");
    }

    #[test]
    fn object_missing_truncates_list() {
        let ids: Vec<_> = (0..40u8).map(|i| ObjectId::of_blob(&[i])).collect();
        let s = object_missing(&ids);
        assert_eq!(s.code(), Code::NotFound);
        let (reason, _, md) = info(&s);
        assert_eq!(reason, "OBJECT_MISSING");
        assert_eq!(md["count"], "40");
        assert_eq!(md["ids"].split(',').count(), MAX_IDS_IN_ERROR);
    }

    #[test]
    fn object_missing_single() {
        let id = ObjectId::of_blob(b"x");
        let (_, _, md) = info(&object_missing(&[id]));
        assert_eq!(md["ids"], id.to_string());
    }

    #[test]
    fn objects_not_uploaded_is_failed_precondition() {
        let id = ObjectId::of_blob(b"x");
        let s = objects_not_uploaded(&[id]);
        assert_eq!(s.code(), Code::FailedPrecondition);
        let (reason, _, md) = info(&s);
        assert_eq!(reason, "OBJECT_MISSING");
        assert_eq!(md["ids"], id.to_string());
    }

    #[test]
    fn write_and_query_reasons() {
        let s = invalid_changeset("a/b", "bad");
        assert_eq!(
            (s.code(), info(&s).0.as_str(), info(&s).2["path"].as_str()),
            (Code::InvalidArgument, "INVALID_CHANGESET", "a/b")
        );
        let tip = ObjectId::hash(scion_object::ObjectKind::Changeset, b"t");
        let s = not_fast_forward("main", &tip);
        assert_eq!(s.code(), Code::Aborted);
        assert_eq!(info(&s).0, "NOT_FAST_FORWARD");
        assert_eq!(info(&s).2["tip"], tip.to_string());
        let s = ref_not_found("main");
        assert_eq!((s.code(), info(&s).0.as_str()), (Code::NotFound, "REF_NOT_FOUND"));
        let s = path_not_found("x");
        assert_eq!((s.code(), info(&s).0.as_str()), (Code::NotFound, "PATH_NOT_FOUND"));
    }

    #[test]
    fn from_tree_maps_every_variant() {
        use scion_tree::TreeError as E;
        for e in [
            E::DeleteMissing("a".into()),
            E::DeleteDirectory("a".into()),
            E::PathConflict("a".into()),
        ] {
            let s = from_tree(e);
            assert_eq!(s.code(), Code::InvalidArgument);
            assert_eq!(info(&s).2["path"], "a");
        }
        let id = ObjectId::hash(scion_object::ObjectKind::Tree, b"t");
        assert_eq!(from_tree(E::BadTree(id, "x".into())).code(), Code::Internal);
        let s = from_tree(E::Storage("secret".into()));
        assert_eq!(s.code(), Code::Unavailable);
        assert!(!s.message().contains("secret"));
    }

    #[test]
    fn from_store_maps_every_variant() {
        use scion_store::Error as E;
        assert_eq!(from_store(E::RepoNotFound("r".into())).code(), Code::NotFound);
        assert_eq!(from_store(E::RepoAlreadyExists("r".into())).code(), Code::AlreadyExists);
        assert_eq!(from_store(E::InvalidRepoName("r".into())).code(), Code::InvalidArgument);
        let s = from_store(E::RefVersionMismatch {
            name: "main".into(),
            current: Some(4),
        });
        assert_eq!(s.code(), Code::FailedPrecondition);
        let (reason, _, md) = info(&s);
        assert_eq!(reason, "REF_VERSION_MISMATCH");
        assert_eq!(md["current_version"], "4");
        let s = from_store(E::RefVersionMismatch {
            name: "main".into(),
            current: None,
        });
        assert_eq!(info(&s).2["current_version"], "");
        let id = ObjectId::of_blob(b"secret");
        let s = from_store(E::Redacted {
            id,
            redaction_id: "rd1".into(),
        });
        let (reason, _, md) = info(&s);
        assert_eq!((s.code(), reason.as_str()), (Code::NotFound, "REDACTED"));
        assert_eq!(
            (md["ids"].clone(), md["redaction_ids"].as_str()),
            (id.to_string(), "rd1")
        );
        assert!(s.message().contains("rd1"));
        let many: Vec<(ObjectId, String)> = (0..40u8).map(|i| (ObjectId::of_blob(&[i]), format!("rd{i}"))).collect();
        let s = redacted(&many);
        let (_, _, md) = info(&s);
        assert_eq!(md["count"], "40");
        assert_eq!(md["ids"].split(',').count(), MAX_IDS_IN_ERROR);
        assert_eq!(md["redaction_ids"].split(',').count(), MAX_IDS_IN_ERROR);
        assert_eq!(
            from_land(scion_land::LandError::Redacted {
                id,
                redaction_id: "rd1".into()
            })
            .code(),
            Code::NotFound
        );
        let s = from_store(E::Backend("db down: password=secret".into()));
        assert_eq!(s.code(), Code::Unavailable);
        assert!(!s.message().contains("secret"), "backend details must not leak");
    }

    #[test]
    fn from_land_maps_every_variant() {
        use scion_land::LandError as E;
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        assert_eq!(from_land(E::RepoNotFound("r".into())).code(), Code::NotFound);
        assert_eq!(from_land(E::InvalidRef("x".into())).code(), Code::InvalidArgument);
        assert_eq!(
            from_land(E::ObjectsNotUploaded(vec![id])).code(),
            Code::FailedPrecondition
        );
        let s = from_land(E::InvalidChangeset {
            path: "p".into(),
            message: "bad".into(),
        });
        assert_eq!((s.code(), info(&s).2["path"].as_str()), (Code::InvalidArgument, "p"));
        let s = from_land(E::NotFastForward { tip: id });
        assert_eq!((s.code(), info(&s).0.as_str()), (Code::Aborted, "NOT_FAST_FORWARD"));
        for e in [E::UnrelatedHistory { tip: id }, E::MergeInStack(id), E::StackTooDeep(3)] {
            assert_eq!(from_land(e).code(), Code::FailedPrecondition);
        }
        assert_eq!(from_land(E::RequestIdReused("r".into())).code(), Code::InvalidArgument);
        let s = from_land(E::NotLeader {
            leader: "http://n2".into(),
        });
        assert_eq!(
            (s.code(), info(&s).2["leader"].as_str()),
            (Code::Unavailable, "http://n2")
        );
        let s = from_land(E::Overloaded);
        assert_eq!(
            (s.code(), info(&s).0.as_str()),
            (Code::ResourceExhausted, "RATE_LIMITED")
        );
        let s = from_land(E::Contention);
        assert_eq!((s.code(), info(&s).0.as_str()), (Code::Unavailable, "LAND_CONTENTION"));
        let s = from_land(E::Corrupt("secret".into()));
        assert!(s.code() == Code::Internal && !s.message().contains("secret"));
        let s = from_land(E::Unavailable("secret".into()));
        assert!(s.code() == Code::Unavailable && !s.message().contains("secret"));
    }

    #[test]
    fn merge_conflict_lists_at_most_32_conflicts() {
        use scion_merge::{Conflict, ConflictKind};
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        let blob = ObjectId::of_blob(b"b");
        let conflicts: Vec<Conflict> = (0..40)
            .map(|i| Conflict {
                path: format!("f{i}"),
                kind: ConflictKind::ModifyDelete,
                base: Some(blob),
                ours: Some(blob),
                theirs: None,
            })
            .collect();
        let s = from_land(scion_land::LandError::Conflict {
            changeset: id,
            conflicts,
        });
        let (reason, _, md) = info(&s);
        assert_eq!((s.code(), reason.as_str()), (Code::Aborted, "MERGE_CONFLICT"));
        assert_eq!((md["count"].as_str(), md["kind.0"].as_str()), ("40", "modify_delete"));
        assert_eq!((md["theirs.0"].as_str(), md["base.31"].clone()), ("", blob.to_string()));
        assert!(!md.contains_key("path.32"));
        assert!(s.message().contains("\"f0\""));
        // 空列表也能生成（防御性）
        let s = merge_conflict(&id, &[]);
        assert_eq!(info(&s).2["count"], "0");
    }
}
