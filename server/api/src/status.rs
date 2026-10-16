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
        let s = from_store(E::Backend("db down: password=secret".into()));
        assert_eq!(s.code(), Code::Unavailable);
        assert!(!s.message().contains("secret"), "backend details must not leak");
    }
}
