//! SDK 错误类型。重试语义严格按服务端设计文档 §3.5。

use std::collections::HashMap;

use scion_object::ObjectId;
use scion_proto::v1::ErrorReason;
use thiserror::Error;
use tonic::Code;
use tonic_types::StatusExt;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid endpoint {0:?}")]
    InvalidEndpoint(String),
    #[error("no repository specified")]
    MissingRepo,
    #[error("cannot connect to {endpoint}: {message}")]
    Connect { endpoint: String, message: String },
    /// 服务端返回的错误。`reason` 来自 `ErrorInfo`；未知原因（新版服务端新增）为 `None`。
    #[error("{code:?}: {message}")]
    Rpc {
        code: Code,
        reason: Option<ErrorReason>,
        message: String,
        metadata: HashMap<String, String>,
    },
    #[error("server protocol {server} is incompatible with client protocol {client}")]
    IncompatibleProtocol { server: String, client: &'static str },
    /// 收到的对象与其 ID 不符。不信任任何缓存层，调用方应重新拉取。
    #[error("object {0} failed integrity check")]
    Corrupt(ObjectId),
    /// 服务端响应违反协议。
    #[error("protocol violation: {0}")]
    Protocol(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 合入时的冲突详情（服务端最多列出 32 个路径，`count` 为总数）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflict {
    /// 冲突的原提交（待合入栈中的提交）。
    pub changeset: Option<ObjectId>,
    pub count: usize,
    pub paths: Vec<ConflictPath>,
}

/// 一个冲突路径。blob id 为 `None` 表示该方没有这个文件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictPath {
    pub path: String,
    /// content、add_add、modify_delete、delete_modify、binary、too_large、symlink、path。
    pub kind: String,
    pub base: Option<ObjectId>,
    pub ours: Option<ObjectId>,
    pub theirs: Option<ObjectId>,
}

impl Error {
    /// `ErrorInfo.reason`（仅 `Rpc` 错误）。
    pub fn reason(&self) -> Option<ErrorReason> {
        match self {
            Error::Rpc { reason, .. } => *reason,
            _ => None,
        }
    }

    /// 对象已被 redact（不可重试）。
    pub fn is_redacted(&self) -> bool {
        self.reason() == Some(ErrorReason::Redacted)
    }

    /// 按协议约定是否可以原样重试。
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connect { .. } => true,
            Error::Rpc { code, reason, .. } => match reason {
                Some(ErrorReason::LandContention | ErrorReason::RateLimited) => true,
                Some(_) => false,
                None => matches!(
                    code,
                    Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
                ),
            },
            Error::Corrupt(_) => true,
            _ => false,
        }
    }

    /// MERGE_CONFLICT 错误的详情（服务端 rebase 时与 ref 上的改动冲突）。
    pub fn merge_conflict(&self) -> Option<MergeConflict> {
        let Error::Rpc {
            reason: Some(ErrorReason::MergeConflict),
            metadata,
            ..
        } = self
        else {
            return None;
        };
        let id = |k: &str| metadata.get(k).and_then(|v| v.parse::<ObjectId>().ok());
        let mut paths = Vec::new();
        for i in 0.. {
            let Some(path) = metadata.get(&format!("path.{i}")) else {
                break;
            };
            paths.push(ConflictPath {
                path: path.clone(),
                kind: metadata.get(&format!("kind.{i}")).cloned().unwrap_or_default(),
                base: id(&format!("base.{i}")),
                ours: id(&format!("ours.{i}")),
                theirs: id(&format!("theirs.{i}")),
            });
        }
        Some(MergeConflict {
            changeset: id("changeset"),
            count: metadata
                .get("count")
                .and_then(|c| c.parse().ok())
                .unwrap_or(paths.len()),
            paths,
        })
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Error::Rpc {
                code: Code::NotFound,
                ..
            }
        )
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        let details = s.get_error_details();
        let (reason, metadata) = match details.error_info() {
            Some(info) if info.domain == scion_proto::ERROR_DOMAIN => {
                (scion_proto::parse_reason(&info.reason), info.metadata.clone())
            }
            _ => (None, HashMap::new()),
        };
        Error::Rpc {
            code: s.code(),
            reason,
            message: s.message().to_string(),
            metadata,
        }
    }
}

impl Error {
    /// 转回 gRPC 状态（daemon 等代理把上游错误原样交给自己的调用方）：保留状态码、消息、
    /// `ErrorInfo` 的 reason 与 metadata；非服务端错误映射为相应的状态码。
    pub fn to_status(&self) -> tonic::Status {
        match self {
            Error::Rpc {
                code,
                reason,
                message,
                metadata,
            } => match reason {
                Some(r) => tonic::Status::with_error_details(
                    *code,
                    message.clone(),
                    tonic_types::ErrorDetails::with_error_info(
                        scion_proto::reason_name(*r),
                        scion_proto::ERROR_DOMAIN,
                        metadata.clone(),
                    ),
                ),
                None => tonic::Status::new(*code, message.clone()),
            },
            Error::Connect { .. } => tonic::Status::unavailable(self.to_string()),
            Error::Corrupt(_) => tonic::Status::data_loss(self.to_string()),
            Error::MissingRepo | Error::InvalidEndpoint(_) => tonic::Status::invalid_argument(self.to_string()),
            Error::IncompatibleProtocol { .. } | Error::Protocol(_) => tonic::Status::internal(self.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_types::ErrorDetails;

    #[test]
    fn to_status_round_trips() {
        let e = Error::from(status(Code::FailedPrecondition, "OBJECT_MISSING", "scion.dev"));
        let back = Error::from(e.to_status());
        assert_eq!(back.reason(), Some(ErrorReason::ObjectMissing));
        assert_eq!(e.to_status().code(), Code::FailedPrecondition);
        assert_eq!(
            Error::from(tonic::Status::not_found("x")).to_status().code(),
            Code::NotFound
        );
        let connect = Error::Connect {
            endpoint: "e".into(),
            message: "m".into(),
        };
        assert_eq!(connect.to_status().code(), Code::Unavailable);
        assert_eq!(
            Error::Corrupt(ObjectId::of_blob(b"")).to_status().code(),
            Code::DataLoss
        );
        assert_eq!(Error::MissingRepo.to_status().code(), Code::InvalidArgument);
        assert_eq!(Error::Protocol("p".into()).to_status().code(), Code::Internal);
    }

    fn status(code: Code, reason: &str, domain: &str) -> tonic::Status {
        let details =
            ErrorDetails::with_error_info(reason, domain, HashMap::from([("k".to_string(), "v".to_string())]));
        tonic::Status::with_error_details(code, "msg", details)
    }

    #[test]
    fn from_status_parses_reason_and_metadata() {
        let e = Error::from(status(Code::NotFound, "OBJECT_MISSING", "scion.dev"));
        assert_eq!(e.reason(), Some(ErrorReason::ObjectMissing));
        assert!(e.is_not_found());
        match e {
            Error::Rpc { metadata, message, .. } => {
                assert_eq!(metadata["k"], "v");
                assert_eq!(message, "msg");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn merge_conflict_details_are_parsed() {
        let blob = ObjectId::of_blob(b"b");
        let cs = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        let md = HashMap::from([
            ("changeset".to_string(), cs.to_string()),
            ("count".to_string(), "40".to_string()),
            ("path.0".to_string(), "f".to_string()),
            ("kind.0".to_string(), "content".to_string()),
            ("base.0".to_string(), blob.to_string()),
            ("ours.0".to_string(), blob.to_string()),
            ("theirs.0".to_string(), String::new()),
            ("path.1".to_string(), "g".to_string()),
        ]);
        let s = tonic::Status::with_error_details(
            Code::Aborted,
            "conflict",
            ErrorDetails::with_error_info("MERGE_CONFLICT", "scion.dev", md),
        );
        let c = Error::from(s).merge_conflict().expect("conflict");
        assert_eq!((c.changeset, c.count, c.paths.len()), (Some(cs), 40, 2));
        assert_eq!(
            c.paths[0],
            ConflictPath {
                path: "f".into(),
                kind: "content".into(),
                base: Some(blob),
                ours: Some(blob),
                theirs: None,
            }
        );
        assert_eq!((c.paths[1].kind.as_str(), c.paths[1].base), ("", None));
        // 缺少 count 时按列出的路径数
        let s = tonic::Status::with_error_details(
            Code::Aborted,
            "conflict",
            ErrorDetails::with_error_info("MERGE_CONFLICT", "scion.dev", HashMap::new()),
        );
        let c = Error::from(s).merge_conflict().expect("conflict");
        assert_eq!((c.changeset, c.count), (None, 0));
        assert!(
            Error::from(status(Code::Aborted, "NOT_FAST_FORWARD", "scion.dev"))
                .merge_conflict()
                .is_none()
        );
        assert!(Error::MissingRepo.merge_conflict().is_none());
    }

    #[test]
    fn from_status_ignores_foreign_domain() {
        let e = Error::from(status(Code::NotFound, "OBJECT_MISSING", "example.com"));
        assert_eq!(e.reason(), None);
    }

    #[test]
    fn from_status_unknown_reason_is_none() {
        let e = Error::from(status(Code::Aborted, "SOMETHING_NEW", "scion.dev"));
        assert_eq!(e.reason(), None);
    }

    #[test]
    fn from_status_without_details() {
        let e = Error::from(tonic::Status::unavailable("down"));
        assert_eq!(e.reason(), None);
        assert!(e.is_retryable());
    }

    #[test]
    fn is_retryable_follows_protocol_table() {
        let rpc = |code, reason| Error::Rpc {
            code,
            reason,
            message: String::new(),
            metadata: HashMap::new(),
        };
        assert!(rpc(Code::Unavailable, Some(ErrorReason::LandContention)).is_retryable());
        assert!(rpc(Code::ResourceExhausted, Some(ErrorReason::RateLimited)).is_retryable());
        assert!(!rpc(Code::Aborted, Some(ErrorReason::MergeConflict)).is_retryable());
        assert!(!rpc(Code::FailedPrecondition, Some(ErrorReason::ObjectMissing)).is_retryable());
        assert!(!rpc(Code::Unavailable, Some(ErrorReason::Redacted)).is_retryable());
        assert!(rpc(Code::NotFound, Some(ErrorReason::Redacted)).is_redacted());
        assert!(!rpc(Code::NotFound, Some(ErrorReason::ObjectMissing)).is_redacted());
        assert!(rpc(Code::DeadlineExceeded, None).is_retryable());
        assert!(!rpc(Code::InvalidArgument, None).is_retryable());
        assert!(
            Error::Connect {
                endpoint: "e".into(),
                message: "m".into()
            }
            .is_retryable()
        );
        assert!(Error::Corrupt(ObjectId::of_blob(b"")).is_retryable());
        assert!(!Error::MissingRepo.is_retryable());
        assert!(
            !Error::IncompatibleProtocol {
                server: "2.0".into(),
                client: "1.0"
            }
            .is_retryable()
        );
    }

    #[test]
    fn reason_and_not_found_for_non_rpc() {
        assert_eq!(Error::MissingRepo.reason(), None);
        assert!(!Error::Protocol("x".into()).is_not_found());
    }

    #[test]
    fn display_messages() {
        assert_eq!(Error::InvalidEndpoint("x".into()).to_string(), "invalid endpoint \"x\"");
        assert_eq!(Error::MissingRepo.to_string(), "no repository specified");
        assert!(Error::Corrupt(ObjectId::of_blob(b"")).to_string().contains("integrity"));
    }
}
