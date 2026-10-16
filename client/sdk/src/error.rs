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

impl Error {
    /// `ErrorInfo.reason`（仅 `Rpc` 错误）。
    pub fn reason(&self) -> Option<ErrorReason> {
        match self {
            Error::Rpc { reason, .. } => *reason,
            _ => None,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_types::ErrorDetails;

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
