use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error("repository {0:?} not found")]
    RepoNotFound(String),
    #[error("repository {0:?} already exists")]
    RepoAlreadyExists(String),
    #[error("invalid repository name {0:?}")]
    InvalidRepoName(String),
    /// ref CAS 条件不满足。`current` 为 `None` 表示 ref 不存在。
    #[error("ref {name:?} version mismatch (current: {current:?})")]
    RefVersionMismatch { name: String, current: Option<u64> },
    /// 对象已被 redaction，不能读取。
    #[error("object {id} is redacted ({redaction_id})")]
    Redacted {
        id: scion_object::ObjectId,
        redaction_id: String,
    },
    /// 后端故障（数据库、网络等），通常可重试。
    #[error("storage backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_context() {
        assert_eq!(
            Error::RepoNotFound("a".into()).to_string(),
            "repository \"a\" not found"
        );
        assert_eq!(
            Error::RepoAlreadyExists("a".into()).to_string(),
            "repository \"a\" already exists"
        );
        assert_eq!(
            Error::InvalidRepoName("".into()).to_string(),
            "invalid repository name \"\""
        );
        assert_eq!(
            Error::RefVersionMismatch {
                name: "main".into(),
                current: Some(3)
            }
            .to_string(),
            "ref \"main\" version mismatch (current: Some(3))"
        );
        assert_eq!(Error::Backend("io".into()).to_string(), "storage backend error: io");
        let id = scion_object::ObjectId::of_blob(b"x");
        assert_eq!(
            Error::Redacted {
                id,
                redaction_id: "rd1".into()
            }
            .to_string(),
            format!("object {id} is redacted (rd1)")
        );
    }
}
