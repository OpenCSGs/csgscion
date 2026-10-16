//! 两个后端共用的小工具。

use std::time::{SystemTime, UNIX_EPOCH};

use scion_object::ObjectId;
use scion_store::{Error, Result};

/// 当前 Unix 毫秒时间（仅用于审计字段）。
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 数据库错误统一映射为可重试的后端错误；细节只进日志，不返回给调用方。
pub(crate) fn backend(context: &'static str, e: sqlx::Error) -> Error {
    tracing::warn!(error = %e, context, "metastore error");
    Error::Backend(format!("{context}: {}", short(&e)))
}

/// 错误摘要：不含连接串等可能敏感的细节。
fn short(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Database(d) => format!("database error {}", d.code().unwrap_or_default()),
        sqlx::Error::PoolTimedOut => "connection pool timed out".into(),
        sqlx::Error::Io(_) => "i/o error".into(),
        other => other.to_string().split(':').next().unwrap_or("error").to_string(),
    }
}

pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error().is_some_and(|d| d.is_unique_violation())
}

pub(crate) fn version_to_db(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| Error::Backend(format!("ref version {v} exceeds i64")))
}

pub(crate) fn version_from_db(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| Error::Backend(format!("corrupt ref version {v}")))
}

pub(crate) fn id_from_db(b: &[u8]) -> Result<ObjectId> {
    ObjectId::from_bytes(b).map_err(|e| Error::Backend(format!("corrupt object id in metastore: {e}")))
}

/// URL 中的密码替换为 `***`，用于错误信息和日志。
pub(crate) fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((userinfo, host)) = rest.split_once('@') else {
        return url.to_string();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{host}"),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_is_after_2020() {
        assert!(now_ms() > 1_577_836_800_000);
    }

    #[test]
    fn version_conversions() {
        assert_eq!(version_to_db(7), Ok(7));
        assert!(matches!(version_to_db(u64::MAX), Err(Error::Backend(_))));
        assert_eq!(version_from_db(7), Ok(7));
        assert!(matches!(version_from_db(-1), Err(Error::Backend(_))));
    }

    #[test]
    fn id_from_db_roundtrip_and_corrupt() {
        let id = ObjectId::of_blob(b"x");
        assert_eq!(id_from_db(&id.to_bytes()), Ok(id));
        assert!(matches!(id_from_db(b"short"), Err(Error::Backend(_))));
    }

    #[test]
    fn redact_url_hides_password_only() {
        assert_eq!(redact_url("postgres://u:pw@h:5432/db"), "postgres://u:***@h:5432/db");
        assert_eq!(redact_url("postgres://u@h/db"), "postgres://u@h/db");
        assert_eq!(redact_url("postgres://h/db"), "postgres://h/db");
        assert_eq!(redact_url("sqlite::memory:"), "sqlite::memory:");
    }

    #[test]
    fn backend_error_is_short() {
        let e = backend("ctx", sqlx::Error::PoolTimedOut);
        assert_eq!(e, Error::Backend("ctx: connection pool timed out".into()));
        let e = backend("ctx", sqlx::Error::Io(std::io::Error::other("secret path")));
        assert!(!e.to_string().contains("secret"));
        let e = backend("ctx", sqlx::Error::RowNotFound);
        assert!(matches!(e, Error::Backend(m) if m.starts_with("ctx: ")));
    }

    #[test]
    fn unique_violation_false_for_non_db_errors() {
        assert!(!is_unique_violation(&sqlx::Error::RowNotFound));
    }
}
