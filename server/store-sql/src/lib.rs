//! 基于 SQL 的 [`MetaStore`] 实现（服务端设计文档 §4.4）：
//!
//! - [`SqliteMetaStore`]：Lite 档，单文件、WAL 模式；
//! - [`PgMetaStore`]：Standard 档（PostgreSQL）与 Distributed 档（YugabyteDB，走 PG 协议）。
//!
//! 两者表结构一致，SQL 只在占位符写法上不同。ref 的修改只用单行条件写（`INSERT … ON CONFLICT DO NOTHING`
//! 或 `UPDATE … WHERE version = ?`），不依赖事务隔离级别，因此在三种数据库上语义相同。

mod common;
mod postgres;
mod sqlite;

use std::sync::Arc;

use scion_store::{Error, MetaStore, Result};

pub use postgres::PgMetaStore;
pub use sqlite::SqliteMetaStore;

/// 按 URL 选择后端：`sqlite:` 开头为 SQLite，`postgres://` / `postgresql://` 为 PostgreSQL。
/// 连接后自动执行表结构迁移。
pub async fn connect(url: &str) -> Result<Arc<dyn MetaStore>> {
    match Backend::from_url(url)? {
        Backend::Sqlite => Ok(Arc::new(SqliteMetaStore::connect(url).await?)),
        Backend::Postgres => Ok(Arc::new(PgMetaStore::connect(url).await?)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Sqlite,
    Postgres,
}

impl Backend {
    fn from_url(url: &str) -> Result<Self> {
        if url.starts_with("sqlite:") {
            Ok(Backend::Sqlite)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Backend::Postgres)
        } else {
            Err(Error::Backend(format!(
                "unsupported metastore url {:?}; expected sqlite:… or postgres://…",
                common::redact_url(url)
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_from_url() {
        assert_eq!(Backend::from_url("sqlite:///tmp/x.db"), Ok(Backend::Sqlite));
        assert_eq!(Backend::from_url("sqlite::memory:"), Ok(Backend::Sqlite));
        assert_eq!(Backend::from_url("postgres://u@h/db"), Ok(Backend::Postgres));
        assert_eq!(Backend::from_url("postgresql://u@h/db"), Ok(Backend::Postgres));
        assert!(matches!(Backend::from_url("mysql://h"), Err(Error::Backend(_))));
    }

    #[test]
    fn backend_from_url_error_hides_password() {
        let err = Backend::from_url("cockroach://u:hunter2@h/db").expect_err("unsupported");
        assert!(!err.to_string().contains("hunter2"));
    }

    #[tokio::test]
    async fn connect_dispatches_to_sqlite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let store = connect(&url).await.expect("connect");
        store.create_repo("a").await.expect("create");
        assert_eq!(store.list_repos().await, Ok(vec!["a".to_string()]));
    }

    #[tokio::test]
    async fn connect_rejects_unknown_scheme() {
        assert!(matches!(connect("redis://x").await.map(|_| ()), Err(Error::Backend(_))));
    }
}
