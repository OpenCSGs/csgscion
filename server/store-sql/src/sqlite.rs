//! Lite 档：SQLite（WAL 模式）。

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use scion_object::ObjectId;
use scion_store::{ChangesetRecord, Error, IndexedObject, MetaStore, RefExpect, RefState, Result, validate_repo_name};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous};

use crate::common::{
    MAX_IDS_PER_QUERY, backend, id_from_db, is_unique_violation, now_ms, size_to_db, subtract_present, version_from_db,
    version_to_db,
};

/// 写锁等待上限；SQLite 同一时刻只有一个写者，其余写者在此期间排队。
const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

pub struct SqliteMetaStore {
    pool: SqlitePool,
}

impl SqliteMetaStore {
    /// 打开（不存在则创建）数据库文件并执行迁移。URL 形如 `sqlite:///var/lib/scion/meta.db`。
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url).map_err(|e| backend("parse sqlite url", e))?;
        Self::connect_with(opts).await
    }

    pub async fn connect_with(opts: SqliteConnectOptions) -> Result<Self> {
        let opts = opts
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            // FULL：每次提交都 fsync，断电也不丢已确认的 land
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(|e| backend("open sqlite", e))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| backend("migrate sqlite", e.into()))?;
        Ok(Self { pool })
    }

    async fn repo_id(&self, repo: &str) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT repo_id FROM repos WHERE name = ?")
            .bind(repo)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("lookup repo", e))?
            .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
    }

    async fn current_version(&self, repo_id: i64, name: &str) -> Result<Option<u64>> {
        sqlx::query_scalar::<_, i64>("SELECT version FROM refs WHERE repo_id = ? AND name = ?")
            .bind(repo_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("read ref version", e))?
            .map(version_from_db)
            .transpose()
    }
}

#[async_trait]
impl MetaStore for SqliteMetaStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        validate_repo_name(repo)?;
        sqlx::query("INSERT INTO repos (name, created_at_ms) VALUES (?, ?)")
            .bind(repo)
            .bind(now_ms())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    Error::RepoAlreadyExists(repo.to_owned())
                } else {
                    backend("create repo", e)
                }
            })?;
        Ok(())
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        match self.repo_id(repo).await {
            Ok(_) => Ok(true),
            Err(Error::RepoNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        // 按 UTF-8 字节序排序，与内存实现一致
        let mut names = sqlx::query_scalar::<_, String>("SELECT name FROM repos")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("list repos", e))?;
        names.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        Ok(names)
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let repo_id = self.repo_id(repo).await?;
        let row =
            sqlx::query_as::<_, (Vec<u8>, i64)>("SELECT target, version FROM refs WHERE repo_id = ? AND name = ?")
                .bind(repo_id)
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| backend("get ref", e))?;
        row.map(|(target, version)| {
            Ok(RefState {
                target: id_from_db(&target)?,
                version: version_from_db(version)?,
            })
        })
        .transpose()
    }

    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
        let repo_id = self.repo_id(repo).await?;
        let target_bytes = target.to_bytes().to_vec();
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 第一条语句就是写操作，SQLite 直接取得写锁，避免「先读后写」的锁升级冲突
        let (affected, new_version) = match expect {
            RefExpect::Absent => {
                let r = sqlx::query(
                    "INSERT INTO refs (repo_id, name, target, version) VALUES (?, ?, ?, 1) \
                     ON CONFLICT (repo_id, name) DO NOTHING",
                )
                .bind(repo_id)
                .bind(name)
                .bind(&target_bytes)
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("create ref", e))?;
                (r.rows_affected(), 1)
            }
            RefExpect::Version(v) => {
                let next = v
                    .checked_add(1)
                    .ok_or_else(|| Error::Backend("ref version overflow".into()))?;
                let r = sqlx::query(
                    "UPDATE refs SET target = ?, version = version + 1 WHERE repo_id = ? AND name = ? AND version = ?",
                )
                .bind(&target_bytes)
                .bind(repo_id)
                .bind(name)
                .bind(version_to_db(v)?)
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("update ref", e))?;
                (r.rows_affected(), next)
            }
        };
        if affected != 1 {
            tx.rollback().await.map_err(|e| backend("rollback", e))?;
            let current = self.current_version(repo_id, name).await?;
            return Err(Error::RefVersionMismatch {
                name: name.to_owned(),
                current,
            });
        }
        sqlx::query("INSERT INTO ref_log (repo_id, name, version, target, updated_at_ms) VALUES (?, ?, ?, ?, ?)")
            .bind(repo_id)
            .bind(name)
            .bind(version_to_db(new_version)?)
            .bind(&target_bytes)
            .bind(now_ms())
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("append ref log", e))?;
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(RefState {
            target,
            version: new_version,
        })
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if objects.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        for chunk in objects.chunks(MAX_IDS_PER_QUERY) {
            let rows = chunk
                .iter()
                .map(|o| Ok((o.id.to_bytes().to_vec(), size_to_db(o.size)?)))
                .collect::<Result<Vec<_>>>()?;
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("INSERT INTO objects_index (repo_id, id, size) ");
            qb.push_values(rows, |mut b, (id, size)| {
                b.push_bind(repo_id).push_bind(id).push_bind(size);
            });
            qb.push(" ON CONFLICT (repo_id, id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index objects", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let repo_id = self.repo_id(repo).await?;
        let mut present = std::collections::HashSet::new();
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT id FROM objects_index WHERE repo_id = ");
            qb.push_bind(repo_id).push(" AND id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let found: Vec<Vec<u8>> = qb
                .build_query_scalar()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("find missing", e))?;
            present.extend(found);
        }
        Ok(subtract_present(ids, &present))
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT (repo_id, cs_id) DO NOTHING",
        )
        .bind(repo_id)
        .bind(rec.id.to_bytes().to_vec())
        .bind(rec.root_tree.to_bytes().to_vec())
        .bind(rec.first_parent.map(|p| p.to_bytes().to_vec()))
        .bind(version_to_db(rec.generation)?)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put changeset", e))?;
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let row = sqlx::query_as::<_, (Vec<u8>, Option<Vec<u8>>, i64)>(
            "SELECT root_tree, first_parent, generation FROM changesets WHERE repo_id = ? AND cs_id = ?",
        )
        .bind(repo_id)
        .bind(id.to_bytes().to_vec())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get changeset", e))?;
        row.map(|(root, parent, generation)| {
            Ok(ChangesetRecord {
                id: *id,
                root_tree: id_from_db(&root)?,
                first_parent: parent.as_deref().map(id_from_db).transpose()?,
                generation: version_from_db(generation)?,
            })
        })
        .transpose()
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        let repo_id = self.repo_id(repo).await?;
        let rows =
            sqlx::query_as::<_, (String, Vec<u8>, i64)>("SELECT name, target, version FROM refs WHERE repo_id = ?")
                .bind(repo_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("list refs", e))?;
        let mut refs = rows
            .into_iter()
            .map(|(name, target, version)| {
                Ok((
                    name,
                    RefState {
                        target: id_from_db(&target)?,
                        version: version_from_db(version)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // 按字节序排序，不受数据库 collation 影响
        refs.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Ok(refs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::testsuite;
    use std::sync::Arc;

    async fn fresh() -> (tempfile::TempDir, SqliteMetaStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let store = SqliteMetaStore::connect(&url).await.expect("open");
        (dir, store)
    }

    #[tokio::test]
    async fn conformance_meta() {
        let (_dir, s) = fresh().await;
        testsuite::meta_store(&s).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        let (_dir, s) = fresh().await;
        testsuite::object_index(&s).await;
    }

    #[tokio::test]
    async fn index_objects_large_batch_spans_chunks() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let objs: Vec<_> = (0..1_234u32)
            .map(|i| IndexedObject {
                id: ObjectId::of_blob(&i.to_le_bytes()),
                size: u64::from(i),
            })
            .collect();
        s.index_objects("r", &objs).await.expect("index");
        let mut ids: Vec<_> = objs.iter().map(|o| o.id).collect();
        let extra = ObjectId::of_blob(b"extra");
        ids.push(extra);
        assert_eq!(s.missing_objects("r", &ids).await, Ok(vec![extra]));
    }

    #[tokio::test]
    async fn index_objects_size_overflow_rejected() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let o = IndexedObject {
            id: ObjectId::of_blob(b"x"),
            size: u64::MAX,
        };
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        let (_dir, s) = fresh().await;
        testsuite::changesets_and_refs(&s).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        let (_dir, s) = fresh().await;
        testsuite::concurrent_cas(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn data_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        {
            let s = SqliteMetaStore::connect(&url).await.expect("open");
            s.create_repo("r").await.expect("create");
            s.cas_ref("r", "main", RefExpect::Absent, id).await.expect("cas");
        }
        let s = SqliteMetaStore::connect(&url)
            .await
            .expect("reopen runs migrations idempotently");
        assert_eq!(
            s.get_ref("r", "main").await,
            Ok(Some(RefState { target: id, version: 1 }))
        );
    }

    #[tokio::test]
    async fn cas_appends_ref_log() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let a = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        let b = ObjectId::hash(scion_object::ObjectKind::Changeset, b"b");
        s.cas_ref("r", "main", RefExpect::Absent, a).await.expect("v1");
        s.cas_ref("r", "main", RefExpect::Version(1), b).await.expect("v2");
        let _ = s
            .cas_ref("r", "main", RefExpect::Version(1), a)
            .await
            .expect_err("stale");
        let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM ref_log ORDER BY version")
            .fetch_all(&s.pool)
            .await
            .expect("query");
        assert_eq!(versions, [1, 2], "failed CAS must not be logged");
    }

    #[tokio::test]
    async fn cas_version_overflow_rejected() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let a = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        assert!(matches!(
            s.cas_ref("r", "main", RefExpect::Version(u64::MAX), a).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn corrupt_rows_reported_as_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'bad', x'00', 1)")
            .execute(&s.pool)
            .await
            .expect("insert");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'neg', ?, -1)")
            .bind(ObjectId::of_blob(b"").to_bytes().to_vec())
            .execute(&s.pool)
            .await
            .expect("insert");
        assert!(matches!(s.get_ref("r", "bad").await, Err(Error::Backend(_))));
        assert!(matches!(s.get_ref("r", "neg").await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn closed_pool_reports_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        s.pool.close().await;
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        assert!(matches!(s.create_repo("x").await, Err(Error::Backend(_))));
        assert!(matches!(s.repo_exists("r").await, Err(Error::Backend(_))));
        assert!(matches!(s.list_repos().await, Err(Error::Backend(_))));
        assert!(matches!(
            s.cas_ref("r", "m", RefExpect::Absent, id).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn object_index_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        // 让读写失败：删掉索引表
        sqlx::query("DROP TABLE objects_index")
            .execute(&s.pool)
            .await
            .expect("drop");
        let o = IndexedObject {
            id: ObjectId::of_blob(b"x"),
            size: 1,
        };
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
        assert!(matches!(s.missing_objects("r", &[o.id]).await, Err(Error::Backend(_))));
        s.pool.close().await;
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn changeset_corrupt_rows_and_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        sqlx::query("INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation) VALUES (1, ?, x'00', NULL, 1)")
            .bind(id.to_bytes().to_vec())
            .execute(&s.pool)
            .await
            .expect("insert");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'bad', x'00', 1)")
            .execute(&s.pool)
            .await
            .expect("insert");
        assert!(matches!(s.get_changeset("r", &id).await, Err(Error::Backend(_))));
        assert!(matches!(s.list_refs("r").await, Err(Error::Backend(_))));
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            first_parent: None,
            generation: u64::MAX,
        };
        assert!(matches!(s.put_changeset("r", &rec).await, Err(Error::Backend(_))));
        sqlx::query("DROP TABLE changesets")
            .execute(&s.pool)
            .await
            .expect("drop");
        let rec = ChangesetRecord { generation: 1, ..rec };
        assert!(matches!(s.put_changeset("r", &rec).await, Err(Error::Backend(_))));
        assert!(matches!(s.get_changeset("r", &id).await, Err(Error::Backend(_))));
        s.pool.close().await;
        assert!(matches!(s.list_refs("r").await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn connect_bad_url_and_unwritable_path() {
        assert!(matches!(
            SqliteMetaStore::connect("sqlite://\0").await.map(|_| ()),
            Err(Error::Backend(_))
        ));
        let r = SqliteMetaStore::connect("sqlite:///nonexistent-dir-scion/x/meta.db").await;
        assert!(matches!(r.map(|_| ()), Err(Error::Backend(_))));
    }
}
