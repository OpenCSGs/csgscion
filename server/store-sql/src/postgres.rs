//! Standard / Distributed 档：PostgreSQL，YugabyteDB（YSQL）走同一实现。

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use scion_object::ObjectId;
use scion_store::{
    ChangesetRecord, Error, IdempotencyRecord, IndexedObject, LandedChange, Lease, MetaStore, PathChange,
    PathHistoryEntry, Redaction, RedactionFilter, RefExpect, RefState, Result, TokenRecord, validate_repo_name,
};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

use crate::common::{
    CHANGESET_COLUMNS, ChangesetRow, MAX_IDS_PER_QUERY, REDACTION_COLUMNS, RedactionRow, backend, id_from_db,
    ids_to_db, is_unique_violation, now_ms, record_from_row, redaction_from_row, size_to_db, subtract_present,
    version_from_db, version_to_db,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

pub struct PgMetaStore {
    pool: PgPool,
}

impl PgMetaStore {
    /// 连接并执行迁移。URL 形如 `postgres://scion:***@db:5432/scion`。
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = PgConnectOptions::from_str(url).map_err(|e| backend("parse postgres url", e))?;
        Self::connect_with(opts).await
    }

    pub async fn connect_with(opts: PgConnectOptions) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(opts)
            .await
            .map_err(|e| backend("connect postgres", e))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| backend("migrate postgres", e.into()))?;
        Ok(Self { pool })
    }

    async fn repo_id(&self, repo: &str) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT repo_id FROM repos WHERE name = $1")
            .bind(repo)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("lookup repo", e))?
            .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
    }

    async fn current_version(&self, repo_id: i64, name: &str) -> Result<Option<u64>> {
        sqlx::query_scalar::<_, i64>("SELECT version FROM refs WHERE repo_id = $1 AND name = $2")
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
impl MetaStore for PgMetaStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        validate_repo_name(repo)?;
        sqlx::query("INSERT INTO repos (name, created_at_ms) VALUES ($1, $2)")
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
        // COLLATE "C"：按字节序，与其它后端一致，不受数据库 locale 影响
        sqlx::query_scalar::<_, String>(r#"SELECT name FROM repos ORDER BY name COLLATE "C""#)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("list repos", e))
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let repo_id = self.repo_id(repo).await?;
        let row =
            sqlx::query_as::<_, (Vec<u8>, i64)>("SELECT target, version FROM refs WHERE repo_id = $1 AND name = $2")
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

    async fn land_commit(
        &self,
        repo: &str,
        name: &str,
        expect: RefExpect,
        target: ObjectId,
        landed: &[LandedChange],
        requests: &[IdempotencyRecord],
    ) -> Result<RefState> {
        let repo_id = self.repo_id(repo).await?;
        let target_bytes = target.to_bytes().to_vec();
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 单行条件写：UPDATE 持有行锁，并发写者在行锁上排队，之后条件不再满足而失败，
        // 在 READ COMMITTED 下即可保证线性一致，不需要 SERIALIZABLE
        let (affected, new_version) = match expect {
            RefExpect::Absent => {
                let r = sqlx::query(
                    "INSERT INTO refs (repo_id, name, target, version) VALUES ($1, $2, $3, 1) \
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
                    "UPDATE refs SET target = $1, version = version + 1 \
                     WHERE repo_id = $2 AND name = $3 AND version = $4",
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
        sqlx::query("INSERT INTO ref_log (repo_id, name, version, target, updated_at_ms) VALUES ($1, $2, $3, $4, $5)")
            .bind(repo_id)
            .bind(name)
            .bind(version_to_db(new_version)?)
            .bind(&target_bytes)
            .bind(now_ms())
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("append ref log", e))?;
        let now = now_ms();
        for l in landed {
            sqlx::query(
                "INSERT INTO landed (repo_id, ref, change_id, changeset, landed, ref_version, landed_at_ms) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (repo_id, ref, change_id) DO NOTHING",
            )
            .bind(repo_id)
            .bind(name)
            .bind(l.change_id.to_vec())
            .bind(l.changeset.to_bytes().to_vec())
            .bind(l.landed.to_bytes().to_vec())
            .bind(version_to_db(l.ref_version)?)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("record landed change", e))?;
        }
        for r in requests {
            sqlx::query(
                "INSERT INTO idempotency (repo_id, request_id, fingerprint, response, created_at_ms) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (repo_id, request_id) DO NOTHING",
            )
            .bind(repo_id)
            .bind(&r.request_id)
            .bind(&r.fingerprint)
            .bind(&r.response)
            .bind(r.created_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("record request result", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(RefState {
            target,
            version: new_version,
        })
    }

    async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = Vec::new();
        for chunk in change_ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "SELECT change_id, changeset, landed, ref_version FROM landed WHERE repo_id = ",
            );
            qb.push_bind(repo_id)
                .push(" AND ref = ")
                .push_bind(name)
                .push(" AND change_id IN (");
            let mut sep = qb.separated(", ");
            for c in chunk {
                sep.push_bind(c.to_vec());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, i64)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("landed changes", e))?;
            for (change_id, changeset, landed, version) in rows {
                out.push(LandedChange {
                    change_id: change_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::Backend(format!("corrupt change id ({} bytes)", change_id.len())))?,
                    changeset: id_from_db(&changeset)?,
                    landed: id_from_db(&landed)?,
                    ref_version: version_from_db(version)?,
                });
            }
        }
        Ok(out)
    }

    async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let row: Option<(Vec<u8>, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT fingerprint, response, created_at_ms FROM idempotency WHERE repo_id = $1 AND request_id = $2",
        )
        .bind(repo_id)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get request result", e))?;
        Ok(row.map(|(fingerprint, response, created_at_ms)| IdempotencyRecord {
            request_id: request_id.to_owned(),
            fingerprint,
            response,
            created_at_ms,
        }))
    }

    async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease> {
        // 条件 upsert：没有、已过期或自己持有时写入；否则不变。随后读出当前租约
        sqlx::query(
            "INSERT INTO leases (scope, holder, token, expires_at_ms) VALUES ($1, $2, 1, $3) \
             ON CONFLICT (scope) DO UPDATE SET \
               token = CASE WHEN leases.holder = excluded.holder THEN leases.token ELSE leases.token + 1 END, \
               holder = excluded.holder, expires_at_ms = excluded.expires_at_ms \
             WHERE leases.holder = excluded.holder OR leases.expires_at_ms <= $4",
        )
        .bind(scope)
        .bind(holder)
        .bind(now_ms.saturating_add(ttl_ms))
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("acquire lease", e))?;
        let (holder, token, expires_at_ms): (String, i64, i64) =
            sqlx::query_as("SELECT holder, token, expires_at_ms FROM leases WHERE scope = $1")
                .bind(scope)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| backend("read lease", e))?;
        Ok(Lease {
            holder,
            token: version_from_db(token)?,
            expires_at_ms,
        })
    }

    async fn put_token(&self, t: &TokenRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO tokens (id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&t.id)
        .bind(&t.hash[..])
        .bind(&t.kind)
        .bind(&t.subject)
        .bind(crate::common::join_groups(&t.groups))
        .bind(&t.description)
        .bind(t.created_at_ms)
        .bind(t.expires_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put token", e))?;
        Ok(())
    }

    async fn get_token(&self, hash: &[u8; 32]) -> Result<Option<TokenRecord>> {
        let row: Option<crate::common::TokenRow> = sqlx::query_as(
            "SELECT id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms \
             FROM tokens WHERE hash = $1",
        )
        .bind(&hash[..])
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get token", e))?;
        row.map(crate::common::token_from_row).transpose()
    }

    async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>> {
        let rows: Vec<crate::common::TokenRow> = sqlx::query_as(
            "SELECT id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms \
             FROM tokens WHERE $1::TEXT IS NULL OR subject = $2 ORDER BY created_at_ms, id",
        )
        .bind(subject)
        .bind(subject)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list tokens", e))?;
        rows.into_iter().map(crate::common::token_from_row).collect()
    }

    async fn delete_token(&self, id: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM tokens WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("delete token", e))?;
        Ok(r.rows_affected() > 0)
    }

    async fn get_setting(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT value FROM settings WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("get setting", e))?;
        Ok(row.map(|(v,)| v))
    }

    async fn put_setting(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put setting", e))?;
        Ok(())
    }

    async fn put_redactions(&self, entries: &[Redaction]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut repo_ids = std::collections::HashMap::new();
        for e in entries {
            if !repo_ids.contains_key(&e.repo) {
                repo_ids.insert(e.repo.clone(), self.repo_id(&e.repo).await?);
            }
        }
        let n = i64::try_from(entries.len()).map_err(|_| Error::Backend("too many redactions".into()))?;
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 计数器的行锁让写入按 seq 顺序提交
        let last: i64 = sqlx::query_scalar("UPDATE redaction_seq SET seq = seq + $1 WHERE id = 1 RETURNING seq")
            .bind(n)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| backend("allocate redaction seq", e))?;
        for (i, e) in entries.iter().enumerate() {
            let seq = last - n + 1 + i as i64;
            sqlx::query(
                "INSERT INTO redactions (repo_id, object_id, redaction_id, reason, created_by, created_at_ms, \
                 active, purge_after_ms, purged_at_ms, seq) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                 ON CONFLICT (repo_id, object_id) DO UPDATE SET redaction_id = excluded.redaction_id, \
                 reason = excluded.reason, created_by = excluded.created_by, created_at_ms = excluded.created_at_ms, \
                 active = excluded.active, purge_after_ms = excluded.purge_after_ms, \
                 purged_at_ms = excluded.purged_at_ms, seq = excluded.seq",
            )
            .bind(repo_ids[&e.repo])
            .bind(e.id.to_bytes().to_vec())
            .bind(&e.redaction_id)
            .bind(&e.reason)
            .bind(&e.created_by)
            .bind(e.created_at_ms)
            .bind(e.active)
            .bind(e.purge_after_ms)
            .bind(e.purged_at_ms)
            .bind(seq)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("put redaction", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))
    }

    async fn get_redactions(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<Redaction>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = Vec::new();
        let mut unique = ids.to_vec();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        for chunk in unique.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
                "SELECT {} FROM redactions x JOIN repos r ON r.repo_id = x.repo_id WHERE x.repo_id = ",
                REDACTION_COLUMNS
            ));
            qb.push_bind(repo_id).push(" AND x.object_id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let rows: Vec<RedactionRow> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("get redactions", e))?;
            for row in rows {
                out.push(redaction_from_row(row)?);
            }
        }
        Ok(out)
    }

    async fn list_redactions(&self, filter: &RedactionFilter) -> Result<Vec<Redaction>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
            "SELECT {} FROM redactions x JOIN repos r ON r.repo_id = x.repo_id WHERE x.seq > ",
            REDACTION_COLUMNS
        ));
        qb.push_bind(version_to_db(filter.after_seq)?);
        if let Some(repo) = &filter.repo {
            qb.push(" AND r.name = ").push_bind(repo.clone());
        }
        if let Some(rid) = &filter.redaction_id {
            qb.push(" AND x.redaction_id = ").push_bind(rid.clone());
        }
        if let Some(t) = filter.purge_due_at {
            qb.push(" AND x.active = ")
                .push_bind(true)
                .push(" AND x.purged_at_ms IS NULL AND x.purge_after_ms <= ")
                .push_bind(t);
        }
        qb.push(" ORDER BY x.seq");
        if filter.limit > 0 {
            qb.push(" LIMIT ")
                .push_bind(i64::try_from(filter.limit).unwrap_or(i64::MAX));
        }
        let rows: Vec<RedactionRow> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("list redactions", e))?;
        rows.into_iter().map(redaction_from_row).collect()
    }

    async fn object_repos(&self, id: &ObjectId) -> Result<Vec<String>> {
        // 逐个仓库按主键探测：objects_index 没有按 id 的索引（写入热路径上不值得多维护一个）
        sqlx::query_scalar(
            "SELECT r.name FROM repos r WHERE EXISTS \
             (SELECT 1 FROM objects_index o WHERE o.repo_id = r.repo_id AND o.id = $1) ORDER BY r.name",
        )
        .bind(id.to_bytes().to_vec())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("object repos", e))
    }

    async fn list_leases(&self) -> Result<Vec<(String, Lease)>> {
        let rows: Vec<(String, String, i64, i64)> =
            sqlx::query_as("SELECT scope, holder, token, expires_at_ms FROM leases ORDER BY scope")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("list leases", e))?;
        rows.into_iter()
            .map(|(scope, holder, token, expires_at_ms)| {
                Ok((
                    scope,
                    Lease {
                        holder,
                        token: version_from_db(token)?,
                        expires_at_ms,
                    },
                ))
            })
            .collect()
    }

    async fn release_lease(&self, scope: &str, holder: &str) -> Result<()> {
        sqlx::query("UPDATE leases SET expires_at_ms = 0 WHERE scope = $1 AND holder = $2")
            .bind(scope)
            .bind(holder)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("release lease", e))?;
        Ok(())
    }

    async fn expire_idempotency(&self, before_ms: i64) -> Result<u64> {
        let r = sqlx::query("DELETE FROM idempotency WHERE created_at_ms < $1")
            .bind(before_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("expire request results", e))?;
        Ok(r.rows_affected())
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
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("INSERT INTO objects_index (repo_id, id, size) ");
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
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT id FROM objects_index WHERE repo_id = ");
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

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<std::collections::HashMap<ObjectId, u64>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = std::collections::HashMap::new();
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb =
                sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT id, size FROM objects_index WHERE repo_id = ");
            qb.push_bind(repo_id).push(" AND id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, i64)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("object sizes", e))?;
            for (id, size) in rows {
                out.insert(id_from_db(&id)?, version_from_db(size)?);
            }
        }
        Ok(out)
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth, fp_jump) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (repo_id, cs_id) DO NOTHING",
        )
        .bind(repo_id)
        .bind(rec.id.to_bytes().to_vec())
        .bind(rec.root_tree.to_bytes().to_vec())
        .bind(rec.first_parent().map(|p| p.to_bytes().to_vec()))
        .bind(version_to_db(rec.generation)?)
        .bind(ids_to_db(&rec.parents))
        .bind(version_to_db(rec.fp_depth)?)
        .bind(rec.fp_jump.map(|j| j.to_bytes().to_vec()))
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put changeset", e))?;
        Ok(())
    }

    async fn put_changesets(&self, repo: &str, recs: &[ChangesetRecord]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if recs.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 每行 8 个参数
        for chunk in recs.chunks(MAX_IDS_PER_QUERY / 8) {
            let mut rows = Vec::with_capacity(chunk.len());
            for r in chunk {
                rows.push((
                    r.id.to_bytes().to_vec(),
                    r.root_tree.to_bytes().to_vec(),
                    r.first_parent().map(|p| p.to_bytes().to_vec()),
                    version_to_db(r.generation)?,
                    ids_to_db(&r.parents),
                    version_to_db(r.fp_depth)?,
                    r.fp_jump.map(|j| j.to_bytes().to_vec()),
                ));
            }
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth, fp_jump) ",
            );
            qb.push_values(rows, |mut b, (id, root, first, generation, parents, depth, jump)| {
                b.push_bind(repo_id)
                    .push_bind(id)
                    .push_bind(root)
                    .push_bind(first)
                    .push_bind(generation)
                    .push_bind(parents)
                    .push_bind(depth)
                    .push_bind(jump);
            });
            qb.push(" ON CONFLICT (repo_id, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("put changesets", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let sql = format!("SELECT {CHANGESET_COLUMNS} FROM changesets WHERE repo_id = $1 AND cs_id = $2");
        sqlx::query_as::<_, ChangesetRow>(&sql)
            .bind(repo_id)
            .bind(id.to_bytes().to_vec())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("get changeset", e))?
            .map(record_from_row)
            .transpose()
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let sql = format!(
            "SELECT {CHANGESET_COLUMNS} FROM changesets WHERE repo_id = $1 AND generation <= $2 \
             ORDER BY generation DESC LIMIT $3"
        );
        sqlx::query_as::<_, ChangesetRow>(&sql)
            .bind(repo_id)
            .bind(version_to_db(max_generation.min(i64::MAX as u64))?)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("changesets below", e))?
            .into_iter()
            .map(record_from_row)
            .collect()
    }

    async fn repair_lineage(
        &self,
        repo: &str,
        id: &ObjectId,
        parents: &[ObjectId],
        fp_depth: u64,
        fp_jump: Option<ObjectId>,
    ) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "UPDATE changesets SET parents = $1, first_parent = $2, fp_depth = $3, fp_jump = $4 \
             WHERE repo_id = $5 AND cs_id = $6",
        )
        .bind(ids_to_db(parents))
        .bind(parents.first().map(|p| p.to_bytes().to_vec()))
        .bind(version_to_db(fp_depth)?)
        .bind(fp_jump.map(|j| j.to_bytes().to_vec()))
        .bind(repo_id)
        .bind(id.to_bytes().to_vec())
        .execute(&self.pool)
        .await
        .map_err(|e| backend("repair lineage", e))?;
        Ok(())
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if changes.is_empty() {
            return Ok(());
        }
        let generation = version_to_db(generation)?;
        let cs_bytes = cs.to_bytes().to_vec();
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        for chunk in changes.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) ",
            );
            qb.push_values(chunk, |mut b, c| {
                b.push_bind(repo_id)
                    .push_bind(c.path.clone())
                    .push_bind(cs_bytes.clone())
                    .push_bind(generation)
                    .push_bind(c.deleted);
            });
            qb.push(" ON CONFLICT (repo_id, path, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index paths", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn index_paths_many(&self, repo: &str, entries: &[(ObjectId, u64, Vec<PathChange>)]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        let mut rows = Vec::new();
        for (cs, generation, changes) in entries {
            let generation = version_to_db(*generation)?;
            let cs = cs.to_bytes().to_vec();
            rows.extend(
                changes
                    .iter()
                    .map(|c| (c.path.clone(), cs.clone(), generation, c.deleted)),
            );
        }
        if rows.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 每行 5 个参数
        for chunk in rows.chunks(MAX_IDS_PER_QUERY / 5) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) ",
            );
            qb.push_values(chunk, |mut b, (path, cs, generation, deleted)| {
                b.push_bind(repo_id)
                    .push_bind(path.clone())
                    .push_bind(cs.clone())
                    .push_bind(*generation)
                    .push_bind(*deleted);
            });
            qb.push(" ON CONFLICT (repo_id, path, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index paths", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        let repo_id = self.repo_id(repo).await?;
        // 目录下的路径：区间 [path + "/", path + "0")，'0' 是 '/' 的下一个字符
        let rows = sqlx::query_as::<_, (Vec<u8>, i64, String, bool)>(
            "SELECT cs_id, generation, path, deleted FROM path_changes \
             WHERE repo_id = $1 AND (path = $2 OR (path >= $3 AND path < $4)) AND generation <= $5 \
             ORDER BY generation DESC, path LIMIT $6",
        )
        .bind(repo_id)
        .bind(path)
        .bind(format!("{path}/"))
        .bind(format!("{path}0"))
        .bind(version_to_db(max_generation.min(i64::MAX as u64))?)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("path history", e))?;
        rows.into_iter()
            .map(|(cs, generation, path, deleted)| {
                Ok(PathHistoryEntry {
                    cs: id_from_db(&cs)?,
                    generation: version_from_db(generation)?,
                    path,
                    deleted,
                })
            })
            .collect()
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT value FROM derived WHERE repo_id = $1 AND kind = $2 AND version = $3 AND key = $4",
        )
        .bind(repo_id)
        .bind(kind)
        .bind(i64::from(version))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get derived", e))
    }

    async fn get_derived_many(
        &self,
        repo: &str,
        kind: &str,
        version: u32,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let repo_id = self.repo_id(repo).await?;
        let mut found: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT key, value FROM derived WHERE repo_id = ");
            qb.push_bind(repo_id)
                .push(" AND kind = ")
                .push_bind(kind)
                .push(" AND version = ")
                .push_bind(i64::from(version))
                .push(" AND key IN (");
            let mut sep = qb.separated(", ");
            for key in chunk {
                sep.push_bind(key.as_slice());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, Vec<u8>)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("get derived", e))?;
            found.extend(rows);
        }
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "INSERT INTO derived (repo_id, kind, version, key, value) VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (repo_id, kind, version, key) DO NOTHING",
        )
        .bind(repo_id)
        .bind(kind)
        .bind(i64::from(version))
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put derived", e))?;
        Ok(())
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        let repo_id = self.repo_id(repo).await?;
        let rows =
            sqlx::query_as::<_, (String, Vec<u8>, i64)>("SELECT name, target, version FROM refs WHERE repo_id = $1")
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

/// PG 测试：需要环境变量 `SCION_TEST_PG_URL`（CI 中由 PostgreSQL 服务容器提供）；
/// 未设置时跳过。每个测试使用独立的 schema，互不干扰。
#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::testsuite;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    async fn fresh() -> Option<PgMetaStore> {
        let url = std::env::var("SCION_TEST_PG_URL").ok()?;
        static N: AtomicU32 = AtomicU32::new(0);
        let schema = format!("t{}_{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed));
        let admin = PgPool::connect(&url).await.expect("connect admin");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .expect("create schema");
        let opts = PgConnectOptions::from_str(&url)
            .expect("url")
            .options([("search_path", schema.as_str())]);
        Some(PgMetaStore::connect_with(opts).await.expect("connect"))
    }

    macro_rules! pg_or_skip {
        () => {
            match fresh().await {
                Some(s) => s,
                None => {
                    eprintln!("SCION_TEST_PG_URL not set; skipping");
                    return;
                }
            }
        };
    }

    /// 从 M1 升级：parents 为空、只有 first_parent 的旧行被修复，跳表被标记为未知；
    /// 没有旧行的仓库不受影响。
    #[tokio::test]
    async fn upgrade_restores_parents_of_m1_rows() {
        let Ok(url) = std::env::var("SCION_TEST_PG_URL") else {
            eprintln!("SCION_TEST_PG_URL not set; skipping");
            return;
        };
        static N: AtomicU32 = AtomicU32::new(0);
        let schema = format!("up{}_{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed));
        let admin = PgPool::connect(&url).await.expect("connect admin");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .expect("create schema");
        let opts = PgConnectOptions::from_str(&url)
            .expect("url")
            .options([("search_path", schema.as_str())]);
        let before = sqlx::migrate::Migrator {
            migrations: MIGRATOR
                .migrations
                .iter()
                .filter(|m| m.version <= 7)
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        let id = |s: &[u8]| ObjectId::hash(scion_object::ObjectKind::Changeset, s);
        let (a, b, c, d) = (id(b"a"), id(b"b"), id(b"c"), id(b"d"));
        {
            let pool = PgPool::connect_with(opts.clone()).await.expect("open");
            before.run(&pool).await.expect("migrate to 0007");
            sqlx::query("INSERT INTO repos (repo_id, name, created_at_ms) VALUES (1, 'old', 0), (2, 'new', 0)")
                .execute(&pool)
                .await
                .expect("repos");
            // old：M1 写入的行（父提交只在 first_parent），深度已被错误地算成 1
            // new：M2 写入的正确数据
            for (repo, cs, first, parents, depth) in [
                (1, a, None, vec![], 1),
                (1, b, Some(a), vec![], 1),
                (2, c, None, vec![], 1),
                (2, d, Some(c), c.to_bytes().to_vec(), 2),
            ] {
                sqlx::query(
                    "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth) \
                     VALUES ($1, $2, $6, $3, 1, $4, $5)",
                )
                .bind(repo as i64)
                .bind(cs.to_bytes().to_vec())
                .bind(first.map(|f: ObjectId| f.to_bytes().to_vec()))
                .bind(parents)
                .bind(depth as i64)
                .bind(cs.to_bytes().to_vec())
                .execute(&pool)
                .await
                .expect("row");
            }
            pool.close().await;
        }
        let s = PgMetaStore::connect_with(opts).await.expect("upgrade");
        let get = |repo: &'static str, cs| {
            let s = &s;
            async move {
                let r = s.get_changeset(repo, &cs).await.expect("ok").expect("rec");
                (r.parents, r.fp_depth)
            }
        };
        assert_eq!(get("old", b).await, (vec![a], 0), "parent restored, skip unknown");
        assert_eq!(get("old", a).await, (vec![], 0), "whole repo marked for reindex");
        assert_eq!(get("new", d).await, (vec![c], 2), "untouched");
        assert_eq!(get("new", c).await, (vec![], 1), "untouched");
    }

    #[tokio::test]
    async fn conformance_meta() {
        let s = pg_or_skip!();
        testsuite::meta_store(&s).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        let s = pg_or_skip!();
        testsuite::object_index(&s).await;
    }

    #[tokio::test]
    async fn index_objects_large_batch_spans_chunks() {
        let s = pg_or_skip!();
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
        let s = pg_or_skip!();
        s.create_repo("r").await.expect("create");
        let o = IndexedObject {
            id: ObjectId::of_blob(b"x"),
            size: u64::MAX,
        };
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        let s = pg_or_skip!();
        testsuite::changesets_and_refs(&s).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        let s = pg_or_skip!();
        testsuite::derived(&s).await;
    }

    #[tokio::test]
    async fn conformance_landing() {
        let s = pg_or_skip!();
        testsuite::landing(&s).await;
    }

    #[tokio::test]
    async fn conformance_leases() {
        let s = pg_or_skip!();
        testsuite::leases(&s).await;
        testsuite::tokens_and_settings(&s).await;
        testsuite::redactions(&s).await;
    }

    #[tokio::test]
    async fn conformance_batch_writes() {
        let s = pg_or_skip!();
        testsuite::batch_writes(&s).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        let s = pg_or_skip!();
        testsuite::history_index(&s).await;
    }

    #[tokio::test]
    async fn conformance_fp_skip_chain() {
        let s = pg_or_skip!();
        testsuite::fp_skip_chain(&s).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        let s = pg_or_skip!();
        testsuite::concurrent_cas(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn cas_appends_ref_log_and_rejects_overflow() {
        let s = pg_or_skip!();
        s.create_repo("r").await.expect("create");
        let a = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        s.cas_ref("r", "main", RefExpect::Absent, a).await.expect("v1");
        let _ = s
            .cas_ref("r", "main", RefExpect::Version(9), a)
            .await
            .expect_err("stale");
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM ref_log")
            .fetch_one(&s.pool)
            .await
            .expect("q");
        assert_eq!(n, 1);
        assert!(matches!(
            s.cas_ref("r", "main", RefExpect::Version(u64::MAX), a).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn closed_pool_reports_backend_errors() {
        let s = pg_or_skip!();
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
    async fn connect_bad_url_fails() {
        assert!(matches!(
            PgMetaStore::connect("postgres://[bad").await.map(|_| ()),
            Err(Error::Backend(_))
        ));
    }
}
