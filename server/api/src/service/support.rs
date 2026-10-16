//! 各服务共用的小工具。

use scion_object::{ObjectId, ObjectKind};
use scion_store::{ChangesetRecord, MetaStore};
use tonic::Status;

use crate::status;

pub(crate) fn parse_id(raw: &[u8]) -> Result<ObjectId, Status> {
    ObjectId::from_bytes(raw).map_err(|e| status::invalid_object_id(raw, &e))
}

pub(crate) async fn check_repo(meta: &dyn MetaStore, repo: &str) -> Result<(), Status> {
    match meta.repo_exists(repo).await.map_err(status::from_store)? {
        true => Ok(()),
        false => Err(status::repo_not_found(repo)),
    }
}

/// 把 revision（ref 名，或提交 id 的文本形式 `c1…`）解析为提交记录。
pub(crate) async fn resolve_revision(
    meta: &dyn MetaStore,
    repo: &str,
    revision: &str,
) -> Result<ChangesetRecord, Status> {
    check_repo(meta, repo).await?;
    let cs_id = match revision.parse::<ObjectId>() {
        Ok(id) if id.kind() == ObjectKind::Changeset => id,
        _ => match meta.get_ref(repo, revision).await.map_err(status::from_store)? {
            Some(r) => r.target,
            None => return Err(status::ref_not_found(revision)),
        },
    };
    meta.get_changeset(repo, &cs_id)
        .await
        .map_err(status::from_store)?
        .ok_or_else(|| status::ref_not_found(revision))
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::{MemStore, RefExpect};
    use tonic::Code;

    fn cs(n: u8) -> ObjectId {
        ObjectId::hash(ObjectKind::Changeset, &[n])
    }

    #[test]
    fn parse_id_cases() {
        let id = ObjectId::of_blob(b"x");
        assert_eq!(parse_id(&id.to_bytes()).ok(), Some(id));
        assert_eq!(
            parse_id(b"short").map_err(|s| s.code()).err(),
            Some(Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn check_repo_cases() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        assert!(check_repo(&s, "r").await.is_ok());
        assert_eq!(
            check_repo(&s, "x").await.map_err(|s| s.code()).err(),
            Some(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn resolve_revision_by_ref_and_id() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let tree = ObjectId::hash(ObjectKind::Tree, b"t");
        let rec = ChangesetRecord {
            id: cs(1),
            root_tree: tree,
            first_parent: None,
            generation: 1,
        };
        s.put_changeset("r", &rec).await.expect("put");
        s.cas_ref("r", "main", RefExpect::Absent, cs(1)).await.expect("ref");
        assert_eq!(resolve_revision(&s, "r", "main").await.ok(), Some(rec));
        assert_eq!(resolve_revision(&s, "r", &cs(1).to_string()).await.ok(), Some(rec));
        let code = |r: Result<ChangesetRecord, Status>| r.map_err(|s| s.code()).err();
        assert_eq!(code(resolve_revision(&s, "r", "nope").await), Some(Code::NotFound));
        assert_eq!(
            code(resolve_revision(&s, "r", &cs(9).to_string()).await),
            Some(Code::NotFound)
        );
        assert_eq!(code(resolve_revision(&s, "x", "main").await), Some(Code::NotFound));
        // 指向未记录提交的 ref（不应出现，但要报错而不是崩溃）
        s.cas_ref("r", "dangling", RefExpect::Absent, cs(7)).await.expect("ref");
        assert_eq!(code(resolve_revision(&s, "r", "dangling").await), Some(Code::NotFound));
        // 不是提交 id 的 ObjectId 文本按 ref 名处理
        let blob = ObjectId::of_blob(b"x").to_string();
        assert_eq!(code(resolve_revision(&s, "r", &blob).await), Some(Code::NotFound));
    }
}
