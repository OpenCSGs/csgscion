//! QueryService 中的历史查询：Log、IsAncestor、MergeBase、Diff。

use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind, Signature};
use scion_proto::v1::{
    self, DiffRequest, DiffResponse, IsAncestorRequest, IsAncestorResponse, LogRequest, LogResponse, MergeBaseRequest,
    MergeBaseResponse,
};
use scion_query::{LogEntry, QueryError};
use scion_store::{BlobStore, MetaStore};
use scion_tree::BlobTreeSource;
use tonic::Status;

use crate::service::query::{check_path, proto_entry};
use crate::service::support::resolve_revision;
use crate::status;

pub(crate) const DEFAULT_PAGE_SIZE: u32 = 50;
pub(crate) const MAX_PAGE_SIZE: u32 = 1000;
/// 每页最多检查的提交数（按路径过滤时的延迟上限）。
pub(crate) const MAX_SCAN: usize = 10_000;

/// 分页令牌：前沿提交 id 依次拼接后的十六进制。
pub(crate) fn encode_token(ids: &[ObjectId]) -> String {
    hex::encode(ids.iter().flat_map(|id| id.to_bytes()).collect::<Vec<u8>>())
}

pub(crate) fn decode_token(token: &str) -> Result<Vec<ObjectId>, Status> {
    let bad = || Status::invalid_argument("invalid page_token");
    let raw = hex::decode(token).map_err(|_| bad())?;
    if raw.is_empty() || raw.len() % scion_object::ID_LEN != 0 {
        return Err(bad());
    }
    raw.chunks(scion_object::ID_LEN)
        .map(|c| match ObjectId::from_bytes(c) {
            Ok(id) if id.kind() == ObjectKind::Changeset => Ok(id),
            _ => Err(bad()),
        })
        .collect()
}

pub(crate) fn from_query(e: QueryError) -> Status {
    match e {
        QueryError::NotFound(id) => Status::not_found(format!("changeset {id} not found")),
        QueryError::Corrupt(id, msg) => {
            tracing::error!(%id, error = %msg, "changeset object missing or corrupt");
            Status::internal(format!("changeset {id} is missing or corrupt"))
        }
        QueryError::Store(e) => status::from_store(e),
    }
}

fn proto_sig(s: &Signature) -> v1::Signature {
    v1::Signature {
        name: s.name.clone(),
        email: s.email.clone(),
        time_ms: s.time_ms,
        tz_offset_minutes: s.tz_offset_minutes,
    }
}

fn id_bytes(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

fn log_entry(e: LogEntry) -> v1::LogEntry {
    v1::LogEntry {
        changeset_id: id_bytes(&e.record.id),
        parents: e.record.parents.iter().map(id_bytes).collect(),
        author: Some(proto_sig(&e.changeset.author)),
        committer: Some(proto_sig(&e.changeset.committer)),
        message: e.changeset.message,
        generation: e.record.generation,
        root_tree: id_bytes(&e.record.root_tree),
        extensions: e
            .changeset
            .extensions
            .into_iter()
            .map(|(k, v)| (k, Bytes::from(v)))
            .collect(),
    }
}

pub(crate) async fn log(meta: &dyn MetaStore, blobs: &dyn BlobStore, req: LogRequest) -> Result<LogResponse, Status> {
    check_path(&req.path)?;
    let start = if req.page_token.is_empty() {
        vec![resolve_revision(meta, &req.repo, &req.revision).await?.id]
    } else {
        decode_token(&req.page_token)?
    };
    let limit = match req.page_size {
        0 => DEFAULT_PAGE_SIZE,
        n => n.min(MAX_PAGE_SIZE),
    } as usize;
    let page = scion_query::log(meta, blobs, &req.repo, &start, Some(req.path.as_str()), limit, MAX_SCAN)
        .await
        .map_err(from_query)?;
    Ok(LogResponse {
        entries: page.entries.into_iter().map(log_entry).collect(),
        next_page_token: page.next.map(|f| encode_token(&f)).unwrap_or_default(),
    })
}

pub(crate) async fn is_ancestor(meta: &dyn MetaStore, req: IsAncestorRequest) -> Result<IsAncestorResponse, Status> {
    let a = resolve_revision(meta, &req.repo, &req.ancestor).await?.id;
    let d = resolve_revision(meta, &req.repo, &req.descendant).await?.id;
    let yes = scion_query::is_ancestor(meta, &req.repo, a, d)
        .await
        .map_err(from_query)?;
    Ok(IsAncestorResponse { is_ancestor: yes })
}

pub(crate) async fn merge_base(meta: &dyn MetaStore, req: MergeBaseRequest) -> Result<MergeBaseResponse, Status> {
    let a = resolve_revision(meta, &req.repo, &req.a).await?.id;
    let b = resolve_revision(meta, &req.repo, &req.b).await?.id;
    let bases = scion_query::merge_bases(meta, &req.repo, a, b)
        .await
        .map_err(from_query)?;
    Ok(MergeBaseResponse {
        bases: bases.iter().map(id_bytes).collect(),
    })
}

pub(crate) async fn diff(
    meta: &dyn MetaStore,
    blobs: std::sync::Arc<dyn BlobStore>,
    req: DiffRequest,
) -> Result<DiffResponse, Status> {
    check_path(&req.path)?;
    let from = if req.from.is_empty() {
        None
    } else {
        Some(resolve_revision(meta, &req.repo, &req.from).await?.root_tree)
    };
    let to = resolve_revision(meta, &req.repo, &req.to).await?.root_tree;
    let files = scion_tree::diff(&BlobTreeSource(blobs), from, Some(to), &req.path)
        .await
        .map_err(status::from_tree)?;
    Ok(DiffResponse {
        files: files
            .into_iter()
            .map(|d| v1::FileDiff {
                path: d.path,
                old: d.old.as_ref().map(proto_entry),
                new: d.new.as_ref().map(proto_entry),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use scion_store::ChangesetRecord;
    use std::sync::Arc;
    use tonic::Code;

    fn cs(n: u8) -> ObjectId {
        ObjectId::hash(ObjectKind::Changeset, &[n])
    }

    #[test]
    fn token_roundtrip_and_validation() {
        let ids = vec![cs(1), cs(2)];
        assert_eq!(decode_token(&encode_token(&ids)).ok(), Some(ids));
        let code = |s: &str| decode_token(s).map(|_| ()).map_err(|e| e.code());
        assert_eq!(code("zz"), Err(Code::InvalidArgument), "not hex");
        assert_eq!(code(""), Err(Code::InvalidArgument), "empty");
        assert_eq!(code("0102"), Err(Code::InvalidArgument), "wrong length");
        let blob = encode_token(&[ObjectId::of_blob(b"x")]);
        assert_eq!(code(&blob), Err(Code::InvalidArgument), "not a changeset id");
    }

    #[test]
    fn from_query_maps_every_variant() {
        assert_eq!(from_query(QueryError::NotFound(cs(1))).code(), Code::NotFound);
        assert_eq!(
            from_query(QueryError::Corrupt(cs(1), "x".into())).code(),
            Code::Internal
        );
        assert_eq!(
            from_query(QueryError::Store(scion_store::Error::Backend("x".into()))).code(),
            Code::Unavailable
        );
    }

    /// main: a ─ b ─ c，另有从 a 分出的 d。
    async fn setup() -> (Arc<scion_store::MemStore>, [ChangesetRecord; 4]) {
        let (store, w) = svc().await;
        for c in [&b"1"[..], b"2", b"3", b"4"] {
            upload(&w, c).await;
        }
        let mk = |p: &[ObjectId], d| spec_to_changeset(spec(p, d), 10).expect("spec");
        let a = w.create(REPO, &mk(&[], vec![put("README", b"1")])).await.expect("a");
        let b = w
            .create(REPO, &mk(&[a.id], vec![put("src/x.rs", b"2")]))
            .await
            .expect("b");
        let c = w
            .create(REPO, &mk(&[b.id], vec![put("README", b"3")]))
            .await
            .expect("c");
        let d = w.create(REPO, &mk(&[a.id], vec![put("other", b"4")])).await.expect("d");
        let land = LandSvc::new(store.clone());
        for x in [&a, &b, &c] {
            land.land(REPO, "main", x.id).await.expect("land");
        }
        (store, [a, b, c, d])
    }

    fn log_req(revision: &str, path: &str, page_size: u32, token: &str) -> LogRequest {
        LogRequest {
            repo: REPO.into(),
            revision: revision.into(),
            path: path.into(),
            page_size,
            page_token: token.into(),
        }
    }

    #[tokio::test]
    async fn log_pages_and_filters() {
        let (store, [a, b, c, _d]) = setup().await;
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "", 2, ""))
            .await
            .expect("page 1");
        let ids: Vec<_> = r
            .entries
            .iter()
            .map(|e| ObjectId::from_bytes(&e.changeset_id).expect("id"))
            .collect();
        assert_eq!(ids, vec![c.id, b.id]);
        assert_eq!(r.entries[0].author.as_ref().expect("author").name, "Ada");
        assert_eq!(r.entries[0].generation, 3);
        assert_eq!(ObjectId::from_bytes(&r.entries[0].root_tree).ok(), Some(c.root_tree));
        let r2 = log(store.as_ref(), store.as_ref(), log_req("", "", 2, &r.next_page_token))
            .await
            .expect("page 2");
        assert_eq!(r2.entries.len(), 1);
        assert_eq!(ObjectId::from_bytes(&r2.entries[0].changeset_id).ok(), Some(a.id));
        assert!(r2.next_page_token.is_empty());
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "README", 0, ""))
            .await
            .expect("filtered");
        assert_eq!(r.entries.len(), 2, "default page size; only commits touching README");
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "", 5000, ""))
            .await
            .expect("clamped");
        assert_eq!(r.entries.len(), 3);
    }

    #[tokio::test]
    async fn log_errors() {
        let (store, _) = setup().await;
        let code = |r: Result<LogResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("nope", "", 1, "")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("main", "/bad", 1, "")).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("", "", 1, "zz")).await),
            Err(Code::InvalidArgument)
        );
        let unknown = encode_token(&[cs(99)]);
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("", "", 1, &unknown)).await),
            Err(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn ancestry_queries() {
        let (store, [a, _b, c, d]) = setup().await;
        let anc = |x: &ObjectId, y: &str| IsAncestorRequest {
            repo: REPO.into(),
            ancestor: x.to_string(),
            descendant: y.into(),
        };
        assert!(
            is_ancestor(store.as_ref(), anc(&a.id, "main"))
                .await
                .expect("ok")
                .is_ancestor
        );
        assert!(
            !is_ancestor(store.as_ref(), anc(&d.id, "main"))
                .await
                .expect("ok")
                .is_ancestor
        );
        let mb = merge_base(
            store.as_ref(),
            MergeBaseRequest {
                repo: REPO.into(),
                a: "main".into(),
                b: d.id.to_string(),
            },
        )
        .await
        .expect("ok");
        assert_eq!(mb.bases.len(), 1);
        assert_eq!(ObjectId::from_bytes(&mb.bases[0]).ok(), Some(a.id));
        let r = is_ancestor(
            store.as_ref(),
            IsAncestorRequest {
                repo: REPO.into(),
                ancestor: "nope".into(),
                descendant: c.id.to_string(),
            },
        )
        .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = merge_base(
            store.as_ref(),
            MergeBaseRequest {
                repo: REPO.into(),
                a: "main".into(),
                b: "nope".into(),
            },
        )
        .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    #[tokio::test]
    async fn diff_between_revisions() {
        let (store, [a, _b, _c, _d]) = setup().await;
        let blobs: Arc<dyn BlobStore> = store.clone();
        let req = |from: &str, path: &str| DiffRequest {
            repo: REPO.into(),
            from: from.into(),
            to: "main".into(),
            path: path.into(),
        };
        let r = diff(store.as_ref(), blobs.clone(), req(&a.id.to_string(), ""))
            .await
            .expect("diff");
        let paths: Vec<_> = r.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["README", "src/x.rs"]);
        assert!(r.files[0].old.is_some() && r.files[0].new.is_some(), "modified");
        assert!(r.files[1].old.is_none(), "added");
        let r = diff(store.as_ref(), blobs.clone(), req("", ""))
            .await
            .expect("from empty");
        assert_eq!(r.files.len(), 2);
        let r = diff(store.as_ref(), blobs.clone(), req(&a.id.to_string(), "src"))
            .await
            .expect("limited");
        assert_eq!(r.files.len(), 1);
        let code = |r: Result<DiffResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(diff(store.as_ref(), blobs.clone(), req("nope", "")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(diff(store.as_ref(), blobs, req("", "a//b")).await),
            Err(Code::InvalidArgument)
        );
    }
}
