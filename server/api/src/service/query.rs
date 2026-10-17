//! QueryService：不需要本地仓库的读文件与列目录。

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_object::{EntryMode, ObjectId, TreeEntry};
use scion_proto::v1::query_service_server::QueryService;
use scion_proto::v1::{
    self, BlameRequest, BlameResponse, DiffRequest, DiffResponse, GetDerivedRequest, GetDerivedResponse,
    IsAncestorRequest, IsAncestorResponse, ListDirRequest, ListDirResponse, LogRequest, LogResponse, MergeBaseRequest,
    MergeBaseResponse, ReadFileRequest, ReadFileResponse, ResolveRevisionRequest, ResolveRevisionResponse, StatRequest,
    StatResponse,
};
use scion_store::{BlobStore, MetaStore};
use scion_tree::{BlobTreeSource, TreeSource, lookup};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::auth::{authorize, authorize_any, principal, to_status};
use crate::config::ServerConfig;
use crate::service::history;
use crate::service::support::resolve_revision;
use crate::status;
use scion_auth::{Action, Guard, Principal, Request as AuthRequest};

pub type ReadFileStream = Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send>>;
pub type DiffStream = Pin<Box<dyn Stream<Item = Result<DiffResponse, Status>> + Send>>;

pub struct QuerySvc {
    config: Arc<ServerConfig>,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
    guard: Arc<Guard>,
}

pub(crate) fn proto_mode(m: EntryMode) -> v1::EntryMode {
    match m {
        EntryMode::File => v1::EntryMode::File,
        EntryMode::Executable => v1::EntryMode::Executable,
        EntryMode::Symlink => v1::EntryMode::Symlink,
        EntryMode::Tree => v1::EntryMode::Tree,
    }
}

pub(crate) fn proto_entry(e: &TreeEntry) -> v1::TreeEntry {
    v1::TreeEntry {
        name: e.name.clone(),
        mode: proto_mode(e.mode).into(),
        id: Bytes::copy_from_slice(&e.id.to_bytes()),
        size: e.size,
    }
}

pub(crate) fn check_path(path: &str) -> Result<(), Status> {
    if path.is_empty() {
        return Ok(());
    }
    scion_object::validate_path(path).map_err(|_| Status::invalid_argument(format!("invalid path {path:?}")))
}

/// 把文件内容从 `start` 开始切成分块。`start == len` 时返回一个空的结尾块。
fn chunk_file(entry: &TreeEntry, data: Bytes, start: usize, chunk: usize) -> Vec<ReadFileResponse> {
    let total = data.len();
    let id = Bytes::copy_from_slice(&entry.id.to_bytes());
    let mode = proto_mode(entry.mode).into();
    let mut out = Vec::with_capacity((total - start).div_ceil(chunk).max(1));
    let mut off = start;
    loop {
        let end = (off + chunk).min(total);
        out.push(ReadFileResponse {
            blob_id: id.clone(),
            offset: off as u64,
            data: data.slice(off..end),
            last: end == total,
            total_size: total as u64,
            mode,
        });
        if end == total {
            return out;
        }
        off = end;
    }
}

impl QuerySvc {
    pub fn new(config: Arc<ServerConfig>, meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            config,
            meta,
            blobs,
            guard: Guard::disabled(),
        }
    }

    pub fn with_guard(mut self, guard: Arc<Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// 按路径的读：路径为空表示整个仓库中有任何读权限即可（例如不限路径的 log）。
    async fn authorize_read(&self, p: &Principal, repo: &str, path: &str) -> Result<(), Status> {
        if path.is_empty() {
            authorize_any(&self.guard, p, repo, Action::Read).await
        } else {
            authorize(&self.guard, p, AuthRequest::repo(repo, Action::Read).with_path(path)).await
        }
    }

    /// 是否有整个仓库的读权限（有则不需要逐个过滤路径）。
    async fn reads_everything(&self, p: &Principal, repo: &str) -> Result<bool, Status> {
        self.guard
            .check(p, &AuthRequest::repo(repo, Action::Read))
            .await
            .map_err(to_status)
    }

    fn source(&self) -> BlobTreeSource {
        BlobTreeSource(self.blobs.clone())
    }

    /// 读文件内容用的 BlobStore：redact 过的 blob 返回 REDACTED。
    fn contents(&self, repo: &str) -> scion_store::RedactedBlobs {
        scion_store::RedactedBlobs::new(self.blobs.clone(), self.meta.clone(), repo)
    }

    pub(crate) async fn read_file(&self, repo: &str, revision: &str, path: &str) -> Result<(TreeEntry, Bytes), Status> {
        check_path(path)?;
        if path.is_empty() {
            return Err(Status::failed_precondition("path is required"));
        }
        let rec = resolve_revision(self.meta.as_ref(), repo, revision).await?;
        let entry = lookup(&self.source(), rec.root_tree, path)
            .await
            .map_err(status::from_tree)?
            .ok_or_else(|| status::path_not_found(path))?;
        if entry.mode == EntryMode::Tree {
            return Err(Status::failed_precondition(format!("{path:?} is a directory")));
        }
        let data = self
            .contents(repo)
            .get(&entry.id)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| {
                tracing::error!(id = %entry.id, repo, "tree references a missing blob");
                Status::internal(format!("blob {} is missing", entry.id))
            })?;
        Ok((entry, data))
    }

    pub(crate) async fn list_dir(&self, repo: &str, revision: &str, path: &str) -> Result<ListDirResponse, Status> {
        check_path(path)?;
        let rec = resolve_revision(self.meta.as_ref(), repo, revision).await?;
        let src = self.source();
        let tree_id: ObjectId = if path.is_empty() {
            rec.root_tree
        } else {
            match lookup(&src, rec.root_tree, path).await.map_err(status::from_tree)? {
                Some(e) if e.mode == EntryMode::Tree => e.id,
                Some(_) => return Err(Status::failed_precondition(format!("{path:?} is not a directory"))),
                None => return Err(status::path_not_found(path)),
            }
        };
        let tree = src.load_tree(&tree_id).await.map_err(status::from_tree)?;
        Ok(ListDirResponse {
            changeset_id: Bytes::copy_from_slice(&rec.id.to_bytes()),
            tree_id: Bytes::copy_from_slice(&tree_id.to_bytes()),
            entries: tree.entries().iter().map(proto_entry).collect(),
        })
    }
}

#[tonic::async_trait]
impl QueryService for QuerySvc {
    type ReadFileStream = ReadFileStream;

    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<Self::ReadFileStream>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        check_path(&req.path)?;
        if req.path.is_empty() {
            return Err(Status::failed_precondition("path is required"));
        }
        self.authorize_read(&p, &req.repo, &req.path).await?;
        let (entry, data) = QuerySvc::read_file(self, &req.repo, &req.revision, &req.path).await?;
        if req.offset > data.len() as u64 {
            return Err(Status::out_of_range(format!(
                "offset {} beyond size {}",
                req.offset,
                data.len()
            )));
        }
        let chunk = usize::try_from(self.config.limits.max_chunk_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let parts = chunk_file(&entry, data, req.offset as usize, chunk);
        Ok(Response::new(Box::pin(tokio_stream::iter(parts.into_iter().map(Ok)))))
    }

    async fn list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        check_path(&req.path)?;
        self.authorize_read(&p, &req.repo, "").await?;
        let mut resp = QuerySvc::list_dir(self, &req.repo, &req.revision, &req.path).await?;
        // 只限某些路径的读者只看到可读的条目，以及通向可读路径的上级目录
        if !self.reads_everything(&p, &req.repo).await? {
            let mut kept = Vec::with_capacity(resp.entries.len());
            for e in resp.entries {
                let full = if req.path.is_empty() {
                    e.name.clone()
                } else {
                    format!("{}/{}", req.path.trim_end_matches('/'), e.name)
                };
                if self.guard.path_visible(&p, &req.repo, &full).await.map_err(to_status)? {
                    kept.push(e);
                }
            }
            resp.entries = kept;
        }
        Ok(Response::new(resp))
    }

    async fn log(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, &req.path).await?;
        Ok(Response::new(
            history::log(self.meta.as_ref(), self.blobs.as_ref(), req).await?,
        ))
    }

    async fn is_ancestor(&self, request: Request<IsAncestorRequest>) -> Result<Response<IsAncestorResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, "").await?;
        Ok(Response::new(history::is_ancestor(self.meta.as_ref(), req).await?))
    }

    async fn merge_base(&self, request: Request<MergeBaseRequest>) -> Result<Response<MergeBaseResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, "").await?;
        Ok(Response::new(history::merge_base(self.meta.as_ref(), req).await?))
    }

    async fn blame(&self, request: Request<BlameRequest>) -> Result<Response<BlameResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        if req.path.is_empty() {
            return Err(Status::invalid_argument("path is required"));
        }
        self.authorize_read(&p, &req.repo, &req.path).await?;
        Ok(Response::new(
            history::blame(self.meta.as_ref(), &self.contents(&req.repo), req).await?,
        ))
    }

    type DiffStream = DiffStream;

    async fn diff(&self, request: Request<DiffRequest>) -> Result<Response<Self::DiffStream>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, &req.path).await?;
        let repo = req.repo.clone();
        let mut resp = history::diff(self.meta.as_ref(), self.blobs.clone(), req).await?;
        if !self.reads_everything(&p, &repo).await? {
            let mut kept = Vec::with_capacity(resp.files.len());
            for f in resp.files {
                let req = AuthRequest::repo(&repo, Action::Read).with_path(&f.path);
                if self.guard.check(&p, &req).await.map_err(to_status)? {
                    kept.push(f);
                }
            }
            resp.files = kept;
        }
        let batches = history::diff_batches(resp, history::DIFF_BATCH);
        Ok(Response::new(Box::pin(tokio_stream::iter(batches.into_iter().map(Ok)))))
    }

    async fn resolve_revision(
        &self,
        request: Request<ResolveRevisionRequest>,
    ) -> Result<Response<ResolveRevisionResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, "").await?;
        Ok(Response::new(history::resolve(self.meta.as_ref(), req).await?))
    }

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, &req.path).await?;
        Ok(Response::new(
            history::stat(self.meta.as_ref(), self.blobs.clone(), req).await?,
        ))
    }

    async fn get_derived(&self, request: Request<GetDerivedRequest>) -> Result<Response<GetDerivedResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        self.authorize_read(&p, &req.repo, "").await?;
        Ok(Response::new(
            history::get_derived(self.meta.as_ref(), self.blobs.as_ref(), req).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use scion_store::IndexedObject;
    use tokio_stream::StreamExt;
    use tonic::Code;

    async fn setup() -> (Arc<scion_store::MemStore>, QuerySvc, ObjectId) {
        let (store, w) = svc().await;
        upload(&w, b"hello world").await;
        upload(&w, b"#!/bin/sh").await;
        let mut run = put("bin/run.sh", b"#!/bin/sh");
        if let Some(v1::file_change::Op::Put(p)) = &mut run.op {
            p.mode = v1::EntryMode::Executable.into();
        }
        let cs = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[], vec![put("docs/readme.md", b"hello world"), run]), 10).expect("s"),
            )
            .await
            .expect("create");
        LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default()))
            .land(REPO, "main", cs.id, false)
            .await
            .expect("land");
        let cfg = ServerConfig {
            limits: crate::config::Limits {
                max_chunk_bytes: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        (store.clone(), QuerySvc::new(Arc::new(cfg), store.clone(), store), cs.id)
    }

    #[test]
    fn proto_mode_all() {
        assert_eq!(proto_mode(EntryMode::File), v1::EntryMode::File);
        assert_eq!(proto_mode(EntryMode::Executable), v1::EntryMode::Executable);
        assert_eq!(proto_mode(EntryMode::Symlink), v1::EntryMode::Symlink);
        assert_eq!(proto_mode(EntryMode::Tree), v1::EntryMode::Tree);
    }

    #[test]
    fn check_path_cases() {
        assert!(check_path("").is_ok());
        assert!(check_path("a/b").is_ok());
        assert!(check_path("/a").is_err());
    }

    #[tokio::test]
    async fn read_file_streams_chunks_and_resumes() {
        let (_s, q, cs) = setup().await;
        let req = |offset| ReadFileRequest {
            repo: REPO.into(),
            revision: "main".into(),
            path: "docs/readme.md".into(),
            offset,
        };
        let parts: Vec<_> = QueryService::read_file(&q, Request::new(req(0)))
            .await
            .expect("read")
            .into_inner()
            .collect::<Result<_, _>>()
            .await
            .expect("stream");
        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts.iter().flat_map(|p| p.data.to_vec()).collect::<Vec<_>>(),
            b"hello world"
        );
        assert!(parts[2].last);
        let tail: Vec<_> = QueryService::read_file(&q, Request::new(req(6)))
            .await
            .expect("resume")
            .into_inner()
            .collect::<Result<_, _>>()
            .await
            .expect("stream");
        assert_eq!(tail.iter().flat_map(|p| p.data.to_vec()).collect::<Vec<_>>(), b"world");
        let r = QueryService::read_file(&q, Request::new(req(12))).await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::OutOfRange));
        // 按提交 id 读取
        let (e, data) = q.read_file(REPO, &cs.to_string(), "bin/run.sh").await.expect("by id");
        assert_eq!((e.mode, data.as_ref()), (EntryMode::Executable, &b"#!/bin/sh"[..]));
    }

    #[tokio::test]
    async fn read_file_errors() {
        let (store, q, _cs) = setup().await;
        let code = |r: Result<(TreeEntry, Bytes), Status>| r.map_err(|s| s.code()).err();
        assert_eq!(
            code(q.read_file(REPO, "main", "docs").await),
            Some(Code::FailedPrecondition),
            "directory"
        );
        assert_eq!(
            code(q.read_file(REPO, "main", "").await),
            Some(Code::FailedPrecondition),
            "empty path"
        );
        assert_eq!(code(q.read_file(REPO, "main", "nope").await), Some(Code::NotFound));
        assert_eq!(
            code(q.read_file(REPO, "main", "a//b").await),
            Some(Code::InvalidArgument)
        );
        assert_eq!(
            code(q.read_file(REPO, "nope", "docs/readme.md").await),
            Some(Code::NotFound)
        );
        // 目录树引用了存储里不存在的 blob（数据损坏）
        let (_, w) = (
            store.clone(),
            crate::service::write::WriteSvc::new(Arc::new(ServerConfig::default()), store.clone(), store.clone()),
        );
        let lost = ObjectId::of_blob(b"lost");
        store
            .index_objects(REPO, &[IndexedObject { id: lost, size: 4 }])
            .await
            .expect("index only");
        let head = store.get_ref(REPO, "main").await.expect("ok").expect("ref").target;
        let cs = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[head], vec![put("lost", b"lost")]), 10).expect("s"),
            )
            .await
            .expect("cs");
        assert_eq!(
            code(q.read_file(REPO, &cs.id.to_string(), "lost").await),
            Some(Code::Internal)
        );
    }

    #[tokio::test]
    async fn list_dir_root_and_subdir() {
        let (_s, q, cs) = setup().await;
        let root = QueryService::list_dir(
            &q,
            Request::new(ListDirRequest {
                repo: REPO.into(),
                revision: "main".into(),
                path: String::new(),
            }),
        )
        .await
        .expect("root")
        .into_inner();
        assert_eq!(ObjectId::from_bytes(&root.changeset_id), Ok(cs));
        let names: Vec<_> = root.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["bin", "docs"]);
        assert_eq!(root.entries[0].mode(), v1::EntryMode::Tree);
        let sub = q.list_dir(REPO, "main", "docs").await.expect("docs");
        assert_eq!(sub.entries[0].name, "readme.md");
        assert_eq!(sub.entries[0].size, 11);
    }

    #[tokio::test]
    async fn list_dir_errors() {
        let (_s, q, _cs) = setup().await;
        let code = |r: Result<ListDirResponse, Status>| r.map_err(|s| s.code()).err();
        assert_eq!(
            code(q.list_dir(REPO, "main", "docs/readme.md").await),
            Some(Code::FailedPrecondition)
        );
        assert_eq!(code(q.list_dir(REPO, "main", "nope").await), Some(Code::NotFound));
        assert_eq!(code(q.list_dir(REPO, "main", "/x").await), Some(Code::InvalidArgument));
        assert_eq!(code(q.list_dir(REPO, "nope", "").await), Some(Code::NotFound));
    }
}
