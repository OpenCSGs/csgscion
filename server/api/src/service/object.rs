//! ObjectService：按 id 读取对象、FindMissing。

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_object::ObjectId;
use scion_proto::v1::object_service_server::ObjectService;
use scion_proto::v1::{Encoding, GetObjectsRequest, GetObjectsResponse, HasObjectsRequest, HasObjectsResponse};
use scion_store::{BlobStore, MetaStore};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::auth::{authorize, principal};
use crate::config::ServerConfig;
use crate::service::support::{check_repo, parse_id};
use crate::status;
use scion_auth::{Action, Guard, Request as AuthRequest};

pub type GetObjectsStream = Pin<Box<dyn Stream<Item = Result<GetObjectsResponse, Status>> + Send>>;

pub struct ObjectSvc {
    config: Arc<ServerConfig>,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
    guard: Arc<Guard>,
}

impl ObjectSvc {
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

    fn check_batch(&self, n: usize) -> Result<(), Status> {
        let max = self.config.limits.max_batch_objects as usize;
        if n > max {
            return Err(status::limit_exceeded("max_batch_objects", n, max));
        }
        Ok(())
    }
}

/// 选择传输编码。目前只实现 IDENTITY；未知的枚举值忽略。
fn negotiate_encoding(accept: &[i32]) -> Result<Encoding, Status> {
    if accept.is_empty() || accept.contains(&(Encoding::Identity as i32)) {
        Ok(Encoding::Identity)
    } else {
        Err(Status::failed_precondition(
            "none of accept_encodings is supported; IDENTITY is always supported",
        ))
    }
}

/// 把一个对象从 `start` 开始切成若干分块。`start == len` 时返回一个空的结尾块。
fn chunk_object(id: ObjectId, data: Bytes, start: usize, chunk: usize) -> Vec<GetObjectsResponse> {
    let total = data.len();
    let id_bytes = Bytes::copy_from_slice(&id.to_bytes());
    let mut out = Vec::with_capacity((total - start).div_ceil(chunk).max(1));
    let mut off = start;
    loop {
        let end = (off + chunk).min(total);
        out.push(GetObjectsResponse {
            id: id_bytes.clone(),
            offset: off as u64,
            data: data.slice(off..end),
            encoding: Encoding::Identity.into(),
            last: end == total,
            total_size: total as u64,
        });
        if end == total {
            return out;
        }
        off = end;
    }
}

#[tonic::async_trait]
impl ObjectService for ObjectSvc {
    type GetObjectsStream = GetObjectsStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        // 按 id 读对象无法按路径限制：需要整个仓库的读权限
        authorize(&self.guard, &p, AuthRequest::repo(&req.repo, Action::Read)).await?;
        check_repo(self.meta.as_ref(), &req.repo).await?;
        self.check_batch(req.wants.len())?;
        negotiate_encoding(&req.accept_encodings)?;

        let ids = req
            .wants
            .iter()
            .map(|w| parse_id(&w.id))
            .collect::<Result<Vec<_>, _>>()?;
        // 先按仓库的对象索引确认全部可见，再开始发送：要么完整成功，要么一开始就报 OBJECT_MISSING
        let missing = self
            .meta
            .missing_objects(&req.repo, &ids)
            .await
            .map_err(status::from_store)?;
        if !missing.is_empty() {
            return Err(status::object_missing(&missing));
        }
        let mut found = Vec::with_capacity(ids.len());
        for (id, want) in ids.into_iter().zip(&req.wants) {
            match self.blobs.get(&id).await.map_err(status::from_store)? {
                Some(data) => found.push((id, data, want.offset)),
                // 索引里有但字节不在：违反写入顺序不变式，属于数据损坏，需要运维介入
                None => {
                    tracing::error!(%id, repo = %req.repo, "indexed object missing from blob store");
                    return Err(Status::internal(format!(
                        "object {id} is indexed but its bytes are missing"
                    )));
                }
            }
        }
        for (id, data, offset) in &found {
            if *offset > data.len() as u64 {
                return Err(Status::out_of_range(format!(
                    "offset {offset} beyond size {} of {id}",
                    data.len()
                )));
            }
        }

        let chunk = usize::try_from(self.config.limits.max_chunk_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let stream = tokio_stream::iter(
            found
                .into_iter()
                .flat_map(move |(id, data, offset)| chunk_object(id, data, offset as usize, chunk))
                .map(Ok),
        );
        Ok(Response::new(Box::pin(stream)))
    }

    async fn has_objects(&self, request: Request<HasObjectsRequest>) -> Result<Response<HasObjectsResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        // 写入前查询缺失对象：只限某些路径的写入者也需要
        if !self
            .guard
            .check_any(&p, &req.repo, Action::Write)
            .await
            .map_err(crate::auth::to_status)?
        {
            authorize(&self.guard, &p, AuthRequest::repo(&req.repo, Action::Read)).await?;
        }
        check_repo(self.meta.as_ref(), &req.repo).await?;
        self.check_batch(req.ids.len())?;
        let ids = req.ids.iter().map(|b| parse_id(b)).collect::<Result<Vec<_>, _>>()?;
        let missing = self
            .meta
            .missing_objects(&req.repo, &ids)
            .await
            .map_err(status::from_store)?;
        Ok(Response::new(HasObjectsResponse {
            missing_ids: missing
                .iter()
                .map(|id| Bytes::copy_from_slice(&id.to_bytes()))
                .collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::v1::ObjectWant;
    use scion_store::{IndexedObject, MemStore};
    use tokio_stream::StreamExt;
    use tonic::Code;

    const REPO: &str = "r";

    async fn svc_with(limits: crate::config::Limits, blobs: &[&[u8]]) -> ObjectSvc {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        for b in blobs {
            let id = ObjectId::of_blob(b);
            store.put(id, Bytes::copy_from_slice(b)).await.expect("put");
            store
                .index_objects(
                    REPO,
                    &[IndexedObject {
                        id,
                        size: b.len() as u64,
                    }],
                )
                .await
                .expect("index");
        }
        let cfg = ServerConfig {
            limits,
            ..ServerConfig::default()
        };
        ObjectSvc::new(Arc::new(cfg), store.clone(), store)
    }

    fn want(data: &[u8], offset: u64) -> ObjectWant {
        ObjectWant {
            id: Bytes::copy_from_slice(&ObjectId::of_blob(data).to_bytes()),
            offset,
        }
    }

    async fn collect(svc: &ObjectSvc, req: GetObjectsRequest) -> Result<Vec<GetObjectsResponse>, Status> {
        let stream = svc.get_objects(Request::new(req)).await?.into_inner();
        stream.collect::<Result<Vec<_>, _>>().await
    }

    fn small_chunks(n: u64) -> crate::config::Limits {
        crate::config::Limits {
            max_chunk_bytes: n,
            ..Default::default()
        }
    }

    #[test]
    fn chunk_object_splits_and_marks_last() {
        let id = ObjectId::of_blob(b"abcdefg");
        let parts = chunk_object(id, Bytes::from_static(b"abcdefg"), 0, 3);
        let data: Vec<_> = parts.iter().map(|p| p.data.as_ref()).collect();
        assert_eq!(data, [&b"abc"[..], b"def", b"g"]);
        assert_eq!(parts.iter().map(|p| p.offset).collect::<Vec<_>>(), [0, 3, 6]);
        assert_eq!(parts.iter().map(|p| p.last).collect::<Vec<_>>(), [false, false, true]);
        assert!(parts.iter().all(|p| p.total_size == 7));
    }

    #[test]
    fn chunk_object_exact_multiple() {
        let parts = chunk_object(ObjectId::of_blob(b"abcd"), Bytes::from_static(b"abcd"), 0, 2);
        assert_eq!(parts.len(), 2);
        assert!(parts[1].last);
    }

    #[test]
    fn chunk_object_empty_and_resume_at_end() {
        let empty = chunk_object(ObjectId::of_blob(b""), Bytes::new(), 0, 4);
        assert_eq!(empty.len(), 1);
        assert!(empty[0].last && empty[0].data.is_empty());
        let at_end = chunk_object(ObjectId::of_blob(b"ab"), Bytes::from_static(b"ab"), 2, 4);
        assert_eq!(at_end.len(), 1);
        assert_eq!((at_end[0].offset, at_end[0].last), (2, true));
    }

    #[test]
    fn chunk_object_resume_mid_object() {
        let parts = chunk_object(ObjectId::of_blob(b"abcdef"), Bytes::from_static(b"abcdef"), 4, 10);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].data.as_ref(), b"ef");
        assert_eq!(parts[0].offset, 4);
    }

    #[test]
    fn negotiate_encoding_cases() {
        assert_eq!(negotiate_encoding(&[]).ok(), Some(Encoding::Identity));
        assert_eq!(
            negotiate_encoding(&[Encoding::Zstd as i32, Encoding::Identity as i32]).ok(),
            Some(Encoding::Identity)
        );
        assert_eq!(
            negotiate_encoding(&[Encoding::Zstd as i32]).map_err(|s| s.code()),
            Err(Code::FailedPrecondition)
        );
        assert_eq!(
            negotiate_encoding(&[99]).map_err(|s| s.code()),
            Err(Code::FailedPrecondition)
        );
    }

    #[tokio::test]
    async fn get_objects_returns_all_chunks_in_order() {
        let svc = svc_with(small_chunks(2), &[b"hello", b"xy"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"hello", 0), want(b"xy", 0)],
            accept_encodings: vec![],
        };
        let parts = collect(&svc, req).await.expect("ok");
        let joined: Vec<u8> = parts.iter().flat_map(|p| p.data.to_vec()).collect();
        assert_eq!(joined, b"helloxy");
        assert_eq!(parts.iter().filter(|p| p.last).count(), 2);
    }

    #[tokio::test]
    async fn get_objects_resume_from_offset() {
        let svc = svc_with(small_chunks(1 << 20), &[b"hello"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"hello", 3)],
            accept_encodings: vec![],
        };
        let parts = collect(&svc, req).await.expect("ok");
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].data.as_ref(), b"lo");
    }

    #[tokio::test]
    async fn get_objects_offset_beyond_size_out_of_range() {
        let svc = svc_with(small_chunks(1 << 20), &[b"hello"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"hello", 6)],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::OutOfRange)
        );
    }

    #[tokio::test]
    async fn get_objects_missing_reports_all_missing_ids() {
        let svc = svc_with(Default::default(), &[b"have"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"have", 0), want(b"nope1", 0), want(b"nope2", 0)],
            accept_encodings: vec![],
        };
        let err = collect(&svc, req).await.expect_err("missing");
        assert_eq!(err.code(), Code::NotFound);
        assert!(err.message().starts_with("2 object(s)"));
    }

    #[tokio::test]
    async fn get_objects_unknown_repo_not_found() {
        let svc = svc_with(Default::default(), &[]).await;
        let req = GetObjectsRequest {
            repo: "other".into(),
            wants: vec![],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn get_objects_batch_limit_enforced() {
        let limits = crate::config::Limits {
            max_batch_objects: 1,
            ..Default::default()
        };
        let svc = svc_with(limits, &[b"a", b"b"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"a", 0), want(b"b", 0)],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn get_objects_bad_id_invalid_argument() {
        let svc = svc_with(Default::default(), &[]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![ObjectWant {
                id: Bytes::from_static(b"short"),
                offset: 0,
            }],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn get_objects_unsupported_encoding_rejected() {
        let svc = svc_with(Default::default(), &[b"a"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"a", 0)],
            accept_encodings: vec![Encoding::Zstd as i32],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::FailedPrecondition)
        );
    }

    #[tokio::test]
    async fn get_objects_zero_chunk_limit_still_progresses() {
        let svc = svc_with(small_chunks(0), &[b"ab"]).await;
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"ab", 0)],
            accept_encodings: vec![],
        };
        assert_eq!(collect(&svc, req).await.expect("ok").len(), 2);
    }

    #[tokio::test]
    async fn get_objects_bytes_in_other_repo_not_visible() {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        // 字节存在于全局存储，但本仓库没有登记：不可见
        store
            .put(ObjectId::of_blob(b"secret"), Bytes::from_static(b"secret"))
            .await
            .expect("put");
        let svc = ObjectSvc::new(Arc::new(ServerConfig::default()), store.clone(), store);
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"secret", 0)],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn get_objects_indexed_but_missing_bytes_is_internal() {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        let id = ObjectId::of_blob(b"lost");
        store
            .index_objects(REPO, &[IndexedObject { id, size: 4 }])
            .await
            .expect("index");
        let svc = ObjectSvc::new(Arc::new(ServerConfig::default()), store.clone(), store);
        let req = GetObjectsRequest {
            repo: REPO.into(),
            wants: vec![want(b"lost", 0)],
            accept_encodings: vec![],
        };
        assert_eq!(
            collect(&svc, req).await.map_err(|s| s.code()).err(),
            Some(Code::Internal)
        );
    }

    #[tokio::test]
    async fn has_objects_returns_missing_in_order() {
        let svc = svc_with(Default::default(), &[b"have"]).await;
        let ids = [b"z".as_slice(), b"have", b"a"]
            .iter()
            .map(|d| Bytes::copy_from_slice(&ObjectId::of_blob(d).to_bytes()))
            .collect::<Vec<_>>();
        let resp = svc
            .has_objects(Request::new(HasObjectsRequest {
                repo: REPO.into(),
                ids: ids.clone(),
            }))
            .await
            .expect("ok")
            .into_inner();
        assert_eq!(resp.missing_ids, vec![ids[0].clone(), ids[2].clone()]);
    }

    #[tokio::test]
    async fn has_objects_errors() {
        let limits = crate::config::Limits {
            max_batch_objects: 1,
            ..Default::default()
        };
        let svc = svc_with(limits, &[]).await;
        let two = vec![Bytes::from_static(&[0; 34]), Bytes::from_static(&[0; 34])];
        let code = |r: Result<Response<HasObjectsResponse>, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(
                svc.has_objects(Request::new(HasObjectsRequest {
                    repo: REPO.into(),
                    ids: two
                }))
                .await
            ),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(
                svc.has_objects(Request::new(HasObjectsRequest {
                    repo: "x".into(),
                    ids: vec![]
                }))
                .await
            ),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(
                svc.has_objects(Request::new(HasObjectsRequest {
                    repo: REPO.into(),
                    ids: vec![Bytes::from_static(b"bad")]
                }))
                .await
            ),
            Err(Code::InvalidArgument)
        );
    }
}
