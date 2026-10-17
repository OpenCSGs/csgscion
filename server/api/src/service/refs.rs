//! RefService：解析与列出 ref。

use std::sync::Arc;

use bytes::Bytes;
use scion_proto::v1::ref_service_server::RefService;
use scion_proto::v1::{ListRefsRequest, ListRefsResponse, RefInfo, ResolveRefRequest, ResolveRefResponse};
use scion_store::MetaStore;
use tonic::{Request, Response, Status};

use crate::service::support::{check_repo, resolve_revision};
use crate::status;

pub struct RefSvc {
    meta: Arc<dyn MetaStore>,
}

impl RefSvc {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Self { meta }
    }
}

#[tonic::async_trait]
impl RefService for RefSvc {
    async fn resolve_ref(&self, request: Request<ResolveRefRequest>) -> Result<Response<ResolveRefResponse>, Status> {
        let req = request.into_inner();
        let state = self
            .meta
            .get_ref(&req.repo, &req.r#ref)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| status::ref_not_found(&req.r#ref))?;
        let rec = resolve_revision(self.meta.as_ref(), &req.repo, &state.target.to_string()).await?;
        Ok(Response::new(ResolveRefResponse {
            changeset_id: Bytes::copy_from_slice(&rec.id.to_bytes()),
            root_tree: Bytes::copy_from_slice(&rec.root_tree.to_bytes()),
            version: state.version,
        }))
    }

    async fn list_refs(&self, request: Request<ListRefsRequest>) -> Result<Response<ListRefsResponse>, Status> {
        let req = request.into_inner();
        check_repo(self.meta.as_ref(), &req.repo).await?;
        let refs = self.meta.list_refs(&req.repo).await.map_err(status::from_store)?;
        Ok(Response::new(ListRefsResponse {
            refs: refs
                .into_iter()
                .map(|(name, s)| RefInfo {
                    name,
                    changeset_id: Bytes::copy_from_slice(&s.target.to_bytes()),
                    version: s.version,
                })
                .collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, spec, svc};
    use scion_object::ObjectId;
    use tonic::Code;

    #[tokio::test]
    async fn resolve_and_list() {
        let (store, w) = svc().await;
        let refs = RefSvc::new(store.clone());
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default()))
            .land(REPO, "main", a.id, false)
            .await
            .expect("land");

        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await
            .expect("resolve")
            .into_inner();
        assert_eq!(ObjectId::from_bytes(&r.changeset_id), Ok(a.id));
        assert_eq!(ObjectId::from_bytes(&r.root_tree), Ok(a.root_tree));
        assert_eq!(r.version, 1);

        let l = refs
            .list_refs(Request::new(ListRefsRequest { repo: REPO.into() }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(l.refs.len(), 1);
        assert_eq!(l.refs[0].name, "main");
    }

    #[tokio::test]
    async fn errors() {
        let (store, _w) = svc().await;
        let refs = RefSvc::new(store);
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: "x".into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs.list_refs(Request::new(ListRefsRequest { repo: "x".into() })).await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }
}
