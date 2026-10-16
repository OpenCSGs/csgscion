//! LandService：把 ref 移到指定提交。M1 只允许快进，服务端 rebase 在 M3 加入。

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use scion_object::ObjectId;
use scion_proto::v1::land_service_server::LandService;
use scion_proto::v1::{ErrorReason, LandRequest, LandResponse};
use scion_store::{MetaStore, RefExpect, RefState};
use tonic::{Code, Request, Response, Status};

use crate::service::support::{check_repo, parse_id};
use crate::status;

pub struct LandSvc {
    meta: Arc<dyn MetaStore>,
}

/// ref 名规则：与路径相同（`main`、`feature/x`），不允许空串、`.`、`..` 等。
fn validate_ref_name(name: &str) -> Result<(), Status> {
    scion_object::validate_path(name).map_err(|_| Status::invalid_argument(format!("invalid ref name {name:?}")))
}

impl LandSvc {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Self { meta }
    }

    pub(crate) async fn land(&self, repo: &str, reference: &str, cs_id: ObjectId) -> Result<RefState, Status> {
        check_repo(self.meta.as_ref(), repo).await?;
        validate_ref_name(reference)?;
        let rec = self
            .meta
            .get_changeset(repo, &cs_id)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| status::objects_not_uploaded(&[cs_id]))?;

        let current = self.meta.get_ref(repo, reference).await.map_err(status::from_store)?;
        let expect = match current {
            // 已经合入：直接返回，超时后重试是安全的
            Some(cur) if cur.target == cs_id => return Ok(cur),
            Some(cur) if rec.first_parent == Some(cur.target) => RefExpect::Version(cur.version),
            Some(cur) => return Err(status::not_fast_forward(reference, &cur.target)),
            None => RefExpect::Absent,
        };
        match self.meta.cas_ref(repo, reference, expect, cs_id).await {
            Ok(state) => Ok(state),
            // 读和写之间 ref 被别人移动了：可重试，重试时会重新判断是否快进
            Err(scion_store::Error::RefVersionMismatch { .. }) => Err(status::with_reason(
                Code::Unavailable,
                ErrorReason::LandContention,
                format!("ref {reference:?} changed concurrently; retry"),
                HashMap::from([("ref".to_string(), reference.to_string())]),
            )),
            Err(e) => Err(status::from_store(e)),
        }
    }
}

#[tonic::async_trait]
impl LandService for LandSvc {
    async fn land(&self, request: Request<LandRequest>) -> Result<Response<LandResponse>, Status> {
        let req = request.into_inner();
        let cs_id = parse_id(&req.changeset_id)?;
        let state = LandSvc::land(self, &req.repo, &req.r#ref, cs_id).await?;
        tracing::info!(repo = %req.repo, r#ref = %req.r#ref, tip = %state.target, version = state.version,
            request_id = %req.request_id, "landed");
        Ok(Response::new(LandResponse {
            tip: Bytes::copy_from_slice(&state.target.to_bytes()),
            ref_version: state.version,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use tonic::Code;

    #[test]
    fn ref_names() {
        assert!(validate_ref_name("main").is_ok());
        assert!(validate_ref_name("feature/x").is_ok());
        for bad in ["", "/main", "a//b", "..", "a/"] {
            assert!(validate_ref_name(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn land_create_fast_forward_and_idempotent() {
        let (store, w) = svc().await;
        let land = LandSvc::new(store.clone());
        upload(&w, b"1").await;
        let a = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[], vec![put("f", b"1")]), 10).expect("s"),
            )
            .await
            .expect("a");
        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 10).expect("s"))
            .await
            .expect("b");

        let s1 = land.land(REPO, "main", a.id).await.expect("create ref");
        assert_eq!((s1.target, s1.version), (a.id, 1));
        let s2 = land.land(REPO, "main", b.id).await.expect("fast forward");
        assert_eq!((s2.target, s2.version), (b.id, 2));
        // 重试：已经是当前提交
        assert_eq!(land.land(REPO, "main", b.id).await.expect("retry"), s2);
    }

    #[tokio::test]
    async fn land_rejects_non_fast_forward_and_unknown() {
        let (store, w) = svc().await;
        let land = LandSvc::new(store.clone());
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 10).expect("s"))
            .await
            .expect("b");
        let mut s = spec(&[a.id], vec![]);
        s.message = "sibling".into();
        let c = w.create(REPO, &spec_to_changeset(s, 10).expect("s")).await.expect("c");
        land.land(REPO, "main", b.id).await.expect("b lands");
        let code = |r: Result<RefState, Status>| r.map_err(|s| s.code()).err();
        assert_eq!(code(land.land(REPO, "main", c.id).await), Some(Code::Aborted));
        assert_eq!(
            code(land.land(REPO, "main", a.id).await),
            Some(Code::Aborted),
            "moving backwards"
        );
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(
            code(land.land(REPO, "main", ghost).await),
            Some(Code::FailedPrecondition)
        );
        assert_eq!(
            code(land.land(REPO, "bad//ref", b.id).await),
            Some(Code::InvalidArgument)
        );
        assert_eq!(code(land.land("nope", "main", b.id).await), Some(Code::NotFound));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_lands_exactly_one_wins_per_parent() {
        let (store, w) = svc().await;
        let land = Arc::new(LandSvc::new(store.clone()));
        let base = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("base");
        land.land(REPO, "main", base.id).await.expect("base lands");
        let mut children = Vec::new();
        for i in 0..8u8 {
            let mut s = spec(&[base.id], vec![]);
            s.message = format!("child {i}");
            children.push(
                w.create(REPO, &spec_to_changeset(s, 10).expect("s"))
                    .await
                    .expect("child")
                    .id,
            );
        }
        let tasks: Vec<_> = children
            .into_iter()
            .map(|id| {
                let land = land.clone();
                tokio::spawn(async move { land.land(REPO, "main", id).await })
            })
            .collect();
        let mut ok = 0;
        for t in tasks {
            match t.await.expect("join") {
                Ok(_) => ok += 1,
                Err(s) => assert!(matches!(s.code(), Code::Aborted | Code::Unavailable), "{s:?}"),
            }
        }
        assert_eq!(ok, 1);
        assert_eq!(store.get_ref(REPO, "main").await.expect("get").expect("ref").version, 2);
    }
}
