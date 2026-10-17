//! LandService：把 ref 移到指定提交，落后时在服务端 rebase（实现在 `scion-land`）。
//!
//! 集群模式下每个 ref 由持有租约的节点（leader）处理：本节点不是 leader 时把请求原样转给它，
//! 转发的请求带上 [`FORWARDED_HEADER`]，收到的节点不会再转发（避免环路），而是返回可重试的错误。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use scion_land::{Coordinator, LandError, LandOutcome, LandRequest as Request_, Landed};
use scion_object::ObjectId;
use scion_proto::v1::land_service_client::LandServiceClient;
use scion_proto::v1::land_service_server::LandService;
use scion_proto::v1::{
    GetLandStatusRequest, GetLandStatusResponse, LandRequest, LandResponse, LandState, LandedChangeset,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::limit::RateLimiter;
use crate::service::support::{check_repo, parse_id};
use crate::status;

/// 节点间转发的请求带上的元数据。
pub const FORWARDED_HEADER: &str = "x-scion-forwarded";

pub struct LandSvc {
    coordinator: Coordinator,
    /// 到其它节点的连接（按地址缓存）。
    peers: Mutex<HashMap<String, Channel>>,
    limiter: Option<Arc<RateLimiter>>,
}

impl LandSvc {
    pub fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator,
            peers: Mutex::new(HashMap::new()),
            limiter: None,
        }
    }

    /// 按仓库限制合入速率。转发来的请求已在第一个节点计过数，不再检查。
    pub fn with_rate_limit(mut self, limiter: Arc<RateLimiter>) -> Self {
        self.limiter = Some(limiter);
        self
    }

    fn peer(&self, addr: &str) -> Result<Channel, Status> {
        let mut peers = self.peers.lock().map_err(|_| Status::internal("peer cache poisoned"))?;
        if let Some(c) = peers.get(addr) {
            return Ok(c.clone());
        }
        let channel = Endpoint::from_shared(addr.to_owned())
            .map_err(|e| Status::internal(format!("invalid peer address {addr:?}: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(2))
            .connect_lazy();
        peers.insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }

    /// 把请求转给 leader，原样返回它的结果。
    async fn forward(
        &self,
        leader: &str,
        req: LandRequest,
        auth: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    ) -> Result<Response<LandResponse>, Status> {
        let mut fwd = Request::new(req);
        fwd.metadata_mut()
            .insert(FORWARDED_HEADER, tonic::metadata::MetadataValue::from_static("1"));
        if let Some(a) = auth {
            fwd.metadata_mut().insert("authorization", a);
        }
        tracing::debug!(leader, "forwarding land to the ref's leader");
        let resp = LandServiceClient::new(self.peer(leader)?).land(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }

    #[cfg(test)]
    pub(crate) async fn land(
        &self,
        repo: &str,
        reference: &str,
        tip: ObjectId,
        fast_forward_only: bool,
    ) -> Result<LandOutcome, Status> {
        self.land_with_id(repo, reference, tip, fast_forward_only, None)
            .await
            .map_err(|e| to_status(reference, e))
    }

    async fn land_with_id(
        &self,
        repo: &str,
        reference: &str,
        tip: ObjectId,
        fast_forward_only: bool,
        request_id: Option<&str>,
    ) -> Result<LandOutcome, LandError> {
        self.coordinator
            .land(Request_ {
                repo,
                reference,
                tip,
                fast_forward_only,
                request_id,
            })
            .await
    }
}

fn to_status(reference: &str, e: LandError) -> Status {
    match e {
        LandError::NotFastForward { tip } => status::not_fast_forward(reference, &tip),
        e => status::from_land(e),
    }
}

fn id_bytes(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

fn to_proto(list: &[Landed]) -> Vec<LandedChangeset> {
    list.iter()
        .map(|l| LandedChangeset {
            changeset_id: id_bytes(&l.original),
            landed_id: id_bytes(&l.landed),
            change_id: Bytes::copy_from_slice(&l.change_id),
        })
        .collect()
}

fn response(out: &LandOutcome) -> LandResponse {
    LandResponse {
        tip: id_bytes(&out.state.target),
        ref_version: out.state.version,
        landed: to_proto(&out.landed),
        previously_landed: to_proto(&out.previously_landed),
    }
}

#[tonic::async_trait]
impl LandService for LandSvc {
    async fn land(&self, request: Request<LandRequest>) -> Result<Response<LandResponse>, Status> {
        let forwarded = request.metadata().contains_key(FORWARDED_HEADER);
        let auth = request.metadata().get("authorization").cloned();
        let req = request.into_inner();
        let cs_id = parse_id(&req.changeset_id)?;
        if !forwarded {
            RateLimiter::admit(self.limiter.as_deref(), &req.repo)?;
        }
        let request_id = Some(req.request_id.as_str()).filter(|id| !id.is_empty());
        let out = match self
            .land_with_id(&req.repo, &req.r#ref, cs_id, req.fast_forward_only, request_id)
            .await
        {
            Ok(out) => out,
            // 别的节点是 leader：转给它（转发来的请求不再转发，由客户端重试）
            Err(LandError::NotLeader { leader }) if !forwarded => return self.forward(&leader, req, auth).await,
            Err(e) => return Err(to_status(&req.r#ref, e)),
        };
        let rebased = out.landed.iter().filter(|l| l.original != l.landed).count();
        tracing::info!(repo = %req.repo, r#ref = %req.r#ref, tip = %out.state.target, version = out.state.version,
            landed = out.landed.len(), rebased, previously_landed = out.previously_landed.len(),
            request_id = %req.request_id, "landed");
        Ok(Response::new(response(&out)))
    }

    async fn get_land_status(
        &self,
        request: Request<GetLandStatusRequest>,
    ) -> Result<Response<GetLandStatusResponse>, Status> {
        let req = request.into_inner();
        if req.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        check_repo(self.coordinator.writer().meta().as_ref(), &req.repo).await?;
        let found = self
            .coordinator
            .status(&req.repo, &req.request_id)
            .await
            .map_err(status::from_land)?;
        Ok(Response::new(match found {
            Some(out) => GetLandStatusResponse {
                state: LandState::Landed as i32,
                result: Some(response(&out)),
            },
            None => GetLandStatusResponse {
                state: LandState::Unknown as i32,
                result: None,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::write::WriteSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use tonic::Code;
    use tonic_types::StatusExt;

    fn land_svc(w: &WriteSvc) -> LandSvc {
        LandSvc::new(Coordinator::new(w.writer().clone(), Default::default()))
    }

    fn reason(s: &Status) -> (Code, String, std::collections::HashMap<String, String>) {
        let d = s.get_error_details();
        let info = d
            .error_info()
            .map(|i| (i.reason.clone(), i.metadata.clone()))
            .unwrap_or_default();
        (s.code(), info.0, info.1)
    }

    async fn commit(w: &WriteSvc, parents: &[ObjectId], files: &[(&str, &[u8])], msg: &str) -> ObjectId {
        let mut delta = Vec::new();
        for (path, data) in files {
            upload(w, data).await;
            delta.push(put(path, data));
        }
        let mut s = spec(parents, delta);
        s.message = msg.into();
        // 每个提交一个独立的 change id（同一 ref 上同一 change 只合入一次）
        s.change_id = Bytes::copy_from_slice(&ObjectId::of_blob(msg.as_bytes()).to_bytes()[2..18]);
        w.create(REPO, &spec_to_changeset(s, 10).expect("s"))
            .await
            .expect("create")
            .id
    }

    #[tokio::test]
    async fn grpc_land_reports_landed_changesets() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[("f", b"1")], "a").await;
        let side = commit(&w, &[a], &[("g", b"1")], "side").await;
        let b = commit(&w, &[a], &[("f", b"2")], "b").await;
        let call = |id: ObjectId| LandRequest {
            repo: REPO.into(),
            request_id: String::new(),
            r#ref: "main".into(),
            changeset_id: id_bytes(&id),
            fast_forward_only: false,
        };
        let r = LandService::land(&land, Request::new(call(a)))
            .await
            .expect("create")
            .into_inner();
        assert_eq!((r.ref_version, r.landed.len()), (1, 1));
        assert_eq!(r.landed[0].changeset_id, r.landed[0].landed_id);
        let r = LandService::land(&land, Request::new(call(b)))
            .await
            .expect("ff")
            .into_inner();
        assert_eq!((r.tip.clone(), r.ref_version), (id_bytes(&b), 2));
        // 落后的提交：服务端 rebase，返回新旧 id
        let r = LandService::land(&land, Request::new(call(side)))
            .await
            .expect("rebase")
            .into_inner();
        assert_eq!(r.ref_version, 3);
        assert_eq!(r.landed[0].changeset_id, id_bytes(&side));
        assert_ne!(r.landed[0].landed_id, id_bytes(&side));
        assert_eq!(r.landed[0].landed_id, r.tip);
        assert_eq!(r.landed[0].change_id.len(), 16);
        // 已在历史中：返回当前状态，不再合入
        let r = LandService::land(&land, Request::new(call(b)))
            .await
            .expect("noop")
            .into_inner();
        assert_eq!((r.ref_version, r.landed.len()), (3, 0));
        // 带 request id：重试返回第一次的结果；GetLandStatus 可以查到
        let d = commit(&w, &[a], &[("h", b"1")], "d").await;
        let with_id = LandRequest {
            request_id: "req-d".into(),
            ..call(d)
        };
        let first = LandService::land(&land, Request::new(with_id.clone()))
            .await
            .expect("d")
            .into_inner();
        let retry = LandService::land(&land, Request::new(with_id.clone()))
            .await
            .expect("retry")
            .into_inner();
        assert_eq!(retry, first);
        let status_of = |id: &str| GetLandStatusRequest {
            repo: REPO.into(),
            request_id: id.into(),
        };
        let st = land
            .get_land_status(Request::new(status_of("req-d")))
            .await
            .expect("status")
            .into_inner();
        assert_eq!((st.state, st.result), (LandState::Landed as i32, Some(first)));
        let st = land
            .get_land_status(Request::new(status_of("nope")))
            .await
            .expect("status")
            .into_inner();
        assert_eq!((st.state, st.result), (LandState::Unknown as i32, None));
        let code = |r: Result<Response<GetLandStatusResponse>, Status>| r.err().map(|s| s.code());
        assert_eq!(
            code(land.get_land_status(Request::new(status_of(""))).await),
            Some(Code::InvalidArgument)
        );
        let other_repo = GetLandStatusRequest {
            repo: "missing".into(),
            request_id: "x".into(),
        };
        assert_eq!(
            code(land.get_land_status(Request::new(other_repo)).await),
            Some(Code::NotFound)
        );
        // 同一个 id 用于不同的请求
        let reused = LandRequest {
            request_id: "req-d".into(),
            ..call(a)
        };
        assert_eq!(
            LandService::land(&land, Request::new(reused))
                .await
                .err()
                .map(|s| s.code()),
            Some(Code::InvalidArgument)
        );

        let mut bad = call(a);
        bad.changeset_id = Bytes::from_static(b"x");
        assert_eq!(
            LandService::land(&land, Request::new(bad))
                .await
                .err()
                .map(|s| s.code()),
            Some(Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn conflicts_and_fast_forward_only_are_reported_with_details() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[("f", b"1\n")], "a").await;
        let b = commit(&w, &[a], &[("f", b"theirs\n")], "b").await;
        let c = commit(&w, &[a], &[("f", b"ours\n")], "c").await;
        land.land(REPO, "main", b, false).await.expect("b");

        let (code, why, md) = reason(&land.land(REPO, "main", c, false).await.expect_err("conflict"));
        assert_eq!((code, why.as_str()), (Code::Aborted, "MERGE_CONFLICT"));
        assert_eq!(md["changeset"], c.to_string());
        assert_eq!(
            (md["count"].as_str(), md["path.0"].as_str(), md["kind.0"].as_str()),
            ("1", "f", "content")
        );
        assert_eq!(md["base.0"], ObjectId::of_blob(b"1\n").to_string());
        assert_eq!(md["ours.0"], ObjectId::of_blob(b"ours\n").to_string());
        assert_eq!(md["theirs.0"], ObjectId::of_blob(b"theirs\n").to_string());

        let d = commit(&w, &[a], &[("other", b"x")], "d").await;
        let (code, why, md) = reason(&land.land(REPO, "main", d, true).await.expect_err("ff only"));
        assert_eq!((code, why.as_str()), (Code::Aborted, "NOT_FAST_FORWARD"));
        assert_eq!((md["ref"].as_str(), md["tip"].clone()), ("main", b.to_string()));
    }

    #[tokio::test]
    async fn lands_are_rate_limited_per_repo_except_forwarded() {
        let (_store, w) = svc().await;
        let land = land_svc(&w).with_rate_limit(Arc::new(RateLimiter::new(1, 1)));
        let a = commit(&w, &[], &[], "a").await;
        let b = commit(&w, &[a], &[("f", b"1")], "b").await;
        let call = |id: ObjectId| LandRequest {
            repo: REPO.into(),
            request_id: String::new(),
            r#ref: "main".into(),
            changeset_id: id_bytes(&id),
            fast_forward_only: false,
        };
        LandService::land(&land, Request::new(call(a))).await.expect("first");
        let err = LandService::land(&land, Request::new(call(b)))
            .await
            .expect_err("limited");
        assert_eq!(err.code(), Code::ResourceExhausted);
        let mut fwd = Request::new(call(b));
        fwd.metadata_mut()
            .insert(FORWARDED_HEADER, tonic::metadata::MetadataValue::from_static("1"));
        LandService::land(&land, fwd)
            .await
            .expect("forwarded requests were counted on the first node");
    }

    #[tokio::test]
    async fn land_rejects_bad_input() {
        let (_store, w) = svc().await;
        let land = land_svc(&w);
        let a = commit(&w, &[], &[], "a").await;
        let code = |r: Result<LandOutcome, Status>| r.err().map(|s| s.code());
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(
            code(land.land(REPO, "main", ghost, false).await),
            Some(Code::FailedPrecondition)
        );
        assert_eq!(
            code(land.land(REPO, "bad//ref", a, false).await),
            Some(Code::InvalidArgument)
        );
        assert_eq!(code(land.land("nope", "main", a, false).await), Some(Code::NotFound));
        // 无共同历史
        land.land(REPO, "main", a, false).await.expect("a");
        let other = commit(&w, &[], &[("x", b"x")], "other").await;
        assert_eq!(
            code(land.land(REPO, "main", other, false).await),
            Some(Code::FailedPrecondition)
        );
    }
}
