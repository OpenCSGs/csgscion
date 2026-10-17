//! AuthService（身份、换取令牌、device login、权限查询）与 AdminService（策略、令牌与 redaction 管理）。

use std::sync::Arc;
use std::time::Duration;

use scion_auth::oidc::DevicePoll;
use scion_auth::{Action, Guard, Request as AuthRequest};
use scion_proto::v1::admin_service_server::AdminService;
use scion_proto::v1::auth_service_server::AuthService;
use scion_proto::v1::{
    self, CheckAccessRequest, CheckAccessResponse, CreateTokenRequest, CreateTokenResponse, DeviceLoginState,
    ExchangeTokenRequest, ExchangeTokenResponse, GetPolicyRequest, GetPolicyResponse, ListRedactionsRequest,
    ListRedactionsResponse, ListTokensRequest, ListTokensResponse, PollDeviceLoginRequest, PollDeviceLoginResponse,
    PurgeRequest, PurgeResponse, RedactRequest, RedactResponse, RedactionInfo, RevokeTokenRequest, RevokeTokenResponse,
    SetPolicyRequest, SetPolicyResponse, StartDeviceLoginRequest, StartDeviceLoginResponse, TokenInfo, UnredactRequest,
    UnredactResponse, WhoAmIRequest, WhoAmIResponse,
};
use scion_store::{Redaction, TokenRecord};
use tonic::{Request, Response, Status};

use crate::auth::{authorize, principal, to_status};
use crate::redaction::Redactor;
use crate::service::support::parse_id;

pub struct AuthSvc {
    guard: Arc<Guard>,
}

impl AuthSvc {
    pub fn new(guard: Arc<Guard>) -> Self {
        Self { guard }
    }
}

fn action_of(a: i32) -> Result<Action, Status> {
    match v1::Action::try_from(a) {
        Ok(v1::Action::Read) => Ok(Action::Read),
        Ok(v1::Action::Write) => Ok(Action::Write),
        Ok(v1::Action::Land) => Ok(Action::Land),
        Ok(v1::Action::Admin) => Ok(Action::Admin),
        _ => Err(Status::invalid_argument("action is required")),
    }
}

#[tonic::async_trait]
impl AuthService for AuthSvc {
    async fn who_am_i(&self, request: Request<WhoAmIRequest>) -> Result<Response<WhoAmIResponse>, Status> {
        let p = principal(&request);
        Ok(Response::new(WhoAmIResponse {
            subject: p.subject,
            groups: p.groups,
            auth_required: self.guard.required(),
            expires_at_ms: p.expires_at_ms.unwrap_or(0),
        }))
    }

    async fn exchange_token(
        &self,
        request: Request<ExchangeTokenRequest>,
    ) -> Result<Response<ExchangeTokenResponse>, Status> {
        let req = request.into_inner();
        let (access_token, expires_at_ms, subject) =
            self.guard.exchange(&req.subject_token).await.map_err(to_status)?;
        Ok(Response::new(ExchangeTokenResponse {
            access_token,
            expires_at_ms,
            subject,
        }))
    }

    async fn start_device_login(
        &self,
        _request: Request<StartDeviceLoginRequest>,
    ) -> Result<Response<StartDeviceLoginResponse>, Status> {
        let s = self.guard.start_device_login().await.map_err(to_status)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Ok(Response::new(StartDeviceLoginResponse {
            device_code: s.device_code,
            user_code: s.user_code,
            verification_uri: s.verification_uri,
            verification_uri_complete: s.verification_uri_complete.unwrap_or_default(),
            interval_seconds: u32::try_from(s.interval).unwrap_or(5),
            expires_at_ms: now + i64::try_from(s.expires_in).unwrap_or(600) * 1000,
        }))
    }

    async fn poll_device_login(
        &self,
        request: Request<PollDeviceLoginRequest>,
    ) -> Result<Response<PollDeviceLoginResponse>, Status> {
        let code = request.into_inner().device_code;
        if code.is_empty() {
            return Err(Status::invalid_argument("device_code is required"));
        }
        let (poll, tokens) = self.guard.poll_device_login(&code).await.map_err(to_status)?;
        let state = match &poll {
            DevicePoll::Pending => DeviceLoginState::Pending,
            DevicePoll::SlowDown => DeviceLoginState::SlowDown,
            DevicePoll::Denied => DeviceLoginState::Denied,
            DevicePoll::Expired => DeviceLoginState::Expired,
            DevicePoll::Complete(_) => DeviceLoginState::Complete,
        };
        let subject = match &poll {
            DevicePoll::Complete(p) => p.subject.clone(),
            _ => String::new(),
        };
        let (refresh_token, access_token, access_expires_at_ms) = tokens.unwrap_or_default();
        Ok(Response::new(PollDeviceLoginResponse {
            state: state.into(),
            refresh_token,
            access_token,
            access_expires_at_ms,
            subject,
        }))
    }

    async fn check_access(
        &self,
        request: Request<CheckAccessRequest>,
    ) -> Result<Response<CheckAccessResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        if req.repo.is_empty() {
            return Err(Status::invalid_argument("repo is required"));
        }
        let mut ar = AuthRequest::repo(&req.repo, action_of(req.action)?);
        if !req.r#ref.is_empty() {
            ar = ar.with_ref(&req.r#ref);
        }
        if !req.path.is_empty() {
            ar = ar.with_path(&req.path);
        }
        let allowed = self.guard.check(&p, &ar).await.map_err(to_status)?;
        Ok(Response::new(CheckAccessResponse { allowed }))
    }
}

pub struct AdminSvc {
    guard: Arc<Guard>,
    redactor: Option<Arc<Redactor>>,
    purge_grace: Duration,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn duration_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

pub(crate) fn redaction_info(r: &Redaction) -> RedactionInfo {
    RedactionInfo {
        repo: r.repo.clone(),
        id: bytes::Bytes::copy_from_slice(&r.id.to_bytes()),
        redaction_id: r.redaction_id.clone(),
        reason: r.reason.clone(),
        created_by: r.created_by.clone(),
        created_at_ms: r.created_at_ms,
        active: r.active,
        purge_after_ms: r.purge_after_ms.unwrap_or(0),
        purged_at_ms: r.purged_at_ms.unwrap_or(0),
    }
}

impl AdminSvc {
    pub fn new(guard: Arc<Guard>) -> Self {
        Self {
            guard,
            redactor: None,
            purge_grace: Duration::from_secs(24 * 3600),
        }
    }

    /// 开启 redaction 接口；`purge_grace` 为 Purge 未指定宽限期时的默认值。
    pub fn with_redactor(mut self, redactor: Arc<Redactor>, purge_grace: Duration) -> Self {
        self.redactor = Some(redactor);
        self.purge_grace = purge_grace;
        self
    }

    fn redactor(&self) -> Result<&Redactor, Status> {
        self.redactor
            .as_deref()
            .ok_or_else(|| Status::unimplemented("redaction is not enabled on this server"))
    }

    /// 需要 `repo` 的 admin 权限。
    async fn require_repo_admin(&self, p: &scion_auth::Principal, repo: &str) -> Result<(), Status> {
        authorize(&self.guard, p, AuthRequest::repo(repo, Action::Admin)).await
    }

    /// 一次 redaction 涉及的每个仓库都需要 admin 权限。
    async fn require_admin_of(&self, p: &scion_auth::Principal, recs: &[Redaction]) -> Result<(), Status> {
        let repos: std::collections::BTreeSet<&str> = recs.iter().map(|r| r.repo.as_str()).collect();
        for repo in repos {
            self.require_repo_admin(p, repo).await?;
        }
        Ok(())
    }

    async fn require_admin<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.require_system_admin(&principal(request)).await
    }

    async fn require_system_admin(&self, p: &scion_auth::Principal) -> Result<(), Status> {
        if self.guard.is_admin(p).await.map_err(to_status)? {
            return Ok(());
        }
        if !p.authenticated {
            return Err(Status::unauthenticated("authentication required"));
        }
        Err(to_status(scion_auth::AuthError::Denied {
            repo: "*".into(),
            action: "admin",
            reference: None,
            path: None,
        }))
    }
}

pub(crate) fn token_info(t: &TokenRecord) -> TokenInfo {
    TokenInfo {
        id: t.id.clone(),
        subject: t.subject.clone(),
        groups: t.groups.clone(),
        description: t.description.clone(),
        kind: t.kind.clone(),
        created_at_ms: t.created_at_ms,
        expires_at_ms: t.expires_at_ms.unwrap_or(0),
    }
}

#[tonic::async_trait]
impl AdminService for AdminSvc {
    async fn get_policy(&self, request: Request<GetPolicyRequest>) -> Result<Response<GetPolicyResponse>, Status> {
        self.require_admin(&request).await?;
        let policy = self.guard.policy_text().await.map_err(to_status)?;
        Ok(Response::new(GetPolicyResponse { policy }))
    }

    async fn set_policy(&self, request: Request<SetPolicyRequest>) -> Result<Response<SetPolicyResponse>, Status> {
        self.require_admin(&request).await?;
        let who = principal(&request).subject;
        let n = self
            .guard
            .set_policy(&request.into_inner().policy)
            .await
            .map_err(to_status)?;
        tracing::info!(by = %who, rules = n, "access policy updated");
        Ok(Response::new(SetPolicyResponse {
            rules: u32::try_from(n).unwrap_or(u32::MAX),
        }))
    }

    async fn create_token(
        &self,
        request: Request<CreateTokenRequest>,
    ) -> Result<Response<CreateTokenResponse>, Status> {
        self.require_admin(&request).await?;
        let who = principal(&request).subject;
        let req = request.into_inner();
        let ttl = (req.ttl_seconds > 0).then(|| Duration::from_secs(req.ttl_seconds));
        let (token, rec) = self
            .guard
            .create_token("pat", &req.subject, req.groups, &req.description, ttl)
            .await
            .map_err(to_status)?;
        tracing::info!(by = %who, id = %rec.id, subject = %rec.subject, "token created");
        Ok(Response::new(CreateTokenResponse {
            token,
            info: Some(token_info(&rec)),
        }))
    }

    async fn list_tokens(&self, request: Request<ListTokensRequest>) -> Result<Response<ListTokensResponse>, Status> {
        self.require_admin(&request).await?;
        let subject = request.into_inner().subject;
        let tokens = self
            .guard
            .list_tokens(Some(subject.as_str()).filter(|s| !s.is_empty()))
            .await
            .map_err(to_status)?;
        Ok(Response::new(ListTokensResponse {
            tokens: tokens.iter().map(token_info).collect(),
        }))
    }

    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        self.require_admin(&request).await?;
        let who = principal(&request).subject;
        let id = request.into_inner().id;
        let revoked = self.guard.revoke_token(&id).await.map_err(to_status)?;
        tracing::info!(by = %who, %id, revoked, "token revoked");
        Ok(Response::new(RevokeTokenResponse { revoked }))
    }

    async fn redact(&self, request: Request<RedactRequest>) -> Result<Response<RedactResponse>, Status> {
        let red = self.redactor()?;
        let p = principal(&request);
        let req = request.into_inner();
        if req.all_repos {
            self.require_system_admin(&p).await?;
        } else {
            self.require_repo_admin(&p, &req.repo).await?;
        }
        let ids = req.ids.iter().map(|b| parse_id(b)).collect::<Result<Vec<_>, _>>()?;
        let (redaction_id, recs) = red
            .redact(
                &req.repo,
                &ids,
                &req.paths,
                &req.reason,
                &p.subject,
                req.all_repos,
                now_ms(),
            )
            .await?;
        Ok(Response::new(RedactResponse {
            redaction_id,
            redactions: recs.iter().map(redaction_info).collect(),
        }))
    }

    async fn unredact(&self, request: Request<UnredactRequest>) -> Result<Response<UnredactResponse>, Status> {
        let red = self.redactor()?;
        let p = principal(&request);
        let rid = request.into_inner().redaction_id;
        self.require_admin_of(&p, &red.records(&rid).await?).await?;
        let n = red.unredact(&rid).await?;
        tracing::warn!(by = %p.subject, redaction_id = %rid, "redaction withdrawn via API");
        Ok(Response::new(UnredactResponse {
            count: u32::try_from(n).unwrap_or(u32::MAX),
        }))
    }

    async fn purge(&self, request: Request<PurgeRequest>) -> Result<Response<PurgeResponse>, Status> {
        let red = self.redactor()?;
        let p = principal(&request);
        let req = request.into_inner();
        self.require_admin_of(&p, &red.records(&req.redaction_id).await?)
            .await?;
        let grace = req.grace_seconds.map_or(self.purge_grace, Duration::from_secs);
        let (n, after) = red.purge(&req.redaction_id, duration_ms(grace), now_ms()).await?;
        tracing::warn!(by = %p.subject, redaction_id = %req.redaction_id, purge_after_ms = after, "purge scheduled via API");
        Ok(Response::new(PurgeResponse {
            count: u32::try_from(n).unwrap_or(u32::MAX),
            purge_after_ms: after,
        }))
    }

    async fn list_redactions(
        &self,
        request: Request<ListRedactionsRequest>,
    ) -> Result<Response<ListRedactionsResponse>, Status> {
        let red = self.redactor()?;
        let p = principal(&request);
        let req = request.into_inner();
        let repo = Some(req.repo.as_str()).filter(|r| !r.is_empty());
        match repo {
            Some(r) => self.require_repo_admin(&p, r).await?,
            None => self.require_system_admin(&p).await?,
        }
        let recs = red
            .list(repo, Some(req.redaction_id.as_str()).filter(|r| !r.is_empty()))
            .await?;
        Ok(Response::new(ListRedactionsResponse {
            redactions: recs.iter().map(redaction_info).collect(),
        }))
    }
}
