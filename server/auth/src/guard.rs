//! [`Guard`]：认证与鉴权的入口，服务端各接口通过它判断请求方是谁、能做什么。
//!
//! **认证**（`authorization: Bearer <token>`）：
//! - 没有令牌：anonymous；
//! - `scn_` 开头：不透明令牌（PAT / service token），查 `tokens` 表；
//! - JWT：`iss` 为本服务时用自己的公钥验证；否则交给配置的外部签发方（JWKS 或 HS256）；
//! - 其它：配置了 RFC 7662 内省时交给内省接口。
//!
//! **鉴权**：允许 = ACL 策略允许，或令牌自带的授权允许，或外部鉴权回调允许。没有配置策略和回调时，
//! 已认证的身份可以读写和合入所有仓库（系统管理始终需要策略明确授予）。未开启认证时一切允许。

use std::sync::{Arc, RwLock};
use std::time::Duration;

use scion_store::{MetaStore, TokenRecord};

use crate::config::AuthConfig;
use crate::external::{ExternalIssuer, VerifyError};
use crate::oidc::{DevicePoll, DeviceStart, Oidc};
use crate::policy::{Action, Policy, Principal, Request, grants_allow};
use crate::remote::{AuthzWebhook, Introspection, Lookup};
use crate::token::{Issuer, OPAQUE_PREFIX, hash_opaque, new_opaque, peek};

/// 访问控制策略在设置表中的 key。
pub const POLICY_KEY: &str = "acl";

/// device login 得到的刷新令牌的有效期。
const REFRESH_TTL: Duration = Duration::from_secs(30 * 86_400);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("authentication required")]
    Missing,
    #[error("token expired")]
    Expired,
    #[error("invalid credentials: {0}")]
    Invalid(String),
    #[error("permission denied: {action} on {repo}")]
    Denied {
        repo: String,
        action: &'static str,
        reference: Option<String>,
        path: Option<String>,
    },
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    BadRequest(String),
}

impl From<VerifyError> for AuthError {
    fn from(e: VerifyError) -> Self {
        match e {
            VerifyError::Expired => AuthError::Expired,
            VerifyError::Invalid(m) => AuthError::Invalid(m),
            VerifyError::Unavailable(m) => AuthError::Unavailable(m),
        }
    }
}

impl From<scion_store::Error> for AuthError {
    fn from(e: scion_store::Error) -> Self {
        AuthError::Unavailable(e.to_string())
    }
}

/// 构建 [`Guard`] 的参数。
#[derive(Debug, Clone, Default)]
pub struct GuardConfig {
    /// 是否要求认证。
    pub required: bool,
    pub auth: AuthConfig,
    /// 自签令牌的私钥（PKCS#8 PEM）；不提供时不能签发访问令牌（ExchangeToken、device login 不可用）。
    pub signing_key_pem: Option<String>,
}

pub struct Guard {
    required: bool,
    meta: Option<Arc<dyn MetaStore>>,
    issuer: Option<Issuer>,
    externals: Vec<ExternalIssuer>,
    introspection: Option<Introspection>,
    webhook: Option<AuthzWebhook>,
    oidc: Option<Oidc>,
    policy: RwLock<Option<Arc<Policy>>>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

impl Guard {
    /// 不要求认证、一切允许（Lite 模式、测试）。
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            required: false,
            meta: None,
            issuer: None,
            externals: Vec::new(),
            introspection: None,
            webhook: None,
            oidc: None,
            policy: RwLock::new(None),
        })
    }

    pub fn build(config: GuardConfig, meta: Arc<dyn MetaStore>, lookup: Lookup) -> Result<Arc<Self>, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("HTTP client: {e}"))?;
        let ttl = i64::try_from(config.auth.token.ttl().as_secs()).unwrap_or(900);
        let issuer = config
            .signing_key_pem
            .as_deref()
            .map(|pem| Issuer::from_pem(pem, &config.auth.token.issuer, ttl))
            .transpose()
            .map_err(|e| e.to_string())?;
        let externals = config
            .auth
            .issuers
            .into_iter()
            .map(|i| ExternalIssuer::new(i, http.clone(), &*lookup).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Self {
            required: config.required,
            meta: Some(meta),
            issuer,
            externals,
            introspection: config
                .auth
                .introspection
                .map(|c| Introspection::new(c, http.clone(), lookup.clone())),
            webhook: config
                .auth
                .authz_webhook
                .map(|c| AuthzWebhook::new(c, http.clone(), lookup.clone())),
            oidc: config.auth.oidc.map(|c| Oidc::new(c, http, lookup)),
            policy: RwLock::new(None),
        }))
    }

    pub fn required(&self) -> bool {
        self.required
    }

    /// 能否签发访问令牌。
    pub fn can_issue(&self) -> bool {
        self.issuer.is_some()
    }

    pub fn device_login(&self) -> bool {
        self.oidc.is_some() && self.issuer.is_some()
    }

    fn meta(&self) -> Result<&Arc<dyn MetaStore>, AuthError> {
        self.meta
            .as_ref()
            .ok_or_else(|| AuthError::Unavailable("authentication is not configured".into()))
    }

    // ---------- 认证 ----------

    async fn opaque(&self, token: &str, allow_refresh: bool) -> Result<Principal, AuthError> {
        let rec = self
            .meta()?
            .get_token(&hash_opaque(token))
            .await?
            .ok_or_else(|| AuthError::Invalid("unknown token".into()))?;
        if rec.expires_at_ms.is_some_and(|e| e <= now_ms()) {
            return Err(AuthError::Expired);
        }
        if rec.kind == "refresh" && !allow_refresh {
            return Err(AuthError::Invalid(
                "refresh tokens can only be exchanged for an access token".into(),
            ));
        }
        let mut p = Principal::user(rec.subject, rec.groups);
        p.expires_at_ms = rec.expires_at_ms;
        Ok(p)
    }

    async fn jwt(&self, token: &str, allow_own: bool) -> Result<Principal, AuthError> {
        let iss = peek(token).and_then(|(iss, _)| iss);
        if let (Some(own), Some(iss)) = (&self.issuer, &iss) {
            if own.issuer() == iss {
                if !allow_own {
                    return Err(AuthError::Invalid("access tokens cannot be exchanged".into()));
                }
                let c = own.verify(token).map_err(VerifyError::from)?;
                let mut p = Principal::user(c.sub, c.groups);
                p.expires_at_ms = Some(c.exp * 1000);
                return Ok(p);
            }
        }
        match self.externals.iter().find(|e| Some(e.issuer()) == iss.as_deref()) {
            Some(e) => Ok(e.verify(token).await?),
            None => self.introspect(token).await,
        }
    }

    async fn introspect(&self, token: &str) -> Result<Principal, AuthError> {
        match &self.introspection {
            Some(i) => i
                .verify(token)
                .await?
                .ok_or_else(|| AuthError::Invalid("token is not active".into())),
            None => Err(AuthError::Invalid("token is not issued by a trusted issuer".into())),
        }
    }

    /// 认证一个请求。没有令牌时为 anonymous（是否允许由鉴权决定）。
    pub async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let Some(token) = bearer.map(str::trim).filter(|t| !t.is_empty()) else {
            return Ok(Principal::anonymous());
        };
        if !self.required && self.meta.is_none() {
            return Ok(Principal::anonymous());
        }
        if token.starts_with(OPAQUE_PREFIX) {
            self.opaque(token, false).await
        } else if token.split('.').count() == 3 {
            self.jwt(token, true).await
        } else {
            self.introspect(token).await
        }
    }

    // ---------- 令牌 ----------

    fn issue(&self, p: &Principal) -> Result<(String, i64), AuthError> {
        let issuer = self.issuer.as_ref().ok_or_else(|| {
            AuthError::Unavailable("this server does not issue access tokens (no signing key)".into())
        })?;
        Ok(issuer.issue(&p.subject, &p.groups, now_ms() / 1000))
    }

    /// 用长期凭据换取访问令牌：(令牌, 过期时间, 身份)。
    pub async fn exchange(&self, subject_token: &str) -> Result<(String, i64, String), AuthError> {
        let t = subject_token.trim();
        if t.is_empty() {
            return Err(AuthError::BadRequest("subject_token is required".into()));
        }
        let p = if t.starts_with(OPAQUE_PREFIX) {
            self.opaque(t, true).await?
        } else if t.split('.').count() == 3 {
            self.jwt(t, false).await?
        } else {
            self.introspect(t).await?
        };
        let (token, exp) = self.issue(&p)?;
        Ok((token, exp, p.subject))
    }

    /// 创建不透明令牌，返回令牌（只出现这一次）与记录。
    pub async fn create_token(
        &self,
        kind: &str,
        subject: &str,
        groups: Vec<String>,
        description: &str,
        ttl: Option<Duration>,
    ) -> Result<(String, TokenRecord), AuthError> {
        if subject.trim().is_empty() {
            return Err(AuthError::BadRequest("subject is required".into()));
        }
        if groups.iter().any(|g| g.is_empty() || g.contains('\n')) {
            return Err(AuthError::BadRequest(
                "group names must be non-empty single lines".into(),
            ));
        }
        let (token, id, hash) = new_opaque(kind);
        let now = now_ms();
        let rec = TokenRecord {
            id,
            hash,
            kind: kind.to_owned(),
            subject: subject.to_owned(),
            groups,
            description: description.to_owned(),
            created_at_ms: now,
            expires_at_ms: ttl.map(|t| now.saturating_add(i64::try_from(t.as_millis()).unwrap_or(i64::MAX))),
        };
        self.meta()?.put_token(&rec).await?;
        Ok((token, rec))
    }

    pub async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>, AuthError> {
        Ok(self.meta()?.list_tokens(subject).await?)
    }

    pub async fn revoke_token(&self, id: &str) -> Result<bool, AuthError> {
        Ok(self.meta()?.delete_token(id).await?)
    }

    pub async fn start_device_login(&self) -> Result<DeviceStart, AuthError> {
        match (&self.oidc, &self.issuer) {
            (Some(o), Some(_)) => Ok(o.start().await?),
            _ => Err(AuthError::Unavailable(
                "device login is not configured on this server".into(),
            )),
        }
    }

    /// 轮询 device login；完成时返回 (刷新令牌, 访问令牌, 过期时间, 身份)。
    pub async fn poll_device_login(
        &self,
        device_code: &str,
    ) -> Result<(DevicePoll, Option<(String, String, i64)>), AuthError> {
        let o = match (&self.oidc, &self.issuer) {
            (Some(o), Some(_)) => o,
            _ => {
                return Err(AuthError::Unavailable(
                    "device login is not configured on this server".into(),
                ));
            }
        };
        let poll = o.poll(device_code).await?;
        let DevicePoll::Complete(p) = &poll else {
            return Ok((poll, None));
        };
        let (refresh, _) = self
            .create_token("refresh", &p.subject, p.groups.clone(), "scn login", Some(REFRESH_TTL))
            .await?;
        let (access, exp) = self.issue(p)?;
        Ok((poll, Some((refresh, access, exp))))
    }

    // ---------- 策略 ----------

    /// 校验并保存策略（本节点立即生效），返回规则数。
    pub async fn set_policy(&self, text: &str) -> Result<usize, AuthError> {
        let p = Policy::parse(text).map_err(|e| AuthError::BadRequest(e.to_string()))?;
        self.meta()?.put_setting(POLICY_KEY, text.as_bytes()).await?;
        let n = p.len();
        self.install(Some(p));
        Ok(n)
    }

    pub async fn policy_text(&self) -> Result<String, AuthError> {
        let v = self.meta()?.get_setting(POLICY_KEY).await?;
        Ok(v.map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default())
    }

    /// 从元数据库重新加载策略（定期调用，让其它节点的修改生效）。
    pub async fn reload_policy(&self) -> Result<usize, AuthError> {
        let text = self.policy_text().await?;
        if text.trim().is_empty() {
            self.install(None);
            return Ok(0);
        }
        let p = Policy::parse(&text).map_err(|e| AuthError::Unavailable(format!("stored policy: {e}")))?;
        let n = p.len();
        self.install(Some(p));
        Ok(n)
    }

    fn install(&self, p: Option<Policy>) {
        if let Ok(mut cur) = self.policy.write() {
            *cur = p.map(Arc::new);
        }
    }

    fn current_policy(&self) -> Option<Arc<Policy>> {
        self.policy.read().ok().and_then(|p| p.clone())
    }

    // ---------- 鉴权 ----------

    async fn webhook_allows(&self, p: &Principal, req: &Request<'_>) -> Result<bool, AuthError> {
        match &self.webhook {
            Some(w) => match w.allows(p, req).await {
                Ok(b) => Ok(b),
                Err(e) if w.fail_open() => {
                    tracing::warn!(error = %e, "authz webhook unavailable; falling back to the ACL");
                    Ok(false)
                }
                Err(e) => Err(AuthError::Unavailable(e)),
            },
            None => Ok(false),
        }
    }

    /// 是否允许。
    pub async fn check(&self, p: &Principal, req: &Request<'_>) -> Result<bool, AuthError> {
        if !self.required {
            return Ok(true);
        }
        let policy = self.current_policy();
        match &policy {
            Some(pol) if pol.allows(p, req) => return Ok(true),
            None if self.webhook.is_none() => return Ok(p.authenticated && req.action <= Action::Land),
            _ => {}
        }
        if grants_allow(p, req) {
            return Ok(true);
        }
        self.webhook_allows(p, req).await
    }

    /// 不允许时返回 [`AuthError::Denied`]（未认证的请求返回 [`AuthError::Missing`]）。
    pub async fn authorize(&self, p: &Principal, req: &Request<'_>) -> Result<(), AuthError> {
        if self.check(p, req).await? {
            return Ok(());
        }
        if !p.authenticated {
            return Err(AuthError::Missing);
        }
        Err(AuthError::Denied {
            repo: req.repo.to_owned(),
            action: req.action.as_str(),
            reference: req.reference.map(str::to_owned),
            path: req.path.map(str::to_owned),
        })
    }

    /// 列目录时条目 `path` 能否显示（可读，或是某个可读路径的上级目录）。
    pub async fn path_visible(&self, p: &Principal, repo: &str, path: &str) -> Result<bool, AuthError> {
        if self
            .check(p, &Request::repo(repo, Action::Read).with_path(path))
            .await?
        {
            return Ok(true);
        }
        Ok(self.current_policy().is_some_and(|pol| pol.path_visible(p, repo, path)))
    }

    /// 是否有系统管理权限（策略、令牌）。
    pub async fn is_admin(&self, p: &Principal) -> Result<bool, AuthError> {
        if !self.required {
            return Ok(true);
        }
        if self.current_policy().is_some_and(|pol| pol.is_system_admin(p)) {
            return Ok(true);
        }
        self.webhook_allows(p, &Request::repo("*", Action::Admin)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientAuth, IssuerConfig, IssuerKey, OidcConfig, Secret, WebhookConfig};
    use crate::testing::FakeIdp;
    use scion_store::MemStore;
    use serde_json::json;

    fn lookup() -> Lookup {
        Arc::new(|k: &str| (k == "STARHUB_JWT_SIGNING_KEY").then(|| "signing-key".to_string()))
    }

    fn guard(idp: &FakeIdp, webhook: bool) -> Arc<Guard> {
        let mut auth = AuthConfig::default();
        auth.token.issuer = "https://scion.test".into();
        auth.issuers = vec![
            IssuerConfig {
                issuer: "OpenCSG".into(),
                key: IssuerKey::Hs256(Secret {
                    env: Some("STARHUB_JWT_SIGNING_KEY".into()),
                    ..Secret::default()
                }),
                audience: None,
                subject_claim: "current_user".into(),
                groups_claim: None,
                repos_claim: None,
            },
            IssuerConfig {
                issuer: idp.base(),
                key: IssuerKey::Jwks(idp.url("/jwks")),
                audience: Some("scion".into()),
                subject_claim: "sub".into(),
                groups_claim: Some("groups".into()),
                repos_claim: Some("repos".into()),
            },
        ];
        auth.oidc = Some(OidcConfig {
            issuer: idp.base(),
            client_id: "scn".into(),
            client_secret_env: None,
            scopes: vec!["openid".into()],
            subject_claim: "preferred_username".into(),
            groups_claim: Some("groups".into()),
        });
        if webhook {
            auth.authz_webhook = Some(WebhookConfig {
                url: idp.url("/authz"),
                auth: ClientAuth::Bearer(Secret {
                    value: Some("opa".into()),
                    ..Secret::default()
                }),
                cache_ttl: Duration::from_secs(30),
                timeout: Duration::from_secs(2),
                fail_open: false,
            });
        }
        Guard::build(
            GuardConfig {
                required: true,
                auth,
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard")
    }

    fn now() -> i64 {
        now_ms() / 1000
    }

    fn starhub_jwt(user: &str, exp: i64) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({"uuid": "u", "current_user": user, "iss": "OpenCSG", "exp": exp}),
            &EncodingKey::from_secret(b"signing-key"),
        )
        .expect("sign")
    }

    #[tokio::test]
    async fn disabled_guard_allows_everything() {
        let g = Guard::disabled();
        assert!(!g.required() && !g.can_issue() && !g.device_login());
        let p = g.authenticate(Some("whatever")).await.expect("anonymous");
        assert!(!p.authenticated);
        assert!(g.check(&p, &Request::repo("x", Action::Admin)).await.expect("ok"));
        assert!(g.is_admin(&p).await.expect("ok"));
        assert!(g.create_token("pat", "a", vec![], "", None).await.is_err(), "no store");
        assert!(g.exchange("scn_pat_x").await.is_err());
        assert!(g.start_device_login().await.is_err());
        assert!(g.poll_device_login("x").await.is_err());
    }

    #[tokio::test]
    async fn authenticates_every_token_kind() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false);
        assert!(g.required() && g.can_issue() && g.device_login());
        assert!(!g.authenticate(None).await.expect("anon").authenticated);
        assert!(!g.authenticate(Some("  ")).await.expect("anon").authenticated);

        // PAT
        let (pat, rec) = g
            .create_token("pat", "ci-bot", vec!["ci".into()], "CI", Some(Duration::from_secs(60)))
            .await
            .expect("pat");
        let p = g.authenticate(Some(&pat)).await.expect("pat");
        assert_eq!(
            (p.subject.as_str(), p.groups.as_slice()),
            ("ci-bot", &["ci".to_string()][..])
        );
        assert_eq!(
            g.list_tokens(Some("ci-bot")).await.expect("list"),
            std::slice::from_ref(&rec)
        );
        assert!(g.revoke_token(&rec.id).await.expect("revoke"));
        assert!(matches!(g.authenticate(Some(&pat)).await, Err(AuthError::Invalid(_))));
        let (old, _) = g
            .create_token("pat", "x", vec![], "", Some(Duration::ZERO))
            .await
            .expect("pat");
        assert_eq!(g.authenticate(Some(&old)).await, Err(AuthError::Expired));
        let (refresh, _) = g.create_token("refresh", "x", vec![], "", None).await.expect("refresh");
        assert!(
            g.authenticate(Some(&refresh)).await.is_err(),
            "refresh tokens are not access tokens"
        );
        assert!(g.create_token("pat", " ", vec![], "", None).await.is_err());
        assert!(g.create_token("pat", "a", vec!["a\nb".into()], "", None).await.is_err());

        // starhub 风格的 HS256 JWT
        let p = g
            .authenticate(Some(&starhub_jwt("alice", now() + 60)))
            .await
            .expect("starhub");
        assert_eq!(p.subject, "alice");
        assert_eq!(
            g.authenticate(Some(&starhub_jwt("alice", now() - 3600))).await,
            Err(AuthError::Expired)
        );

        // JWKS 签发方，带 repos claim
        let t = idp.sign(
            &json!({"iss": idp.base(), "aud": "scion", "sub": "svc", "groups": ["g"],
            "repos": {"models/*": "write"}, "exp": now() + 60}),
        );
        let p = g.authenticate(Some(&t)).await.expect("jwks");
        assert_eq!((p.subject.as_str(), p.grants.len()), ("svc", 1));
        let wrong_aud = idp.sign(&json!({"iss": idp.base(), "aud": "other", "sub": "svc", "exp": now() + 60}));
        assert!(g.authenticate(Some(&wrong_aud)).await.is_err());

        // 自签令牌：exchange 得到，可以直接用，但不能再拿去 exchange
        let (access, exp, subject) = g.exchange(&starhub_jwt("alice", now() + 60)).await.expect("exchange");
        assert_eq!(subject, "alice");
        assert!(exp > now_ms());
        let p = g.authenticate(Some(&access)).await.expect("own jwt");
        assert_eq!((p.subject.as_str(), p.expires_at_ms), ("alice", Some(exp)));
        assert!(g.exchange(&access).await.is_err());
        assert_eq!(g.exchange(&refresh).await.expect("refresh exchange").2, "x");
        assert!(matches!(g.exchange(" ").await, Err(AuthError::BadRequest(_))));

        // 未知签发方、无法识别的令牌（没有配置内省）
        let unknown = starhub_jwt("a", now() + 60).replace("eyJ", "eyK");
        assert!(g.authenticate(Some(&unknown)).await.is_err());
        assert!(matches!(
            g.authenticate(Some("opaque-foreign")).await,
            Err(AuthError::Invalid(_))
        ));
        assert!(g.exchange("opaque-foreign").await.is_err());
    }

    #[tokio::test]
    async fn device_login_creates_refresh_and_access_tokens() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false);
        let s = g.start_device_login().await.expect("start");
        let (state, tokens) = g.poll_device_login(&s.device_code).await.expect("poll");
        assert_eq!((state, tokens), (DevicePoll::Pending, None));
        idp.approve(&s.device_code);
        let (state, tokens) = g.poll_device_login(&s.device_code).await.expect("poll");
        assert!(matches!(state, DevicePoll::Complete(_)));
        let (refresh, access, _) = tokens.expect("tokens");
        assert_eq!(g.authenticate(Some(&access)).await.expect("access").subject, "alice");
        assert_eq!(g.exchange(&refresh).await.expect("refresh").2, "alice");
        let listed = g.list_tokens(Some("alice")).await.expect("list");
        assert_eq!((listed.len(), listed[0].kind.as_str()), (1, "refresh"));
    }

    #[tokio::test]
    async fn authorization_combines_policy_grants_and_webhook() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false);
        let alice = Principal::user("alice", vec!["eng".into()]);
        let anon = Principal::anonymous();
        // 没有策略和回调：已认证的身份可以读写合入，不能管理；匿名不行
        assert!(g.check(&alice, &Request::repo("any", Action::Land)).await.expect("ok"));
        assert!(!g.is_admin(&alice).await.expect("ok"));
        assert_eq!(
            g.authorize(&anon, &Request::repo("any", Action::Read)).await,
            Err(AuthError::Missing)
        );

        assert_eq!(g.policy_text().await.expect("text"), "");
        let n = g
            .set_policy(
                "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"write\"]\npaths=[\"src\"]\n\n\
                 [[rule]]\nsubjects=[\"user:root\"]\nrepos=[\"*\"]\nactions=[\"admin\"]\n",
            )
            .await
            .expect("set");
        assert_eq!(n, 2);
        assert!(
            g.check(&alice, &Request::repo("mono", Action::Write).with_path("src/a"))
                .await
                .expect("ok")
        );
        let denied = g.authorize(&alice, &Request::repo("mono", Action::Read)).await;
        assert!(
            matches!(denied, Err(AuthError::Denied { action: "read", .. })),
            "{denied:?}"
        );
        assert!(g.path_visible(&alice, "mono", "src").await.expect("ok"));
        assert!(!g.path_visible(&alice, "mono", "secret").await.expect("ok"));
        assert!(g.is_admin(&Principal::user("root", vec![])).await.expect("ok"));
        assert!(matches!(g.set_policy("[[rule]]").await, Err(AuthError::BadRequest(_))));

        // 令牌自带的授权
        let mut svc = Principal::user("svc", vec![]);
        svc.grants = vec![crate::policy::Grant {
            repo: "models/*".into(),
            action: Action::Write,
        }];
        assert!(
            g.check(&svc, &Request::repo("models/x", Action::Write))
                .await
                .expect("ok")
        );

        // 其它节点修改了策略：重新加载
        g.meta()
            .expect("meta")
            .put_setting(POLICY_KEY, b"")
            .await
            .expect("clear");
        assert_eq!(g.reload_policy().await.expect("reload"), 0);
        assert!(
            g.check(&alice, &Request::repo("other", Action::Read))
                .await
                .expect("default again")
        );
        g.meta()
            .expect("meta")
            .put_setting(
                POLICY_KEY,
                b"[[rule]]\nsubjects=[\"*\"]\nrepos=[\"r\"]\nactions=[\"read\"]",
            )
            .await
            .expect("set");
        assert_eq!(g.reload_policy().await.expect("reload"), 1);
        g.meta()
            .expect("meta")
            .put_setting(POLICY_KEY, b"broken [")
            .await
            .expect("set");
        assert!(
            g.reload_policy().await.is_err(),
            "a broken stored policy keeps the old one"
        );
        assert!(
            g.check(&alice, &Request::repo("r", Action::Read))
                .await
                .expect("old policy")
        );
    }

    #[tokio::test]
    async fn webhook_decides_when_configured() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, true);
        let alice = Principal::user("alice", vec![]);
        // 有回调时没有「已认证即可」的默认
        assert!(
            g.check(&alice, &Request::repo("models/a", Action::Read))
                .await
                .expect("ok")
        );
        assert!(
            !g.check(&alice, &Request::repo("models/a", Action::Write))
                .await
                .expect("ok")
        );
        assert!(
            g.check(&Principal::anonymous(), &Request::repo("public", Action::Read))
                .await
                .expect("public repo")
        );
        assert!(!g.is_admin(&alice).await.expect("ok"));
        // 回调不可用（on_error = closed）：错误
        let mut auth = AuthConfig::default();
        auth.authz_webhook = Some(WebhookConfig {
            url: "http://127.0.0.1:1/authz".into(),
            auth: ClientAuth::None,
            cache_ttl: Duration::from_secs(1),
            timeout: Duration::from_millis(200),
            fail_open: false,
        });
        let closed = Guard::build(
            GuardConfig {
                required: true,
                auth: auth.clone(),
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        assert!(matches!(
            closed.check(&alice, &Request::repo("x", Action::Read)).await,
            Err(AuthError::Unavailable(_))
        ));
        assert!(closed.exchange("scn_pat_x").await.is_err(), "unknown token");
        if let Some(w) = auth.authz_webhook.as_mut() {
            w.fail_open = true;
        }
        let open = Guard::build(
            GuardConfig {
                required: true,
                auth,
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        assert!(
            !open
                .check(&alice, &Request::repo("x", Action::Read))
                .await
                .expect("falls back to deny")
        );
    }

    #[test]
    fn build_rejects_bad_configuration() {
        let bad_key = GuardConfig {
            required: true,
            auth: AuthConfig::default(),
            signing_key_pem: Some("not a key".into()),
        };
        assert!(Guard::build(bad_key, Arc::new(MemStore::new()), lookup()).is_err());
        let auth = AuthConfig {
            issuers: vec![IssuerConfig {
                issuer: "x".into(),
                key: IssuerKey::Hs256(Secret {
                    env: Some("UNSET".into()),
                    ..Secret::default()
                }),
                audience: None,
                subject_claim: "sub".into(),
                groups_claim: None,
                repos_claim: None,
            }],
            ..AuthConfig::default()
        };
        let missing_secret = GuardConfig {
            required: true,
            auth,
            signing_key_pem: None,
        };
        assert!(Guard::build(missing_secret, Arc::new(MemStore::new()), lookup()).is_err());
    }

    #[test]
    fn error_conversions() {
        assert_eq!(AuthError::from(VerifyError::Expired), AuthError::Expired);
        assert!(matches!(
            AuthError::from(VerifyError::Unavailable("x".into())),
            AuthError::Unavailable(_)
        ));
        assert!(matches!(
            AuthError::from(scion_store::Error::Backend("db".into())),
            AuthError::Unavailable(_)
        ));
        assert!(AuthError::Missing.to_string().contains("required"));
    }
}
