//! OIDC device flow（RFC 8628），供 `scn login` 使用。服务端代为调用 IdP：客户端不需要知道 IdP 的
//! 地址和 client id，IdP 返回的 ID token 由服务端按 JWKS 验证后换成 Scion 的令牌。

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::config::{IssuerConfig, IssuerKey, OidcConfig};
use crate::external::{ExternalIssuer, VerifyError};
use crate::policy::Principal;
use crate::remote::Lookup;

#[derive(Debug, Clone, Deserialize)]
struct Discovery {
    issuer: String,
    device_authorization_endpoint: Option<String>,
    token_endpoint: String,
    jwks_uri: String,
}

/// device flow 第一步的结果。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DeviceStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    #[serde(default = "default_expires")]
    pub expires_in: u64,
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_expires() -> u64 {
    600
}

fn default_interval() -> u64 {
    5
}

/// 轮询结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DevicePoll {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Complete(Principal),
}

struct Endpoints {
    device: String,
    token: String,
    id_tokens: ExternalIssuer,
}

pub struct Oidc {
    config: OidcConfig,
    http: reqwest::Client,
    lookup: Lookup,
    endpoints: OnceCell<Endpoints>,
}

impl Oidc {
    pub fn new(config: OidcConfig, http: reqwest::Client, lookup: Lookup) -> Self {
        Self {
            config,
            http,
            lookup,
            endpoints: OnceCell::new(),
        }
    }

    /// 读取 IdP 的发现文档（成功后缓存）。
    async fn endpoints(&self) -> Result<&Endpoints, VerifyError> {
        self.endpoints
            .get_or_try_init(|| async {
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    self.config.issuer.trim_end_matches('/')
                );
                let d: Discovery = self
                    .http
                    .get(&url)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                    .map_err(|e| VerifyError::Unavailable(format!("OIDC discovery: {e}")))?
                    .json()
                    .await
                    .map_err(|e| VerifyError::Unavailable(format!("OIDC discovery document: {e}")))?;
                let device = d.device_authorization_endpoint.ok_or_else(|| {
                    VerifyError::Unavailable("the identity provider does not support the device flow".into())
                })?;
                let id_tokens = ExternalIssuer::new(
                    IssuerConfig {
                        issuer: d.issuer,
                        key: IssuerKey::Jwks(d.jwks_uri),
                        audience: Some(self.config.client_id.clone()),
                        subject_claim: self.config.subject_claim.clone(),
                        groups_claim: self.config.groups_claim.clone(),
                        repos_claim: None,
                    },
                    self.http.clone(),
                    &*self.lookup,
                )
                .map_err(|e| VerifyError::Unavailable(e.to_string()))?;
                Ok(Endpoints {
                    device,
                    token: d.token_endpoint,
                    id_tokens,
                })
            })
            .await
    }

    fn client_form(&self) -> Result<Vec<(&'static str, String)>, VerifyError> {
        let mut form = vec![("client_id", self.config.client_id.clone())];
        if let Some(env) = &self.config.client_secret_env {
            let secret = (self.lookup)(env)
                .ok_or_else(|| VerifyError::Unavailable(format!("environment variable {env} is not set")))?;
            form.push(("client_secret", secret));
        }
        Ok(form)
    }

    pub async fn start(&self) -> Result<DeviceStart, VerifyError> {
        let ep = self.endpoints().await?;
        let mut form = self.client_form()?;
        form.push(("scope", self.config.scopes.join(" ")));
        self.http
            .post(&ep.device)
            .form(&form)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| VerifyError::Unavailable(format!("device authorization: {e}")))?
            .json()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("device authorization response: {e}")))
    }

    pub async fn poll(&self, device_code: &str) -> Result<DevicePoll, VerifyError> {
        let ep = self.endpoints().await?;
        let mut form = self.client_form()?;
        form.push(("grant_type", "urn:ietf:params:oauth:grant-type:device_code".into()));
        form.push(("device_code", device_code.to_owned()));
        let resp = self
            .http
            .post(&ep.token)
            .form(&form)
            .send()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("token endpoint: {e}")))?;
        let ok = resp.status().is_success();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("token endpoint response: {e}")))?;
        if !ok {
            return Ok(match body.get("error").and_then(Value::as_str) {
                Some("authorization_pending") => DevicePoll::Pending,
                Some("slow_down") => DevicePoll::SlowDown,
                Some("access_denied") => DevicePoll::Denied,
                Some("expired_token") => DevicePoll::Expired,
                other => return Err(VerifyError::Invalid(format!("token endpoint error {other:?}"))),
            });
        }
        let id_token = body
            .get("id_token")
            .and_then(Value::as_str)
            .ok_or_else(|| VerifyError::Invalid("token response has no id_token".into()))?;
        ep.id_tokens.verify(id_token).await.map(DevicePoll::Complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeIdp;

    fn oidc(issuer: String, secret_env: Option<&str>) -> Oidc {
        Oidc::new(
            OidcConfig {
                issuer,
                client_id: "scn".into(),
                client_secret_env: secret_env.map(str::to_owned),
                scopes: vec!["openid".into()],
                subject_claim: "preferred_username".into(),
                groups_claim: Some("groups".into()),
            },
            reqwest::Client::new(),
            std::sync::Arc::new(|k: &str| (k == "CLIENT_SECRET").then(|| "cs".to_string())),
        )
    }

    #[tokio::test]
    async fn device_flow_end_to_end() {
        let idp = FakeIdp::start().await;
        let o = oidc(idp.base(), Some("CLIENT_SECRET"));
        let s = o.start().await.expect("start");
        assert_eq!((s.user_code.as_str(), s.interval), ("ABCD-EFGH", 1));
        assert_eq!(o.poll(&s.device_code).await, Ok(DevicePoll::Pending));
        assert_eq!(o.poll("slow").await, Ok(DevicePoll::SlowDown));
        assert_eq!(o.poll("denied").await, Ok(DevicePoll::Denied));
        assert_eq!(o.poll("expired").await, Ok(DevicePoll::Expired));
        assert!(matches!(o.poll("weird").await, Err(VerifyError::Invalid(_))));
        assert!(matches!(o.poll("no-id-token").await, Err(VerifyError::Invalid(_))));
        idp.approve(&s.device_code);
        match o.poll(&s.device_code).await.expect("poll") {
            DevicePoll::Complete(p) => {
                assert_eq!(p.subject, "alice");
                assert_eq!(p.groups, ["eng"]);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(idp.calls("/.well-known/openid-configuration"), 1, "discovery cached");
    }

    #[tokio::test]
    async fn discovery_and_configuration_errors() {
        let idp = FakeIdp::start().await;
        assert!(oidc("http://127.0.0.1:1".into(), None).start().await.is_err());
        assert!(
            oidc(idp.url("/no-device"), None).start().await.is_err(),
            "no device endpoint"
        );
        assert!(oidc(idp.url("/not-json"), None).start().await.is_err());
        let missing_secret = oidc(idp.base(), Some("UNSET"));
        assert!(missing_secret.start().await.is_err());
        assert!(missing_secret.poll("x").await.is_err());
    }
}
