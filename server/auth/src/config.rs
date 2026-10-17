//! 认证配置（`auth.toml`）。全部是标准协议，不含任何特定系统的代码：
//!
//! ```toml
//! [token]                          # 服务端自签的访问令牌（ES256）
//! issuer = "https://scion.example.com"
//! ttl = "15m"
//!
//! [[issuer]]                       # 受信任的外部 JWT 签发方，可以有多个
//! issuer = "https://keycloak.example.com/realms/dev"
//! jwks_url = "https://keycloak.example.com/realms/dev/protocol/openid-connect/certs"
//! audience = "scion"               # 可选
//! subject_claim = "preferred_username"   # 默认 sub
//! groups_claim = "groups"          # 可选
//!
//! [[issuer]]                       # 共享密钥（HS256）签发的 JWT，例如 starhub-server
//! issuer = "OpenCSG"
//! hs256_secret_env = "STARHUB_JWT_SIGNING_KEY"   # 或 hs256_secret_file
//! subject_claim = "current_user"
//!
//! [introspection]                  # RFC 7662：校验外部的不透明令牌
//! url = "https://idp.example.com/oauth2/introspect"
//! bearer_env = "INTROSPECT_TOKEN"  # 或 client_id + client_secret_env（HTTP Basic）
//! cache_ttl = "60s"
//!
//! [authz_webhook]                  # 外部鉴权，兼容 OPA 的 REST 接口
//! url = "http://opa:8181/v1/data/scion/allow"
//! cache_ttl = "30s"
//! timeout = "2s"
//!
//! [oidc]                           # scn login 的 device flow
//! issuer = "https://casdoor.example.com"
//! client_id = "scn"
//! ```

use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("invalid auth config: {0}")]
    Syntax(String),
    #[error("invalid auth config: {0}")]
    Invalid(String),
}

/// 时长：数字加 ms、s、m、h、d 后缀。
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
    let secs = |k: u64| Duration::from_secs(n.saturating_mul(k));
    match unit {
        "ms" => Ok(Duration::from_millis(n)),
        "s" => Ok(secs(1)),
        "m" => Ok(secs(60)),
        "h" => Ok(secs(3600)),
        "d" => Ok(secs(86_400)),
        _ => Err(format!("invalid duration unit in {s:?}; use ms, s, m, h or d")),
    }
}

fn duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    s.map(|s| parse_duration(&s).map_err(serde::de::Error::custom))
        .transpose()
}

/// 密钥的来源：直接写在配置里、环境变量或文件（推荐后两者）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Secret {
    pub value: Option<String>,
    pub env: Option<String>,
    pub file: Option<String>,
}

impl Secret {
    fn from_parts(value: Option<String>, env: Option<String>, file: Option<String>) -> Option<Self> {
        (value.is_some() || env.is_some() || file.is_some()).then_some(Self { value, env, file })
    }

    /// 读取密钥。`lookup` 读环境变量（测试可替换）。
    pub fn resolve(&self, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, ConfigError> {
        if let Some(v) = &self.value {
            return Ok(v.clone());
        }
        if let Some(e) = &self.env {
            return lookup(e)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| ConfigError::Invalid(format!("environment variable {e} is not set")));
        }
        if let Some(f) = &self.file {
            return std::fs::read_to_string(f)
                .map(|s| s.trim().to_owned())
                .map_err(|e| ConfigError::Invalid(format!("read {f}: {e}")));
        }
        Err(ConfigError::Invalid("empty secret".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    #[serde(default = "default_issuer")]
    pub issuer: String,
    #[serde(default, deserialize_with = "duration")]
    pub ttl: Option<Duration>,
}

fn default_issuer() -> String {
    "scion".into()
}

impl Default for TokenConfig {
    fn default() -> Self {
        Self {
            issuer: default_issuer(),
            ttl: None,
        }
    }
}

impl TokenConfig {
    pub fn ttl(&self) -> Duration {
        self.ttl.unwrap_or(Duration::from_secs(15 * 60))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIssuer {
    issuer: String,
    jwks_url: Option<String>,
    hs256_secret: Option<String>,
    hs256_secret_env: Option<String>,
    hs256_secret_file: Option<String>,
    audience: Option<String>,
    subject_claim: Option<String>,
    groups_claim: Option<String>,
    repos_claim: Option<String>,
}

/// 外部签发方的验证密钥。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssuerKey {
    /// 非对称密钥（RS256 / ES256 等），从 JWKS 获取。
    Jwks(String),
    /// HS256 共享密钥。
    Hs256(Secret),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerConfig {
    pub issuer: String,
    pub key: IssuerKey,
    pub audience: Option<String>,
    /// 身份所在的 claim，默认 `sub`。
    pub subject_claim: String,
    /// 组列表所在的 claim（字符串数组）。
    pub groups_claim: Option<String>,
    /// 仓库授权所在的 claim：`{"repo": "read|write|land|admin"}`。
    pub repos_claim: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIntrospection {
    url: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    client_secret_env: Option<String>,
    bearer: Option<String>,
    bearer_env: Option<String>,
    #[serde(default, deserialize_with = "duration")]
    cache_ttl: Option<Duration>,
    subject_claim: Option<String>,
    groups_claim: Option<String>,
}

/// 调用外部接口时的认证方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientAuth {
    None,
    Bearer(Secret),
    Basic { client_id: String, secret: Secret },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionConfig {
    pub url: String,
    pub auth: ClientAuth,
    pub cache_ttl: Duration,
    pub subject_claim: String,
    pub groups_claim: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWebhook {
    url: String,
    bearer: Option<String>,
    bearer_env: Option<String>,
    #[serde(default, deserialize_with = "duration")]
    cache_ttl: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    timeout: Option<Duration>,
    /// 回调不可用时：closed（拒绝，默认）或 open（只按 ACL 判断）。
    on_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookConfig {
    pub url: String,
    pub auth: ClientAuth,
    pub cache_ttl: Duration,
    pub timeout: Duration,
    pub fail_open: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret_env: Option<String>,
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    #[serde(default = "default_subject_claim")]
    pub subject_claim: String,
    pub groups_claim: Option<String>,
}

fn default_scopes() -> Vec<String> {
    vec!["openid".into(), "profile".into(), "email".into()]
}

fn default_subject_claim() -> String {
    "sub".into()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    token: TokenConfig,
    #[serde(default)]
    issuer: Vec<RawIssuer>,
    introspection: Option<RawIntrospection>,
    authz_webhook: Option<RawWebhook>,
    oidc: Option<OidcConfig>,
}

/// 完整的认证配置。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthConfig {
    pub token: TokenConfig,
    pub issuers: Vec<IssuerConfig>,
    pub introspection: Option<IntrospectionConfig>,
    pub authz_webhook: Option<WebhookConfig>,
    pub oidc: Option<OidcConfig>,
}

fn client_auth(
    bearer: Option<String>,
    bearer_env: Option<String>,
    basic: Option<(String, Option<String>, Option<String>)>,
) -> ClientAuth {
    if let Some(s) = Secret::from_parts(bearer, bearer_env, None) {
        return ClientAuth::Bearer(s);
    }
    match basic {
        Some((client_id, value, env)) => ClientAuth::Basic {
            client_id,
            secret: Secret::from_parts(value, env, None).unwrap_or_default(),
        },
        None => ClientAuth::None,
    }
}

impl AuthConfig {
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| ConfigError::Syntax(e.to_string()))?;
        let mut issuers = Vec::with_capacity(raw.issuer.len());
        for i in raw.issuer {
            let hs = Secret::from_parts(i.hs256_secret, i.hs256_secret_env, i.hs256_secret_file);
            let key = match (i.jwks_url, hs) {
                (Some(url), None) => IssuerKey::Jwks(url),
                (None, Some(s)) => IssuerKey::Hs256(s),
                _ => {
                    return Err(ConfigError::Invalid(format!(
                        "issuer {:?}: set exactly one of jwks_url or hs256_secret[_env|_file]",
                        i.issuer
                    )));
                }
            };
            issuers.push(IssuerConfig {
                issuer: i.issuer,
                key,
                audience: i.audience,
                subject_claim: i.subject_claim.unwrap_or_else(default_subject_claim),
                groups_claim: i.groups_claim,
                repos_claim: i.repos_claim,
            });
        }
        let introspection = raw.introspection.map(|r| IntrospectionConfig {
            url: r.url,
            auth: client_auth(
                r.bearer,
                r.bearer_env,
                r.client_id.map(|id| (id, r.client_secret, r.client_secret_env)),
            ),
            cache_ttl: r.cache_ttl.unwrap_or(Duration::from_secs(60)),
            subject_claim: r.subject_claim.unwrap_or_else(|| "username".into()),
            groups_claim: r.groups_claim,
        });
        let authz_webhook = match raw.authz_webhook {
            Some(r) => {
                let fail_open = match r.on_error.as_deref() {
                    None | Some("closed") => false,
                    Some("open") => true,
                    Some(other) => {
                        return Err(ConfigError::Invalid(format!(
                            "authz_webhook.on_error must be closed or open, got {other:?}"
                        )));
                    }
                };
                Some(WebhookConfig {
                    url: r.url,
                    auth: client_auth(r.bearer, r.bearer_env, None),
                    cache_ttl: r.cache_ttl.unwrap_or(Duration::from_secs(30)),
                    timeout: r.timeout.unwrap_or(Duration::from_secs(2)),
                    fail_open,
                })
            }
            None => None,
        };
        Ok(Self {
            token: raw.token,
            issuers,
            introspection,
            authz_webhook,
            oidc: raw.oidc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
[token]
issuer = "https://scion.test"
ttl = "10m"

[[issuer]]
issuer = "https://kc.test/realms/dev"
jwks_url = "https://kc.test/certs"
audience = "scion"
subject_claim = "preferred_username"
groups_claim = "groups"
repos_claim = "repos"

[[issuer]]
issuer = "OpenCSG"
hs256_secret_env = "STARHUB_JWT_SIGNING_KEY"
subject_claim = "current_user"

[introspection]
url = "https://idp.test/introspect"
client_id = "scion"
client_secret_env = "SECRET"
cache_ttl = "2m"

[authz_webhook]
url = "http://opa:8181/v1/data/scion/allow"
bearer_env = "OPA_TOKEN"
timeout = "500ms"
on_error = "open"

[oidc]
issuer = "https://casdoor.test"
client_id = "scn"
"#;

    #[test]
    fn parses_full_config() {
        let c = AuthConfig::parse(FULL).expect("parse");
        assert_eq!(c.token.issuer, "https://scion.test");
        assert_eq!(c.token.ttl(), Duration::from_secs(600));
        assert_eq!(c.issuers.len(), 2);
        assert_eq!(c.issuers[0].key, IssuerKey::Jwks("https://kc.test/certs".into()));
        assert_eq!(c.issuers[0].subject_claim, "preferred_username");
        assert_eq!(c.issuers[1].subject_claim, "current_user");
        assert!(
            matches!(&c.issuers[1].key, IssuerKey::Hs256(s) if s.env.as_deref() == Some("STARHUB_JWT_SIGNING_KEY"))
        );
        let i = c.introspection.expect("introspection");
        assert_eq!(i.cache_ttl, Duration::from_secs(120));
        assert!(matches!(i.auth, ClientAuth::Basic { ref client_id, .. } if client_id == "scion"));
        assert_eq!(i.subject_claim, "username");
        let w = c.authz_webhook.expect("webhook");
        assert_eq!(
            (w.timeout, w.cache_ttl, w.fail_open),
            (Duration::from_millis(500), Duration::from_secs(30), true)
        );
        assert!(matches!(w.auth, ClientAuth::Bearer(_)));
        let o = c.oidc.expect("oidc");
        assert_eq!((o.scopes.len(), o.subject_claim.as_str()), (3, "sub"));
    }

    #[test]
    fn defaults_and_errors() {
        let c = AuthConfig::parse("").expect("empty");
        assert_eq!(c, AuthConfig::default());
        assert_eq!(c.token.ttl(), Duration::from_secs(900));
        assert!(matches!(AuthConfig::parse("[token\n"), Err(ConfigError::Syntax(_))));
        let both = "[[issuer]]\nissuer=\"x\"\njwks_url=\"u\"\nhs256_secret=\"s\"";
        assert!(matches!(AuthConfig::parse(both), Err(ConfigError::Invalid(_))));
        let neither = "[[issuer]]\nissuer=\"x\"";
        assert!(matches!(AuthConfig::parse(neither), Err(ConfigError::Invalid(_))));
        let bad_mode = "[authz_webhook]\nurl=\"u\"\non_error=\"maybe\"";
        assert!(matches!(AuthConfig::parse(bad_mode), Err(ConfigError::Invalid(_))));
        let bad_ttl = "[token]\nttl=\"forever\"";
        assert!(matches!(AuthConfig::parse(bad_ttl), Err(ConfigError::Syntax(_))));
        let none = "[introspection]\nurl=\"u\"";
        assert_eq!(
            AuthConfig::parse(none).expect("ok").introspection.map(|i| i.auth),
            Some(ClientAuth::None)
        );
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_duration("1d"), Ok(Duration::from_secs(86_400)));
        assert!(parse_duration("3y").is_err());
        assert!(parse_duration("m").is_err());
    }

    #[test]
    fn secrets_resolve_from_value_env_or_file() {
        let none = |_: &str| None;
        let env = |k: &str| (k == "K").then(|| "from-env".to_string());
        let v = Secret::from_parts(Some("inline".into()), None, None).expect("secret");
        assert_eq!(v.resolve(&none), Ok("inline".into()));
        let e = Secret::from_parts(None, Some("K".into()), None).expect("secret");
        assert_eq!(e.resolve(&env), Ok("from-env".into()));
        assert!(e.resolve(&none).is_err());
        let dir = std::env::temp_dir().join(format!("scion-secret-{}", std::process::id()));
        std::fs::write(&dir, "from-file\n").expect("write");
        let f = Secret::from_parts(None, None, Some(dir.display().to_string())).expect("secret");
        assert_eq!(f.resolve(&none), Ok("from-file".into()));
        let _ = std::fs::remove_file(&dir);
        let missing = Secret::from_parts(None, None, Some("/nonexistent/secret".into())).expect("secret");
        assert!(missing.resolve(&none).is_err());
        assert!(Secret::default().resolve(&none).is_err());
        assert!(Secret::from_parts(None, None, None).is_none());
    }
}
