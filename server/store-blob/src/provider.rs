//! S3 兼容服务的适配层：识别具体服务（MinIO、SeaweedFS、AWS S3 或其他），
//! 按服务选择默认请求风格、健康检查接口和出错提示。
//!
//! 数据读写只用 S3 核心子集（PUT / GET / HEAD），各服务行为一致；差异集中在部署与运维层面：
//!
//! | 服务 | 识别（`Server` 响应头） | 健康检查 | 建 bucket |
//! |---|---|---|---|
//! | MinIO | `MinIO` | `/minio/health/ready` | `mc mb` 或 `MINIO_DEFAULT_BUCKETS` |
//! | SeaweedFS | `SeaweedFS …` | `/healthz` | `weed shell` 中 `s3.bucket.create` |
//! | AWS S3 | `AmazonS3` | 无 | 控制台或 `aws s3 mb` |

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use scion_store::{Error, Result};

/// S3 兼容服务的种类。`Auto` 在启动检查时根据响应头识别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum S3Provider {
    #[default]
    Auto,
    Minio,
    SeaweedFs,
    Aws,
    /// 其他兼容 S3 的服务（Ceph RGW、阿里云 OSS 等），或前面有反向代理、无法识别的服务。
    Generic,
}

impl S3Provider {
    /// 配置值的写法，与 [`FromStr`] 对应。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Minio => "minio",
            Self::SeaweedFs => "seaweedfs",
            Self::Aws => "aws",
            Self::Generic => "generic",
        }
    }

    /// 根据 `Server` 响应头识别服务；无法识别时为 `Generic`。
    pub fn from_server_header(server: Option<&str>) -> Self {
        let s = server.unwrap_or("").trim().to_ascii_lowercase();
        if s.starts_with("minio") {
            Self::Minio
        } else if s.starts_with("seaweedfs") {
            Self::SeaweedFs
        } else if s.starts_with("amazons3") {
            Self::Aws
        } else {
            Self::Generic
        }
    }

    /// 自建服务必须给出 endpoint。
    pub fn requires_endpoint(self) -> bool {
        matches!(self, Self::Minio | Self::SeaweedFs)
    }

    /// 未显式配置时是否使用 virtual-hosted 风格。只有 AWS 默认使用；自建服务都支持 path 风格，
    /// 而 virtual-hosted 风格需要额外的通配 DNS。
    pub fn default_virtual_hosted(self) -> bool {
        self == Self::Aws
    }

    /// 不需要鉴权的健康检查接口。
    pub fn health_path(self) -> Option<&'static str> {
        match self {
            Self::Minio => Some("/minio/health/ready"),
            Self::SeaweedFs => Some("/healthz"),
            _ => None,
        }
    }

    /// bucket 不存在时告诉运维如何创建。
    pub fn create_bucket_hint(self, bucket: &str) -> String {
        match self {
            Self::Minio => format!(
                "create it with `mc mb <alias>/{bucket}`, or set MINIO_DEFAULT_BUCKETS={bucket} on the MinIO container"
            ),
            Self::SeaweedFs => format!("create it with `echo 's3.bucket.create -name {bucket}' | weed shell`"),
            Self::Aws => format!("create it with `aws s3 mb s3://{bucket}`"),
            Self::Auto | Self::Generic => format!("create bucket {bucket:?} with your storage service's tools"),
        }
    }
}

impl fmt::Display for S3Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for S3Provider {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "minio" => Ok(Self::Minio),
            "seaweedfs" | "seaweed" => Ok(Self::SeaweedFs),
            "aws" | "s3" => Ok(Self::Aws),
            "generic" => Ok(Self::Generic),
            other => Err(Error::Backend(format!(
                "unknown s3 provider {other:?}; expected auto, minio, seaweedfs, aws or generic"
            ))),
        }
    }
}

/// 启动检查的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Detected {
    /// 最终采用的服务种类（不会是 `Auto`）。
    pub provider: S3Provider,
    /// 服务返回的 `Server` 响应头，用于日志。
    pub server: Option<String>,
}

/// 访问 endpoint 根路径，按 `Server` 响应头识别服务，并与配置比对，然后检查健康接口。
///
/// - 配置为 `Auto`：采用识别结果；
/// - 配置为具体服务、识别出的是另一种已知服务：报错（多半是 endpoint 指错了）；
/// - 识别不出（例如前面有反向代理）：相信配置。
///
/// 与数据读写一样遵循系统代理设置（`HTTP(S)_PROXY` / `NO_PROXY`）。
pub async fn detect(endpoint: &str, configured: S3Provider) -> Result<Detected> {
    detect_with(&http_client()?, endpoint, configured).await
}

async fn detect_with(client: &reqwest::Client, endpoint: &str, configured: S3Provider) -> Result<Detected> {
    let base = endpoint.trim_end_matches('/');
    // 根路径未鉴权时返回 403，但响应头里已有服务标识
    let resp = client
        .get(format!("{base}/"))
        .send()
        .await
        .map_err(|e| unreachable(endpoint, &e))?;
    let server = resp
        .headers()
        .get(reqwest::header::SERVER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let seen = S3Provider::from_server_header(server.as_deref());
    let provider = match configured {
        S3Provider::Auto => seen,
        c if seen == S3Provider::Generic || seen == c => c,
        c => {
            return Err(Error::Backend(format!(
                "s3 provider is configured as {c} but {endpoint} identifies as {seen} (Server: {}); \
                 fix SCION_S3_PROVIDER or SCION_S3_ENDPOINT",
                server.as_deref().unwrap_or("")
            )));
        }
    };
    if let Some(path) = provider.health_path() {
        let status = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .map_err(|e| unreachable(endpoint, &e))?
            .status();
        if !status.is_success() {
            return Err(Error::Backend(format!(
                "{provider} at {endpoint} is not ready ({path} returned {})",
                status.as_u16()
            )));
        }
    }
    Ok(Detected { provider, server })
}

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| Error::Backend(format!("build http client: {e}")))
}

fn unreachable(endpoint: &str, e: &reqwest::Error) -> Error {
    tracing::warn!(error = %e, endpoint, "s3 endpoint unreachable");
    Error::Backend(format!("s3 endpoint {endpoint} is unreachable"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 极简 HTTP 服务：按路径返回 (状态码, Server 头)，未列出的路径返回 403。
    async fn fake_server(routes: &'static [(&'static str, u16)], server: Option<&'static str>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    let status = routes.iter().find(|(p, _)| *p == path).map_or(403, |(_, s)| *s);
                    let header = server.map(|s| format!("Server: {s}\r\n")).unwrap_or_default();
                    let resp = format!("HTTP/1.1 {status} X\r\n{header}Content-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// 测试直连本地假服务，不受开发机代理设置影响。
    fn direct() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().expect("client")
    }

    #[tokio::test]
    async fn detect_uses_default_client() {
        let err = detect("http://127.0.0.1:1", S3Provider::Minio).await.expect_err("down");
        assert!(matches!(err, Error::Backend(_)));
    }

    #[test]
    fn parse_and_display_round_trip() {
        for p in [
            S3Provider::Auto,
            S3Provider::Minio,
            S3Provider::SeaweedFs,
            S3Provider::Aws,
            S3Provider::Generic,
        ] {
            assert_eq!(p.to_string().parse::<S3Provider>(), Ok(p));
        }
        assert_eq!(" MinIO ".parse::<S3Provider>(), Ok(S3Provider::Minio));
        assert_eq!("seaweed".parse::<S3Provider>(), Ok(S3Provider::SeaweedFs));
        assert_eq!("s3".parse::<S3Provider>(), Ok(S3Provider::Aws));
        assert_eq!("".parse::<S3Provider>(), Ok(S3Provider::Auto));
        assert!(matches!("ceph".parse::<S3Provider>(), Err(Error::Backend(m)) if m.contains("ceph")));
    }

    #[test]
    fn server_header_identifies_provider() {
        assert_eq!(S3Provider::from_server_header(Some("MinIO")), S3Provider::Minio);
        assert_eq!(
            S3Provider::from_server_header(Some("SeaweedFS 30GB 4.48")),
            S3Provider::SeaweedFs
        );
        assert_eq!(S3Provider::from_server_header(Some("AmazonS3")), S3Provider::Aws);
        assert_eq!(S3Provider::from_server_header(Some("nginx/1.25")), S3Provider::Generic);
        assert_eq!(S3Provider::from_server_header(None), S3Provider::Generic);
    }

    #[test]
    fn per_provider_defaults() {
        assert!(S3Provider::Minio.requires_endpoint() && S3Provider::SeaweedFs.requires_endpoint());
        assert!(!S3Provider::Aws.requires_endpoint() && !S3Provider::Generic.requires_endpoint());
        assert!(S3Provider::Aws.default_virtual_hosted());
        assert!(!S3Provider::Minio.default_virtual_hosted() && !S3Provider::SeaweedFs.default_virtual_hosted());
        assert_eq!(S3Provider::Minio.health_path(), Some("/minio/health/ready"));
        assert_eq!(S3Provider::SeaweedFs.health_path(), Some("/healthz"));
        assert_eq!(S3Provider::Generic.health_path(), None);
        assert!(S3Provider::Minio.create_bucket_hint("b").contains("mc mb <alias>/b"));
        assert!(
            S3Provider::SeaweedFs
                .create_bucket_hint("b")
                .contains("s3.bucket.create -name b")
        );
        assert!(S3Provider::Aws.create_bucket_hint("b").contains("aws s3 mb s3://b"));
        assert!(S3Provider::Generic.create_bucket_hint("b").contains("\"b\""));
    }

    #[tokio::test]
    async fn detect_identifies_and_checks_health() {
        let minio = fake_server(&[("/minio/health/ready", 200)], Some("MinIO")).await;
        let d = detect_with(&direct(), &minio, S3Provider::Auto).await.expect("minio");
        assert_eq!((d.provider, d.server.as_deref()), (S3Provider::Minio, Some("MinIO")));
        // 结尾的 / 不影响拼接
        assert_eq!(
            detect_with(&direct(), &format!("{minio}/"), S3Provider::Minio)
                .await
                .expect("minio")
                .provider,
            S3Provider::Minio
        );

        let weed = fake_server(&[("/healthz", 200)], Some("SeaweedFS 30GB 4.48")).await;
        assert_eq!(
            detect_with(&direct(), &weed, S3Provider::Auto)
                .await
                .expect("weed")
                .provider,
            S3Provider::SeaweedFs
        );
    }

    #[tokio::test]
    async fn detect_trusts_config_behind_proxy_and_rejects_mismatch() {
        // 反向代理遮住了服务标识：相信配置，并按配置做健康检查
        let proxied = fake_server(&[("/minio/health/ready", 200)], Some("nginx")).await;
        assert_eq!(
            detect_with(&direct(), &proxied, S3Provider::Minio)
                .await
                .expect("trust")
                .provider,
            S3Provider::Minio
        );
        assert_eq!(
            detect_with(&direct(), &proxied, S3Provider::Auto)
                .await
                .expect("generic")
                .provider,
            S3Provider::Generic
        );
        let bare = fake_server(&[], None).await;
        assert_eq!(
            detect_with(&direct(), &bare, S3Provider::Generic)
                .await
                .expect("generic")
                .provider,
            S3Provider::Generic
        );
        // 配置是 SeaweedFS，实际是 MinIO
        let minio = fake_server(&[("/minio/health/ready", 200)], Some("MinIO")).await;
        let err = detect_with(&direct(), &minio, S3Provider::SeaweedFs)
            .await
            .expect_err("mismatch");
        assert!(matches!(&err, Error::Backend(m) if m.contains("configured as seaweedfs") && m.contains("minio")));
    }

    #[tokio::test]
    async fn detect_reports_unready_and_unreachable() {
        let starting = fake_server(&[("/minio/health/ready", 503)], Some("MinIO")).await;
        let err = detect_with(&direct(), &starting, S3Provider::Auto)
            .await
            .expect_err("not ready");
        assert!(matches!(&err, Error::Backend(m) if m.contains("not ready") && m.contains("503")));

        let err = detect_with(&direct(), "http://127.0.0.1:1", S3Provider::Auto)
            .await
            .expect_err("down");
        assert!(matches!(&err, Error::Backend(m) if m.contains("unreachable")));
    }
}
