//! 基于对象存储的 [`BlobStore`]（服务端设计文档 §4.5、§4.6）：
//!
//! - [`ObjectBlobStore::local`]：本地文件系统，Lite 档使用；
//! - [`ObjectBlobStore::s3`]：任意兼容 S3 的服务（AWS S3、MinIO、SeaweedFS、Ceph RGW、阿里云 OSS 等）。
//!
//! 对象按内容寻址、全局只存一份，key 为 `{prefix}objects/{kind}/{hh}/{hex}`。
//! 只使用 S3 的核心子集（PUT / GET / HEAD），不依赖条件写：同一 key 的内容永远相同，重复写入天然幂等。
//! 读取时校验 BLAKE3，磁盘或存储服务的静默损坏会以错误形式暴露，而不是把坏数据发给客户端。
//!
//! 不同 S3 兼容服务（MinIO、SeaweedFS 等）的差异由 [`provider`] 适配：启动检查
//! [`ObjectBlobStore::check`] 识别服务、做对应的健康检查，并在 bucket 缺失时给出该服务的创建方法。

mod provider;

pub use provider::{Detected, S3Provider, detect};

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use scion_object::ObjectId;
use scion_store::{BlobStore, Error, Result};

/// S3 兼容存储的连接参数。
#[derive(Clone, Default, PartialEq, Eq)]
pub struct S3Config {
    /// 服务地址，如 `http://minio:9000`；访问 AWS S3 时留空。
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// 服务种类；`Auto` 时在 [`ObjectBlobStore::check`] 中识别。
    pub provider: S3Provider,
    /// 是否使用 virtual-hosted 风格（`bucket.host`）。`None` 按服务决定：只有 AWS 默认使用，
    /// MinIO、SeaweedFS 等自建服务使用 path 风格。
    pub virtual_hosted_style: Option<bool>,
    /// key 前缀，多个部署共用一个 bucket 时用来隔离，如 `scion/`。
    pub prefix: String,
    /// 单个请求的最大重试次数；`None` 使用默认值（10 次，指数退避）。
    pub max_retries: Option<usize>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("provider", &self.provider)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("virtual_hosted_style", &self.virtual_hosted_style)
            .field("prefix", &self.prefix)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// S3 后端的服务信息；本地目录为 None。
    s3: Option<S3Target>,
}

#[derive(Debug, Clone)]
struct S3Target {
    endpoint: Option<String>,
    bucket: String,
    provider: S3Provider,
}

/// 启动检查识别出的后端。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobBackend {
    Local,
    S3(Detected),
}

impl std::fmt::Display for BlobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::S3(d) => match &d.server {
                Some(s) => write!(f, "s3 ({}, Server: {s})", d.provider),
                None => write!(f, "s3 ({})", d.provider),
            },
        }
    }
}

impl ObjectBlobStore {
    /// 本地目录（不存在则创建）。
    pub fn local(dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| Error::Backend(format!("create blob dir: {e}")))?;
        let fs = LocalFileSystem::new_with_prefix(dir).map_err(|e| backend("open blob dir", e))?;
        Ok(Self {
            store: Arc::new(fs),
            prefix: String::new(),
            s3: None,
        })
    }

    /// S3 兼容存储。只构造客户端，不发起网络请求；用 [`ObjectBlobStore::self_test`] 检查连通性。
    pub fn s3(cfg: &S3Config) -> Result<Self> {
        let mut retry = RetryConfig::default();
        if let Some(n) = cfg.max_retries {
            retry.max_retries = n;
        }
        Self::s3_with_retry(cfg, retry)
    }

    fn s3_with_retry(cfg: &S3Config, retry: RetryConfig) -> Result<Self> {
        if cfg.bucket.trim().is_empty() {
            return Err(Error::Backend("s3 bucket must not be empty".into()));
        }
        if cfg.endpoint.is_none() && cfg.provider.requires_endpoint() {
            return Err(Error::Backend(format!(
                "s3 provider {} needs an endpoint (SCION_S3_ENDPOINT)",
                cfg.provider
            )));
        }
        // 没有 endpoint 只能是 AWS
        let provider = match (cfg.provider, &cfg.endpoint) {
            (S3Provider::Auto, None) => S3Provider::Aws,
            (p, _) => p,
        };
        let virtual_hosted = cfg
            .virtual_hosted_style
            .unwrap_or_else(|| provider.default_virtual_hosted());
        let mut b = AmazonS3Builder::new()
            .with_retry(retry)
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_access_key_id(&cfg.access_key_id)
            .with_secret_access_key(&cfg.secret_access_key)
            .with_virtual_hosted_style_request(virtual_hosted);
        if let Some(endpoint) = &cfg.endpoint {
            b = b
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let store = b.build().map_err(|e| backend("configure s3", e))?;
        Ok(Self {
            store: Arc::new(store),
            prefix: normalize_prefix(&cfg.prefix),
            s3: Some(S3Target {
                endpoint: cfg.endpoint.clone(),
                bucket: cfg.bucket.clone(),
                provider,
            }),
        })
    }

    /// 启动检查：S3 后端先识别服务并做该服务的健康检查（见 [`provider`]），再做读写自检；
    /// bucket 不存在时报错并给出该服务的创建方法。
    pub async fn check(&self) -> Result<BlobBackend> {
        let Some(t) = &self.s3 else {
            self.self_test().await?;
            return Ok(BlobBackend::Local);
        };
        let detected = match &t.endpoint {
            Some(endpoint) => detect(endpoint, t.provider).await?,
            None => Detected {
                provider: t.provider,
                server: None,
            },
        };
        if !self.bucket_exists().await? {
            return Err(Error::Backend(format!(
                "s3 bucket {:?} does not exist; {}",
                t.bucket,
                detected.provider.create_bucket_hint(&t.bucket)
            )));
        }
        self.self_test().await?;
        Ok(BlobBackend::S3(detected))
    }

    /// 列一次前缀判断 bucket 是否存在。存在但为空时返回空列表；不存在时 MinIO 与 SeaweedFS 都返回
    /// 404 `NoSuchBucket`，object_store 把它包成通用错误，只能从错误链里认出来。
    async fn bucket_exists(&self) -> Result<bool> {
        let prefix = Path::from(format!("{}objects", self.prefix));
        match self.store.list_with_delimiter(Some(&prefix)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) if is_no_such_bucket(&e) => Ok(false),
            Err(e) => Err(backend("list bucket", e)),
        }
    }

    /// 启动自检：写入、读回、校验一个探测对象（服务端设计文档 §4.6 `blobstore-check`）。
    pub async fn self_test(&self) -> Result<()> {
        let probe = Bytes::from_static(b"scion blobstore self-test v1");
        let id = ObjectId::of_blob(&probe);
        self.put(id, probe.clone()).await?;
        match self.get(&id).await? {
            Some(got) if got == probe => Ok(()),
            Some(_) => Err(Error::Backend("blobstore self-test read back different bytes".into())),
            None => Err(Error::Backend(
                "blobstore self-test object not readable after write".into(),
            )),
        }
    }

    /// 读出已有对象并校验 BLAKE3。对象在读取期间消失时视为不完好（随后会被重新写入）。
    async fn intact(&self, key: &Path, id: &ObjectId) -> Result<bool> {
        let res = match self.store.get(key).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(backend("get object", e)),
        };
        let data = res.bytes().await.map_err(|e| backend("read object", e))?;
        Ok(id.verify(&data))
    }

    fn key(&self, id: &ObjectId) -> Path {
        Path::from(format!("{}{}", self.prefix, object_key(id)))
    }
}

/// 对象在存储中的相对 key：`objects/{kind}/{hh}/{hex}`。
/// 以哈希开头的两位做目录，让对象均匀分散到 S3 分区和本地目录。
fn object_key(id: &ObjectId) -> String {
    let hex = id.to_string();
    // 文本形式为 `<kind><algo><hex>`，如 b1af13…
    let (kind_algo, digest) = hex.split_at(2);
    format!("objects/{kind_algo}/{}/{digest}", &digest[..2])
}

/// 错误链中是否有 S3 错误码 `NoSuchBucket`。
fn is_no_such_bucket(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.to_string().contains("NoSuchBucket") {
            return true;
        }
        cur = err.source();
    }
    false
}

/// 规范化前缀：去掉首尾的 `/`，非空时补一个结尾的 `/`。
fn normalize_prefix(p: &str) -> String {
    let p = p.trim_matches('/');
    if p.is_empty() { String::new() } else { format!("{p}/") }
}

fn backend(context: &'static str, e: object_store::Error) -> Error {
    tracing::warn!(error = %e, context, "blobstore error");
    Error::Backend(format!("{context}: {}", short(&e)))
}

/// 错误摘要：只保留类别，不含 URL、密钥等细节。
fn short(e: &object_store::Error) -> &'static str {
    match e {
        object_store::Error::NotFound { .. } => "not found",
        object_store::Error::PermissionDenied { .. } => "permission denied",
        object_store::Error::Unauthenticated { .. } => "unauthenticated",
        object_store::Error::Precondition { .. } => "precondition failed",
        object_store::Error::NotSupported { .. } => "not supported",
        object_store::Error::InvalidPath { .. } => "invalid path",
        _ => "storage request failed",
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        if !id.verify(&data) {
            return Err(Error::Backend(format!(
                "refusing to store bytes that do not match {id}"
            )));
        }
        let key = self.key(&id);
        // 已存在且完好就不再上传。已存在但大小不符或内容校验失败（截断、位翻转）时覆盖写入，
        // 否则损坏的对象永远无法被重新上传修复，跨仓库去重还会让新仓库引用到坏数据。
        match self.store.head(&key).await {
            Ok(meta) if meta.size == data.len() as u64 && self.intact(&key, &id).await? => return Ok(()),
            Ok(_) => tracing::warn!(%id, "existing blob is damaged; overwriting with verified bytes"),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(backend("head object", e)),
        }
        self.store
            .put(&key, PutPayload::from_bytes(data))
            .await
            .map_err(|e| backend("put object", e))?;
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        let res = match self.store.get(&self.key(id)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(backend("get object", e)),
        };
        let data = res.bytes().await.map_err(|e| backend("read object", e))?;
        if !id.verify(&data) {
            tracing::error!(%id, "blob failed integrity check");
            return Err(Error::Backend(format!("object {id} failed integrity check")));
        }
        Ok(Some(data))
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        match self.store.head(&self.key(id)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(backend("head object", e)),
        }
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        match self.store.delete(&self.key(id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(backend("delete object", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::testsuite;

    #[test]
    fn object_key_layout() {
        let id = ObjectId::of_blob(b"");
        assert_eq!(
            object_key(&id),
            "objects/b1/af/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        let t = ObjectId::hash(scion_object::ObjectKind::Tree, b"");
        assert!(object_key(&t).starts_with("objects/t1/af/"));
    }

    #[test]
    fn normalize_prefix_cases() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("/"), "");
        assert_eq!(normalize_prefix("scion"), "scion/");
        assert_eq!(normalize_prefix("/a/b/"), "a/b/");
    }

    #[test]
    fn s3_config_debug_redacts_secret() {
        let cfg = S3Config {
            secret_access_key: "hunter2".into(),
            ..Default::default()
        };
        assert!(!format!("{cfg:?}").contains("hunter2"));
    }

    #[test]
    fn no_such_bucket_found_anywhere_in_chain() {
        #[derive(Debug)]
        struct Wrap(&'static str, Option<Box<Wrap>>);
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|w| w as _)
            }
        }
        let deep = Wrap("list failed", Some(Box::new(Wrap("<Code>NoSuchBucket</Code>", None))));
        assert!(is_no_such_bucket(&deep));
        assert!(!is_no_such_bucket(&Wrap(
            "list failed",
            Some(Box::new(Wrap("AccessDenied", None)))
        )));
    }

    #[test]
    fn short_error_categories() {
        let nf = object_store::Error::NotFound {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&nf), "not found");
        let pd = object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&pd), "permission denied");
        let ua = object_store::Error::Unauthenticated {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&ua), "unauthenticated");
        let pc = object_store::Error::Precondition {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&pc), "precondition failed");
        let ns = object_store::Error::NotSupported { source: "x".into() };
        assert_eq!(short(&ns), "not supported");
        let g = object_store::Error::Generic {
            store: "s",
            source: "secret-url".into(),
        };
        assert_eq!(short(&g), "storage request failed");
        assert_eq!(backend("ctx", g), Error::Backend("ctx: storage request failed".into()));
    }

    #[tokio::test]
    async fn local_conformance_and_self_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path().join("blobs")).expect("open");
        testsuite::blob_store(&s).await;
        s.self_test().await.expect("self test");
    }

    #[tokio::test]
    async fn local_corruption_detected_on_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"good");
        s.put(id, Bytes::from_static(b"good")).await.expect("put");
        std::fs::write(dir.path().join(object_key(&id)), b"evil").expect("tamper");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(m)) if m.contains("integrity")));
    }

    /// 回归测试（PR #1 review）：已损坏的对象必须能被重新上传修复。
    #[tokio::test]
    async fn put_repairs_truncated_and_bit_flipped_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let data = Bytes::from_static(b"precious model weights");
        let id = ObjectId::of_blob(&data);
        let file = dir.path().join(object_key(&id));
        s.put(id, data.clone()).await.expect("put");

        // 截断：大小不一致
        std::fs::write(&file, b"precious").expect("truncate");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        s.put(id, data.clone()).await.expect("re-upload repairs");
        assert_eq!(s.get(&id).await, Ok(Some(data.clone())));

        // 位翻转：大小相同但内容不同
        let mut flipped = data.to_vec();
        flipped[0] ^= 0x01;
        std::fs::write(&file, &flipped).expect("flip");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        s.put(id, data.clone()).await.expect("re-upload repairs");
        assert_eq!(s.get(&id).await, Ok(Some(data)));
    }

    #[tokio::test]
    async fn put_rejects_bytes_not_matching_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"good");
        assert!(matches!(
            s.put(id, Bytes::from_static(b"evil")).await,
            Err(Error::Backend(_))
        ));
        assert_eq!(s.exists(&id).await, Ok(false));
    }

    #[tokio::test]
    async fn intact_missing_object_is_not_intact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"x");
        assert_eq!(s.intact(&s.key(&id), &id).await, Ok(false));
    }

    #[tokio::test]
    async fn local_self_test_detects_tampered_probe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        s.self_test().await.expect("first run");
        let probe = ObjectId::of_blob(b"scion blobstore self-test v1");
        std::fs::remove_file(dir.path().join(object_key(&probe))).expect("remove");
        // 删除后再自检：重新写入并读回，仍应成功
        s.self_test().await.expect("rewrites probe");
    }

    #[test]
    fn local_unusable_dir_rejected() {
        let file = tempfile::NamedTempFile::new().expect("file");
        // 路径是一个普通文件，不能作为目录
        assert!(matches!(
            ObjectBlobStore::local(file.path().join("x")),
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn s3_unreachable_endpoint_reports_backend_error() {
        let cfg = S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            prefix: "p".into(),
            ..Default::default()
        };
        let no_retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let s = ObjectBlobStore::s3_with_retry(&cfg, no_retry).expect("build is offline");
        let id = ObjectId::of_blob(b"x");
        assert!(matches!(s.exists(&id).await, Err(Error::Backend(_))));
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        assert!(matches!(
            s.put(id, Bytes::from_static(b"x")).await,
            Err(Error::Backend(_))
        ));
        assert!(matches!(s.self_test().await, Err(Error::Backend(_))));
    }

    #[test]
    fn s3_max_retries_override_accepted() {
        let cfg = S3Config {
            bucket: "b".into(),
            max_retries: Some(0),
            ..Default::default()
        };
        assert!(ObjectBlobStore::s3(&cfg).is_ok());
    }

    #[test]
    fn s3_missing_bucket_rejected() {
        let cfg = S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            ..Default::default()
        };
        assert!(matches!(ObjectBlobStore::s3(&cfg), Err(Error::Backend(_))));
    }

    #[test]
    fn provider_defaults_applied_at_build() {
        // 自建服务必须有 endpoint
        for provider in [S3Provider::Minio, S3Provider::SeaweedFs] {
            let cfg = S3Config {
                bucket: "b".into(),
                provider,
                ..Default::default()
            };
            assert!(matches!(ObjectBlobStore::s3(&cfg), Err(Error::Backend(m)) if m.contains("needs an endpoint")));
        }
        // 没有 endpoint 的 Auto 即 AWS
        let aws = ObjectBlobStore::s3(&S3Config {
            bucket: "b".into(),
            ..Default::default()
        })
        .expect("aws");
        assert_eq!(aws.s3.expect("s3").provider, S3Provider::Aws);
        let minio = ObjectBlobStore::s3(&S3Config {
            endpoint: Some("http://minio:9000".into()),
            bucket: "b".into(),
            provider: S3Provider::Minio,
            ..Default::default()
        })
        .expect("minio");
        assert_eq!(minio.s3.expect("s3").provider, S3Provider::Minio);
    }

    #[tokio::test]
    async fn local_check_reports_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("local");
        assert_eq!(s.check().await, Ok(BlobBackend::Local));
        assert_eq!(BlobBackend::Local.to_string(), "local");
        let d = Detected {
            provider: S3Provider::Minio,
            server: Some("MinIO".into()),
        };
        assert_eq!(BlobBackend::S3(d.clone()).to_string(), "s3 (minio, Server: MinIO)");
        let bare = Detected { server: None, ..d };
        assert_eq!(BlobBackend::S3(bare).to_string(), "s3 (minio)");
    }

    #[tokio::test]
    async fn s3_check_reports_unreachable_endpoint() {
        let s = ObjectBlobStore::s3(&S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            bucket: "b".into(),
            ..Default::default()
        })
        .expect("build");
        // 直连时在识别阶段报 unreachable；经过代理时代理返回 502，随后列 bucket 失败
        assert!(matches!(s.check().await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn s3_check_identifies_provider() {
        let Some(cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        let s = ObjectBlobStore::s3(&cfg).expect("build");
        let BlobBackend::S3(d) = s.check().await.expect("check") else {
            panic!("expected s3")
        };
        assert_ne!(d.provider, S3Provider::Auto);
        if cfg.provider != S3Provider::Auto {
            assert_eq!(d.provider, cfg.provider);
        }
        // 自动识别得到同样的结果
        let auto = ObjectBlobStore::s3(&S3Config {
            provider: S3Provider::Auto,
            ..cfg.clone()
        })
        .expect("build");
        assert_eq!(auto.check().await, Ok(BlobBackend::S3(d.clone())));
        // 配置成另一种已知服务：报错
        if matches!(d.provider, S3Provider::Minio | S3Provider::SeaweedFs) {
            let other = if d.provider == S3Provider::Minio {
                S3Provider::SeaweedFs
            } else {
                S3Provider::Minio
            };
            let wrong = ObjectBlobStore::s3(&S3Config {
                provider: other,
                ..cfg.clone()
            })
            .expect("build");
            assert!(matches!(wrong.check().await, Err(Error::Backend(m)) if m.contains("configured as")));
        }
        // bucket 不存在：给出该服务的创建方法
        let missing = ObjectBlobStore::s3(&S3Config {
            bucket: format!("scion-missing-{}", std::process::id()),
            ..cfg
        })
        .expect("build");
        let err = missing.check().await.expect_err("missing bucket");
        let hint = d.provider.create_bucket_hint("x");
        let tool = hint
            .split('`')
            .nth(1)
            .unwrap_or("")
            .split_whitespace()
            .next()
            .unwrap_or("");
        assert!(
            matches!(&err, Error::Backend(m) if m.contains("does not exist") && m.contains(tool)),
            "{err:?}"
        );
    }

    /// 真实 S3 兼容服务：需要 `SCION_TEST_S3_ENDPOINT` 等环境变量（CI 中由 SeaweedFS 和 MinIO 提供），未设置时跳过。
    fn s3_from_env() -> Option<S3Config> {
        let endpoint = std::env::var("SCION_TEST_S3_ENDPOINT").ok()?;
        Some(S3Config {
            endpoint: Some(endpoint),
            bucket: std::env::var("SCION_TEST_S3_BUCKET").unwrap_or_else(|_| "scion-test".into()),
            region: "us-east-1".into(),
            access_key_id: std::env::var("SCION_TEST_S3_ACCESS_KEY").unwrap_or_default(),
            secret_access_key: std::env::var("SCION_TEST_S3_SECRET_KEY").unwrap_or_default(),
            // 期望的服务种类（CI 中分别为 seaweedfs、minio）；未设置时自动识别
            provider: std::env::var("SCION_TEST_S3_PROVIDER")
                .ok()
                .map(|p| p.parse().expect("SCION_TEST_S3_PROVIDER"))
                .unwrap_or_default(),
            virtual_hosted_style: None,
            max_retries: None,
            // 每次运行用独立前缀，互不干扰
            prefix: format!(
                "t{}-{}",
                std::process::id(),
                scion_object::ObjectId::of_blob(&rand_bytes()).digest()[0]
            ),
        })
    }

    fn rand_bytes() -> [u8; 16] {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        t.to_le_bytes()
    }

    #[tokio::test]
    async fn s3_conformance_and_self_test() {
        let Some(cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        let s = ObjectBlobStore::s3(&cfg).expect("build");
        testsuite::blob_store(&s).await;
        s.self_test().await.expect("self test");
    }

    #[tokio::test]
    async fn s3_wrong_credentials_rejected() {
        let Some(mut cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        cfg.secret_access_key = "wrong".into();
        let no_retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let s = ObjectBlobStore::s3_with_retry(&cfg, no_retry).expect("build");
        assert!(matches!(s.self_test().await, Err(Error::Backend(_))));
        assert!(matches!(s.check().await, Err(Error::Backend(_))));
    }
}
