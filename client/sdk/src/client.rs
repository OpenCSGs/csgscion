//! 连接、能力协商与对象读取。

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use scion_object::ObjectId;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::server_info_service_client::ServerInfoServiceClient;
use scion_proto::v1::{GetObjectsRequest, GetServerInfoRequest, HasObjectsRequest, ObjectWant};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::error::{Error, Result};
use crate::info::ServerInfo;

/// 认证方式。
#[derive(Clone, Default, PartialEq, Eq)]
pub enum Auth {
    #[default]
    None,
    /// `authorization: Bearer <token>`。
    Bearer(String),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::None => f.write_str("Auth::None"),
            Auth::Bearer(_) => f.write_str("Auth::Bearer(<redacted>)"),
        }
    }
}

impl Auth {
    /// 读取环境变量 `SCION_TOKEN`；未设置或为空时不带认证。
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        match lookup("SCION_TOKEN") {
            Some(t) if !t.trim().is_empty() => Auth::Bearer(t.trim().to_string()),
            _ => Auth::None,
        }
    }
}

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ClientBuilder {
    endpoint: Option<String>,
    repo: Option<String>,
    auth: Auth,
    client_name: String,
    connect_timeout: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            repo: None,
            auth: Auth::None,
            client_name: "scion-client".into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }
}

impl ClientBuilder {
    /// 服务端地址，`http://` 或 `https://` 开头。
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// 默认仓库，读写对象时使用。
    pub fn repo(mut self, repo: impl Into<String>) -> Self {
        self.repo = Some(repo.into());
        self
    }

    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }

    /// 上报给服务端的客户端名称（仅用于日志和统计）。
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = name.into();
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    fn endpoint_config(&self) -> Result<(String, Endpoint)> {
        let url = self
            .endpoint
            .clone()
            .ok_or_else(|| Error::InvalidEndpoint(String::new()))?;
        let is_tls = url.starts_with("https://");
        if !is_tls && !url.starts_with("http://") {
            return Err(Error::InvalidEndpoint(url));
        }
        let mut ep = Endpoint::from_shared(url.clone())
            .map_err(|_| Error::InvalidEndpoint(url.clone()))?
            .connect_timeout(self.connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true);
        if is_tls {
            ep = ep
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(|e| Error::Connect {
                    endpoint: url.clone(),
                    message: e.to_string(),
                })?;
        }
        Ok((url, ep))
    }

    /// 建立连接并完成能力协商（`GetServerInfo`）。协议主版本不兼容时返回错误。
    pub async fn build(self) -> Result<Client> {
        let (url, ep) = self.endpoint_config()?;
        let channel = ep.connect().await.map_err(|e| Error::Connect {
            endpoint: url.clone(),
            message: source_chain(&e),
        })?;
        let req = GetServerInfoRequest {
            client_name: self.client_name,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: scion_proto::PROTOCOL_VERSION.to_string(),
        };
        let resp = ServerInfoServiceClient::new(channel.clone())
            .get_server_info(make_request(&self.auth, req)?)
            .await?
            .into_inner();
        let info = resp
            .info
            .ok_or_else(|| Error::Protocol("GetServerInfo returned no info".into()))?;
        let info = ServerInfo::from_proto(info)?;
        Ok(Client {
            channel,
            auth: self.auth,
            repo: self.repo,
            info,
        })
    }
}

/// 构造请求并附加认证头。
fn make_request<T>(auth: &Auth, msg: T) -> Result<tonic::Request<T>> {
    let mut req = tonic::Request::new(msg);
    if let Auth::Bearer(token) = auth {
        let v = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| Error::Protocol("token contains invalid header characters".into()))?;
        req.metadata_mut().insert("authorization", v);
    }
    Ok(req)
}

/// 把错误及其 source 链拼成一行，连接失败时便于定位（如「connection refused」）。
fn source_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut cur = e.source();
    while let Some(s) = cur {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        cur = s.source();
    }
    msg
}

/// Scion 客户端。克隆开销很小，可在多个任务间共享。
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) channel: Channel,
    auth: Auth,
    repo: Option<String>,
    info: ServerInfo,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// 连接时协商得到的服务端信息。
    pub fn server_info(&self) -> &ServerInfo {
        &self.info
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.server_info().has_feature(feature)
    }

    pub(crate) fn repo(&self) -> Result<String> {
        self.repo.clone().ok_or(Error::MissingRepo)
    }

    pub(crate) fn request<T>(&self, msg: T) -> Result<tonic::Request<T>> {
        make_request(&self.auth, msg)
    }

    /// 返回 `ids` 中服务端不存在的对象。超过服务端批量上限时自动分批。
    pub async fn missing_objects(&self, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let repo = self.repo()?;
        let batch = self.server_info().limits.max_batch_objects as usize;
        let mut svc = ObjectServiceClient::new(self.channel.clone());
        let mut missing = Vec::new();
        for chunk in ids.chunks(batch) {
            let req = HasObjectsRequest {
                repo: repo.clone(),
                ids: chunk.iter().map(|id| Bytes::copy_from_slice(&id.to_bytes())).collect(),
            };
            let resp = svc.has_objects(self.request(req)?).await?.into_inner();
            for raw in resp.missing_ids {
                missing.push(
                    ObjectId::from_bytes(&raw)
                        .map_err(|e| Error::Protocol(format!("bad id in HasObjects response: {e}")))?,
                );
            }
        }
        Ok(missing)
    }

    /// 读取一个对象的完整字节，并校验 BLAKE3（不信任任何缓存层）。
    pub async fn get_object(&self, id: &ObjectId) -> Result<Bytes> {
        let req = GetObjectsRequest {
            repo: self.repo()?,
            wants: vec![ObjectWant {
                id: Bytes::copy_from_slice(&id.to_bytes()),
                offset: 0,
            }],
            accept_encodings: vec![],
        };
        let mut stream = ObjectServiceClient::new(self.channel.clone())
            .get_objects(self.request(req)?)
            .await?
            .into_inner();
        let mut buf = Assembler::new(*id);
        while let Some(part) = stream.message().await? {
            if buf.push(part)? {
                return buf.finish();
            }
        }
        Err(Error::Protocol(format!("stream for {id} ended before last chunk")))
    }
}

/// 按协议拼接单个对象的分块：id 一致、offset 连续、total_size 不变。
struct Assembler {
    id: ObjectId,
    data: BytesMut,
    total: Option<u64>,
}

impl Assembler {
    fn new(id: ObjectId) -> Self {
        Self {
            id,
            data: BytesMut::new(),
            total: None,
        }
    }

    /// 追加一个分块，返回是否已是最后一块。
    fn push(&mut self, part: scion_proto::v1::GetObjectsResponse) -> Result<bool> {
        if part.id.as_ref() != self.id.to_bytes() {
            return Err(Error::Protocol(format!(
                "unexpected object in stream while reading {}",
                self.id
            )));
        }
        if part.offset != self.data.len() as u64 {
            return Err(Error::Protocol(format!(
                "non-contiguous chunk for {}: expected offset {}, got {}",
                self.id,
                self.data.len(),
                part.offset
            )));
        }
        match self.total {
            None => self.total = Some(part.total_size),
            Some(t) if t != part.total_size => {
                return Err(Error::Protocol(format!(
                    "total_size changed mid-stream for {}",
                    self.id
                )));
            }
            Some(_) => {}
        }
        self.data.extend_from_slice(&part.data);
        if self.data.len() as u64 > part.total_size {
            return Err(Error::Protocol(format!("{} exceeds declared total_size", self.id)));
        }
        Ok(part.last)
    }

    fn finish(self) -> Result<Bytes> {
        if Some(self.data.len() as u64) != self.total {
            return Err(Error::Protocol(format!("{} shorter than declared total_size", self.id)));
        }
        if !self.id.verify(&self.data) {
            return Err(Error::Corrupt(self.id));
        }
        Ok(self.data.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::v1::GetObjectsResponse;

    fn part(id: &ObjectId, offset: u64, data: &'static [u8], last: bool, total: u64) -> GetObjectsResponse {
        GetObjectsResponse {
            id: Bytes::copy_from_slice(&id.to_bytes()),
            offset,
            data: Bytes::from_static(data),
            encoding: scion_proto::v1::Encoding::Identity.into(),
            last,
            total_size: total,
        }
    }

    #[test]
    fn auth_from_lookup() {
        assert_eq!(Auth::from_lookup(|_| None), Auth::None);
        assert_eq!(Auth::from_lookup(|_| Some("  ".into())), Auth::None);
        assert_eq!(
            Auth::from_lookup(|_| Some(" t0k \n".into())),
            Auth::Bearer("t0k".into())
        );
    }

    #[test]
    fn auth_debug_redacts_token() {
        assert_eq!(
            format!("{:?}", Auth::Bearer("secret".into())),
            "Auth::Bearer(<redacted>)"
        );
        assert_eq!(format!("{:?}", Auth::None), "Auth::None");
    }

    #[test]
    fn builder_setters() {
        let b = Client::builder()
            .endpoint("http://x:1")
            .repo("r")
            .auth(Auth::Bearer("t".into()))
            .client_name("scn")
            .connect_timeout(Duration::from_millis(5));
        assert_eq!(b.endpoint.as_deref(), Some("http://x:1"));
        assert_eq!(b.repo.as_deref(), Some("r"));
        assert_eq!(b.client_name, "scn");
        assert_eq!(b.connect_timeout, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn endpoint_config_validates_scheme() {
        assert!(matches!(
            Client::builder().endpoint_config(),
            Err(Error::InvalidEndpoint(_))
        ));
        assert!(matches!(
            Client::builder().endpoint("ftp://x").endpoint_config(),
            Err(Error::InvalidEndpoint(_))
        ));
        assert!(matches!(
            Client::builder().endpoint("http://bad host").endpoint_config(),
            Err(Error::InvalidEndpoint(_))
        ));
        assert!(
            Client::builder()
                .endpoint("http://127.0.0.1:1")
                .endpoint_config()
                .is_ok()
        );
        assert!(
            Client::builder()
                .endpoint("https://scion.example.com")
                .endpoint_config()
                .is_ok()
        );
    }

    #[test]
    fn make_request_sets_bearer_header() {
        let req = make_request(&Auth::Bearer("abc".into()), ()).expect("ok");
        assert_eq!(
            req.metadata().get("authorization").map(|v| v.to_str().ok()),
            Some(Some("Bearer abc"))
        );
        let req = make_request(&Auth::None, ()).expect("ok");
        assert!(req.metadata().get("authorization").is_none());
        assert!(matches!(
            make_request(&Auth::Bearer("bad\ntoken".into()), ()),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn source_chain_joins_sources() {
        #[derive(Debug)]
        struct Outer(std::io::Error);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("outer")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let e = Outer(std::io::Error::other("refused"));
        assert_eq!(source_chain(&e), "outer: refused");
    }

    #[test]
    fn assembler_happy_path() {
        let id = ObjectId::of_blob(b"hello");
        let mut a = Assembler::new(id);
        assert!(!a.push(part(&id, 0, b"hel", false, 5)).expect("ok"));
        assert!(a.push(part(&id, 3, b"lo", true, 5)).expect("ok"));
        assert_eq!(a.finish().expect("verified").as_ref(), b"hello");
    }

    #[test]
    fn assembler_empty_object() {
        let id = ObjectId::of_blob(b"");
        let mut a = Assembler::new(id);
        assert!(a.push(part(&id, 0, b"", true, 0)).expect("ok"));
        assert!(a.finish().expect("ok").is_empty());
    }

    #[test]
    fn assembler_rejects_protocol_violations() {
        let id = ObjectId::of_blob(b"hello");
        let other = ObjectId::of_blob(b"x");
        assert!(matches!(
            Assembler::new(id).push(part(&other, 0, b"x", true, 1)),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            Assembler::new(id).push(part(&id, 2, b"llo", true, 5)),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            Assembler::new(id).push(part(&id, 0, b"hello!", true, 5)),
            Err(Error::Protocol(_))
        ));
        let mut a = Assembler::new(id);
        a.push(part(&id, 0, b"he", false, 5)).expect("ok");
        assert!(matches!(a.push(part(&id, 2, b"llo", true, 6)), Err(Error::Protocol(_))));
    }

    #[test]
    fn assembler_short_object_rejected() {
        let id = ObjectId::of_blob(b"hello");
        let mut a = Assembler::new(id);
        a.push(part(&id, 0, b"hell", true, 5)).expect("ok");
        assert!(matches!(a.finish(), Err(Error::Protocol(_))));
    }

    #[test]
    fn assembler_corrupt_data_detected() {
        let id = ObjectId::of_blob(b"hello");
        let mut a = Assembler::new(id);
        a.push(part(&id, 0, b"jello", true, 5)).expect("ok");
        assert!(matches!(a.finish(), Err(Error::Corrupt(x)) if x == id));
    }
}
