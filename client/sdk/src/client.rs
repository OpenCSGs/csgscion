//! 连接、能力协商与对象读取。

use std::sync::Arc;
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
    /// 直接作为 `authorization: Bearer <token>` 发送。
    Bearer(String),
    /// 长期凭据（PAT、`scn login` 得到的刷新令牌、受信任签发方的 JWT）：连接时用它向服务端换取短期
    /// 访问令牌（`AuthService.ExchangeToken`），并在过期前 2 分钟自动重新换取。服务端不签发访问令牌时
    /// 直接把凭据作为 Bearer 发送。
    Exchange(String),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::None => f.write_str("Auth::None"),
            Auth::Bearer(_) => f.write_str("Auth::Bearer(<redacted>)"),
            Auth::Exchange(_) => f.write_str("Auth::Exchange(<redacted>)"),
        }
    }
}

/// 换取得到的访问令牌，后台任务在过期前刷新。
struct Session {
    credential: String,
    token: std::sync::RwLock<String>,
    expires_at_ms: std::sync::atomic::AtomicI64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

impl Session {
    fn current(&self) -> String {
        self.token.read().map(|t| t.clone()).unwrap_or_default()
    }

    fn set(&self, token: String, expires_at_ms: i64) {
        if let Ok(mut t) = self.token.write() {
            *t = token;
        }
        self.expires_at_ms
            .store(expires_at_ms, std::sync::atomic::Ordering::Relaxed);
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 提前多久刷新访问令牌。
const REFRESH_AHEAD: Duration = Duration::from_secs(120);

/// 用凭据换取访问令牌。服务端不支持（旧版本或没有签名密钥）时返回 `None`。
async fn exchange(
    channel: &Channel,
    upstream: Option<&MetadataValue<tonic::metadata::Ascii>>,
    credential: &str,
) -> Result<Option<(String, i64)>> {
    let req = scion_proto::v1::ExchangeTokenRequest {
        subject_token: credential.to_owned(),
    };
    match scion_proto::v1::auth_service_client::AuthServiceClient::new(channel.clone())
        .exchange_token(make_request(None, upstream, req)?)
        .await
    {
        Ok(r) => {
            let r = r.into_inner();
            Ok(Some((r.access_token, r.expires_at_ms)))
        }
        Err(s) if matches!(s.code(), tonic::Code::Unimplemented | tonic::Code::Unavailable) => Ok(None),
        Err(s) => Err(s.into()),
    }
}

/// 在访问令牌过期前重新换取；所有 [`Client`] 都释放后结束。
fn spawn_refresh(
    session: std::sync::Weak<Session>,
    channel: Channel,
    upstream: Option<MetadataValue<tonic::metadata::Ascii>>,
) {
    tokio::spawn(async move {
        let mut retry = Duration::from_secs(5);
        loop {
            let wait = match session.upgrade() {
                Some(s) => {
                    let left = s.expires_at_ms.load(std::sync::atomic::Ordering::Relaxed) - now_ms();
                    Duration::from_millis(u64::try_from(left).unwrap_or(0))
                        .saturating_sub(REFRESH_AHEAD)
                        .max(Duration::from_secs(1))
                }
                None => return,
            };
            tokio::time::sleep(wait).await;
            let Some(s) = session.upgrade() else {
                return;
            };
            match exchange(&channel, upstream.as_ref(), &s.credential).await {
                Ok(Some((token, exp))) => {
                    s.set(token, exp);
                    retry = Duration::from_secs(5);
                }
                Ok(None) => return,
                Err(_) => {
                    drop(s);
                    tokio::time::sleep(retry).await;
                    retry = (retry * 2).min(Duration::from_secs(60));
                }
            }
        }
    });
}

impl Auth {
    /// 读取环境变量 `SCION_TOKEN`；未设置或为空时不带认证。
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        match lookup("SCION_TOKEN") {
            Some(t) if !t.trim().is_empty() => Auth::Exchange(t.trim().to_string()),
            _ => Auth::None,
        }
    }
}

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// redaction 的一次变化（[`Client::list_redacted_objects`]）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactedObject {
    pub id: ObjectId,
    pub redaction_id: String,
    /// false 表示已撤销。
    pub active: bool,
    pub seq: u64,
}

/// [`Client::list_redacted_objects`] 的一页。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactedPage {
    pub objects: Vec<RedactedObject>,
    /// 下一次请求的 `after_seq`。
    pub next_seq: u64,
    pub more: bool,
}

#[derive(Debug, Clone)]
pub struct ClientBuilder {
    endpoint: Option<String>,
    repo: Option<String>,
    auth: Auth,
    client_name: String,
    connect_timeout: Duration,
    read_endpoints: ReadEndpoints,
    daemon_socket: Option<std::path::PathBuf>,
}

/// 读取对象时是否使用服务端推荐的只读端点（区域缓存）。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReadEndpoints {
    /// 使用 `ServerInfo.read_endpoints` 中与 `region` 相同的端点（`None` 或没有匹配时用第一个）。
    #[default]
    Auto,
    /// 优先使用这个区域的端点。
    Region(String),
    /// 总是直接从服务端读取。
    Off,
}

/// 经 daemon 转发时，告诉 daemon 真正的服务端地址的请求头。
pub const UPSTREAM_HEADER: &str = "x-scion-upstream";

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            repo: None,
            auth: Auth::None,
            client_name: "scion-client".into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_endpoints: ReadEndpoints::Auto,
            daemon_socket: None,
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

    /// 读取对象时是否、以及优先用哪个区域的只读端点（默认 [`ReadEndpoints::Auto`]）。
    pub fn read_endpoints(mut self, mode: ReadEndpoints) -> Self {
        self.read_endpoints = mode;
        self
    }

    /// 经本机 daemon（`scn daemon`）连接：所有请求发到这个 Unix socket，由 daemon 就地应答（对象读取）
    /// 或转发给服务端。daemon 不可达时 [`ClientBuilder::build`] 返回连接错误，调用方可改为直连。
    pub fn daemon_socket(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.daemon_socket = Some(path.into());
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
        let (channel, upstream) = match &self.daemon_socket {
            Some(path) => {
                let header = MetadataValue::try_from(url.as_str()).map_err(|_| Error::InvalidEndpoint(url.clone()))?;
                (connect_daemon(path, self.connect_timeout).await?, Some(header))
            }
            None => (
                ep.connect().await.map_err(|e| Error::Connect {
                    endpoint: url.clone(),
                    message: source_chain(&e),
                })?,
                None,
            ),
        };
        let info = fetch_info(&channel, None, upstream.as_ref(), &self.client_name).await?;
        let session = match &self.auth {
            Auth::Exchange(credential) => match exchange(&channel, upstream.as_ref(), credential).await? {
                Some((token, exp)) => {
                    let s = Arc::new(Session {
                        credential: credential.clone(),
                        token: std::sync::RwLock::new(token),
                        expires_at_ms: std::sync::atomic::AtomicI64::new(exp),
                    });
                    spawn_refresh(Arc::downgrade(&s), channel.clone(), upstream.clone());
                    Some(s)
                }
                None => None,
            },
            _ => None,
        };
        // 经 daemon 时由 daemon 决定从哪里读对象
        let (objects, objects_endpoint) = match (&upstream, pick_read_endpoint(&info, &self.read_endpoints)) {
            (None, Some(read)) => match self.read_channel(&read).await {
                Some(ch) => (ch, read),
                None => (channel.clone(), url.clone()),
            },
            _ => (channel.clone(), url.clone()),
        };
        Ok(Client {
            channel,
            objects,
            objects_endpoint,
            upstream,
            auth: self.auth,
            session,
            repo: self.repo,
            info,
        })
    }

    /// 连接只读端点并确认它能应答；不可达时返回 `None`（改为直接从服务端读取）。
    async fn read_channel(&self, url: &str) -> Option<Channel> {
        let builder = ClientBuilder {
            endpoint: Some(url.to_owned()),
            ..self.clone()
        };
        let (_, ep) = builder.endpoint_config().ok()?;
        let ch = ep.connect().await.ok()?;
        fetch_info(&ch, None, None, &self.client_name).await.ok()?;
        Some(ch)
    }
}

async fn fetch_info(
    channel: &Channel,
    bearer: Option<&str>,
    upstream: Option<&MetadataValue<tonic::metadata::Ascii>>,
    client_name: &str,
) -> Result<ServerInfo> {
    let req = GetServerInfoRequest {
        client_name: client_name.to_owned(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: scion_proto::PROTOCOL_VERSION.to_string(),
    };
    let resp = ServerInfoServiceClient::new(channel.clone())
        .get_server_info(make_request(bearer, upstream, req)?)
        .await?
        .into_inner();
    let info = resp
        .info
        .ok_or_else(|| Error::Protocol("GetServerInfo returned no info".into()))?;
    ServerInfo::from_proto(info)
}

/// 选择只读端点：先按区域匹配，再取第一个。
fn pick_read_endpoint(info: &ServerInfo, mode: &ReadEndpoints) -> Option<String> {
    let region = match mode {
        ReadEndpoints::Off => return None,
        ReadEndpoints::Auto => None,
        ReadEndpoints::Region(r) => Some(r.as_str()),
    };
    let eps = &info.read_endpoints;
    region
        .and_then(|r| eps.iter().find(|e| e.region == r))
        .or_else(|| eps.first())
        .map(|e| e.url.clone())
}

#[cfg(unix)]
async fn connect_daemon(path: &std::path::Path, timeout: Duration) -> Result<Channel> {
    let p = path.to_path_buf();
    let err = |m: String| Error::Connect {
        endpoint: format!("unix:{}", path.display()),
        message: m,
    };
    // URI 只是占位：连接器总是连到 socket
    Endpoint::from_static("http://scion-daemon")
        .connect_timeout(timeout)
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let p = p.clone();
            async move {
                let s = tokio::net::UnixStream::connect(p).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        }))
        .await
        .map_err(|e| err(source_chain(&e)))
}

#[cfg(not(unix))]
async fn connect_daemon(path: &std::path::Path, _timeout: Duration) -> Result<Channel> {
    Err(Error::Connect {
        endpoint: format!("unix:{}", path.display()),
        message: "the daemon is only supported on Unix".into(),
    })
}

/// 构造请求并附加认证头（经 daemon 时还附加服务端地址）。
fn make_request<T>(
    bearer: Option<&str>,
    upstream: Option<&MetadataValue<tonic::metadata::Ascii>>,
    msg: T,
) -> Result<tonic::Request<T>> {
    let mut req = tonic::Request::new(msg);
    if let Some(u) = upstream {
        req.metadata_mut().insert(UPSTREAM_HEADER, u.clone());
    }
    if let Some(token) = bearer {
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
    /// 换取得到的访问令牌（[`Auth::Exchange`]）。
    session: Option<Arc<Session>>,
    /// 读取对象用的连接：只读端点（区域缓存）或与 `channel` 相同。
    objects: Channel,
    objects_endpoint: String,
    /// 经 daemon 时真正的服务端地址（每个请求都带上）。
    upstream: Option<MetadataValue<tonic::metadata::Ascii>>,
    auth: Auth,
    repo: Option<String>,
    info: ServerInfo,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// 测试用：不做能力协商，直接包装一个连接。
    #[cfg(test)]
    pub(crate) fn for_tests(channel: Channel) -> Self {
        let info = scion_proto::v1::ServerInfo {
            protocol_version: scion_proto::PROTOCOL_VERSION.into(),
            hash_algo: scion_proto::v1::HashAlgo::Blake3256.into(),
            limits: Some(scion_proto::v1::Limits {
                max_batch_objects: 1000,
                max_chunk_bytes: 1 << 20,
                ..Default::default()
            }),
            ..Default::default()
        };
        Self {
            session: None,
            objects: channel.clone(),
            objects_endpoint: "test".into(),
            upstream: None,
            channel,
            auth: Auth::None,
            repo: Some("r".into()),
            info: ServerInfo::from_proto(info).expect("test info"),
        }
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
        let bearer = match (&self.session, &self.auth) {
            (Some(s), _) => Some(s.current()),
            (None, Auth::Bearer(t) | Auth::Exchange(t)) => Some(t.clone()),
            (None, Auth::None) => None,
        };
        make_request(bearer.as_deref(), self.upstream.as_ref(), msg)
    }

    /// 同一连接、换一个默认仓库（克隆开销很小）。
    pub fn with_repo(&self, repo: impl Into<String>) -> Client {
        Client {
            repo: Some(repo.into()),
            ..self.clone()
        }
    }

    /// 同一连接、换一个身份（例如 daemon 按每个请求的调用者转发）。
    pub fn with_auth(&self, auth: Auth) -> Client {
        Client {
            auth,
            session: None,
            ..self.clone()
        }
    }

    /// 当前使用的访问令牌的过期时间（Unix 毫秒）；没有换取令牌时为 `None`。
    pub fn token_expires_at_ms(&self) -> Option<i64> {
        self.session
            .as_ref()
            .map(|s| s.expires_at_ms.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// 读取对象的地址：服务端推荐的只读端点，或服务端本身。
    pub fn object_endpoint(&self) -> &str {
        &self.objects_endpoint
    }

    /// 是否经本机 daemon 连接。
    pub fn via_daemon(&self) -> bool {
        self.upstream.is_some()
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

    /// 仓库中 `after_seq` 之后的 redaction 变化（一页，按 seq 升序）。总是发给服务端本身，不经只读端点。
    pub async fn list_redacted_objects(&self, after_seq: u64) -> Result<RedactedPage> {
        let req = scion_proto::v1::ListRedactedObjectsRequest {
            repo: self.repo()?,
            after_seq,
            page_size: 0,
        };
        let resp = ObjectServiceClient::new(self.channel.clone())
            .list_redacted_objects(self.request(req)?)
            .await?
            .into_inner();
        let objects = resp
            .objects
            .into_iter()
            .map(|o| {
                Ok(RedactedObject {
                    id: ObjectId::from_bytes(&o.id)
                        .map_err(|e| Error::Protocol(format!("bad id in ListRedactedObjects response: {e}")))?,
                    redaction_id: o.redaction_id,
                    active: o.active,
                    seq: o.seq,
                })
            })
            .collect::<Result<_>>()?;
        Ok(RedactedPage {
            objects,
            next_seq: resp.next_seq,
            more: resp.more,
        })
    }

    /// 读取一个对象的完整字节，并校验 BLAKE3（不信任任何缓存层）。
    pub async fn get_object(&self, id: &ObjectId) -> Result<Bytes> {
        let mut v = self.get_objects(std::slice::from_ref(id)).await?;
        v.pop().ok_or_else(|| Error::Protocol(format!("no data for {id}")))
    }

    /// 批量读取对象（顺序与 `ids` 相同），每个都校验 BLAKE3。超过服务端批量上限时自动分批。
    pub async fn get_objects(&self, ids: &[ObjectId]) -> Result<Vec<Bytes>> {
        let repo = self.repo()?;
        let batch = (self.server_info().limits.max_batch_objects as usize).max(1);
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(batch) {
            let req = GetObjectsRequest {
                repo: repo.clone(),
                wants: chunk
                    .iter()
                    .map(|id| ObjectWant {
                        id: Bytes::copy_from_slice(&id.to_bytes()),
                        offset: 0,
                    })
                    .collect(),
                accept_encodings: vec![],
            };
            let mut stream = ObjectServiceClient::new(self.objects.clone())
                .get_objects(self.request(req)?)
                .await?
                .into_inner();
            let mut pending = chunk.iter();
            let mut buf = pending.next().map(|id| Assembler::new(*id));
            while let Some(part) = stream.message().await? {
                let Some(b) = buf.as_mut() else {
                    return Err(Error::Protocol("GetObjects sent more objects than requested".into()));
                };
                if b.push(part)? {
                    if let Some(done) = buf.take() {
                        out.push(done.finish()?);
                    }
                    buf = pending.next().map(|id| Assembler::new(*id));
                }
            }
            if let Some(b) = buf {
                return Err(Error::Protocol(format!("stream for {} ended before last chunk", b.id)));
            }
        }
        Ok(out)
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
            Auth::Exchange("t0k".into())
        );
    }

    #[test]
    fn auth_debug_redacts_token() {
        assert_eq!(
            format!("{:?}", Auth::Bearer("secret".into())),
            "Auth::Bearer(<redacted>)"
        );
        assert_eq!(format!("{:?}", Auth::None), "Auth::None");
        assert_eq!(
            format!("{:?}", Auth::Exchange("s".into())),
            "Auth::Exchange(<redacted>)"
        );
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
        let req = make_request(Some("abc"), None, ()).expect("ok");
        assert_eq!(
            req.metadata().get("authorization").map(|v| v.to_str().ok()),
            Some(Some("Bearer abc"))
        );
        let up = MetadataValue::from_static("http://server:7080");
        let req = make_request(None, Some(&up), ()).expect("ok");
        assert!(req.metadata().get("authorization").is_none());
        assert_eq!(
            req.metadata().get(UPSTREAM_HEADER).map(|v| v.to_str().ok()),
            Some(Some("http://server:7080"))
        );
        assert!(matches!(
            make_request(Some("bad\ntoken"), None, ()),
            Err(Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn with_repo_and_auth_keep_the_connection() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let c = Client::for_tests(channel);
        let other = c.with_repo("other").with_auth(Auth::Bearer("t".into()));
        assert_eq!(other.repo().expect("repo"), "other");
        assert_eq!(c.repo().expect("repo"), "r");
        let req = other.request(()).expect("req");
        assert!(req.metadata().get("authorization").is_some());
        assert_eq!(other.object_endpoint(), c.object_endpoint());
    }

    #[test]
    fn read_endpoint_selection() {
        let info = ServerInfo::from_proto(scion_proto::v1::ServerInfo {
            protocol_version: scion_proto::PROTOCOL_VERSION.into(),
            hash_algo: scion_proto::v1::HashAlgo::Blake3256.into(),
            read_endpoints: vec![
                scion_proto::v1::Endpoint {
                    url: "http://a".into(),
                    region: "east".into(),
                },
                scion_proto::v1::Endpoint {
                    url: "http://b".into(),
                    region: "west".into(),
                },
            ],
            limits: Some(scion_proto::v1::Limits {
                max_batch_objects: 10,
                max_chunk_bytes: 10,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("info");
        assert_eq!(
            pick_read_endpoint(&info, &ReadEndpoints::Auto).as_deref(),
            Some("http://a")
        );
        assert_eq!(
            pick_read_endpoint(&info, &ReadEndpoints::Region("west".into())).as_deref(),
            Some("http://b")
        );
        assert_eq!(
            pick_read_endpoint(&info, &ReadEndpoints::Region("mars".into())).as_deref(),
            Some("http://a"),
            "falls back to the first endpoint"
        );
        assert_eq!(pick_read_endpoint(&info, &ReadEndpoints::Off), None);
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
