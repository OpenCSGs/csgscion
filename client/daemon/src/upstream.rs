//! 到服务端的连接。一个 daemon 可以同时服务多个服务端：每个请求带着 [`UPSTREAM_HEADER`]
//! 说明真正的服务端地址，daemon 按地址缓存连接。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scion_client::{Auth, Client, UPSTREAM_HEADER};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

/// 一个服务端。
pub struct Upstream {
    pub url: String,
    /// 透明转发用的原始连接。
    pub raw: Channel,
    /// 读取对象、订阅 ref 用的 SDK 客户端（会使用服务端推荐的区域缓存），第一次使用时建立。
    client: OnceCell<Client>,
}

impl Upstream {
    fn new(url: &str) -> Result<Self, Status> {
        let raw = Endpoint::from_shared(url.to_owned())
            .map_err(|e| Status::invalid_argument(format!("invalid upstream {url:?}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect_lazy();
        Ok(Self {
            url: url.to_owned(),
            raw,
            client: OnceCell::new(),
        })
    }

    /// SDK 客户端；建立失败时下次调用重试。
    pub async fn client(&self) -> Result<Client, Status> {
        self.client
            .get_or_try_init(|| async {
                Client::builder()
                    .endpoint(&self.url)
                    .client_name("scn-daemon")
                    .connect_timeout(Duration::from_secs(5))
                    .build()
                    .await
                    .map_err(|e| e.to_status())
            })
            .await
            .cloned()
    }
}

#[derive(Default)]
pub struct Upstreams {
    map: Mutex<HashMap<String, Arc<Upstream>>>,
}

impl Upstreams {
    /// 按地址取连接（第一次使用时创建）。只接受 http:// 和 https:// 地址。
    pub fn get(&self, url: &str) -> Result<Arc<Upstream>, Status> {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(Status::invalid_argument(format!(
                "{UPSTREAM_HEADER} must be an http:// or https:// URL, got {url:?}"
            )));
        }
        let mut map = self
            .map
            .lock()
            .map_err(|_| Status::internal("upstream table poisoned"))?;
        if let Some(u) = map.get(url) {
            return Ok(u.clone());
        }
        let u = Arc::new(Upstream::new(url)?);
        map.insert(url.to_owned(), u.clone());
        Ok(u)
    }

    pub fn len(&self) -> usize {
        self.map.lock().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 请求要转发到的服务端地址。
pub fn upstream_of<T>(req: &Request<T>) -> Result<String, Status> {
    req.metadata()
        .get(UPSTREAM_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| Status::failed_precondition(format!("missing {UPSTREAM_HEADER}; connect with the Scion SDK")))
}

/// 调用者的身份（daemon 不持有凭证，按每个请求的调用者回源）。
pub fn auth_of<T>(req: &Request<T>) -> Auth {
    match req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(t) if !t.trim().is_empty() => Auth::Bearer(t.trim().to_owned()),
        _ => Auth::None,
    }
}

/// 把调用者的 `authorization` 带到转发的请求上。
pub fn forward<T, U>(from: &Request<T>, msg: U) -> Request<U> {
    let mut req = Request::new(msg);
    if let Some(a) = from.metadata().get("authorization") {
        req.metadata_mut().insert("authorization", a.clone());
    }
    req
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[tokio::test]
    async fn upstreams_are_cached_and_validated() {
        let u = Upstreams::default();
        assert!(u.is_empty());
        let a = u.get("http://127.0.0.1:1").expect("a");
        let again = u.get("http://127.0.0.1:1").expect("again");
        assert!(Arc::ptr_eq(&a, &again));
        assert_eq!(u.len(), 1);
        assert_eq!(
            u.get("ftp://x").err().map(|s| s.code()),
            Some(tonic::Code::InvalidArgument)
        );
        assert_eq!(
            u.get("http://bad host").err().map(|s| s.code()),
            Some(tonic::Code::InvalidArgument)
        );
        // 不可达：建立 SDK 客户端失败，返回 UNAVAILABLE（下次再试）
        assert_eq!(a.client().await.err().map(|s| s.code()), Some(tonic::Code::Unavailable));
    }

    #[test]
    fn headers_are_read_and_forwarded() {
        let mut req = Request::new(());
        assert!(upstream_of(&req).is_err());
        assert_eq!(auth_of(&req), Auth::None);
        req.metadata_mut()
            .insert(UPSTREAM_HEADER, MetadataValue::from_static("http://s:7080"));
        req.metadata_mut()
            .insert("authorization", MetadataValue::from_static("Bearer tok"));
        assert_eq!(upstream_of(&req).expect("upstream"), "http://s:7080");
        assert_eq!(auth_of(&req), Auth::Bearer("tok".into()));
        let fwd = forward(&req, 1u8);
        assert!(fwd.metadata().get("authorization").is_some());
        assert!(fwd.metadata().get(UPSTREAM_HEADER).is_none(), "not sent to the server");
        let mut basic = Request::new(());
        basic
            .metadata_mut()
            .insert("authorization", MetadataValue::from_static("Basic x"));
        assert_eq!(auth_of(&basic), Auth::None);
    }
}
