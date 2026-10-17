//! ServerInfoService：能力协商。

use std::sync::Arc;

use scion_proto::v1::server_info_service_server::ServerInfoService;
use scion_proto::v1::{self, GetServerInfoRequest, GetServerInfoResponse};
use scion_proto::{PROTOCOL_VERSION, features};
use tonic::{Request, Response, Status};

use crate::config::ServerConfig;

pub struct ServerInfoSvc {
    config: Arc<ServerConfig>,
    guard: Arc<scion_auth::Guard>,
}

impl ServerInfoSvc {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config,
            guard: scion_auth::Guard::disabled(),
        }
    }

    pub fn with_guard(mut self, guard: Arc<scion_auth::Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// 当前实现的能力。只有真正实现了的能力才能出现在这里。
    fn features(&self) -> Vec<String> {
        let mut out: Vec<String> = [
            features::OBJECTS_GET,
            features::OBJECTS_HAS,
            features::WRITE_UPLOAD,
            features::WRITE_CHANGESET,
            features::LAND_DIRECT,
            features::LAND_REBASE,
            features::LAND_STATUS,
            features::REFS,
            features::REFS_WATCH,
            features::QUERY_READ_FILE,
            features::QUERY_LIST_DIR,
            features::QUERY_LOG,
            features::QUERY_ANCESTRY,
            features::QUERY_DIFF,
            features::QUERY_BLAME,
            features::QUERY_RESOLVE,
            features::QUERY_STAT,
            features::QUERY_DERIVED,
            features::AUTH,
            features::ADMIN,
            features::REDACTION,
        ]
        .map(String::from)
        .to_vec();
        if self.guard.required() {
            out.push(features::AUTH_REQUIRED.into());
        }
        if self.guard.device_login() {
            out.push(features::AUTH_DEVICE.into());
        }
        out
    }

    fn info(&self) -> v1::ServerInfo {
        v1::ServerInfo {
            protocol_version: PROTOCOL_VERSION.to_string(),
            features: self.features(),
            limits: Some((&self.config.limits).into()),
            read_endpoints: self
                .config
                .read_endpoints
                .iter()
                .map(|e| v1::Endpoint {
                    url: e.url.clone(),
                    region: e.region.clone(),
                })
                .collect(),
            hash_algo: v1::HashAlgo::Blake3256.into(),
            server_version: self.config.server_version.clone(),
            role: v1::ServerRole::Server.into(),
        }
    }
}

#[tonic::async_trait]
impl ServerInfoService for ServerInfoSvc {
    async fn get_server_info(
        &self,
        request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        let req = request.get_ref();
        tracing::debug!(client = %req.client_name, client_version = %req.client_version,
            protocol = %req.protocol_version, "GetServerInfo");
        Ok(Response::new(GetServerInfoResponse {
            info: Some(self.info()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReadEndpoint;

    #[tokio::test]
    async fn get_server_info_reports_config() {
        let mut cfg = ServerConfig::default();
        cfg.read_endpoints.push(ReadEndpoint {
            url: "https://cache-a".into(),
            region: "a".into(),
        });
        let svc = ServerInfoSvc::new(Arc::new(cfg));
        let resp = svc
            .get_server_info(Request::new(GetServerInfoRequest::default()))
            .await
            .expect("ok")
            .into_inner();
        let info = resp.info.expect("info");
        assert_eq!(info.protocol_version, PROTOCOL_VERSION);
        assert_eq!(info.hash_algo(), v1::HashAlgo::Blake3256);
        assert_eq!(info.role(), v1::ServerRole::Server);
        assert_eq!(info.read_endpoints.len(), 1);
        assert_eq!(info.read_endpoints[0].region, "a");
        assert_eq!(info.limits.expect("limits").max_batch_objects, 1_000);
        assert!(info.features.contains(&"objects.get".to_string()));
        assert!(info.features.contains(&"objects.has".to_string()));
    }
}
