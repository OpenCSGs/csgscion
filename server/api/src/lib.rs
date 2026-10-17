//! Scion 服务端的 gRPC 服务实现与启动入口。

pub mod config;
mod service;
pub mod status;

use std::future::Future;
use std::sync::Arc;

use scion_proto::v1::land_service_server::LandServiceServer;
use scion_proto::v1::object_service_server::ObjectServiceServer;
use scion_proto::v1::query_service_server::QueryServiceServer;
use scion_proto::v1::ref_service_server::RefServiceServer;
use scion_proto::v1::server_info_service_server::ServerInfoServiceServer;
use scion_proto::v1::write_service_server::WriteServiceServer;
use scion_store::{BlobStore, MetaStore};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub use config::{Limits, ReadEndpoint, ServerConfig};
pub use scion_proto::PROTOCOL_VERSION;
pub use service::{LandSvc, ObjectSvc, QuerySvc, RefSvc, ServerInfoSvc, WriteSvc};

/// 服务端依赖的全部状态。
#[derive(Clone)]
pub struct App {
    pub config: Arc<ServerConfig>,
    pub meta: Arc<dyn MetaStore>,
    pub blobs: Arc<dyn BlobStore>,
}

/// 预热全部仓库的提交记录缓存。失败只记录日志：缓存未命中时照常按需读取。
async fn warm(cache: Arc<scion_store::CachedMeta>) {
    let repos = match scion_store::MetaStore::list_repos(cache.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "record cache warm-up skipped");
            return;
        }
    };
    let started = std::time::Instant::now();
    let mut total = 0;
    for repo in repos {
        match cache.warm(&repo, 4096).await {
            Ok(n) => total += n,
            Err(e) => tracing::warn!(%repo, error = %e, "record cache warm-up failed"),
        }
    }
    tracing::info!(
        records = total,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "record cache warmed"
    );
}

/// 在给定监听器上提供全部 gRPC 服务（含标准健康检查），直到 `shutdown` 完成。
pub async fn serve(
    listener: TcpListener,
    app: App,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let (health, health_svc) = tonic_health::server::health_reporter();
    health.set_serving::<ServerInfoServiceServer<ServerInfoSvc>>().await;
    health.set_serving::<ObjectServiceServer<ObjectSvc>>().await;
    health.set_serving::<WriteServiceServer<WriteSvc>>().await;
    health.set_serving::<LandServiceServer<LandSvc>>().await;
    health.set_serving::<RefServiceServer<RefSvc>>().await;
    health.set_serving::<QueryServiceServer<QuerySvc>>().await;

    // 所有服务共享一份提交记录缓存；启动后在后台预热，不阻塞监听
    let cached = Arc::new(scion_store::CachedMeta::new(app.meta, app.config.record_cache_capacity));
    if app.config.warm_record_cache {
        tokio::spawn(warm(cached.clone()));
    }
    let app = App { meta: cached, ..app };
    let max_msg = usize::try_from(app.config.limits.max_chunk_bytes)
        .unwrap_or(usize::MAX)
        .saturating_add(64 * 1024);
    let info = ServerInfoServiceServer::new(ServerInfoSvc::new(app.config.clone()));
    let objects = ObjectServiceServer::new(ObjectSvc::new(app.config.clone(), app.meta.clone(), app.blobs.clone()))
        .max_encoding_message_size(max_msg);
    // 上传的分块可能和读取分块一样大
    let write_svc = WriteSvc::new(app.config.clone(), app.meta.clone(), app.blobs.clone());
    // 合入与创建提交共用同一个写入器
    let land = LandServiceServer::new(LandSvc::new(scion_land::Lander::new(write_svc.writer().clone())));
    let write = WriteServiceServer::new(write_svc).max_decoding_message_size(max_msg);
    let refs = RefServiceServer::new(RefSvc::new(app.meta.clone()));
    let query = QueryServiceServer::new(QuerySvc::new(app.config.clone(), app.meta, app.blobs))
        .max_encoding_message_size(max_msg);

    tonic::transport::Server::builder()
        .add_service(health_svc)
        .add_service(info)
        .add_service(objects)
        .add_service(write)
        .add_service(land)
        .add_service(refs)
        .add_service(query)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}
