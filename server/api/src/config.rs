//! 服务端对外声明的限额与拓扑提示。

use scion_proto::v1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_batch_objects: u32,
    pub max_chunk_bytes: u64,
    pub max_changes_per_changeset: u32,
    pub max_inline_bytes: u64,
    /// UploadBlobs 单个对象上限。M1 在内存中整体校验后写入；更大的文件在分块存储加入后支持。
    pub max_upload_object_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_batch_objects: 1_000,
            max_chunk_bytes: 1 << 20,
            max_changes_per_changeset: 100_000,
            max_inline_bytes: 1 << 20,
            max_upload_object_bytes: 256 << 20,
        }
    }
}

impl From<&Limits> for v1::Limits {
    fn from(l: &Limits) -> Self {
        v1::Limits {
            max_batch_objects: l.max_batch_objects,
            max_chunk_bytes: l.max_chunk_bytes,
            max_changes_per_changeset: l.max_changes_per_changeset,
            max_inline_bytes: l.max_inline_bytes,
            max_upload_object_bytes: l.max_upload_object_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEndpoint {
    pub url: String,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub limits: Limits,
    pub read_endpoints: Vec<ReadEndpoint>,
    /// 服务端实现版本，仅供展示。
    pub server_version: String,
    /// 进程内缓存的提交记录条数上限（记录不可变，缓存无需失效）。
    pub record_cache_capacity: u64,
    /// 启动后在后台把各仓库的提交记录预热进缓存。
    pub warm_record_cache: bool,
    /// 写请求结果（幂等表）的保留时间。
    pub idempotency_ttl: std::time::Duration,
    /// 清理过期幂等记录的间隔。
    pub janitor_interval: std::time::Duration,
    /// 合入批处理参数（每个 ref 一个批处理任务）。
    pub land: scion_land::CoordinatorConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            read_endpoints: Vec::new(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            record_cache_capacity: 1_000_000,
            warm_record_cache: true,
            idempotency_ttl: std::time::Duration::from_secs(24 * 3600),
            janitor_interval: std::time::Duration::from_secs(600),
            land: scion_land::CoordinatorConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_chunk_not_larger_than_grpc_default_message() {
        // tonic 默认最大解码消息为 4 MiB，分块必须留出余量
        assert!(Limits::default().max_chunk_bytes < 4 << 20);
        assert!(Limits::default().max_batch_objects > 0);
    }

    #[test]
    fn limits_into_proto_copies_all_fields() {
        let l = Limits {
            max_batch_objects: 1,
            max_chunk_bytes: 2,
            max_changes_per_changeset: 3,
            max_inline_bytes: 4,
            max_upload_object_bytes: 5,
        };
        let p: v1::Limits = (&l).into();
        assert_eq!(
            (
                p.max_batch_objects,
                p.max_chunk_bytes,
                p.max_changes_per_changeset,
                p.max_inline_bytes,
                p.max_upload_object_bytes
            ),
            (1, 2, 3, 4, 5)
        );
    }

    #[test]
    fn server_config_default_version_is_crate_version() {
        let c = ServerConfig::default();
        assert_eq!(c.server_version, env!("CARGO_PKG_VERSION"));
        assert!(c.read_endpoints.is_empty());
        assert!(c.record_cache_capacity > 0);
    }
}
