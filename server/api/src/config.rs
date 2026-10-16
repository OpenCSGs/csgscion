//! 服务端对外声明的限额与拓扑提示。

use scion_proto::v1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_batch_objects: u32,
    pub max_chunk_bytes: u64,
    pub max_changes_per_changeset: u32,
    pub max_inline_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_batch_objects: 1_000,
            max_chunk_bytes: 1 << 20,
            max_changes_per_changeset: 100_000,
            max_inline_bytes: 1 << 20,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            read_endpoints: Vec::new(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
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
        };
        let p: v1::Limits = (&l).into();
        assert_eq!(
            (
                p.max_batch_objects,
                p.max_chunk_bytes,
                p.max_changes_per_changeset,
                p.max_inline_bytes
            ),
            (1, 2, 3, 4)
        );
    }

    #[test]
    fn server_config_default_version_is_crate_version() {
        let c = ServerConfig::default();
        assert_eq!(c.server_version, env!("CARGO_PKG_VERSION"));
        assert!(c.read_endpoints.is_empty());
    }
}
