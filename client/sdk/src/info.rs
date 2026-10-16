//! 能力协商结果。

use std::collections::BTreeSet;
use std::fmt;

use scion_proto::v1;

use crate::error::{Error, Result};

/// 协议版本 `主版本.次版本`。主版本相同即兼容。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
}

impl ProtocolVersion {
    /// ```
    /// use scion_client::ProtocolVersion;
    /// assert_eq!(ProtocolVersion::parse("1.3"), Some(ProtocolVersion { major: 1, minor: 3 }));
    /// assert_eq!(ProtocolVersion::parse("1"), None);
    /// ```
    pub fn parse(s: &str) -> Option<Self> {
        let (major, minor) = s.split_once('.')?;
        let num = |p: &str| {
            (!p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
                .then(|| p.parse().ok())
                .flatten()
        };
        Some(Self {
            major: num(major)?,
            minor: num(minor)?,
        })
    }

    /// 本 SDK 实现的协议版本。
    pub fn client() -> Self {
        Self::parse(scion_proto::PROTOCOL_VERSION).unwrap_or(Self { major: 1, minor: 0 })
    }

    pub fn compatible_with(&self, other: &Self) -> bool {
        self.major == other.major
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// 服务端的角色（应答方处于缓存层级中的哪一层）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Server,
    Cache,
    Daemon,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_batch_objects: u32,
    pub max_chunk_bytes: u64,
    pub max_changes_per_changeset: u32,
    pub max_inline_bytes: u64,
    /// UploadBlobs 单个对象上限；0 表示服务端未声明。
    pub max_upload_object_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEndpoint {
    pub url: String,
    pub region: String,
}

/// `GetServerInfo` 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub protocol_version: ProtocolVersion,
    pub features: BTreeSet<String>,
    pub limits: Limits,
    pub read_endpoints: Vec<ReadEndpoint>,
    pub server_version: String,
    pub role: Role,
}

impl ServerInfo {
    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.contains(feature)
    }

    /// 校验并转换服务端响应：协议主版本必须兼容，哈希算法必须是 BLAKE3-256。
    pub(crate) fn from_proto(info: v1::ServerInfo) -> Result<Self> {
        let client = ProtocolVersion::client();
        let protocol_version = ProtocolVersion::parse(&info.protocol_version)
            .ok_or_else(|| Error::Protocol(format!("bad protocol_version {:?}", info.protocol_version)))?;
        if !protocol_version.compatible_with(&client) {
            return Err(Error::IncompatibleProtocol {
                server: info.protocol_version,
                client: scion_proto::PROTOCOL_VERSION,
            });
        }
        if info.hash_algo() != v1::HashAlgo::Blake3256 {
            return Err(Error::Protocol(format!(
                "unsupported hash algorithm {:?}",
                info.hash_algo()
            )));
        }
        let role = match info.role() {
            v1::ServerRole::Server => Role::Server,
            v1::ServerRole::Cache => Role::Cache,
            v1::ServerRole::Daemon => Role::Daemon,
            v1::ServerRole::Unspecified => Role::Unknown,
        };
        let l = info.limits.ok_or_else(|| Error::Protocol("missing limits".into()))?;
        if l.max_batch_objects == 0 {
            return Err(Error::Protocol("limits.max_batch_objects must be positive".into()));
        }
        Ok(Self {
            protocol_version,
            features: info.features.into_iter().collect(),
            limits: Limits {
                max_batch_objects: l.max_batch_objects,
                max_chunk_bytes: l.max_chunk_bytes,
                max_changes_per_changeset: l.max_changes_per_changeset,
                max_inline_bytes: l.max_inline_bytes,
                max_upload_object_bytes: l.max_upload_object_bytes,
            },
            read_endpoints: info
                .read_endpoints
                .into_iter()
                .map(|e| ReadEndpoint {
                    url: e.url,
                    region: e.region,
                })
                .collect(),
            server_version: info.server_version,
            role,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto(version: &str) -> v1::ServerInfo {
        v1::ServerInfo {
            protocol_version: version.into(),
            features: vec!["objects.get".into(), "objects.get".into()],
            limits: Some(v1::Limits {
                max_batch_objects: 10,
                max_chunk_bytes: 20,
                max_changes_per_changeset: 30,
                max_inline_bytes: 40,
                max_upload_object_bytes: 50,
            }),
            read_endpoints: vec![v1::Endpoint {
                url: "u".into(),
                region: "r".into(),
            }],
            hash_algo: v1::HashAlgo::Blake3256.into(),
            server_version: "9.9.9".into(),
            role: v1::ServerRole::Cache.into(),
        }
    }

    #[test]
    fn parse_versions() {
        assert_eq!(
            ProtocolVersion::parse("1.0"),
            Some(ProtocolVersion { major: 1, minor: 0 })
        );
        assert_eq!(
            ProtocolVersion::parse("12.34"),
            Some(ProtocolVersion { major: 12, minor: 34 })
        );
        for bad in ["", "1", "1.", ".1", "a.b", "1.2.3", "+1.2", "1.-2", "99999999999.0"] {
            assert_eq!(ProtocolVersion::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn display_and_compat() {
        let v = ProtocolVersion { major: 1, minor: 7 };
        assert_eq!(v.to_string(), "1.7");
        assert!(v.compatible_with(&ProtocolVersion { major: 1, minor: 0 }));
        assert!(!v.compatible_with(&ProtocolVersion { major: 2, minor: 7 }));
        assert_eq!(ProtocolVersion::client().to_string(), scion_proto::PROTOCOL_VERSION);
    }

    #[test]
    fn from_proto_converts_all_fields() {
        let info = ServerInfo::from_proto(proto("1.5")).expect("ok");
        assert_eq!(info.protocol_version, ProtocolVersion { major: 1, minor: 5 });
        assert_eq!(info.features.len(), 1, "duplicates collapse");
        assert!(info.has_feature("objects.get"));
        assert!(!info.has_feature("land.batch"));
        assert_eq!(info.limits.max_inline_bytes, 40);
        assert_eq!(info.limits.max_upload_object_bytes, 50);
        assert_eq!(
            info.read_endpoints,
            vec![ReadEndpoint {
                url: "u".into(),
                region: "r".into()
            }]
        );
        assert_eq!(info.server_version, "9.9.9");
        assert_eq!(info.role, Role::Cache);
    }

    #[test]
    fn from_proto_roles() {
        for (r, want) in [
            (v1::ServerRole::Server, Role::Server),
            (v1::ServerRole::Daemon, Role::Daemon),
            (v1::ServerRole::Unspecified, Role::Unknown),
        ] {
            let mut p = proto("1.0");
            p.role = r.into();
            assert_eq!(ServerInfo::from_proto(p).expect("ok").role, want);
        }
    }

    #[test]
    fn from_proto_incompatible_major() {
        assert!(matches!(
            ServerInfo::from_proto(proto("2.0")),
            Err(Error::IncompatibleProtocol { .. })
        ));
    }

    #[test]
    fn from_proto_malformed() {
        assert!(matches!(ServerInfo::from_proto(proto("v1")), Err(Error::Protocol(_))));
        let mut p = proto("1.0");
        p.hash_algo = v1::HashAlgo::Unspecified.into();
        assert!(matches!(ServerInfo::from_proto(p), Err(Error::Protocol(_))));
        let mut p = proto("1.0");
        p.limits = None;
        assert!(matches!(ServerInfo::from_proto(p), Err(Error::Protocol(_))));
        let mut p = proto("1.0");
        p.limits = Some(v1::Limits::default());
        assert!(matches!(ServerInfo::from_proto(p), Err(Error::Protocol(_))));
    }
}
