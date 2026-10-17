//! Scion v1 协议的生成代码。proto 源文件位于仓库根目录 `proto/`。

/// `scion.v1` 包。
#[allow(clippy::all, missing_docs)]
pub mod v1 {
    tonic::include_proto!("scion.v1");
}

/// 当前实现的协议版本（主版本.次版本）。
pub const PROTOCOL_VERSION: &str = "1.0";

/// 错误详情 `google.rpc.ErrorInfo` 的 domain。
pub const ERROR_DOMAIN: &str = "scion.dev";

/// `ErrorInfo.reason` 使用的字符串：枚举名去掉 `ERROR_REASON_` 前缀，如 `OBJECT_MISSING`。
pub fn reason_name(r: v1::ErrorReason) -> &'static str {
    let full = r.as_str_name();
    full.strip_prefix("ERROR_REASON_").unwrap_or(full)
}

/// [`reason_name`] 的逆操作；未知字符串返回 `None`（新版服务端可能新增原因）。
pub fn parse_reason(s: &str) -> Option<v1::ErrorReason> {
    v1::ErrorReason::from_str_name(&format!("ERROR_REASON_{s}")).filter(|r| *r != v1::ErrorReason::Unspecified)
}

/// 能力标记（ServerInfo.features）。
pub mod features {
    pub const OBJECTS_GET: &str = "objects.get";
    pub const OBJECTS_HAS: &str = "objects.has";
    pub const WRITE_UPLOAD: &str = "write.upload";
    pub const WRITE_CHANGESET: &str = "write.changeset";
    /// 合入（条件更新 ref）；`fast_forward_only` 时只允许快进。
    pub const LAND_DIRECT: &str = "land.direct";
    /// 合入时在服务端 rebase 落后的提交栈，冲突返回 MERGE_CONFLICT。
    pub const LAND_REBASE: &str = "land.rebase";
    /// GetLandStatus 与按 request id 的幂等合入。
    pub const LAND_STATUS: &str = "land.status";
    pub const REFS: &str = "refs";
    /// WatchRefs：订阅 ref 变化。
    pub const REFS_WATCH: &str = "refs.watch";
    pub const QUERY_READ_FILE: &str = "query.read_file";
    pub const QUERY_LIST_DIR: &str = "query.list_dir";
    pub const QUERY_LOG: &str = "query.log";
    /// IsAncestor 与 MergeBase。
    pub const QUERY_ANCESTRY: &str = "query.ancestry";
    pub const QUERY_DIFF: &str = "query.diff";
    pub const QUERY_BLAME: &str = "query.blame";
    pub const QUERY_RESOLVE: &str = "query.resolve_revision";
    pub const QUERY_STAT: &str = "query.stat";
    pub const QUERY_DERIVED: &str = "query.derived";
    /// AuthService 的 WhoAmI / ExchangeToken / CheckAccess。
    pub const AUTH: &str = "auth";
    /// 服务端要求认证（未带有效令牌的请求返回 UNAUTHENTICATED）。
    pub const AUTH_REQUIRED: &str = "auth.required";
    /// OIDC device flow 登录（StartDeviceLogin / PollDeviceLogin）。
    pub const AUTH_DEVICE: &str = "auth.device";
    /// AdminService：策略与令牌管理。
    pub const ADMIN: &str = "admin";
    /// redaction：读取 redact 过的对象返回 REDACTED，`ListRedactedObjects` 与 AdminService 的 redaction 接口可用。
    pub const REDACTION: &str = "redaction";
}

#[cfg(test)]
mod tests {
    use super::v1;

    #[test]
    fn enum_values_match_spec_bytes() {
        assert_eq!(v1::ObjectKind::Blob as i32, 0x01);
        assert_eq!(v1::ObjectKind::Tree as i32, 0x02);
        assert_eq!(v1::ObjectKind::Changeset as i32, 0x03);
        assert_eq!(v1::ObjectKind::ChunkList as i32, 0x05);
        assert_eq!(v1::HashAlgo::Blake3256 as i32, 0x01);
    }

    #[test]
    fn error_reason_str_name_strips_prefix() {
        assert_eq!(
            v1::ErrorReason::ObjectMissing.as_str_name(),
            "ERROR_REASON_OBJECT_MISSING"
        );
    }

    #[test]
    fn reason_name_and_parse_roundtrip() {
        for r in [
            v1::ErrorReason::ObjectMissing,
            v1::ErrorReason::RepoNotFound,
            v1::ErrorReason::MergeConflict,
        ] {
            assert_eq!(super::parse_reason(super::reason_name(r)), Some(r));
        }
        assert_eq!(super::reason_name(v1::ErrorReason::ObjectMissing), "OBJECT_MISSING");
    }

    #[test]
    fn parse_reason_unknown_and_unspecified() {
        assert_eq!(super::parse_reason("NOPE"), None);
        assert_eq!(super::parse_reason("UNSPECIFIED"), None);
        assert_eq!(super::parse_reason(""), None);
    }

    #[test]
    fn protocol_version_is_major_minor() {
        let parts: Vec<_> = super::PROTOCOL_VERSION.split('.').collect();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| p.parse::<u32>().is_ok()));
    }
}
