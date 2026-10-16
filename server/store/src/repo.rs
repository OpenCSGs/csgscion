//! 仓库名规则。

use crate::error::{Error, Result};

pub const MAX_REPO_NAME_LEN: usize = 256;

/// 仓库名：一段或多段 `[A-Za-z0-9._-]+`，用 `/` 分隔（兼容 CSGHub 的 `namespace/name`）；
/// 每段不能以 `.` 开头，总长不超过 256 字节。
///
/// ```
/// assert!(scion_store::validate_repo_name("opencsg/csghub").is_ok());
/// assert!(scion_store::validate_repo_name("../etc").is_err());
/// ```
pub fn validate_repo_name(name: &str) -> Result<()> {
    let seg_ok = |s: &str| {
        !s.is_empty()
            && !s.starts_with('.')
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    if name.len() > MAX_REPO_NAME_LEN || !name.split('/').all(seg_ok) {
        return Err(Error::InvalidRepoName(name.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_repo_name_accepts() {
        for n in ["mono", "a/b", "opencsg/Qwen2.5-7B", "x_y-z", "a/b/c", "a.b"] {
            assert_eq!(validate_repo_name(n), Ok(()), "{n}");
        }
        assert_eq!(validate_repo_name(&"a".repeat(MAX_REPO_NAME_LEN)), Ok(()));
    }

    #[test]
    fn validate_repo_name_rejects() {
        let long = "a".repeat(MAX_REPO_NAME_LEN + 1);
        for n in [
            "",
            "/a",
            "a/",
            "a//b",
            ".hidden",
            "a/..",
            "a b",
            "模型",
            "a\0",
            long.as_str(),
        ] {
            assert!(matches!(validate_repo_name(n), Err(Error::InvalidRepoName(_))), "{n:?}");
        }
    }
}
