//! 文件名与路径规则（SPEC.md §8）。

use crate::error::{Error, Result};

pub const MAX_NAME_LEN: usize = 255;
pub const MAX_PATH_LEN: usize = 4096;

/// 名字是否合法（不分配，供 `validate_name` 和路径校验共用）。
fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != "."
        && name != ".."
        && !name.bytes().any(|b| b == b'/' || b == 0)
}

/// 校验单个文件名（Tree 条目名或路径中的一段）。
///
/// ```
/// assert!(scion_object::validate_name("main.rs").is_ok());
/// assert!(scion_object::validate_name("a/b").is_err());
/// ```
pub fn validate_name(name: &str) -> Result<()> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(Error::InvalidName(name.to_owned()))
    }
}

/// 校验仓库内的相对路径，如 `src/main.rs`。
///
/// ```
/// assert!(scion_object::validate_path("src/main.rs").is_ok());
/// assert!(scion_object::validate_path("/abs").is_err());
/// ```
pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_PATH_LEN || !path.split('/').all(is_valid_name) {
        return Err(Error::InvalidPath(path.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_name_accepts_normal_names() {
        for n in [
            "a",
            "main.rs",
            ".gitignore",
            "...",
            "模型.safetensors",
            "a b",
            "x".repeat(255).as_str(),
        ] {
            assert_eq!(validate_name(n), Ok(()), "{n:?}");
        }
    }

    #[test]
    fn validate_name_rejects_bad_names() {
        let too_long = "x".repeat(256);
        for n in ["", ".", "..", "a/b", "/", "a\0b", too_long.as_str()] {
            assert!(matches!(validate_name(n), Err(Error::InvalidName(_))), "{n:?}");
        }
    }

    #[test]
    fn validate_path_accepts_nested() {
        for p in ["a", "a/b", "src/lib/mod.rs", "x/.hidden/y"] {
            assert_eq!(validate_path(p), Ok(()), "{p:?}");
        }
    }

    #[test]
    fn validate_path_rejects_bad_paths() {
        for p in ["", "/a", "a/", "a//b", "./a", "a/../b", "a/./b", "a/\0", "/"] {
            assert!(matches!(validate_path(p), Err(Error::InvalidPath(_))), "{p:?}");
        }
    }

    #[test]
    fn validate_path_length_boundary() {
        // 16 段 254 字节 + 15 个分隔符 = 4079，再补一段 16 字节凑满 4096
        let seg = "x".repeat(254);
        let mut p = vec![seg.as_str(); 16].join("/");
        let rest = MAX_PATH_LEN - p.len() - 1;
        p.push('/');
        p.push_str(&"y".repeat(rest));
        assert_eq!(p.len(), MAX_PATH_LEN);
        assert_eq!(validate_path(&p), Ok(()));
        p.push('z');
        assert!(matches!(validate_path(&p), Err(Error::InvalidPath(_))));
    }

    #[test]
    fn validate_path_segment_too_long_rejected() {
        let p = format!("a/{}", "x".repeat(256));
        assert!(matches!(validate_path(&p), Err(Error::InvalidPath(_))));
    }
}
