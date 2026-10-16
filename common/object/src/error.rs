//! 解码与校验错误。每个变体对应 SPEC.md §6 中的一个稳定错误码。

use thiserror::Error;

/// 规范编码的解码或结构校验失败。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("{0} trailing byte(s) after object")]
    TrailingBytes(usize),
    #[error("varint overflows u64")]
    VarintOverflow,
    #[error("varint is not minimally encoded")]
    VarintNonMinimal,
    #[error("length {0} exceeds remaining input")]
    LengthOutOfRange(u64),
    #[error("expected object kind {expected:#04x}, found {found:#04x}")]
    WrongKind { expected: u8, found: u8 },
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown hash algorithm {0:#04x}")]
    UnknownAlgo(u8),
    #[error("unknown object kind {0:#04x}")]
    UnknownKind(u8),
    #[error("unknown entry mode {0:#04x}")]
    UnknownMode(u8),
    #[error("unknown file change op {0:#04x}")]
    UnknownOp(u8),
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    #[error("invalid file name {0:?}")]
    InvalidName(String),
    #[error("invalid path {0:?}")]
    InvalidPath(String),
    #[error("{0} not strictly sorted or contains duplicates")]
    NotSorted(&'static str),
    #[error("object id kind does not match {0}")]
    IdKindMismatch(&'static str),
    #[error("tree entry {0:?} is a directory but has non-zero size")]
    NonzeroTreeSize(String),
    #[error("duplicate parent")]
    DuplicateParent,
    #[error("timezone offset {0} out of range [-1440, 1440]")]
    InvalidTzOffset(i64),
    #[error("extension key must not be empty")]
    InvalidExtensionKey,
    #[error("invalid chunk list: {0}")]
    InvalidChunkList(&'static str),
    #[error("invalid object id text {0:?}")]
    InvalidIdText(String),
    #[error("object id must be 34 bytes, got {0}")]
    InvalidIdLength(usize),
}

impl Error {
    /// SPEC.md §6 中定义的稳定错误码，用于 golden 向量和跨语言对照。
    ///
    /// ```
    /// assert_eq!(scion_object::Error::UnexpectedEof.code(), "unexpected_eof");
    /// ```
    pub fn code(&self) -> &'static str {
        match self {
            Error::UnexpectedEof => "unexpected_eof",
            Error::TrailingBytes(_) => "trailing_bytes",
            Error::VarintOverflow => "varint_overflow",
            Error::VarintNonMinimal => "varint_non_minimal",
            Error::LengthOutOfRange(_) => "length_out_of_range",
            Error::WrongKind { .. } => "wrong_kind",
            Error::UnsupportedVersion(_) => "unsupported_version",
            Error::UnknownAlgo(_) => "unknown_algo",
            Error::UnknownKind(_) => "unknown_kind",
            Error::UnknownMode(_) => "unknown_mode",
            Error::UnknownOp(_) => "unknown_op",
            Error::InvalidUtf8 => "invalid_utf8",
            Error::InvalidName(_) => "invalid_name",
            Error::InvalidPath(_) => "invalid_path",
            Error::NotSorted(_) => "not_sorted",
            Error::IdKindMismatch(_) => "id_kind_mismatch",
            Error::NonzeroTreeSize(_) => "nonzero_tree_size",
            Error::DuplicateParent => "duplicate_parent",
            Error::InvalidTzOffset(_) => "invalid_tz_offset",
            Error::InvalidExtensionKey => "invalid_extension_key",
            Error::InvalidChunkList(_) => "invalid_chunk_list",
            Error::InvalidIdText(_) => "invalid_id_text",
            Error::InvalidIdLength(_) => "invalid_id_length",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn all_variants() -> Vec<Error> {
        vec![
            Error::UnexpectedEof,
            Error::TrailingBytes(1),
            Error::VarintOverflow,
            Error::VarintNonMinimal,
            Error::LengthOutOfRange(9),
            Error::WrongKind { expected: 2, found: 3 },
            Error::UnsupportedVersion(2),
            Error::UnknownAlgo(9),
            Error::UnknownKind(9),
            Error::UnknownMode(9),
            Error::UnknownOp(9),
            Error::InvalidUtf8,
            Error::InvalidName("a/b".into()),
            Error::InvalidPath("/a".into()),
            Error::NotSorted("tree entries"),
            Error::IdKindMismatch("tree entry"),
            Error::NonzeroTreeSize("d".into()),
            Error::DuplicateParent,
            Error::InvalidTzOffset(9999),
            Error::InvalidExtensionKey,
            Error::InvalidChunkList("empty"),
            Error::InvalidIdText("x".into()),
            Error::InvalidIdLength(3),
        ]
    }

    #[test]
    fn code_every_variant_unique_snake_case() {
        let variants = all_variants();
        let codes: HashSet<_> = variants.iter().map(Error::code).collect();
        assert_eq!(codes.len(), variants.len(), "error codes must be unique");
        for c in codes {
            assert!(
                c.chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_'),
                "{c}"
            );
        }
    }

    #[test]
    fn display_every_variant_non_empty() {
        for e in all_variants() {
            assert!(!e.to_string().is_empty());
        }
    }

    #[test]
    fn display_wrong_kind_shows_hex() {
        let e = Error::WrongKind { expected: 2, found: 3 };
        assert_eq!(e.to_string(), "expected object kind 0x02, found 0x03");
    }
}
