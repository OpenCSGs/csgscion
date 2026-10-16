//! ObjectId（SPEC.md §1）。

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// ObjectId 二进制长度：algo(1) + kind(1) + digest(32)。
pub const ID_LEN: usize = 34;
/// digest 长度。
pub const DIGEST_LEN: usize = 32;

/// 哈希算法。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum HashAlgo {
    Blake3_256 = 0x01,
}

impl HashAlgo {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x01 => Ok(HashAlgo::Blake3_256),
            other => Err(Error::UnknownAlgo(other)),
        }
    }
}

/// 对象类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum ObjectKind {
    Blob = 0x01,
    Tree = 0x02,
    Changeset = 0x03,
    ChunkList = 0x05,
}

impl ObjectKind {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x01 => Ok(ObjectKind::Blob),
            0x02 => Ok(ObjectKind::Tree),
            0x03 => Ok(ObjectKind::Changeset),
            0x05 => Ok(ObjectKind::ChunkList),
            other => Err(Error::UnknownKind(other)),
        }
    }

    /// 文本形式的前缀字母。
    pub fn prefix(self) -> char {
        match self {
            ObjectKind::Blob => 'b',
            ObjectKind::Tree => 't',
            ObjectKind::Changeset => 'c',
            ObjectKind::ChunkList => 'l',
        }
    }

    fn from_prefix(c: char) -> Option<Self> {
        match c {
            'b' => Some(ObjectKind::Blob),
            't' => Some(ObjectKind::Tree),
            'c' => Some(ObjectKind::Changeset),
            'l' => Some(ObjectKind::ChunkList),
            _ => None,
        }
    }
}

/// 内容寻址的对象 ID。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId {
    algo: HashAlgo,
    kind: ObjectKind,
    digest: [u8; DIGEST_LEN],
}

impl ObjectId {
    pub fn new(algo: HashAlgo, kind: ObjectKind, digest: [u8; DIGEST_LEN]) -> Self {
        Self { algo, kind, digest }
    }

    /// 对给定字节计算 BLAKE3 并生成对应 kind 的 ID。
    ///
    /// ```
    /// use scion_object::{ObjectId, ObjectKind};
    /// let id = ObjectId::hash(ObjectKind::Blob, b"");
    /// assert_eq!(
    ///     id.to_string(),
    ///     "b1af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    /// );
    /// ```
    pub fn hash(kind: ObjectKind, bytes: &[u8]) -> Self {
        Self::new(HashAlgo::Blake3_256, kind, *blake3::hash(bytes).as_bytes())
    }

    /// 文件内容的 Blob ID。
    pub fn of_blob(content: &[u8]) -> Self {
        Self::hash(ObjectKind::Blob, content)
    }

    pub fn algo(&self) -> HashAlgo {
        self.algo
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub fn digest(&self) -> &[u8; DIGEST_LEN] {
        &self.digest
    }

    /// 校验对象字节与 ID 相符（SPEC.md §1.2）。任何从网络或缓存拿到的对象都应先调用它。
    pub fn verify(&self, bytes: &[u8]) -> bool {
        match self.algo {
            HashAlgo::Blake3_256 => blake3::hash(bytes).as_bytes() == &self.digest,
        }
    }

    pub fn to_bytes(&self) -> [u8; ID_LEN] {
        let mut out = [0u8; ID_LEN];
        out[0] = self.algo as u8;
        out[1] = self.kind as u8;
        out[2..].copy_from_slice(&self.digest);
        out
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if b.len() != ID_LEN {
            return Err(Error::InvalidIdLength(b.len()));
        }
        let algo = HashAlgo::from_u8(b[0])?;
        let kind = ObjectKind::from_u8(b[1])?;
        let mut digest = [0u8; DIGEST_LEN];
        digest.copy_from_slice(&b[2..]);
        Ok(Self { algo, kind, digest })
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.kind.prefix(),
            self.algo as u8,
            hex::encode(self.digest)
        )
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({self})")
    }
}

impl FromStr for ObjectId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let bad = || Error::InvalidIdText(s.to_owned());
        // 前缀 1 字符 + algo 1 位数字 + 64 位十六进制
        if s.len() != 2 + DIGEST_LEN * 2 || !s.is_ascii() {
            return Err(bad());
        }
        let mut chars = s.chars();
        let kind = chars.next().and_then(ObjectKind::from_prefix).ok_or_else(bad)?;
        let algo_digit = chars.next().and_then(|c| c.to_digit(10)).ok_or_else(bad)?;
        let algo = HashAlgo::from_u8(algo_digit as u8).map_err(|_| bad())?;
        let hex_part = &s[2..];
        if !hex_part
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(bad());
        }
        let mut digest = [0u8; DIGEST_LEN];
        hex::decode_to_slice(hex_part, &mut digest).map_err(|_| bad())?;
        Ok(Self { algo, kind, digest })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const EMPTY_BLOB: &str = "b1af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

    #[test]
    fn of_blob_empty_matches_blake3_reference() {
        assert_eq!(ObjectId::of_blob(b"").to_string(), EMPTY_BLOB);
    }

    #[test]
    fn hash_same_bytes_different_kind_differs() {
        let a = ObjectId::hash(ObjectKind::Blob, b"x");
        let b = ObjectId::hash(ObjectKind::Tree, b"x");
        assert_ne!(a, b);
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn accessors_return_parts() {
        let id = ObjectId::new(HashAlgo::Blake3_256, ObjectKind::Changeset, [7; 32]);
        assert_eq!(id.algo(), HashAlgo::Blake3_256);
        assert_eq!(id.kind(), ObjectKind::Changeset);
        assert_eq!(id.digest(), &[7; 32]);
    }

    #[test]
    fn verify_matching_and_tampered() {
        let id = ObjectId::of_blob(b"hello");
        assert!(id.verify(b"hello"));
        assert!(!id.verify(b"hellp"));
        assert!(!id.verify(b""));
    }

    #[test]
    fn to_bytes_layout() {
        let id = ObjectId::new(HashAlgo::Blake3_256, ObjectKind::Tree, [0xab; 32]);
        let b = id.to_bytes();
        assert_eq!(b[0], 0x01);
        assert_eq!(b[1], 0x02);
        assert!(b[2..].iter().all(|&x| x == 0xab));
    }

    #[test]
    fn from_bytes_wrong_length_rejected() {
        assert_eq!(ObjectId::from_bytes(&[1; 33]), Err(Error::InvalidIdLength(33)));
        assert_eq!(ObjectId::from_bytes(&[]), Err(Error::InvalidIdLength(0)));
    }

    #[test]
    fn from_bytes_unknown_algo_rejected() {
        let mut b = ObjectId::of_blob(b"").to_bytes();
        b[0] = 0x02;
        assert_eq!(ObjectId::from_bytes(&b), Err(Error::UnknownAlgo(2)));
    }

    #[test]
    fn from_bytes_unknown_kind_rejected() {
        let mut b = ObjectId::of_blob(b"").to_bytes();
        b[1] = 0x04;
        assert_eq!(ObjectId::from_bytes(&b), Err(Error::UnknownKind(4)));
    }

    #[test]
    fn kind_from_u8_all_known() {
        for k in [
            ObjectKind::Blob,
            ObjectKind::Tree,
            ObjectKind::Changeset,
            ObjectKind::ChunkList,
        ] {
            assert_eq!(ObjectKind::from_u8(k as u8), Ok(k));
            assert_eq!(ObjectKind::from_prefix(k.prefix()), Some(k));
        }
        assert_eq!(ObjectKind::from_u8(0), Err(Error::UnknownKind(0)));
        assert_eq!(ObjectKind::from_prefix('x'), None);
    }

    #[test]
    fn algo_from_u8_unknown_rejected() {
        assert_eq!(HashAlgo::from_u8(1), Ok(HashAlgo::Blake3_256));
        assert_eq!(HashAlgo::from_u8(0), Err(Error::UnknownAlgo(0)));
    }

    #[test]
    fn display_prefixes_per_kind() {
        for (k, p) in [
            (ObjectKind::Blob, "b1"),
            (ObjectKind::Tree, "t1"),
            (ObjectKind::Changeset, "c1"),
            (ObjectKind::ChunkList, "l1"),
        ] {
            assert!(ObjectId::hash(k, b"").to_string().starts_with(p));
        }
    }

    #[test]
    fn debug_wraps_text_form() {
        assert_eq!(
            format!("{:?}", ObjectId::of_blob(b"")),
            format!("ObjectId({EMPTY_BLOB})")
        );
    }

    #[test]
    fn from_str_roundtrip() {
        let id: ObjectId = EMPTY_BLOB.parse().expect("valid id");
        assert_eq!(id, ObjectId::of_blob(b""));
    }

    #[test]
    fn from_str_invalid_inputs_rejected() {
        let upper = EMPTY_BLOB.to_uppercase();
        let cases = [
            "",
            "b1",
            &EMPTY_BLOB[..65],                                                    // 太短
            &upper,                                                               // 大写
            "x1af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // 未知前缀
            "b2af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // 未知 algo
            "bxaf1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // algo 非数字
            "b1zf1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // 非十六进制
            "b1Af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // 混入大写
            "b1模f1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f32",  // 非 ASCII
        ];
        for c in cases {
            assert!(
                matches!(c.parse::<ObjectId>(), Err(Error::InvalidIdText(_))),
                "should reject {c:?}"
            );
        }
    }

    fn any_kind() -> impl Strategy<Value = ObjectKind> {
        prop_oneof![
            Just(ObjectKind::Blob),
            Just(ObjectKind::Tree),
            Just(ObjectKind::Changeset),
            Just(ObjectKind::ChunkList),
        ]
    }

    proptest! {
        #[test]
        fn bytes_and_text_roundtrip_any(kind in any_kind(), digest: [u8; 32]) {
            let id = ObjectId::new(HashAlgo::Blake3_256, kind, digest);
            prop_assert_eq!(ObjectId::from_bytes(&id.to_bytes()), Ok(id));
            prop_assert_eq!(id.to_string().parse::<ObjectId>(), Ok(id));
        }

        #[test]
        fn from_str_never_panics(s in ".{0,80}") {
            let _ = s.parse::<ObjectId>();
        }
    }
}
