//! Changeset（SPEC.md §5）。

use std::collections::BTreeMap;

use crate::codec::{Reader, put_bytes, put_id, put_str, put_svarint, put_uvarint};
use crate::error::{Error, Result};
use crate::id::{ObjectId, ObjectKind};
use crate::path::validate_path;
use crate::tree::EntryMode;

/// 时区偏移上限（分钟）。
pub const MAX_TZ_OFFSET_MINUTES: i32 = 1440;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
    pub email: String,
    /// Unix 毫秒。
    pub time_ms: i64,
    /// 相对 UTC 的分钟数。
    pub tz_offset_minutes: i32,
}

impl Signature {
    fn encode_into(&self, out: &mut Vec<u8>) {
        put_str(out, &self.name);
        put_str(out, &self.email);
        put_svarint(out, self.time_ms);
        put_svarint(out, i64::from(self.tz_offset_minutes));
    }

    fn decode_from(r: &mut Reader<'_>) -> Result<Self> {
        let name = r.string()?;
        let email = r.string()?;
        let time_ms = r.svarint()?;
        let tz = r.svarint()?;
        Ok(Self {
            name,
            email,
            time_ms,
            tz_offset_minutes: tz_from_i64(tz)?,
        })
    }
}

fn tz_from_i64(tz: i64) -> Result<i32> {
    let max = i64::from(MAX_TZ_OFFSET_MINUTES);
    if !(-max..=max).contains(&tz) {
        return Err(Error::InvalidTzOffset(tz));
    }
    Ok(tz as i32)
}

/// 对单个路径的改动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileOp {
    Put { mode: EntryMode, id: ObjectId, size: u64 },
    Delete,
}

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    pub path: String,
    pub op: FileOp,
}

impl FileChange {
    pub fn put(path: impl Into<String>, mode: EntryMode, id: ObjectId, size: u64) -> Self {
        Self {
            path: path.into(),
            op: FileOp::Put { mode, id, size },
        }
    }

    pub fn delete(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            op: FileOp::Delete,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_path(&self.path)?;
        if let FileOp::Put { mode, id, .. } = &self.op {
            if *mode == EntryMode::Tree {
                return Err(Error::UnknownMode(EntryMode::Tree as u8));
            }
            if id.kind() != ObjectKind::Blob {
                return Err(Error::IdKindMismatch("file change"));
            }
        }
        Ok(())
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        put_str(out, &self.path);
        match &self.op {
            FileOp::Put { mode, id, size } => {
                out.push(OP_PUT);
                out.push(*mode as u8);
                put_id(out, id);
                put_uvarint(out, *size);
            }
            FileOp::Delete => out.push(OP_DELETE),
        }
    }

    fn decode_from(r: &mut Reader<'_>) -> Result<Self> {
        let path = r.string()?;
        let op = match r.u8()? {
            OP_PUT => {
                let mode = EntryMode::from_u8(r.u8()?)?;
                let id = r.id()?;
                let size = r.uvarint()?;
                FileOp::Put { mode, id, size }
            }
            OP_DELETE => FileOp::Delete,
            other => return Err(Error::UnknownOp(other)),
        };
        Ok(Self { path, op })
    }
}

/// 一次提交：相对 first parent 的增量。ID 只由本结构决定，不含 root tree。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changeset {
    pub parents: Vec<ObjectId>,
    pub author: Signature,
    pub committer: Signature,
    pub message: String,
    pub change_id: [u8; 16],
    /// 按路径字节序严格升序。用 [`Changeset::new`] 构造时会自动排序。
    pub delta: Vec<FileChange>,
    pub extensions: BTreeMap<String, Vec<u8>>,
}

const KIND: u8 = ObjectKind::Changeset as u8;

impl Changeset {
    /// 构造并校验：delta 自动按路径排序，其余约束不满足时报错。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parents: Vec<ObjectId>,
        author: Signature,
        committer: Signature,
        message: impl Into<String>,
        change_id: [u8; 16],
        mut delta: Vec<FileChange>,
        extensions: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self> {
        delta.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        let cs = Self {
            parents,
            author,
            committer,
            message: message.into(),
            change_id,
            delta,
            extensions,
        };
        cs.validate()?;
        Ok(cs)
    }

    /// first parent；根提交返回 `None`。
    pub fn first_parent(&self) -> Option<&ObjectId> {
        self.parents.first()
    }

    /// 校验 SPEC.md §5 的全部约束。
    pub fn validate(&self) -> Result<()> {
        for (i, p) in self.parents.iter().enumerate() {
            if p.kind() != ObjectKind::Changeset {
                return Err(Error::IdKindMismatch("parent"));
            }
            if self.parents[..i].contains(p) {
                return Err(Error::DuplicateParent);
            }
        }
        for sig in [&self.author, &self.committer] {
            tz_from_i64(i64::from(sig.tz_offset_minutes))?;
        }
        for c in &self.delta {
            c.validate()?;
        }
        if !self
            .delta
            .windows(2)
            .all(|w| w[0].path.as_bytes() < w[1].path.as_bytes())
        {
            return Err(Error::NotSorted("delta"));
        }
        if self.extensions.contains_key("") {
            return Err(Error::InvalidExtensionKey);
        }
        Ok(())
    }

    /// 规范编码。调用方应保证结构已通过 [`Changeset::validate`]。
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![KIND, crate::FORMAT_VERSION];
        put_uvarint(&mut out, self.parents.len() as u64);
        for p in &self.parents {
            put_id(&mut out, p);
        }
        self.author.encode_into(&mut out);
        self.committer.encode_into(&mut out);
        put_str(&mut out, &self.message);
        out.extend_from_slice(&self.change_id);
        put_uvarint(&mut out, self.delta.len() as u64);
        for c in &self.delta {
            c.encode_into(&mut out);
        }
        // BTreeMap<String> 按 UTF-8 字节序迭代，正好是规范顺序
        put_uvarint(&mut out, self.extensions.len() as u64);
        for (k, v) in &self.extensions {
            put_str(&mut out, k);
            put_bytes(&mut out, v);
        }
        out
    }

    /// 严格解码。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        r.header(KIND)?;
        let n = r.count()?;
        let mut parents = Vec::with_capacity(n);
        for _ in 0..n {
            parents.push(r.id()?);
        }
        let author = Signature::decode_from(&mut r)?;
        let committer = Signature::decode_from(&mut r)?;
        let message = r.string()?;
        let change_id = r.fixed::<16>()?;
        let n = r.count()?;
        let mut delta = Vec::with_capacity(n);
        for _ in 0..n {
            delta.push(FileChange::decode_from(&mut r)?);
        }
        let n = r.count()?;
        let mut extensions = BTreeMap::new();
        let mut last: Option<String> = None;
        for _ in 0..n {
            let k = r.string()?;
            let v = r.bytes()?.to_vec();
            if last.as_ref().is_some_and(|l| l.as_bytes() >= k.as_bytes()) {
                return Err(Error::NotSorted("extensions"));
            }
            last = Some(k.clone());
            extensions.insert(k, v);
        }
        r.finish()?;
        let cs = Self {
            parents,
            author,
            committer,
            message,
            change_id,
            delta,
            extensions,
        };
        cs.validate()?;
        Ok(cs)
    }

    pub fn id(&self) -> ObjectId {
        ObjectId::hash(ObjectKind::Changeset, &self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sig() -> Signature {
        Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1_760_000_000_000,
            tz_offset_minutes: 480,
        }
    }

    fn root() -> Changeset {
        Changeset::new(
            vec![],
            sig(),
            sig(),
            "init",
            [1; 16],
            vec![FileChange::put("README", EntryMode::File, ObjectId::of_blob(b"hi"), 2)],
            BTreeMap::new(),
        )
        .expect("valid root")
    }

    #[test]
    fn root_roundtrip_and_first_parent() {
        let cs = root();
        assert!(cs.first_parent().is_none());
        assert_eq!(Changeset::decode(&cs.encode()), Ok(cs.clone()));
        assert!(cs.id().verify(&cs.encode()));
        assert_eq!(cs.id().kind(), ObjectKind::Changeset);
    }

    #[test]
    fn child_first_parent_is_first() {
        let p1 = root().id();
        let p2 = ObjectId::hash(ObjectKind::Changeset, b"other");
        let mut cs = root();
        cs.parents = vec![p1, p2];
        assert_eq!(cs.validate(), Ok(()));
        assert_eq!(cs.first_parent(), Some(&p1));
    }

    #[test]
    fn new_sorts_delta() {
        let cs = Changeset::new(
            vec![],
            sig(),
            sig(),
            "m",
            [0; 16],
            vec![
                FileChange::delete("b"),
                FileChange::delete("a/z"),
                FileChange::delete("a"),
            ],
            BTreeMap::new(),
        )
        .expect("valid");
        let paths: Vec<_> = cs.delta.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, ["a", "a/z", "b"]);
    }

    #[test]
    fn validate_duplicate_path_rejected() {
        let mut cs = root();
        cs.delta = vec![FileChange::delete("a"), FileChange::delete("a")];
        assert_eq!(cs.validate(), Err(Error::NotSorted("delta")));
    }

    #[test]
    fn validate_parent_wrong_kind_rejected() {
        let mut cs = root();
        cs.parents = vec![ObjectId::of_blob(b"x")];
        assert_eq!(cs.validate(), Err(Error::IdKindMismatch("parent")));
    }

    #[test]
    fn validate_duplicate_parent_rejected() {
        let p = root().id();
        let mut cs = root();
        cs.parents = vec![p, p];
        assert_eq!(cs.validate(), Err(Error::DuplicateParent));
    }

    #[test]
    fn validate_tz_out_of_range_rejected() {
        let mut cs = root();
        cs.committer.tz_offset_minutes = 1441;
        assert_eq!(cs.validate(), Err(Error::InvalidTzOffset(1441)));
        cs.committer.tz_offset_minutes = -1440;
        assert_eq!(cs.validate(), Ok(()));
    }

    #[test]
    fn validate_put_tree_mode_rejected() {
        let mut cs = root();
        cs.delta = vec![FileChange::put(
            "d",
            EntryMode::Tree,
            ObjectId::hash(ObjectKind::Tree, b""),
            0,
        )];
        assert_eq!(cs.validate(), Err(Error::UnknownMode(4)));
    }

    #[test]
    fn validate_put_non_blob_rejected() {
        let mut cs = root();
        cs.delta = vec![FileChange::put(
            "f",
            EntryMode::File,
            ObjectId::hash(ObjectKind::Tree, b""),
            0,
        )];
        assert_eq!(cs.validate(), Err(Error::IdKindMismatch("file change")));
    }

    #[test]
    fn validate_bad_path_rejected() {
        let mut cs = root();
        cs.delta = vec![FileChange::delete("/abs")];
        assert!(matches!(cs.validate(), Err(Error::InvalidPath(_))));
    }

    #[test]
    fn validate_empty_extension_key_rejected() {
        let mut cs = root();
        cs.extensions.insert(String::new(), vec![]);
        assert_eq!(cs.validate(), Err(Error::InvalidExtensionKey));
    }

    #[test]
    fn extensions_roundtrip_and_affect_id() {
        let mut cs = root();
        let before = cs.id();
        cs.extensions.insert("git.sha1".into(), vec![0xaa; 20]);
        cs.extensions.insert("a".into(), vec![]);
        assert_ne!(cs.id(), before);
        assert_eq!(Changeset::decode(&cs.encode()), Ok(cs));
    }

    #[test]
    fn decode_unsorted_extensions_rejected() {
        let mut cs = root();
        cs.extensions.insert("a".into(), vec![1]);
        cs.extensions.insert("b".into(), vec![2]);
        let mut bytes = cs.encode();
        // 把末尾两个扩展的 key 字节对调：…01 'a' 01 01 01 'b' 01 02 → 'b' 在前
        let n = bytes.len();
        bytes.swap(n - 7, n - 3);
        assert_eq!(Changeset::decode(&bytes), Err(Error::NotSorted("extensions")));
    }

    #[test]
    fn decode_unknown_op_rejected() {
        let mut cs = root();
        cs.delta = vec![FileChange::delete("a")];
        let mut bytes = cs.encode();
        let n = bytes.len();
        // 结尾为 …'a' 0x02(op) 0x00(extensions 个数)
        assert_eq!(bytes[n - 2], OP_DELETE);
        bytes[n - 2] = 0x07;
        assert_eq!(Changeset::decode(&bytes), Err(Error::UnknownOp(7)));
    }

    #[test]
    fn decode_tz_out_of_range_rejected() {
        let mut cs = root();
        cs.author.tz_offset_minutes = 1440;
        let mut bytes = cs.encode();
        // 不经过 validate 直接构造越界：把 author.tz 的 zigzag(1440)=2880 改为 zigzag(1441)=2882
        let pos = bytes
            .windows(2)
            .position(|w| w == [0xc0, 0x16])
            .expect("tz bytes present");
        bytes[pos] = 0xc2;
        assert_eq!(Changeset::decode(&bytes), Err(Error::InvalidTzOffset(1441)));
    }

    #[test]
    fn decode_truncated_rejected() {
        let bytes = root().encode();
        for cut in 0..bytes.len() {
            assert!(Changeset::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn decode_wrong_kind_rejected() {
        let mut bytes = root().encode();
        bytes[0] = 0x02;
        assert_eq!(
            Changeset::decode(&bytes),
            Err(Error::WrongKind { expected: 3, found: 2 })
        );
    }

    #[test]
    fn tz_from_i64_bounds() {
        assert_eq!(tz_from_i64(1440), Ok(1440));
        assert_eq!(tz_from_i64(-1440), Ok(-1440));
        assert_eq!(tz_from_i64(i64::MIN), Err(Error::InvalidTzOffset(i64::MIN)));
    }

    proptest! {
        #[test]
        fn roundtrip_any(
            msg in ".{0,64}",
            time_ms: i64,
            tz in -1440i32..=1440,
            change_id: [u8; 16],
            paths in proptest::collection::btree_set("[a-z]{1,6}(/[a-z]{1,6}){0,2}", 0..10),
            ext in proptest::collection::btree_map("[a-z.]{1,8}", proptest::collection::vec(any::<u8>(), 0..8), 0..4),
        ) {
            let s = Signature { name: "n".into(), email: "e".into(), time_ms, tz_offset_minutes: tz };
            let delta = paths
                .into_iter()
                .enumerate()
                .map(|(i, p)| if i % 2 == 0 {
                    FileChange::delete(p)
                } else {
                    FileChange::put(p, EntryMode::Executable, ObjectId::of_blob(&[i as u8]), i as u64)
                })
                .collect();
            let cs = Changeset::new(vec![], s.clone(), s, msg, change_id, delta, ext).expect("valid");
            let bytes = cs.encode();
            prop_assert_eq!(Changeset::decode(&bytes), Ok(cs));
        }

        #[test]
        fn decode_never_panics(b in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = Changeset::decode(&b);
        }
    }
}
